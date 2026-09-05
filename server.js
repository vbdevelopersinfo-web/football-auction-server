// Football Auction — online multiplayer relay server
//
// This replaces the Claude-artifact-only "db" sync capability with a small standalone
// WebSocket relay, so online play works from any domain the game is hosted on (not just the
// claude.ai artifact link), and scales to many concurrent rooms on one modest server.
//
// Design: the server holds NO game logic and NO persistent storage. Each room is just two
// live WebSocket connections (host + joiner) plus the last state the host pushed, kept in
// memory. The host's browser is still the one authoritative simulation — exactly like today —
// this server only relays messages between the two sides:
//   host  --state-->   server  --state-->   joiner
//   joiner --intent-->  server  --intent-->  host
// A room disappears the moment both sides are gone; nothing is written to disk anywhere.
//
// Message protocol (JSON, one object per WebSocket frame):
//   Client -> Server
//     {type:'create-room', hostName}
//     {type:'join-room', code, joinerName}
//     {type:'state', state}            (host only)
//     {type:'intent', action}          (joiner only)
//     {type:'leave'}
//     {type:'ping'}
//   Server -> Client
//     {type:'room-created', code}
//     {type:'joined', code, state}     (state is the host's last pushed state, or null)
//     {type:'room-not-found'}
//     {type:'joiner-joined', joinerName}   (to host)
//     {type:'state', state}                (to joiner, relayed from host)
//     {type:'intent', action}              (to host, relayed from joiner)
//     {type:'peer-left'}
//     {type:'error', message}
//     {type:'pong'}

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
// No 0/O/1/I — easy to read aloud, matches the game's original room-code alphabet.
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const MAX_ROOM_AGE_MS = 6 * 60 * 60 * 1000; // sweep abandoned rooms after 6 hours of no activity
const SWEEP_INTERVAL_MS = 10 * 60 * 1000;

/** @type {Map<string, {host: import('ws').WebSocket|null, joiner: import('ws').WebSocket|null,
 *   hostName: string, joinerName: string|null, lastState: any, lastActivity: number}>} */
const rooms = new Map();

function genRoomCode(){
  let code;
  do {
    code = '';
    for(let i=0;i<6;i++) code += CODE_CHARS[Math.floor(Math.random()*CODE_CHARS.length)];
  } while(rooms.has(code)); // guarantee uniqueness — collisions are rare but cheap to rule out
  return code;
}

function send(ws, msg){
  if(ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

function closeRoomIfEmpty(code){
  const room = rooms.get(code);
  if(!room) return;
  if(!room.host && !room.joiner) rooms.delete(code);
}

// Periodic sweep for rooms nobody ever cleaned up (e.g. a crashed tab that never sent a
// close frame) — keeps memory bounded on a long-running server with many rooms.
setInterval(() => {
  const now = Date.now();
  for(const [code, room] of rooms){
    if(now - room.lastActivity > MAX_ROOM_AGE_MS){
      send(room.host, {type:'error', message:'Room closed due to inactivity.'});
      send(room.joiner, {type:'error', message:'Room closed due to inactivity.'});
      rooms.delete(code);
    }
  }
}, SWEEP_INTERVAL_MS).unref();

const server = http.createServer((req, res) => {
  // Plain health-check endpoint — most hosts (Render, Railway, Fly) ping this to confirm the
  // service is alive; it's also just a handy "is my server up" URL to open in a browser.
  res.writeHead(200, {'Content-Type': 'text/plain'});
  res.end(`Football Auction relay server — ${rooms.size} room(s) active.`);
});

const wss = new WebSocketServer({ server, maxPayload: 2 * 1024 * 1024 }); // 2MB/message cap

wss.on('connection', (ws) => {
  // Which room/seat this specific socket belongs to, once it creates or joins one.
  let myCode = null;
  let mySeat = null; // 'host' | 'joiner'

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch(e){ send(ws, {type:'error', message:'Malformed message.'}); return; }
    if(!msg || typeof msg.type !== 'string') return;

    if(msg.type === 'ping'){ send(ws, {type:'pong'}); return; }

    if(msg.type === 'create-room'){
      const code = genRoomCode();
      rooms.set(code, {
        host: ws, joiner: null,
        hostName: String(msg.hostName || 'Player 1').slice(0, 40),
        joinerName: null, lastState: null,
        lastActivity: Date.now(),
      });
      myCode = code; mySeat = 'host';
      send(ws, {type:'room-created', code});
      return;
    }

    if(msg.type === 'join-room'){
      const code = String(msg.code || '').trim().toUpperCase();
      const room = rooms.get(code);
      if(!room){ send(ws, {type:'room-not-found'}); return; }
      room.joiner = ws;
      room.joinerName = String(msg.joinerName || 'Player 2').slice(0, 40);
      room.lastActivity = Date.now();
      myCode = code; mySeat = 'joiner';
      send(ws, {type:'joined', code, state: room.lastState});
      send(room.host, {type:'joiner-joined', joinerName: room.joinerName});
      return;
    }

    // Everything past this point needs an established room + seat.
    const room = myCode ? rooms.get(myCode) : null;
    if(!room){ send(ws, {type:'error', message:'Not in a room.'}); return; }
    room.lastActivity = Date.now();

    if(msg.type === 'state' && mySeat === 'host'){
      room.lastState = msg.state;
      send(room.joiner, {type:'state', state: msg.state});
      return;
    }
    if(msg.type === 'intent' && mySeat === 'joiner'){
      send(room.host, {type:'intent', action: msg.action});
      return;
    }
    if(msg.type === 'leave'){
      if(mySeat === 'host') room.host = null; else room.joiner = null;
      send(mySeat === 'host' ? room.joiner : room.host, {type:'peer-left'});
      closeRoomIfEmpty(myCode);
      myCode = null; mySeat = null;
      return;
    }
  });

  ws.on('close', () => {
    if(!myCode) return;
    const room = rooms.get(myCode);
    if(!room) return;
    if(mySeat === 'host') room.host = null; else room.joiner = null;
    send(mySeat === 'host' ? room.joiner : room.host, {type:'peer-left'});
    closeRoomIfEmpty(myCode);
  });
});

server.listen(PORT, () => {
  console.log(`Football Auction relay server listening on port ${PORT}`);
});
