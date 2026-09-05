# Football Auction — multiplayer relay server

This is the small backend that makes online rooms (Host Room / Join Room) work. It holds no
game logic and no database — it just relays messages between the two browsers in a room. See
the comment block at the top of `server.js` for the full message protocol.

## Deploying to Render (recommended)

Render's free web services don't stay warm for WebSockets reliably, so this needs a **paid**
instance — their cheapest ("Starter", ~$7/month at time of writing) comfortably handles many
concurrent rooms for this game, since each room is just two lightweight JSON messages a few
times a second.

1. Push this folder (`server.js`, `package.json`) to a new GitHub repo (Render deploys from a
   git repo — it doesn't take a raw file upload).
2. Go to render.com, sign up/log in, click **New +** → **Web Service**, and connect that repo.
3. Settings:
   - **Runtime**: Node
   - **Build Command**: `npm install`
   - **Start Command**: `npm start`
   - **Instance type**: Starter (or higher) — not the free tier, for reliable WebSocket uptime.
4. Deploy. Once it's live, Render gives you a URL like `https://football-auction-server.onrender.com`.
5. Your relay server's WebSocket URL is the same address with `wss://` instead of `https://` —
   e.g. `wss://football-auction-server.onrender.com`.
6. Open `game.html`, find this line near the top of the multiplayer section:
   ```js
   const MULTIPLAYER_SERVER_URL = 'REPLACE-WITH-YOUR-SERVER-URL';
   ```
   and replace the placeholder with your real `wss://` URL from step 5. Re-upload `game.html`
   to your site (and re-publish the Artifact link, if you use that too).

### Optional: put it on your own domain

Render lets you attach a custom subdomain (e.g. `ws.yourdomain.com`) to the service for free
under Settings → Custom Domains — you'd then use `wss://ws.yourdomain.com` in step 6 instead.

## Alternatives to Render

- **Railway** (railway.app): similar flow — connect the repo, it auto-detects Node, add a paid
  plan for reliable uptime, use the generated domain's `wss://` address the same way.
- **Fly.io**: more setup (a `fly.toml` and their CLI) but often cheaper at real scale and gives
  you more control over region/instance size.
- **A plain VPS** (DigitalOcean, Linode, Hetzner): most control, cheapest per unit of compute
  at "hundreds of concurrent" scale, but you're responsible for keeping Node running (e.g. via
  `pm2` or a systemd service) and setting up your own reverse proxy for the `wss://` TLS
  termination (nginx/Caddy).

## Testing it works

With the server deployed and `MULTIPLAYER_SERVER_URL` set, open the game on two different
devices (or two browser windows), host a room on one, join with the code on the other, and
play a full match — bids, lineups, and match events should all sync between them.

## A note on scale

Each active room holds two open WebSocket connections and the host's last pushed game state
in server memory — nothing is written to disk. A single small instance comfortably handles
hundreds of concurrent rooms; if you outgrow one instance, this server would need to move to
a design that shares room state across multiple instances (e.g. via Redis pub/sub) since two
players in the same room must currently land on the very same server process.
