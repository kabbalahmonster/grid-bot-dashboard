# Grid Bot Dashboard

A lightweight Flask dashboard for monitoring Robinhood Chain grid bots in real time. Bots submit authenticated JSON snapshots; connected browsers receive updates through Server-Sent Events (SSE).

## Dashboard display

Each bot card shows:

- **AVG P&L** — cost-weighted unrealized P&L across open positions
- **Session Profit** — realized ETH profit since the bot process started
- **Filled / Max Positions** — active capacity, such as `12 / 12`
- The newest three positions, expandable to show all

Each position shows token amount, ETH cost basis, and P&L percentage. **More info** reveals price, buys, sells, ETH balance, token balance, RPC status, and uptime.

## Features

- Live SSE updates with keepalives
- Multiple bots identified by `bot_id`
- Latest state plus 100 in-memory history entries per bot
- `X-API-Key` authentication for writes and deletion
- 100 API requests/minute/IP rate limit
- Recursive private-key detection and rejection
- Bounded SSE client queues
- Responsive inline frontend; no build step or database

## Requirements

- Python 3.10+
- Network access from bot hosts to the dashboard host
- A long random shared API key

## Install

```bash
git clone https://github.com/kabbalahmonster/grid-bot-dashboard.git
cd grid-bot-dashboard
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Configure `.env`:

```dotenv
API_KEY=replace-with-a-long-random-secret
HOST=0.0.0.0
PORT=5000
```

Generate a key:

```bash
python -c 'import secrets; print(secrets.token_hex(32))'
```

`.env`, virtual environments, logs, and Python caches are excluded by `.gitignore`.

## Run

```bash
source .venv/bin/activate
python dashboard_server.py
```

Open `http://SERVER_IP:5000/`. Check health with:

```bash
curl http://127.0.0.1:5000/api/health
```

### Gunicorn

Use exactly one worker because bot state and SSE subscribers are process-local:

```bash
gunicorn dashboard_server:app \
  --bind 0.0.0.0:5000 \
  --workers 1 \
  --threads 8 \
  --worker-class gthread
```

## Persistent systemd user service

Create `~/.config/systemd/user/grid-dashboard.service`:

```ini
[Unit]
Description=Grid Bot Dashboard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/absolute/path/grid-bot-dashboard
ExecStart=/absolute/path/grid-bot-dashboard/.venv/bin/python dashboard_server.py
Restart=always
RestartSec=5
Environment=HOST=0.0.0.0
Environment=PORT=5000

[Install]
WantedBy=default.target
```

Do not put a placeholder `API_KEY` in the service: process environment values override `.env`. Let `python-dotenv` load the project `.env`, or use a secure `EnvironmentFile=`.

```bash
systemctl --user daemon-reload
systemctl --user enable --now grid-dashboard.service
systemctl --user status grid-dashboard.service
journalctl --user -u grid-dashboard.service -f
```

For startup before interactive login, an administrator must run:

```bash
sudo loginctl enable-linger USERNAME
```

## Network access

For direct access through Fedora/firewalld:

```bash
sudo firewall-cmd --add-port=5000/tcp --permanent
sudo firewall-cmd --reload
```

For long-term internet exposure, use HTTPS behind a reverse proxy or Cloudflare Tunnel. Disable proxy buffering for `/api/stream` so SSE remains live.

## Configure bots

Use the grid bot dashboard branch:

```bash
git fetch origin
git switch feature/dashboard-integration
git pull --ff-only origin feature/dashboard-integration
```

Add to each bot `.env`:

```dotenv
DASHBOARD_URL=http://SERVER_IP:5000/api/status
DASHBOARD_API_KEY=the-same-value-as-dashboard-API_KEY
```

Optional:

```dotenv
BOT_ID=MERD
```

`BOT_ID` defaults to `TOKEN_SYMBOL`, then `grid-bot-CHAIN_ID`. Reporting is disabled when `DASHBOARD_URL` is empty. It uses a bounded background thread and does not block trading. Successful POST/response logs appear only at DEBUG; failures remain warnings.

Restart the bot after configuration:

```bash
python grid_bot.py
```

## Status payload

Bots POST JSON to `/api/status` with the shared key in `X-API-Key`:

```json
{
  "dashboard_schema_version": 1,
  "bot_id": "MERD",
  "timestamp": "2026-08-13T08:26:28.236502+00:00",
  "uptime_seconds": 215.1,
  "price": 0.0000000624,
  "eth_balance": 0.00099309,
  "token_balance": 328902.93,
  "positions": [{
    "id": "11",
    "buy_amount_token": 35994.87,
    "cost_basis": 0.00224395,
    "pnl": 0.17,
    "timestamp": null
  }],
  "profit_percent": -49.66,
  "session_profit_eth": 0.0,
  "buys": 0,
  "sells": 0,
  "filled_positions": 12,
  "max_positions": 12,
  "rpc_status": "ok"
}
```

Only `bot_id` is required by the generic server. The current bot sends `dashboard_schema_version: 1` so future dashboard revisions can distinguish payload formats safely. The current UI understands the complete schema above. `profit_percent` is total current position value versus total cost basis. Session profit, buys, sells, and uptime reset with the bot process.

The dashboard displays report age continuously. A bot is inferred as `running` for reports under 2 minutes old, `stale` from 2–5 minutes, and `offline` after 5 minutes. An explicit bot-supplied `status` overrides this inference.

Manual test:

```bash
curl -X POST http://SERVER_IP:5000/api/status \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: YOUR_KEY' \
  -d '{"bot_id":"manual-test","profit_percent":1.25,"buys":0,"sells":0}'
```

## API

| Method | Endpoint | Auth | Purpose |
|---|---|---|---|
| `GET` | `/` | No | Dashboard frontend |
| `POST` | `/api/status` | `X-API-Key` | Submit status |
| `GET` | `/api/bots` | No | List current bot states |
| `GET` | `/api/bots/<bot_id>` | No | Get one current state |
| `GET` | `/api/bots/<bot_id>/history` | No | Get up to 100 updates |
| `DELETE` | `/api/bots/<bot_id>` | `X-API-Key` | Remove bot and history |
| `GET` | `/api/stream` | No | SSE update stream |
| `GET` | `/api/health` | No | Health and counts |

SSE events:

- `snapshot` — all current bot states on connection
- `update` — one accepted status update
- `remove` — one deleted bot ID

## Storage behavior

State and history are memory-only. Restarting the dashboard clears them; running bots repopulate it on their next report. Add persistent storage if long-term analytics are required.

## Security

- Generate a unique API key and rotate it if exposed.
- Read-only endpoints and the browser UI are public by default.
- Never send wallet private keys; matching fields or key material are rejected.
- Plain HTTP exposes dashboard data and the API key in transit; prefer HTTPS or a private network.
- Restrict port 5000 by source IP where practical.
- CORS is permissive by default and should be tightened for production.
- Rate limiting is in-memory and per process.

## Troubleshooting

### UI remains on Connecting

```bash
systemctl --user status grid-dashboard.service
curl http://127.0.0.1:5000/api/health
journalctl --user -u grid-dashboard.service -n 50 --no-pager
```

With a browser connected, health should report at least one `sse_clients`. Hard-refresh after frontend updates.

### Bot gets 401

The bot's `DASHBOARD_API_KEY` must exactly match server `API_KEY`. Check quotes, whitespace, duplicate `.env` entries, and systemd `Environment=API_KEY=...` overrides. Restart after changes.

### Bot does not appear

- Ensure `DASHBOARD_URL` ends in `/api/status`.
- Restart after editing `.env`.
- Confirm the dashboard integration branch is current.
- Test connectivity from the bot host with `curl`.
- Failures log as warnings; successes require DEBUG logging.

### Missing or stale fields

Pull and restart the bot so it reports the current schema. Refresh the browser when dashboard HTML changes.

## Update

Dashboard:

```bash
git pull --ff-only origin main
source .venv/bin/activate
pip install -r requirements.txt
systemctl --user restart grid-dashboard.service
```

Bot:

```bash
git pull --ff-only origin feature/dashboard-integration
python grid_bot.py
```

## License

MIT
