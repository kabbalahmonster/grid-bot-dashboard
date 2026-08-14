# Grid Bot Dashboard

A lightweight Flask dashboard for monitoring Robinhood Chain grid bots in real time. Bots submit authenticated JSON snapshots; connected browsers receive updates through Server-Sent Events (SSE).

## Dashboard display

Each bot card shows:

- **AVG P&L** — cost-weighted unrealized P&L across open positions
- **Session Profit** — realized ETH profit since the bot process started
- **Realized Profit** — confirmed sell profit/loss since the persistent accounting baseline
- **Filled / Max Positions** — active capacity, such as `12 / 12`
- The three highest-P&L positions, expandable to show all positions sorted by P&L descending

Each position shows token amount, ETH cost basis, and P&L percentage. **More info** reveals price, buys, sells, realized sell count/tracking date, ETH and USDG balances, token balance, wallet/token explorer links, RPC status, and uptime. Cards may also show a static **ADD POSITIONS** capacity flag, provider badge, bounded Trade History, and structured Events.

## Features

- Live SSE updates with keepalives
- Multiple bots identified by `bot_id`
- Latest state plus 100 in-memory history entries per bot
- Latest state and bounded history persisted across dashboard restarts
- Aggregate active-bot, session-profit, persistent realized-profit, and filled-position totals
- Bot filtering and sorting by name, chain, P&L, profit, or status
- Reversible ascending/descending sorting with sensible per-field defaults
- Optional bot display names/groups and chain badges
- Bounded ETH-denominated bot trade history with explorer links
- Bounded structured warning/error history with repeat counts and expandable provider details
- Persistent realized-profit totals with non-destructive accounting baselines
- Per-cycle USDG balances when reported by current bots
- Lazy-loaded Dexscreener WETH-pair charts filtered to the bot wallet
- Live report age with running, stale, and offline states
- Optional browser offline notifications when served over HTTPS
- `X-API-Key` authentication for writes and deletion
- Configurable API rate limit (default 600 requests/minute/IP)
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
HOST=127.0.0.1
PORT=5000
STATE_FILE=data/dashboard_state.json
RATE_LIMIT_MAX_REQUESTS=600
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

For local/direct development, open `http://SERVER_IP:5000/`. Check health with:

```bash
curl http://127.0.0.1:5000/api/health
```

### Gunicorn

Use exactly one worker because bot state and SSE subscribers are process-local:

```bash
gunicorn dashboard_server:app \
  --bind 127.0.0.1:5000 \
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
Environment=HOST=127.0.0.1
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

## HTTPS reverse proxy (recommended)

Keep Flask bound to localhost and expose only ports 80/443 through Caddy:

```caddyfile
dashboard.example.com {
    reverse_proxy 127.0.0.1:5000
}
```

Caddy automatically provisions HTTPS and supports the SSE stream used by the dashboard. On Fedora, open HTTP/HTTPS and keep public port 5000 closed:

```bash
sudo firewall-cmd --permanent --add-service=http
sudo firewall-cmd --permanent --add-service=https
sudo firewall-cmd --reload
sudo systemctl enable --now caddy
```

The production deployment for this project is `https://doomdash.ca`, with Flask listening only on `127.0.0.1:5000` behind Caddy.

## Configure bots

Dashboard reporting is included in the bot repository's `main` branch:

```bash
git fetch origin
git switch main
git pull --ff-only origin main
```

Add to each bot `.env`:

```dotenv
DASHBOARD_URL=https://doomdash.ca/api/status
DASHBOARD_API_KEY=the-same-value-as-dashboard-API_KEY
```

Optional:

```dotenv
BOT_ID=MERD
DASHBOARD_NAME=MERD Main
DASHBOARD_GROUP=Robinhood Farm
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
  "usdg_balance": 12.34,
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
  "realized_profit_eth": 0.0025,
  "realized_sales": 4,
  "profit_tracking_started_at": "2026-08-14T16:00:00+00:00",
  "buys": 0,
  "sells": 0,
  "filled_positions": 12,
  "max_positions": 12,
  "capacity_warning": null,
  "chain_id": 4663,
  "swap_provider": "uniswap",
  "token_address": "0x0000000000000000000000000000000000000001",
  "wallet_address": "0x0000000000000000000000000000000000000002",
  "trades_history": [],
  "events": [],
  "rpc_status": "ok"
}
```

Bots may additionally send `display_name`, `group`, and up to 50 entries in `trades_history`. Trade entries contain timestamp, side, ETH amount, token amount, execution price, transaction hash, and sell profit when applicable. They are recorded from swaps the bot already executes, so this adds no RPC or third-party API calls.

Bots may also send up to 50 structured `events`. The dashboard renders them newest-first in a collapsible Events panel, distinguishes green successes, amber warnings, and red errors, and shows a repeat count for deduplicated events. Events with a `tx_hash` include a white explorer link; confirmed USDG banking swaps use this to make the banking transaction directly auditable. This feed is intended for meaningful operational outcomes and blocked actions, not raw bot output or routine no-trade polling.

Bots may send `realized_profit_eth`, `realized_sales`, and `profit_tracking_started_at`. Realized profit is shown beside session profit on each card and aggregated fleet-wide in ETH plus the selected CAD/USD currency. A bot-side baseline reset starts a new displayed accounting period without deleting cumulative totals or transaction-hash deduplication. Older bots remain compatible and contribute zero until updated.

`usdg_balance` is an optional read-only ERC-20 balance. `capacity_warning` drives the static **ADD POSITIONS** flag when gridless slots are full and another buy would otherwise trigger. `swap_provider` supplies the provider badge.

Only `bot_id` is required by the generic server. The current bot sends `dashboard_schema_version: 1` so future dashboard revisions can distinguish payload formats safely. The current UI understands the complete schema above. `profit_percent` is total current position value versus total cost basis. Session profit, buys, sells, and uptime reset with the bot process.

The dashboard displays report age continuously. A bot is inferred as `running` for reports under 2 minutes old, `stale` from 2–5 minutes, and `offline` after 5 minutes. An explicit bot-supplied `status` overrides this inference.

Manual test:

```bash
curl -X POST https://doomdash.ca/api/status \
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
| `GET` | `/api/dexscreener/chart-url` | No | Resolve preferred WETH pair embed URL |

SSE events:

- `snapshot` — all current bot states on connection
- `update` — one accepted status update
- `remove` — one deleted bot ID

## Storage behavior

Latest state and the 100-entry status history are persisted atomically to `STATE_FILE` (default `data/dashboard_state.json`) and restored after restart. Bot-side trade and Event histories are separately persisted and capped at 50 entries each. Persistent profit accounting lives in the bot's `data/profit_totals.json`.

Dexscreener charts are lazy-loaded: the iframe has no URL until its panel is opened. The server resolves the token's preferred WETH pair without an API key and caches the pair for five minutes. Routine SSE updates do not reload an open chart.

The fleet toolbar can search bot IDs, display names, and groups; filter by chain; and sort by name, AVG P&L, session profit, or status. Default directions are name ascending, P&L/profit descending, and status running-to-offline. The direction button reverses the active sort.

## Security

- Generate a unique API key and rotate it if exposed.
- Read-only endpoints and the browser UI are public by default.
- Never send wallet private keys; matching fields or key material are rejected.
- Plain HTTP exposes dashboard data and the ingestion API key in transit; use HTTPS.
- Bind Flask to localhost behind the reverse proxy; do not expose public port 5000.
- The public UI is read-only but exposes public wallet addresses, balances, positions, P&L, trades, providers, and operational Events. Add viewer authentication if that operational data should be private.
- CORS is permissive by default and should be tightened for production.
- Rate limiting is in-memory and per process.
- Browser system notifications require a secure HTTPS context; dashboard status/offline counts work over HTTP.

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
- Confirm the bot's `main` branch is current.
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
git switch main
git pull --ff-only origin main
python grid_bot.py
```

## License

MIT
