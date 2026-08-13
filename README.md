# Grid Bot Dashboard

A lightweight Flask dashboard that receives status updates from grid trading bots and displays them in real-time via Server-Sent Events (SSE).

## Features

- **Live updates** — SSE pushes bot status changes to the browser instantly
- **In-memory storage** — latest state + 100-item history per bot
- **API key auth** — bots authenticate via `X-API-Key` header
- **Rate limiting** — 100 requests/minute per IP on `/api/` routes
- **Private key rejection** — payloads containing private key material are refused
- **Self-contained** — dashboard HTML is served inline; no static files needed

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Set your API key
export API_KEY="your-secret-key-here"

# Run (development)
python dashboard_server.py

# Run (production)
gunicorn dashboard_server:app \
  --bind 0.0.0.0:5000 \
  --workers 1 \
  --threads 8 \
  --worker-class gthread
```

> **Note:** Use `--workers 1` with gunicorn. SSE and in-memory state require a single process.

## API

### `POST /api/status`

Receive a bot status update.

**Headers:** `X-API-Key: your-secret-key`

**Body:**
```json
{
  "bot_id": "btc-usdt-grid-1",
  "status": "running",
  "pair": "BTC/USDT",
  "exchange": "binance",
  "pnl": 142.53,
  "grid_levels": 20,
  "active_orders": 12,
  "current_price": 67890.12
}
```

The only required field is `bot_id`. All other fields are passed through and displayed on the dashboard.

### `GET /api/bots`

Returns all current bot states (latest update per bot).

### `GET /api/bots/<bot_id>/history`

Returns the last 100 status updates for a specific bot.

### `GET /api/stream`

SSE endpoint. Browsers connect here for live updates. Events:

| Event | Description |
|-------|-------------|
| `snapshot` | Full state of all bots (sent on connect) |
| `bot_update` | Single bot status update |

### `GET /api/health`

Health check. Returns bot count and connected SSE client count.

## Configuration

Set via environment variables (see `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `API_KEY` | *(required)* | Secret key for bot authentication |
| `PORT` | `5000` | Server port |
| `HOST` | `0.0.0.0` | Bind address |

## Architecture

```
┌──────────┐     POST /api/status      ┌──────────────────┐
│ Grid Bot  │ ────────────────────────▶ │  Dashboard Server │
│ (bot_id)  │     X-API-Key: ***       │                  │
└──────────┘                           │  ┌────────────┐  │
                                       │  │ In-memory  │  │
┌──────────┐                           │  │ bot states │  │
│ Grid Bot  │ ────────────────────────▶ │  └────────────┘  │
│ (bot_id)  │                           │       │          │
└──────────┘                           │       ▼          │
                                       │  SSE broadcast   │
┌──────────┐     GET /api/stream       │       │          │
│ Browser   │ ◀──────────────────────── │       ▼          │
│ Dashboard │     text/event-stream     │  Live updates    │
└──────────┘                           └──────────────────┘
```

## Security

- API key required for all `POST /api/status` requests
- Rate limiting: 100 req/min per IP on API routes
- Payloads containing private key patterns are rejected
- CORS enabled (configure origins in production if needed)
- No data persisted to disk — all state is in-memory

## License

MIT
