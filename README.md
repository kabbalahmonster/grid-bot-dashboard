# Grid Bot Dashboard

A lightweight Flask dashboard for monitoring Robinhood Chain grid bots in real time. Bots submit authenticated JSON snapshots; connected browsers receive updates through Server-Sent Events (SSE).

## Dashboard display

Each bot card shows:

- **AVG P&L** — cost-weighted unrealized P&L across open positions
- **Session Profit** — realized ETH profit since the bot process started
- **Realized Profit** — confirmed sell profit/loss since the persistent accounting baseline
- **Filled / Max Positions** — active capacity, such as `12 / 12`
- The three highest-P&L positions, expandable to show all positions sorted by P&L descending

Each position shows token amount, ETH cost basis, and P&L percentage. **More info** reveals price, buys, sells, realized sell count/tracking date, ETH and USDG balances, cumulative confirmed USDG treasury sweeps, token balance, wallet/token explorer links, RPC status, and uptime. Cards may also show a static **ADD POSITIONS** capacity flag, provider badge, bounded Trade History, structured Events, and a cyan **SELL CHECK ACTIVE** strip while the current report says a sell quote is below the configured minimum.

## Features

- Live SSE updates with keepalives
- Multiple bots identified by `bot_id`
- Latest state plus 100 in-memory history entries per bot
- Latest state and bounded history persisted across dashboard restarts
- Aggregate active-bot, session-profit, persistent realized-profit with oldest-period age and daily/hourly averages,
  confirmed USDG treasury-sent, and filled-position totals
- A fleet-wide Sell history dialog, newest-first and scrollable, with coin,
  realized profit, relative time, and transaction links. Successful banking is
  paired directly beneath its originating sell when both retained timestamps
  are available; unmatched banking remains visible. The feed follows the
  active dashboard filters and updates in place from live bot status reports
  while preserving the reader's scroll position. Sell/banking history changes
  refresh directly from the SSE update handler rather than waiting for the
  touch-aware full-card render scheduler. Coin names are controls that close
  the feed and focus the corresponding bot card.
- Bot filtering by name/group, chain, swap provider, and manually declared or auto-detected taxed-token status; sorting by AVG or top-position P&L, session profit,
  session buy or sell count, realized profit, confirmed USDG treasury sent, position utilization, ETH or USDG balance, or status
- Reversible ascending/descending sorting with sensible per-field defaults
- Clickable realized-profit summary cycling ETH, CAD, and USD for its total and daily/hourly averages
- Optional bot display names/groups and chain badges
- Manual `TAX n.n%` and guarded `AUTO TAX n.n%` badges, including retained
  detection source and observation evidence from current bot payloads
- Bounded ETH-denominated bot trade history with explorer links
- Bounded structured success/warning/error history with repeat counts and expandable provider details
- Round-scoped sell-check indication that clears automatically with the next report that omits it
- Persistent realized-profit totals with non-destructive accounting baselines
- Per-cycle USDG balances when reported by current bots
- Lazy-loaded Dexscreener WETH-pair charts filtered to the bot wallet
- Built-in Gzip compression for the dashboard, JSON APIs, and flush-safe live
  SSE snapshots, reducing transfer size on constrained mobile connections
- Live report age with running, stale, and offline states
- Optional per-type browser notifications for confirmed trades, capacity, availability, treasury activity, and persistent errors when served over HTTPS
- Filter-aware empty state with one-tap filter clearing, plus live-message/snapshot/reconnect diagnostics behind the connection indicator
- Clickable Active, Stale, and Offline fleet summaries with accessible bot
  lists, report age/context, and one-tap card focus
- Durable Telegram commands for fleet status, exceptions, period profit, recent trades, and per-bot inspection
- Deduplicated Telegram low-ETH/unbanked-USDG warnings and a configurable daily operational digest
- `X-API-Key` authentication for writes and deletion
- Configurable API rate limit (default 600 requests/minute/IP)
- Recursive private-key detection and rejection
- Bounded SSE client queues
- Responsive inline frontend; no build step or database
- DoomScout read-only candidate scoring with exact planned-size buy→sell quotes,
  provider redundancy checks, liquidity/age/volume guards, durable history,
  background watchlist rescans, Telegram alerts, and an on-dashboard ranking

Telegram commands include `/status`, `/attention`, `/profit 24h`, `/trades 24h`,
`/digest`, `/leaderboard`, `/recap`, `/oracle`, `/needs`, `/bot <name>`, `/alerts`,
`/mute`, and `/unmute`. Profit
periods accept `1h`, `6h`, `24h`, `week`, `month`, or `all` (trades omit
`all`). The daily digest is sent once after `TELEGRAM_DAILY_DIGEST_TIME` in UTC;
set that value blank to disable scheduling while retaining `/digest` on demand.

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
STATE_FLUSH_INTERVAL=15
RATE_LIMIT_MAX_REQUESTS=600
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
TELEGRAM_ALERT_STATE_FILE=data/telegram_alert_state.json
TELEGRAM_LOW_FUNDS_BUFFER_ETH=0.0005
TELEGRAM_UNBANKED_USDG_THRESHOLD=10
TELEGRAM_DAILY_DIGEST_TIME=13:00
DOOM_SCOUT_STATE_FILE=data/doom_scout.json
DOOM_SCOUT_INTERVAL_SECONDS=900
UNISWAP_API_KEY=
```

| Variable | Default | Purpose |
|---|---|---|
| `API_KEY` | empty | Shared secret required for bot status writes and card deletion; production must set a strong value |
| `HOST` | `0.0.0.0` | Flask/Gunicorn bind host; use `127.0.0.1` behind a reverse proxy |
| `PORT` | `5000` | HTTP listen port |
| `STATE_FILE` | `data/dashboard_state.json` | Persisted latest states and bounded per-bot status history |
| `STATE_FLUSH_INTERVAL` | `15` | Seconds between coalesced state writes |
| `RATE_LIMIT_MAX_REQUESTS` | `600` | Maximum authenticated status requests per source IP per rolling minute |
| `TELEGRAM_BOT_TOKEN` | empty | BotFather token; Telegram is disabled unless this and the chat ID are set |
| `TELEGRAM_CHAT_ID` | empty | Sole direct-chat ID authorized for commands and alerts |
| `TELEGRAM_ALERT_STATE_FILE` | `data/telegram_alert_state.json` | Durable command offset, dedupe, preferences, mute, and digest state |
| `TELEGRAM_LOW_FUNDS_BUFFER_ETH` | `0.0005` | ETH buffer added to each reported gas reserve for low-funds alerts |
| `TELEGRAM_UNBANKED_USDG_THRESHOLD` | `10` | Wallet USDG balance that triggers an awaiting-banking alert; `0` disables it |
| `TELEGRAM_DAILY_DIGEST_TIME` | `13:00` | UTC `HH:MM`; empty disables scheduled digests |
| `DOOM_SCOUT_STATE_FILE` | `data/doom_scout.json` | Durable candidate watchlist, latest reports, and bounded score history |
| `DOOM_SCOUT_INTERVAL_SECONDS` | `900` | Watchlist rescan interval; minimum 60 seconds |
| `UNISWAP_API_KEY` | empty | Optional read-only Trade API key used for a second independent route; missing keys produce a conservative no-redundancy caution |

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
  --worker-class gthread \
  --timeout 0 \
  --graceful-timeout 30
```

The unlimited worker timeout keeps long-lived SSE connections alive. Systemd
still owns the process lifecycle, and `--graceful-timeout 30` bounds shutdowns.

## Telegram monitoring and commands

Telegram is optional and runs inside the single DoomDash server process. It
never holds a wallet key and has no trade, restart, or strategy-mutation
commands. It reads the same in-memory snapshots used by the dashboard.

1. Create a bot with Telegram's `@BotFather` and copy the token into
   `TELEGRAM_BOT_TOKEN`.
2. Open a direct chat with the new bot and send `/start` once.
3. Obtain the numeric direct-chat ID and set `TELEGRAM_CHAT_ID`. Messages from
   every other chat are ignored.
4. Restart `grid-dashboard.service`. On startup DoomDash registers its command
   menu with Telegram. Send `/test` to verify delivery.

| Command | Result |
|---|---|
| `/status` | Running count, fleet ETH/USDG, session and realized totals |
| `/attention` | Offline/stale bots, capacity, sell checks, repeated errors/RPC trouble, and low funds |
| `/profit 1h\|6h\|24h\|week\|month\|all` | Period realized profit plus best/worst bot |
| `/trades 1h\|6h\|24h\|week\|month` | Counts, realized sell profit, and up to ten recent trades |
| `/digest` | Generate the daily operational report immediately |
| `/leaderboard 24h\|week\|month\|all\|winrate\|treasury` | Rank up to ten bots by the selected measure |
| `/recap 24h\|week\|month` | Generate and send a shareable PNG fleet card |
| `/oracle` | Deterministic daily fleet mood, chosen vessel, sigil key, and omen |
| `/needs` | Bots currently unable to add another position |
| `/bot <name>` | One bot's status, capacity, balances, and profit |
| `/scout <contract> [budget_eth] [positions]` | Run planned-size Sushi and Uniswap buy→sell simulations and return a transparent verdict |
| `/watch <contract> [label]` | Add and immediately assess a candidate; it is rescanned in the background |
| `/unwatch <contract>` | Stop background checks without deleting retained reports |
| `/forget <contract>` | Remove a candidate, its retained report, and score history |
| `/candidates` | Rank retained candidates by DoomScout score |
| `/discover` | List recent Robinhood Chain DexScreener token profiles; discovery is explicitly unscored until `/scout` or `/watch` is run |
| `/alerts` | Inline per-category alert toggles |
| `/mute 1h\|6h\|12h\|24h` / `/unmute` | Suppress unsolicited alerts temporarily; commands still answer |
| `/help` | Command reference |

The optional `fun` alert category adds deterministic trade drama, durable
first-sell/trade-milestone/profit-streak achievements, and 24-hour crown-change
rivalries. Profit and loss reactions draw from expanded magnitude-aware copy
pools, while crown alerts rotate deterministic copy and, using persisted standings
only, recognize narrow or dominant leads, upsets, collapses, returning champions,
profit-line coups, underwater pageants, and repeat-rivalry rematches. They include the contenders' exact 24-hour realized
profit so the drama never invents a result. The existing once-per-pair-per-day
deduplication and event cadence are unchanged. Trade and achievement alerts include buttons to open the bot's
filtered DoomDash card with its chart expanded, mute that bot for six hours, or
disable the alert category. These controls only affect monitoring; Telegram
still has no trading, wallet, restart, or strategy-mutation authority.

## DoomScout

DoomScout never signs, approves, or broadcasts a transaction. For each token it
requests exact-input quotes for the configured total inventory size, feeds the
entire quoted token output into a reverse sell quote on the same provider, and
scores the resulting recovery alongside liquidity, volume, pool age, planned
capital share, and independent sell-provider count. A chart price is never
treated as proof that inventory can exit.

Run a one-off check or add a durable watch:

```bash
./doom-scout 0xTOKEN --budget 0.003 --positions 4
./doom-scout 0xTOKEN --budget 0.003 --positions 4 --watch
./doom-scout --list --json
./doom-scout --discover
```

Verdicts are intentionally asymmetric: recovery below 85%, inadequate
liquidity, planned capital too large for the pool, or no sell route is a hard
reject. A healthy route on only one configured provider is `CAUTION`, never a
clean pass. Contract security remains `unknown` on unsupported chains; that is
displayed explicitly instead of inventing a safety claim. Authenticated
`POST /api/scout/assess` and `POST/DELETE /api/scout/watch` endpoints drive
operations, while `GET /api/scout` and per-address history are public and
read-only for the dashboard.

The DoomDash Scout panel is collapsed by default so fleet operations remain
the primary view. Its summary shows pass/caution/reject and watched counts;
expanding it shows market liquidity/volume/age, planned budget and position
count, assessment freshness, provider-by-provider executable recovery or error,
warnings, and direct chart/contract links. Watched candidates are marked with
an eye and rescanned at `DOOM_SCOUT_INTERVAL_SECONDS` (15 minutes by default).
The panel also reports whether Sushi and the optional Uniswap quote provider
are configured without exposing credentials.

Keep `UNISWAP_API_KEY` in the production `.env` with owner-only permissions.
It is used only for read-only quote requests. A dedicated Scout key is preferred
so watch scans cannot consume the trading fleet's shared quota; until then,
avoid aggressive scan intervals or repeated sampling.

Scout uses the tested-safe `curl/8.0` User-Agent for Uniswap quotes. Uniswap's
edge has intermittently rejected byte-identical requests carrying the default
`python-requests/*` User-Agent with a misleading packet-buffer 409 before a
normal request ID is assigned. Provider errors retain the returned gateway
detail and request ID, when present.

`TELEGRAM_LOW_FUNDS_BUFFER_ETH` is added to each bot's reported gas reserve;
an ETH balance at or below that sum triggers one `funds` alert. It re-arms
after recovery. `TELEGRAM_UNBANKED_USDG_THRESHOLD` triggers once at the
configured wallet balance and re-arms below half the threshold. Both states,
alert preferences, mute expiry, Telegram update offset, and the last digest
date persist in `TELEGRAM_ALERT_STATE_FILE`.

The digest runs once per UTC date after `TELEGRAM_DAILY_DIGEST_TIME` and
contains estimated fleet crypto value, USDG, 24-hour realized profit, buys,
sells, treasury banking, best/worst bot, and the `/attention` report. Set the
time to an empty value to disable scheduling without disabling `/digest`.

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
ExecStart=/absolute/path/grid-bot-dashboard/.venv/bin/gunicorn --bind 127.0.0.1:5000 --workers 1 --worker-class gthread --threads 8 --timeout 30 --graceful-timeout 30 --access-logfile - --error-logfile - dashboard_server:app
Restart=always
RestartSec=5
TimeoutStopSec=40
KillSignal=SIGTERM
Environment=HOST=127.0.0.1
Environment=PORT=5000
Environment=PYTHONUNBUFFERED=1

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

The production deployment for this project is `https://doomdash.ca`, with a
single threaded Gunicorn worker serving the Flask app on `127.0.0.1:5000`
behind Caddy.

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
  "gas_reserve_eth": 0.0005,
  "usdg_balance": 12.34,
  "treasury_sent_usdg": 125.50,
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
  "realized_profit_periods": {"1h": 0.0001, "6h": 0.0004, "24h": 0.0012, "week": 0.0025, "month": 0.0025},
  "realized_sales": 4,
  "profit_tracking_started_at": "2026-08-14T16:00:00+00:00",
  "buys": 0,
  "sells": 0,
  "filled_positions": 12,
  "max_positions": 12,
  "capacity_warning": null,
  "sell_attempt": {
    "status": "quote_below_minimum",
    "position_id": "11",
    "pnl_percent": 8.4,
    "quoted_profit_eth": 0.000206,
    "projected_gas_eth": 0.000122,
    "projected_net_profit_eth": 0.000084,
    "minimum_profit_eth": 0.0001
  },
  "chain_id": 4663,
  "token_symbol": "MERD",
  "buy_point_percent": -10.0,
  "sell_point_percent": 5.0,
  "swap_provider": "uniswap",
  "token_address": "0x0000000000000000000000000000000000000001",
  "wallet_address": "0x0000000000000000000000000000000000000002",
  "trades_history": [],
  "events": [],
  "rpc_status": "ok"
}
```

Bots may additionally send `display_name`, `group`, and up to 50 entries in `trades_history`. Trade entries contain timestamp, side, ETH amount, token amount, execution price, transaction hash, and sell profit when applicable. They are recorded from swaps the bot already executes, so this adds no RPC or third-party API calls.

Fee-on-transfer state is optional and backward compatible. `taxed_token`
enables the badge, `token_transfer_fee_percent` supplies its effective fee, and
`swap_slippage_percent` reports the total bounded provider tolerance.
`token_tax_detection_source` is `manual`, `auto-detected`, or `none`, while
`token_tax_detection_observations` carries the supporting observation count.
Auto-detected tokens render a distinct **AUTO TAX n.n%** badge; manually
configured tokens render **TAX n.n%**. Older payloads may omit every field.

Bots may also send up to 50 structured `events`. The dashboard renders them newest-first in a collapsible Events panel, distinguishes green successes, amber warnings, and red errors, and shows a repeat count for deduplicated events. Events with a `tx_hash` include a white explorer link; confirmed USDG banking swaps use this to make the banking transaction directly auditable. This feed is intended for meaningful operational outcomes and blocked actions, not raw bot output or routine no-trade polling.

Bots may send `realized_profit_eth`, bounded `realized_profit_periods`,
`realized_sales`, and `profit_tracking_started_at`. Realized profit is shown
beside session profit on each card and aggregated fleet-wide in ETH plus the
selected CAD/USD currency. A bot-side baseline reset starts a new displayed
accounting period without deleting cumulative totals or transaction-hash
deduplication. `gas_reserve_eth` drives the dashboard's next-buy estimate and
Telegram low-funds threshold. `buy_point_percent` and `sell_point_percent`
display the configured strategy points. Older bots remain compatible and
contribute zero or omit the corresponding metric until updated.

`usdg_balance` is an optional read-only ERC-20 balance, summed as **USDG** in the fleet header. `treasury_sent_usdg` is the bot's all-time total of successful USDG treasury sweeps from its local receipt log; the dashboard renders it in **More info** and sums it in the fleet header. Older bots remain compatible and contribute zero until updated. `capacity_warning` drives the static **ADD POSITIONS** flag when gridless slots are full and another buy would otherwise trigger. `swap_provider` supplies the provider badge; values are rendered generically, including `0x`, `LIFI`, `UNISWAP`, and `SUSHISWAP`.

`sell_attempt` is optional, transient live state. When its `status` is `quote_below_minimum`, the card renders a gently pulsing cyan **SELL CHECK ACTIVE** strip with “Waiting for minimum quote” and, when both numbers are present, projected net profit after sell gas versus minimum profit. Older payloads fall back to quoted profit minus projected gas, then gross quoted profit when no gas estimate was reported. The bot clears this field at the start of every trading cycle and reports it only when that cycle actually reaches the below-minimum sell-quote path. Consequently, the strip appears on the same reporting round as the attempted sell and disappears on the next report without another blocked attempt. It is not added to the persistent Events feed.

`buy_attempt` is optional transient state for a buy that reached an executable
quote but was refused because projected gas exceeded the configured buy cap.
The fleet summary shows an amber **Buy gas blocked** flag with clickable bot
names. The bot card shows projected fee versus cap, the actual quote provider,
intended ETH amount, and classic-grid position ID when applicable. It is live
operational state rather than a persistent Event; because buys run after status
reporting, it appears on the following report and clears when the next buy check
does not reproduce the block.

Each card independently displays Dexscreener **Market Cap** immediately above
AVG P&L. When Dexscreener does not provide circulating market cap but does
provide fully diluted valuation, the card says **FDV** instead; the two values
are never silently conflated. The browser polls one batched dashboard endpoint
every 60 seconds, and the server deduplicates identical chain/token pairs and
caches the preferred WETH-pair result. Market values update their existing DOM
nodes without rebuilding cards, preserving smooth mobile scrolling and zoom.
Temporary API failures retain stale successful cache values; unavailable values
render as an em dash.

Immediately below market cap, **Day** shows Dexscreener's native 24-hour price
movement in green or red. Clicking that row expands the native **5m**, **1h**,
and **6h** movements. The disclosure state is remembered across live card
rebuilds, while refreshed values are patched in place without opening or
closing it. Missing intervals remain neutral and display an em dash.

Experimental bot payloads may include `sigil: {version, method, key, seed}`. For the `spare-wheel-v1` method, each card receives a collapsed **Sigil** panel. Opening it locally constructs a deterministic inline SVG from the reduced consonant key and SHA-256 seed. The panel performs no network request and ignores malformed or unknown methods. The readable intention is never sent to or stored by the dashboard.

Open sigils animate as seed-derived living drawings: a seamless luminous
current travels along the inscription while the entire glyph, including its
rings and terminal marks, turns as one assembly. Rings breathe and nodes pulse
in a deterministic phase unique to that bot. The traveling line also glimmers
on an independent alternating brightness cycle, avoiding any reset at the dash
loop boundary. Clicking the
sigil or its accessible **Animation:
On/Off** button changes the global preference for that browser profile and
stores it in `localStorage`; the choice is therefore device/browser-specific.
Animation defaults off when the device requests reduced motion. Only open
sigils near the viewport animate through `IntersectionObserver`, and all sigil
motion pauses while the page is hidden or during scrolling, touch gestures,
and pinch-zoom settling. The animation uses existing inline SVG and CSS only:
no network request, canvas loop, video, or per-frame JavaScript is involved.
The animated SVG remains filter-free at every viewport size; the static radial
stage background supplies ambience without forcing the browser to rerasterize
a moving drop shadow on every frame.
Routine status events update cards around open sigil and chart panels. The
living SVG or iframe node remains continuously attached while surrounding
metrics, sell checks, positions, balances, events, and timestamps refresh.
Incoming reports are briefly coalesced and routine rendering touches only the
cards whose bot state changed, preventing fleet-wide layout work from starving
the CSS animation timeline. When an animated sigil is visible, routine batches
also wait for browser idle time (with a bounded timeout), and card/stage paint
containment prevents unrelated dashboard changes from invalidating the sigil.
Changed cards are morphed in place: equal DOM subtrees are skipped, text and
attributes change without replacement, and structural edits remain confined to
the segment where they occurred. Open sigil and chart panels are immutable
boundaries that the morph never enters.
Routine batches generate full card markup only for changed bots; unchanged bots
contribute lightweight rank placeholders, eliminating full-fleet HTML building
and parsing from the hot update path. The summary bar is likewise written only
when its rendered content actually changes.
Automatic updates apply the active sort through each grid item's CSS `order`,
so cards move visually without reparenting their SVG or iframe DOM nodes;
unchanged ranks cause no style mutation at all. Sigil
phase is synchronized to wall-clock time exactly once per living SVG; routine
wiring never rewrites that phase or restarts its CSS timeline. Filters hide
cards containing live panels without detaching them, then reveal and update the
same nodes when the filter is cleared. Stroke lengths are normalized in SVG
path units, keeping short and long sigils equally smooth.

Client-side market-data refreshes are limited to one request per 15 seconds and
identical responses do not trigger DOM work. The regular 60-second refresh
remains in place; the shorter throttle only coalesces staggered bot reports.
While a sigil is visibly animating, report-age labels tick every 10 seconds
instead of every second and only changed text is written. Status transitions
and offline notifications therefore remain live without a fleet-wide DOM write
on every animation second.

The **View Large** control beside the animation toggle opens a viewport-fitted
theater view over a darkened dashboard. It renders a wall-clock-synchronized
copy of the same deterministic SVG, so opening or closing it does not detach or
restart the card's animation. The dialog closes from its corner button, the
backdrop, or Escape and locks background scrolling while open.

The fleet summary also aggregates fresh `capacity_warning` reports beside the offline count. When one or more running bots need capacity, an animated amber **Needs new positions** flag shows both the affected-bot count and names, so blocked buy opportunities are visible without scrolling through cards.

Only `bot_id` is required by the generic server. The current bot sends `dashboard_schema_version: 1` so future dashboard revisions can distinguish payload formats safely. The current UI understands the complete schema above. `poll_interval_seconds` is rendered as **Polling** in More Info when reported. `profit_percent` is total current position value versus total cost basis. Session profit, buys, sells, and uptime reset with the bot process.

The dashboard displays report age continuously. A bot is inferred as `running` for reports under 2 minutes old, `stale` from 2–5 minutes, and `offline` after 5 minutes. An explicit bot-supplied `status` overrides this inference.

Manual test:

```bash
curl -X POST https://doomdash.ca/api/status \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: YOUR_KEY' \
  -d '{"bot_id":"manual-test","profit_percent":1.25,"buys":0,"sells":0}'
```

## Browser operations and recovery controls

The toolbar search covers bot ID, display name, token symbol, group, and swap
provider. Chain and provider selectors apply additional filters. The **Tax
coins** toggle includes both manually declared and auto-detected taxed tokens
and persists locally in the browser. If bots exist
but none match, the empty state says **No bots match your filters** and offers
**Clear all filters**. Search, filters, sort, currencies, notifications, and
sigil animation preferences are stored in that browser profile.

**Refresh cards** does not reload the page. It independently fetches the
authoritative `/api/bots` snapshot, renders it, refreshes price/market data,
and reconnects `/api/stream`. The full snapshot replaces the browser's old bot
set so a removed card cannot linger.

Tap the **Live** indicator to inspect last live-message age, last full-snapshot
age, reconnect count, and cards in browser memory. `Live` means SSE is open;
the message/snapshot ages expose a connected but stalled data path. Returning
to a visible tab, restoring a back-forward-cache page, or regaining network
also requests a reconnect.

The cyan **Sell checks active** and amber **Needs new positions** flags scan
the full tracked fleet, not only the displayed filter subset. They are hidden
at zero and appear only while a fresh/running bot reports the condition. Their
token names focus the corresponding card.

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
| `GET` | `/api/eth-price` | No | Cached ETH/USD/CAD conversion rates |
| `GET` | `/api/dexscreener/market-data` | No | Batched cached market cap/FDV for reported bots |
| `GET` | `/api/dexscreener/chart-url` | No | Resolve preferred WETH pair embed URL |

SSE events:

- `snapshot` — all current bot states on connection
- `update` — one accepted status update
- `remove` — one deleted bot ID

## Storage behavior

Latest state and the 100-entry status history are persisted atomically to `STATE_FILE` (default `data/dashboard_state.json`) and restored after restart. Frequent updates are coalesced and flushed every `STATE_FLUSH_INTERVAL` seconds (default 15), with a final flush on normal shutdown, avoiding a complete history rewrite for every status request. Bot-side trade and Event histories are separately persisted and capped at 50 entries each. Persistent profit accounting lives in the bot's `data/profit_totals.json`.

Dexscreener charts are lazy-loaded: the iframe has no URL until its panel is
opened. Chart resolution and card market values share the token's preferred
WETH-pair cache. The cache refreshes at most once per minute per unique
chain/token pair and serves its last successful value as stale during temporary
Dexscreener failures. Routine SSE updates do not reload an open chart.

The fleet toolbar can search bot IDs, display names, token symbols, groups, and provider names;
filter by chain and swap provider (including older bots with an unreported provider);
and sort by name, estimated portfolio or moonbag value, AVG P&L, highest individual position P&L, session profit, session buy or sell count, realized profit, confirmed USDG treasury sent,
position utilization (`filled_positions / max_positions`), ETH balance, USDG balance, or status.
Default directions are name ascending, numeric metrics descending, and status
running-to-offline. The direction button reverses the active sort.

## Production architecture, backup, and rebuild

The trading bot and dashboard are separate repositories by design:

```text
independent robinhood-grid-bot-py clones
  -> authenticated POST /api/status
  -> DoomDash memory + data/dashboard_state.json
  -> browser snapshot/SSE and server-side Telegram monitoring
```

DoomDash is a monitor, not a bot backup. Its snapshots contain public
operational state but not bot source, `.env` strategy configuration, wallet
keys, or a complete reconstructable bot checkout. Back those up at the fleet
host using the trading repository's fleet guide.

For DoomDash recovery, retain these outside Git:

- `.env` (API key and Telegram credentials; mode `0600`)
- `data/dashboard_state.json` (latest bot snapshots and bounded history)
- `data/telegram_alert_state.json` (dedupe, preferences, mute, and digest state)
- reverse-proxy/DNS configuration and the systemd user unit

To rebuild: clone the repository, create the virtual environment, install
requirements, restore `.env` and optional `data/` files, install/start the
one-worker service, restore HTTPS, then verify `/api/health`, `/api/bots`,
`/api/stream`, browser diagnostics, and Telegram `/test`. Bots recreate current
cards on their next reports even without `dashboard_state.json`; lost bounded
history and Telegram dedupe state do not regenerate.

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

Pull and restart the bot so it reports the current schema. Refresh the browser when dashboard HTML changes. If **SELL CHECK ACTIVE** never appears, inspect the latest `/api/bots/<bot_id>` payload for `sell_attempt`; an absent field means that bot is either running older code or did not hit the below-minimum quote path during its latest reported cycle.

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

Fleet bot repositories laid out under `~/bot-farm/rh-bots` can all be updated to `main` at once while the fleet is stopped:

```bash
find ~/bot-farm/rh-bots -mindepth 3 -maxdepth 3 -type d -name .git \
  -execdir git switch main \; \
  -execdir git pull --ff-only origin main \;
```

Restart the fleet with its normal supervisor/launcher after every repository has updated successfully.

## License

MIT
