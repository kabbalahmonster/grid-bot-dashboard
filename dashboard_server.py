#!/usr/bin/env python3
"""
Grid Bot Dashboard Server
=========================
Flask application that receives status updates from grid trading bots,
stores them in memory, and broadcasts live updates to browsers via SSE.

Endpoints:
    GET  /                        — Serve the dashboard HTML
    POST /api/status              — Receive bot status updates (API key auth)
    GET  /api/bots                — Return all current bot states
    GET  /api/bots/<id>           — Return a single bot's state
    GET  /api/bots/<id>/history   — Return history for one bot
    DELETE /api/bots/<id>         — Remove a bot (API key auth)
    GET  /api/stream              — SSE endpoint for live browser updates
    GET  /api/health              — Health check

Security:
    - API key authentication via X-API-Key header
    - Rate limiting: 100 requests/minute per IP
    - Private key pattern detection (keys and values)
"""

import json
import logging
import os
import queue
import re
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from functools import wraps
from urllib.parse import urlencode

import requests as http_requests
from flask import Flask, Response, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_KEY = os.environ.get("API_KEY", "")
PORT = int(os.environ.get("PORT", "5000"))
HOST = os.environ.get("HOST", "0.0.0.0")

MAX_HISTORY_PER_BOT = 100
RATE_LIMIT_WINDOW = 60          # seconds
RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("RATE_LIMIT_MAX_REQUESTS", "600"))
SSE_KEEPALIVE_INTERVAL = 15     # seconds
SSE_CLIENT_QUEUE_SIZE = 200     # max queued messages per SSE client
DEXSCREENER_TIMEOUT = 8
DEXSCREENER_CACHE_TTL = 300
STATE_FILE = os.environ.get("STATE_FILE", "data/dashboard_state.json")
CHAIN_SLUGS = {4663: "robinhood", 8453: "base", 1: "ethereum"}

# Patterns that suggest private key material (checked against keys AND values)
_PRIVATE_KEY_PATTERNS = [
    re.compile(r"-----BEGIN\s+(RSA|EC|DSA|OPENSSH|PGP|ENCRYPTED)?\s*PRIVATE\s+KEY", re.IGNORECASE),
    re.compile(r"\bprivate[_-]?key\b", re.IGNORECASE),
    re.compile(r"\bsecret[_-]?key\b", re.IGNORECASE),
    re.compile(r"\bapi[_-]?secret\b", re.IGNORECASE),
    re.compile(r"\bwallet[_-]?seed\b", re.IGNORECASE),
    re.compile(r"\bmnemonic\b", re.IGNORECASE),
    re.compile(r"\b0x[0-9a-fA-F]{64}\b"),                    # raw hex private key
    re.compile(r"\b[5KL][1-9A-HJ-NP-Za-km-z]{50,51}\b"),    # WIF bitcoin key
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("dashboard")

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# In-memory storage (thread-safe via locks)
# ---------------------------------------------------------------------------

_lock = threading.Lock()

# bot_id → latest payload dict
bot_states: dict[str, dict] = {}

# bot_id → deque of recent payloads (max 100)
bot_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=MAX_HISTORY_PER_BOT))

# SSE subscriber queues
_sse_subscribers: list[queue.Queue] = []
_sse_lock = threading.Lock()

# Rate-limit store: ip → deque of timestamps
_rate_store: dict[str, deque] = defaultdict(lambda: deque(maxlen=RATE_LIMIT_MAX_REQUESTS + 1))
_rate_lock = threading.Lock()

# (chain_id, token_address) -> (cached_at_monotonic, pair_address)
_dexscreener_pair_cache: dict[tuple[int, str], tuple[float, str]] = {}
_dexscreener_lock = threading.Lock()


def _persist_state_locked():
    """Atomically persist current state and bounded history. Caller holds _lock."""
    directory = os.path.dirname(STATE_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)
    temp_file = STATE_FILE + ".tmp"
    payload = {
        "bot_states": bot_states,
        "bot_history": {bot_id: list(entries) for bot_id, entries in bot_history.items()},
    }
    with open(temp_file, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    os.replace(temp_file, STATE_FILE)


def _load_persisted_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        states = payload.get("bot_states", {})
        histories = payload.get("bot_history", {})
        if isinstance(states, dict):
            bot_states.update(states)
        if isinstance(histories, dict):
            for bot_id, entries in histories.items():
                bot_history[bot_id].extend(entries[-MAX_HISTORY_PER_BOT:])
        logger.info("Restored %d bots from %s", len(bot_states), STATE_FILE)
    except FileNotFoundError:
        pass
    except (OSError, ValueError, TypeError) as exc:
        logger.warning("Could not restore dashboard state: %s", exc)


_load_persisted_state()

# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

def _is_rate_limited(ip: str) -> bool:
    """Return True if *ip* has exceeded the rate limit."""
    now = time.monotonic()
    with _rate_lock:
        dq = _rate_store[ip]
        while dq and now - dq[0] > RATE_LIMIT_WINDOW:
            dq.popleft()
        if len(dq) >= RATE_LIMIT_MAX_REQUESTS:
            return True
        dq.append(now)
    return False


# ---------------------------------------------------------------------------
# Auth decorator
# ---------------------------------------------------------------------------

def require_api_key(f):
    """Enforce X-API-Key header authentication."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not API_KEY:
            logger.error("API_KEY environment variable is not set")
            return jsonify({"error": "Server misconfigured: API_KEY not set"}), 500
        key = request.headers.get("X-API-Key", "")
        if not key or key != API_KEY:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Private-key scanner (checks dict keys AND values recursively)
# ---------------------------------------------------------------------------

def _contains_private_key(obj, depth: int = 0) -> bool:
    """Recursively scan *obj* for anything that looks like a private key.
    Checks both dictionary keys and string values."""
    if depth > 10:
        return False
    if isinstance(obj, str):
        return any(p.search(obj) for p in _PRIVATE_KEY_PATTERNS)
    if isinstance(obj, dict):
        for k, v in obj.items():
            if _contains_private_key(k, depth + 1):
                return True
            # Transaction hashes and raw private keys share the same 0x +
            # 64-hex shape. Permit that shape only under the explicit tx_hash key.
            if str(k).lower() == "tx_hash" and isinstance(v, str) and re.fullmatch(r"0x[0-9a-fA-F]{64}", v):
                continue
            if _contains_private_key(v, depth + 1):
                return True
    if isinstance(obj, (list, tuple)):
        return any(_contains_private_key(item, depth + 1) for item in obj)
    return False


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_status_payload(data) -> tuple[bool, str]:
    """Validate incoming bot status payload. Returns (ok, error_msg)."""
    if not isinstance(data, dict):
        return False, "Payload must be a JSON object"

    bot_id = data.get("bot_id")
    if not bot_id or not isinstance(bot_id, str) or not bot_id.strip():
        return False, "Missing or invalid 'bot_id' (must be a non-empty string)"

    if len(bot_id.strip()) > 128:
        return False, "'bot_id' too long (max 128 chars)"

    if _contains_private_key(data):
        return False, "Payload rejected: appears to contain private key material"

    return True, ""


# ---------------------------------------------------------------------------
# SSE helpers
# ---------------------------------------------------------------------------

def _sse_format(data: dict, event: str = "update") -> str:
    """Format a dict as an SSE message string."""
    payload = json.dumps(data, default=str)
    return f"event: {event}\ndata: {payload}\n\n"


def _broadcast(event: str, data: dict):
    """Push an SSE message to every connected subscriber."""
    msg = _sse_format(data, event)
    with _sse_lock:
        dead = []
        for q in _sse_subscribers:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _sse_subscribers.remove(q)


# ---------------------------------------------------------------------------
# Middleware: rate limiting on /api/ routes
# ---------------------------------------------------------------------------

@app.before_request
def rate_limit():
    if request.path.startswith("/api/"):
        ip = request.remote_addr or "unknown"
        if _is_rate_limited(ip):
            return jsonify({"error": f"Rate limit exceeded. Max {RATE_LIMIT_MAX_REQUESTS} requests/minute."}), 429


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Serve the dashboard HTML."""
    return DASHBOARD_HTML, 200, {"Content-Type": "text/html; charset=utf-8"}


@app.route("/api/status", methods=["POST"])
@require_api_key
def receive_status():
    """Receive a bot status update."""
    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Invalid or missing JSON body"}), 400

    ok, err = _validate_status_payload(data)
    if not ok:
        logger.warning("Rejected payload from %s: %s", request.remote_addr, err)
        return jsonify({"error": err}), 400

    bot_id = data["bot_id"].strip()
    now = datetime.now(timezone.utc).isoformat()
    data["received_at"] = now

    entry = {
        "id": str(uuid.uuid4()),
        "bot_id": bot_id,
        "received_at": now,
        "data": data,
    }

    with _lock:
        bot_states[bot_id] = data
        bot_history[bot_id].append(entry)
        try:
            _persist_state_locked()
        except OSError as exc:
            logger.warning("Could not persist dashboard state: %s", exc)

    _broadcast("update", entry)
    logger.info("Status update from bot=%s", bot_id)

    return jsonify({"ok": True, "id": entry["id"], "received_at": now}), 200


@app.route("/api/bots", methods=["GET"])
def list_bots():
    """Return all current bot states."""
    with _lock:
        result = {}
        for bot_id, state in bot_states.items():
            result[bot_id] = {
                "state": state,
                "history_count": len(bot_history.get(bot_id, [])),
            }
    return jsonify({"bots": result, "count": len(result)}), 200


@app.route("/api/bots/<bot_id>", methods=["GET"])
def get_bot(bot_id: str):
    """Return a single bot's current state."""
    with _lock:
        if bot_id not in bot_states:
            return jsonify({"error": f"Bot '{bot_id}' not found"}), 404
        return jsonify({
            "bot_id": bot_id,
            "state": bot_states[bot_id],
            "history_count": len(bot_history.get(bot_id, [])),
        }), 200


@app.route("/api/bots/<bot_id>/history", methods=["GET"])
def get_bot_history(bot_id: str):
    """Return history for a specific bot."""
    with _lock:
        if bot_id not in bot_history:
            return jsonify({"error": f"Bot '{bot_id}' not found"}), 404
        history = list(bot_history[bot_id])
    return jsonify({"bot_id": bot_id, "history": history, "count": len(history)}), 200


@app.route("/api/bots/<bot_id>", methods=["DELETE"])
@require_api_key
def remove_bot(bot_id: str):
    """Remove a bot from the dashboard."""
    with _lock:
        if bot_id not in bot_states:
            return jsonify({"error": f"Bot '{bot_id}' not found"}), 404
        del bot_states[bot_id]
        bot_history.pop(bot_id, None)
        try:
            _persist_state_locked()
        except OSError as exc:
            logger.warning("Could not persist dashboard state: %s", exc)
    _broadcast("remove", {"bot_id": bot_id})
    logger.info("Bot removed: %s", bot_id)
    return jsonify({"ok": True, "bot_id": bot_id}), 200


@app.route("/api/stream")
def sse_stream():
    """SSE endpoint for live browser updates."""
    q: queue.Queue = queue.Queue(maxsize=SSE_CLIENT_QUEUE_SIZE)

    with _sse_lock:
        _sse_subscribers.append(q)
    logger.info("SSE client connected from %s (%d total)",
                request.remote_addr, len(_sse_subscribers))

    def generate():
        try:
            # Send initial snapshot of all bots
            with _lock:
                snapshot = {bid: state for bid, state in bot_states.items()}
            yield _sse_format({"type": "snapshot", "bots": snapshot}, event="snapshot")

            while True:
                try:
                    msg = q.get(timeout=SSE_KEEPALIVE_INTERVAL)
                    yield msg
                except queue.Empty:
                    yield ": keepalive\n\n"
        except GeneratorExit:
            pass
        finally:
            with _sse_lock:
                if q in _sse_subscribers:
                    _sse_subscribers.remove(q)
            logger.info("SSE client disconnected (%d remaining)", len(_sse_subscribers))

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    with _lock:
        bot_count = len(bot_states)
    with _sse_lock:
        sse_count = len(_sse_subscribers)
    return jsonify({
        "status": "ok",
        "bots_tracked": bot_count,
        "sse_clients": sse_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }), 200


@app.route("/api/dexscreener/chart-url", methods=["GET"])
def dexscreener_chart_url():
    """Resolve the preferred WETH pair and return its direct embed URL."""
    try:
        chain_id = int(request.args.get("chain_id", ""))
    except ValueError:
        return jsonify({"error": "Invalid chain_id"}), 400

    token_address = request.args.get("token_address", "").strip()
    wallet_address = request.args.get("wallet_address", "").strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", token_address):
        return jsonify({"error": "Invalid token_address"}), 400
    if wallet_address and not re.fullmatch(r"0x[0-9a-fA-F]{40}", wallet_address):
        return jsonify({"error": "Invalid wallet_address"}), 400

    chain_slug = CHAIN_SLUGS.get(chain_id)
    if not chain_slug:
        return jsonify({"error": f"Unsupported chain_id: {chain_id}"}), 400

    cache_key = (chain_id, token_address.lower())
    now = time.monotonic()
    with _dexscreener_lock:
        cached = _dexscreener_pair_cache.get(cache_key)
    pair_address = cached[1] if cached and now - cached[0] < DEXSCREENER_CACHE_TTL else ""

    if not pair_address:
        api_url = f"https://api.dexscreener.com/token-pairs/v1/{chain_slug}/{token_address}"
        try:
            response = http_requests.get(api_url, timeout=DEXSCREENER_TIMEOUT)
            response.raise_for_status()
            pairs = response.json()
        except (http_requests.RequestException, ValueError) as exc:
            logger.warning("Dexscreener pair lookup failed: %s", exc)
            return jsonify({"error": "Dexscreener pair lookup failed"}), 502

        if not isinstance(pairs, list) or not pairs:
            return jsonify({"error": "No Dexscreener pair found"}), 404

        def pair_rank(pair):
            quote_symbol = str(pair.get("quoteToken", {}).get("symbol", "")).upper()
            base_symbol = str(pair.get("baseToken", {}).get("symbol", "")).upper()
            is_weth = quote_symbol == "WETH" or base_symbol == "WETH"
            liquidity = float(pair.get("liquidity", {}).get("usd") or 0)
            return (is_weth, liquidity)

        selected = max(pairs, key=pair_rank)
        pair_address = str(selected.get("pairAddress", ""))
        if not pair_address:
            return jsonify({"error": "Dexscreener pair response missing address"}), 502
        with _dexscreener_lock:
            _dexscreener_pair_cache[cache_key] = (now, pair_address)

    params = {"embed": "1", "theme": "dark", "info": "0"}
    if wallet_address:
        params["maker"] = wallet_address
    chart_url = f"https://dexscreener.com/{chain_slug}/{pair_address}?{urlencode(params)}"
    return jsonify({"chart_url": chart_url, "pair_address": pair_address}), 200


# ---------------------------------------------------------------------------
# Error handlers
# ---------------------------------------------------------------------------

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({"error": "Method not allowed"}), 405


@app.errorhandler(500)
def internal_error(e):
    logger.exception("Internal server error")
    return jsonify({"error": "Internal server error"}), 500


# ---------------------------------------------------------------------------
# Dashboard HTML (inline for self-contained deployment)
# ---------------------------------------------------------------------------

DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Grid Bot Dashboard</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }
  .header { background: #1e293b; padding: 1rem 2rem; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid #334155; }
  .header h1 { font-size: 1.25rem; font-weight: 600; }
  .status-dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; }
  .status-dot.connected { background: #22c55e; }
  .status-dot.disconnected { background: #ef4444; }
  .container { max-width: 1200px; margin: 2rem auto; padding: 0 1rem; }
  .summary-bar, .toolbar { display: flex; flex-wrap: wrap; gap: 0.6rem; margin-bottom: 1rem; }
  .summary-item { background: #1e293b; border: 1px solid #334155; border-radius: 0.4rem; padding: 0.55rem 0.75rem; font-size: 0.8rem; }
  .toolbar select, .toolbar input, .toolbar button { background: #1e293b; color: #e2e8f0; border: 1px solid #334155; border-radius: 0.35rem; padding: 0.45rem 0.6rem; }
  .filter-wrap { position: relative; display: inline-flex; }
  .filter-wrap input { padding-right: 2rem; width: 100%; }
  .clear-filter { position: absolute; right: 0.25rem; top: 50%; transform: translateY(-50%); border: 0 !important; background: transparent !important; padding: 0.25rem 0.45rem !important; color: #94a3b8 !important; font-size: 1rem; line-height: 1; display: none; }
  .chain-badge, .group-badge { display: inline-block; color: #cbd5e1; background: #334155; border-radius: 9999px; padding: 0.1rem 0.4rem; font-size: 0.65rem; margin-left: 0.3rem; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(350px, 1fr)); gap: 1rem; }
  .card { background: #1e293b; border: 1px solid #334155; border-radius: 0.5rem; padding: 1.25rem; }
  .card h2 { font-size: 1rem; color: #94a3b8; margin-bottom: 0.5rem; }
  .card .bot-id { font-size: 1.1rem; font-weight: 700; color: #f1f5f9; margin-bottom: 0.75rem; }
  .metric { display: flex; justify-content: space-between; padding: 0.35rem 0; border-bottom: 1px solid #1e293b; font-size: 0.875rem; }
  .metric:last-child { border-bottom: none; }
  .metric .label { color: #94a3b8; }
  .metric .value { color: #f1f5f9; font-weight: 500; }
  .metric .value a { color: inherit; text-decoration: none; }
  .metric .value a:hover { text-decoration: underline; }
  .metric .value.positive { color: #22c55e; }
  .metric .value.negative { color: #ef4444; }
  .positions { margin-top: 1rem; border-top: 1px solid #334155; padding-top: 0.75rem; }
  .positions h3 { font-size: 0.875rem; color: #94a3b8; margin-bottom: 0.5rem; }
  .position { background: #0f172a; padding: 0.5rem; border-radius: 0.25rem; margin-bottom: 0.5rem; font-size: 0.8rem; }
  .position .pos-header { display: flex; justify-content: space-between; margin-bottom: 0.25rem; }
  .position .pos-id { color: #64748b; font-size: 0.7rem; }
  .position .pos-pnl { font-weight: 600; }
  .position .pos-pnl.positive { color: #22c55e; }
  .position .pos-pnl.negative { color: #ef4444; }
  .position .pos-details { color: #94a3b8; font-size: 0.75rem; }
  .position.pos-hidden { display: none; }
  .timestamp { font-size: 0.75rem; color: #64748b; margin-top: 0.5rem; }
  .empty { text-align: center; padding: 4rem 1rem; color: #64748b; }
  .empty p { font-size: 1.1rem; margin-bottom: 0.5rem; }
  #connection-status { font-size: 0.8rem; color: #94a3b8; }
  .badge { display: inline-block; padding: 0.15rem 0.5rem; border-radius: 9999px; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; }
  .badge.running { background: #166534; color: #bbf7d0; }
  .badge.stopped { background: #991b1b; color: #fecaca; }
  .badge.paused { background: #a16207; color: #fef08a; }
  .badge.error { background: #7f1d1d; color: #fca5a5; }
  .badge.stale { background: #a16207; color: #fef08a; }
  .badge.offline { background: #7f1d1d; color: #fecaca; }
  .badge.unknown { background: #334155; color: #94a3b8; }
  pre.raw { background: #0f172a; padding: 0.75rem; border-radius: 0.375rem; font-size: 0.75rem; overflow-x: auto; max-height: 200px; overflow-y: auto; margin-top: 0.5rem; color: #94a3b8; }
  .toggle-raw { background: none; border: 1px solid #334155; color: #94a3b8; padding: 0.2rem 0.6rem; border-radius: 0.25rem; cursor: pointer; font-size: 0.75rem; margin-top: 0.5rem; }
  .toggle-raw:hover { border-color: #64748b; color: #e2e8f0; }
  details.more-info { margin-top: 0.5rem; }
  details.more-info summary { list-style: none; display: inline-block; }
  details.more-info summary::-webkit-details-marker { display: none; }
  details.more-info[open] summary { margin-bottom: 0.35rem; }
  details.chart-panel { margin-top: 0.75rem; }
  .dex-chart { width: 100%; height: 520px; border: 1px solid #334155; border-radius: 0.375rem; margin-top: 0.5rem; background: #0f172a; }
  .trades { margin-top: 0.75rem; }
  .trade { display: grid; grid-template-columns: 3.5rem 1fr auto; gap: 0.5rem; background: #0f172a; border-radius: 0.25rem; padding: 0.45rem; margin-top: 0.35rem; font-size: 0.75rem; }
  .trade .buy { color: #22c55e; } .trade .sell { color: #ef4444; }
  .trade a { color: #f1f5f9; text-decoration: none; }
  .trade a:hover { text-decoration: underline; }
</style>
</head>
<body>

<div class="header">
  <h1>⚡ Grid Bot Dashboard</h1>
  <div>
    <span class="status-dot disconnected" id="dot"></span>
    <span id="connection-status">Connecting…</span>
  </div>
</div>

<div class="container">
  <div class="summary-bar" id="summary-bar"></div>
  <div class="toolbar">
    <span class="filter-wrap"><input id="bot-filter" placeholder="Filter bots or groups"><button id="clear-filter" class="clear-filter" type="button" aria-label="Clear filter">×</button></span>
    <select id="chain-filter"><option value="">All chains</option><option value="4663">Robinhood</option><option value="8453">Base</option><option value="1">Ethereum</option></select>
    <select id="sort-bots"><option value="name">Sort: name</option><option value="pnl">AVG P&L</option><option value="profit" selected>Session profit</option><option value="status">Status</option></select>
    <button id="sort-direction" type="button" title="Reverse sort direction">Descending ↓</button>
    <button id="notifications">Enable offline alerts</button>
  </div>
  <div id="bots-container">
    <div class="empty" id="empty-state">
      <p>No bots reporting yet</p>
      <span>Waiting for status updates…</span>
    </div>
  </div>
</div>

<script>
(function() {
  const container = document.getElementById('bots-container');
  const emptyState = document.getElementById('empty-state');
  const dot = document.getElementById('dot');
  const connStatus = document.getElementById('connection-status');
  const summaryBar = document.getElementById('summary-bar');
  const botFilter = document.getElementById('bot-filter');
  const clearFilter = document.getElementById('clear-filter');
  const chainFilter = document.getElementById('chain-filter');
  const sortBots = document.getElementById('sort-bots');
  const sortDirection = document.getElementById('sort-direction');
  const notificationsButton = document.getElementById('notifications');
  const bots = {};
  const openMoreInfo = new Set();
  const openPositions = new Set();
  const openRawJson = new Set();
  const openCharts = new Set();
  const openTrades = new Set();
  const rawJsonScroll = new Map();
  const notifiedOffline = new Set();
  const defaultSortDirections = { name: 'asc', pnl: 'desc', profit: 'desc', status: 'asc' };
  let sortDirectionValue = defaultSortDirections.profit;
  const chainMetadata = {
    1: { name: 'Ethereum', explorer: 'https://etherscan.io/address/' },
    8453: { name: 'Base', explorer: 'https://base.blockscout.com/address/' },
    4663: { name: 'Robinhood', explorer: 'https://robinhoodchain.blockscout.com/address/' },
  };

  let evtSource = null;
  let reconnectDelay = 1000;
  const maxReconnectDelay = 30000;

  function connect() {
    evtSource = new EventSource('/api/stream');

    evtSource.onopen = function() {
      dot.className = 'status-dot connected';
      connStatus.textContent = 'Live';
      reconnectDelay = 1000;
    };

    evtSource.onerror = function() {
      dot.className = 'status-dot disconnected';
      connStatus.textContent = 'Reconnecting…';
      evtSource.close();
      setTimeout(connect, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 2, maxReconnectDelay);
    };

    evtSource.addEventListener('snapshot', function(e) {
      const data = JSON.parse(e.data);
      if (data.bots) {
        Object.keys(data.bots).forEach(function(botId) {
          bots[botId] = data.bots[botId];
        });
        render();
      }
    });

    evtSource.addEventListener('update', function(e) {
      const entry = JSON.parse(e.data);
      bots[entry.bot_id] = entry.data || entry;
      render();
    });

    evtSource.addEventListener('remove', function(e) {
      const data = JSON.parse(e.data);
      if (data.bot_id && bots[data.bot_id]) {
        delete bots[data.bot_id];
        render();
      }
    });
  }

  function esc(str) {
    const div = document.createElement('div');
    div.textContent = String(str === null || str === undefined ? '' : str);
    return div.innerHTML;
  }

  function formatVal(val) {
    if (val === null || val === undefined) return '—';
    if (typeof val === 'number') {
      return val % 1 !== 0 ? val.toFixed(6).replace(/0+$/, '').replace(/\\.$/, '') : val.toLocaleString();
    }
    if (typeof val === 'boolean') return val ? 'Yes' : 'No';
    return String(val);
  }

  function statusBadge(status) {
    const s = (status || 'unknown').toLowerCase();
    return '<span class="badge ' + esc(s) + '">' + esc(s) + '</span>';
  }

  function reportAge(receivedAt) {
    const timestamp = Date.parse(receivedAt || '');
    if (!Number.isFinite(timestamp)) return { status: 'unknown', text: 'unknown' };
    const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
    let text;
    if (seconds < 60) text = seconds + 's ago';
    else if (seconds < 3600) text = Math.floor(seconds / 60) + 'm ' + (seconds % 60) + 's ago';
    else text = Math.floor(seconds / 3600) + 'h ' + Math.floor((seconds % 3600) / 60) + 'm ago';
    return { status: seconds < 120 ? 'running' : (seconds < 300 ? 'stale' : 'offline'), text: text };
  }

  function refreshReportAges() {
    container.querySelectorAll('[data-received-at]').forEach(function(el) {
      const age = reportAge(el.dataset.receivedAt);
      el.textContent = 'Updated ' + age.text;
      const badge = el.closest('.card').querySelector('.badge');
      if (badge && badge.dataset.inferred === 'true') {
        badge.className = 'badge ' + age.status;
        badge.textContent = age.status;
      }
      const card = el.closest('.card');
      const botId = card ? card.dataset.botId : '';
      if (age.status === 'offline' && botId && !notifiedOffline.has(botId)) {
        notifiedOffline.add(botId);
        if (Notification.permission === 'granted') new Notification(botId + ' is offline', { body: 'Last report ' + age.text });
      } else if (age.status === 'running') notifiedOffline.delete(botId);
    });
  }

  function updateSummary(botIds) {
    const states = botIds.map(function(id) { return bots[id]; });
    const active = states.filter(function(d) { return reportAge(d.received_at).status === 'running'; }).length;
    const offline = states.filter(function(d) { return reportAge(d.received_at).status === 'offline'; }).length;
    const profit = states.reduce(function(total, d) { return total + (parseFloat(d.session_profit_eth) || 0); }, 0);
    const filled = states.reduce(function(total, d) { return total + (parseInt(d.filled_positions, 10) || 0); }, 0);
    summaryBar.innerHTML = '<span class="summary-item">Active: ' + active + ' / ' + states.length + '</span>' +
      '<span class="summary-item">Offline: ' + offline + '</span>' +
      '<span class="summary-item">Session profit: ' + (profit >= 0 ? '+' : '') + profit.toFixed(8) + ' ETH</span>' +
      '<span class="summary-item">Filled positions: ' + filled + '</span>';
  }

  function shortenAddress(address) {
    const value = String(address || '');
    return value.length > 9 ? value.slice(0, 5) + '…' + value.slice(-3) : value;
  }

  function render(force) {
    // Preserve expansion state before live updates rebuild the cards.
    container.querySelectorAll('details.more-info[data-bot-key]').forEach(function(el) {
      if (el.open) openMoreInfo.add(el.dataset.botKey);
      else openMoreInfo.delete(el.dataset.botKey);
    });
    container.querySelectorAll('button[data-pos-key]').forEach(function(el) {
      if (el.dataset.expanded === 'true') openPositions.add(el.dataset.posKey);
      else openPositions.delete(el.dataset.posKey);
    });
    container.querySelectorAll('button[data-raw-key]').forEach(function(el) {
      if (el.dataset.expanded === 'true') openRawJson.add(el.dataset.rawKey);
      else openRawJson.delete(el.dataset.rawKey);
    });
    container.querySelectorAll('pre[data-raw-scroll-key]').forEach(function(el) {
      rawJsonScroll.set(el.dataset.rawScrollKey, el.scrollTop);
    });
    container.querySelectorAll('details.chart-panel[data-chart-key]').forEach(function(el) {
      if (el.open) openCharts.add(el.dataset.chartKey);
      else openCharts.delete(el.dataset.chartKey);
    });
    container.querySelectorAll('details.trades[data-trades-key]').forEach(function(el) {
      if (el.open) openTrades.add(el.dataset.tradesKey);
      else openTrades.delete(el.dataset.tradesKey);
    });
    // Avoid reloading an open third-party iframe on every bot status update.
    if (openCharts.size > 0 && !force) return;

    const query = botFilter.value.trim().toLowerCase();
    const wantedChain = chainFilter.value;
    const rank = { running: 0, stale: 1, offline: 2, unknown: 3 };
    const botIds = Object.keys(bots).filter(function(id) {
      const d = bots[id];
      const haystack = [id, d.display_name, d.group].join(' ').toLowerCase();
      return (!query || haystack.includes(query)) && (!wantedChain || String(d.chain_id) === wantedChain);
    }).sort(function(a, b) {
      const av = bots[a], bv = bots[b], mode = sortBots.value;
      let result;
      if (mode === 'pnl') result = (parseFloat(av.profit_percent) || 0) - (parseFloat(bv.profit_percent) || 0);
      else if (mode === 'profit') result = (parseFloat(av.session_profit_eth) || 0) - (parseFloat(bv.session_profit_eth) || 0);
      else if (mode === 'status') result = rank[reportAge(av.received_at).status] - rank[reportAge(bv.received_at).status];
      else result = a.localeCompare(b);
      return sortDirectionValue === 'asc' ? result : -result;
    });
    updateSummary(botIds);
    if (botIds.length === 0) {
      container.innerHTML = '';
      container.appendChild(emptyState);
      emptyState.style.display = '';
      return;
    }
    emptyState.style.display = 'none';

    let html = '<div class="grid">';
    botIds.forEach(function(botId) {
      const d = bots[botId];
      const age = reportAge(d.received_at);
      const status = d.status || age.status;
      const botKey = encodeURIComponent(botId);
      const moreOpen = openMoreInfo.has(botKey);
      const positionsOpen = openPositions.has(botKey);
      const rawOpen = openRawJson.has(botKey);
      const chartOpen = openCharts.has(botKey);
      html += '<div class="card" data-bot-id="' + esc(botId) + '">';
      html += '<h2>Bot</h2>';
      const chain = chainMetadata[Number(d.chain_id)];
      html += '<div class="bot-id">' + esc(d.display_name || botId) + ' ' + statusBadge(status).replace('<span ', '<span data-inferred="' + (!d.status) + '" ') +
        (chain ? '<span class="chain-badge">' + esc(chain.name) + '</span>' : '') +
        (d.group ? '<span class="group-badge">' + esc(d.group) + '</span>' : '') + '</div>';

      d.buys = d.buys ?? 0;
      d.sells = d.sells ?? 0;

      const metrics = [
        ['AVG P&L', 'profit_percent'],
        ['Session Profit', 'session_profit_eth'],
        ['Filled / Max Positions', 'position_capacity'],
      ];
      const moreMetrics = [
        ['Price', 'price'],
        ['Buys', 'buys'], ['Sells', 'sells'],
        ['ETH Balance', 'eth_balance'], ['Token Balance', 'token_balance'],
        ['Wallet', 'wallet_link'], ['Token', 'token_link'],
        ['RPC', 'rpc_status'], ['Uptime', 'uptime_seconds'],
      ];

      d.wallet_link = d.wallet_address && chain
        ? '<a href="' + esc(chain.explorer + d.wallet_address) + '" target="_blank" rel="noopener noreferrer" title="' + esc(d.wallet_address) + '">' + esc(shortenAddress(d.wallet_address)) + '</a>'
        : (d.wallet_address ? esc(shortenAddress(d.wallet_address)) : null);
      d.token_link = d.token_address && chain
        ? '<a href="' + esc(chain.explorer + d.token_address) + '" target="_blank" rel="noopener noreferrer" title="' + esc(d.token_address) + '">' + esc(shortenAddress(d.token_address)) + '</a>'
        : (d.token_address ? esc(shortenAddress(d.token_address)) : null);

      d.position_capacity = (d.filled_positions !== undefined && d.max_positions !== undefined)
        ? d.filled_positions + ' / ' + d.max_positions
        : null;

      function renderMetric(pair) {
        const label = pair[0], key = pair[1];
        if (d[key] !== undefined && d[key] !== null) {
          let val = d[key];
          let cls = '';
          if (key === 'profit_percent') {
            cls = parseFloat(val) >= 0 ? 'positive' : 'negative';
            val = (parseFloat(val) >= 0 ? '+' : '') + parseFloat(val).toFixed(2) + '%';
          } else if (key === 'session_profit_eth') {
            cls = parseFloat(val) >= 0 ? 'positive' : 'negative';
            val = (parseFloat(val) >= 0 ? '+' : '') + parseFloat(val).toFixed(8) + ' ETH';
          } else if (key === 'price') {
            const n = parseFloat(val);
            val = n.toFixed(10);
          } else if (key === 'uptime_seconds') {
            const s = parseInt(val);
            if (s < 60) val = s + 's';
            else if (s < 3600) val = Math.floor(s/60) + 'm ' + (s%60) + 's';
            else val = Math.floor(s/3600) + 'h ' + Math.floor((s%3600)/60) + 'm';
          } else if (key === 'eth_balance' || key === 'token_balance') {
            val = parseFloat(val).toFixed(key === 'eth_balance' ? 4 : 0);
          }
          const renderedValue = key === 'wallet_link' || key === 'token_link' ? val : esc(val);
          return '<div class="metric"><span class="label">' + esc(label) + '</span><span class="value ' + cls + '">' + renderedValue + '</span></div>';
        }
        return '';
      }

      metrics.forEach(function(pair) { html += renderMetric(pair); });
      html += '<details class="more-info" data-bot-key="' + esc(botKey) + '"' + (moreOpen ? ' open' : '') + '><summary class="toggle-raw">More info</summary>';
      moreMetrics.forEach(function(pair) { html += renderMetric(pair); });
      html += '</details>';

      if (d.chain_id && d.token_address && d.wallet_address) {
        const chartParams = new URLSearchParams({
          chain_id: String(d.chain_id),
          token_address: String(d.token_address),
          wallet_address: String(d.wallet_address),
        });
        html += '<details class="chart-panel" data-chart-key="' + esc(botKey) + '"' + (chartOpen ? ' open' : '') + '><summary class="toggle-raw">Dexscreener chart</summary>';
        html += '<iframe class="dex-chart" loading="lazy" data-resolver="/api/dexscreener/chart-url?' + esc(chartParams.toString()) + '" title="Dexscreener chart"></iframe></details>';
      }

      if (d.trades_history && d.trades_history.length) {
        const recentTrades = d.trades_history.slice().reverse();
        html += '<details class="trades" data-trades-key="' + esc(botKey) + '"' + (openTrades.has(botKey) ? ' open' : '') + '><summary class="toggle-raw">Trade history (' + recentTrades.length + ')</summary>';
        recentTrades.forEach(function(trade) {
          const txUrl = chain && trade.tx_hash ? chain.explorer.replace('/address/', '/tx/') + trade.tx_hash : '';
          html += '<div class="trade"><strong class="' + esc(trade.side) + '">' + esc(String(trade.side).toUpperCase()) + '</strong>' +
            '<span>' + esc(parseFloat(trade.eth_amount || 0).toFixed(8)) + ' ETH · ' + esc(parseFloat(trade.token_amount || 0).toFixed(2)) + ' tokens</span>' +
            (txUrl ? '<a href="' + esc(txUrl) + '" target="_blank" rel="noopener noreferrer">tx</a>' : '') + '</div>';
        });
        html += '</details>';
      }

      // Display positions if available (show 3, expandable)
      if (d.positions && d.positions.length > 0) {
        const sorted = d.positions.slice().sort(function(a, b) {
          const ap = Number.isFinite(parseFloat(a.pnl)) ? parseFloat(a.pnl) : -Infinity;
          const bp = Number.isFinite(parseFloat(b.pnl)) ? parseFloat(b.pnl) : -Infinity;
          if (ap !== bp) return bp - ap;
          return (parseInt(b.id, 10) || 0) - (parseInt(a.id, 10) || 0);
        });
        const showCount = 3;
        html += '<div class="positions"><h3>Positions (' + sorted.length + ')</h3>';
        sorted.forEach(function(pos, i) {
          const pnl = pos.pnl !== undefined ? pos.pnl : null;
          const pnlClass = pnl !== null ? (parseFloat(pnl) >= 0 ? 'positive' : 'negative') : '';
          const hidden = i >= showCount ? ' pos-hidden' : '';
          const visibleStyle = i >= showCount && positionsOpen ? ' style="display:block"' : '';
          html += '<div class="position' + hidden + '"' + visibleStyle + '>';
          html += '<div class="pos-header"><span class="pos-id">#' + esc(pos.id || '—') + '</span>';
          if (pnl !== null) {
            html += '<span class="pos-pnl ' + pnlClass + '">' + (pnl >= 0 ? '+' : '') + esc(parseFloat(pnl).toFixed(1)) + '%</span>';
          }
          html += '</div>';
          html += '<div class="pos-details">';
          html += 'Amount: ' + esc(parseFloat(pos.buy_amount_token || 0).toFixed(0)) + ' | ';
          html += 'Cost: ' + esc(parseFloat(pos.cost_basis || 0).toFixed(8)) + ' ETH';
          html += '</div></div>';
        });
        if (sorted.length > showCount) {
          html += '<button class="toggle-raw" data-pos-key="' + esc(botKey) + '" data-expanded="' + positionsOpen + '" onclick="var h=this.parentElement.querySelectorAll(&quot;.pos-hidden&quot;);var opening=this.dataset.expanded!==&quot;true&quot;;h.forEach(function(e){e.style.display=opening?&quot;block&quot;:&quot;none&quot;});this.dataset.expanded=String(opening);this.textContent=opening?&quot;Show less&quot;:&quot;Show all (' + sorted.length + ')&quot;">' + (positionsOpen ? 'Show less' : 'Show all (' + sorted.length + ')') + '</button>';
        }
        html += '</div>';
      }

      html += '<div class="timestamp" data-received-at="' + esc(d.received_at || '') + '" title="' + esc(d.received_at || '') + '">Updated ' + esc(age.text) + '</div>';
      html += '<button class="toggle-raw" data-raw-key="' + esc(botKey) + '" data-expanded="' + rawOpen + '" onclick="var opening=this.dataset.expanded!==&quot;true&quot;;this.dataset.expanded=String(opening);this.nextElementSibling.style.display=opening?&quot;block&quot;:&quot;none&quot;">Raw JSON</button>';
      html += '<pre class="raw" data-raw-scroll-key="' + esc(botKey) + '" style="display:' + (rawOpen ? 'block' : 'none') + '">' + esc(JSON.stringify(d, null, 2)) + '</pre>';
      html += '</div>';
    });
    html += '</div>';
    container.innerHTML = html;
    container.querySelectorAll('pre[data-raw-scroll-key]').forEach(function(el) {
      el.scrollTop = rawJsonScroll.get(el.dataset.rawScrollKey) || 0;
    });
    container.querySelectorAll('details.chart-panel').forEach(function(panel) {
      const loadChart = function() {
        if (!panel.open) return;
        const frame = panel.querySelector('iframe.dex-chart');
        if (!frame || frame.src) return;
        fetch(frame.dataset.resolver)
          .then(function(response) {
            if (!response.ok) throw new Error('Chart lookup failed: ' + response.status);
            return response.json();
          })
          .then(function(data) { frame.src = data.chart_url; })
          .catch(function(error) {
            frame.replaceWith(document.createTextNode(error.message));
          });
      };
      panel.addEventListener('toggle', loadChart);
      loadChart();
    });
  }

  connect();
  botFilter.addEventListener('input', function() {
    clearFilter.style.display = botFilter.value ? 'block' : 'none';
    render(true);
  });
  clearFilter.addEventListener('click', function() {
    botFilter.value = '';
    clearFilter.style.display = 'none';
    botFilter.focus();
    render(true);
  });
  chainFilter.addEventListener('input', function() { render(true); });
  function updateSortDirectionButton() {
    sortDirection.textContent = sortDirectionValue === 'asc' ? 'Ascending ↑' : 'Descending ↓';
  }
  sortBots.addEventListener('change', function() {
    sortDirectionValue = defaultSortDirections[sortBots.value] || 'asc';
    updateSortDirectionButton();
    render(true);
  });
  sortDirection.addEventListener('click', function() {
    sortDirectionValue = sortDirectionValue === 'asc' ? 'desc' : 'asc';
    updateSortDirectionButton();
    render(true);
  });
  updateSortDirectionButton();
  notificationsButton.addEventListener('click', function() {
    if (!window.isSecureContext) { notificationsButton.textContent = 'HTTPS required for system alerts'; return; }
    if (!('Notification' in window)) { notificationsButton.textContent = 'Alerts unsupported'; return; }
    Notification.requestPermission().then(function(permission) { notificationsButton.textContent = permission === 'granted' ? 'Offline alerts enabled' : 'Offline alerts blocked'; });
  });
  setInterval(refreshReportAges, 1000);
})();
</script>

</body>
</html>
"""

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not API_KEY:
        logger.warning("⚠️  API_KEY environment variable is not set!")
        logger.warning("   Set it before starting: export API_KEY=your-secret-key")
    logger.info("Starting Grid Bot Dashboard on %s:%d", HOST, PORT)
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
