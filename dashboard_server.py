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

import atexit
import json
import logging
import os
import queue
import re
import threading
import time
import uuid
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
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
DEXSCREENER_CACHE_TTL = 60
ETH_PRICE_TIMEOUT = 8
ETH_PRICE_CACHE_TTL = 60
STATE_FILE = os.environ.get("STATE_FILE", "data/dashboard_state.json")
STATE_FLUSH_INTERVAL = float(os.environ.get("STATE_FLUSH_INTERVAL", "15"))
CHAIN_SLUGS = {4663: "robinhood", 8453: "base", 1: "ethereum"}
MAX_STATUS_REQUEST_BYTES = 128 * 1024

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

# The dashboard is a public display, not a general-purpose data sink. Keep the
# accepted schema deliberately narrow so a bot-side regression cannot persist
# arbitrary config or secret material merely because it has the ingest key.
_STATUS_FIELDS = frozenset({
    "dashboard_schema_version", "bot_id", "timestamp", "uptime_seconds",
    "price", "eth_balance", "usdg_balance", "treasury_sent_usdg", "token_balance",
    "positions", "profit_percent", "session_profit_eth", "realized_profit_eth",
    "realized_sales", "profit_tracking_started_at", "buys", "sells",
    "filled_positions", "max_positions", "capacity_warning", "sell_attempt",
    "chain_id", "swap_provider", "token_symbol", "token_address", "wallet_address",
    "display_name", "group", "poll_interval_seconds", "trades_history", "events", "rpc_status", "sigil",
})
_POSITION_FIELDS = frozenset({"id", "buy_amount_token", "cost_basis", "pnl", "timestamp"})
_TRADE_FIELDS = frozenset({"timestamp", "side", "eth_amount", "token_amount", "price", "tx_hash", "profit_eth"})
_EVENT_FIELDS = frozenset({
    "timestamp", "level", "code", "message", "count", "tx_hash",
    "source_amount", "source_asset", "usdg_amount",
})
_CAPACITY_WARNING_FIELDS = frozenset({"highest_position_pnl", "buy_threshold", "max_positions"})
_SELL_ATTEMPT_FIELDS = frozenset({"status", "position_id", "pnl_percent", "quoted_profit_eth", "minimum_profit_eth"})
_SIGIL_FIELDS = frozenset({"version", "method", "key", "seed"})
_MAX_POSITIONS = 100
_MAX_TRADES = 50
_MAX_EVENTS = 50

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
# Normal fleet reports are far smaller than this. A hard ceiling prevents a
# keyed (or accidental) client from making the public dashboard parse/store an
# unbounded JSON document.
app.config["MAX_CONTENT_LENGTH"] = MAX_STATUS_REQUEST_BYTES
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

# (chain_id, token_address) -> (cached_at_monotonic, selected pair market data)
_dexscreener_pair_cache: dict[tuple[int, str], tuple[float, dict]] = {}
_dexscreener_lock = threading.Lock()

# Cached ETH fiat prices: (cached_at_monotonic, {"usd": float, "cad": float})
_eth_price_cache: tuple[float, dict[str, float]] = (0.0, {})
_eth_price_lock = threading.Lock()

# Status updates arrive continuously, so rewriting the complete bounded history
# for every request is needlessly expensive.  A single background writer
# coalesces updates while shutdown flushing keeps normal restarts durable.
_state_dirty_generation = 0
_state_persisted_generation = 0
_state_shutdown = threading.Event()
_state_flush_lock = threading.Lock()


def _sanitize_provider_events(data):
    """Remove opaque provider response metadata from public event payloads."""
    if not isinstance(data, dict) or not isinstance(data.get("events"), list):
        return data
    for event in data["events"]:
        if not isinstance(event, dict):
            continue
        message = str(event.get("message", ""))
        lowered = message.lower()
        if "requestid" in lowered or "request_id" in lowered or message.lstrip().startswith(("Response: {", "Response: [")):
            event["message"] = "Uniswap: no quote available" if "no quotes available" in lowered else "Provider request failed"
        event.pop("requestId", None)
        event.pop("request_id", None)
    return data


def _state_snapshot_locked():
    """Return a stable persistence snapshot. Caller holds _lock."""
    return {
        "bot_states": dict(bot_states),
        "bot_history": {bot_id: list(entries) for bot_id, entries in bot_history.items()},
    }


def _persist_state(snapshot):
    """Atomically persist a previously captured state snapshot."""
    directory = os.path.dirname(STATE_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)
    temp_file = STATE_FILE + ".tmp"
    with open(temp_file, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle)
    os.replace(temp_file, STATE_FILE)


def _mark_state_dirty_locked():
    """Record an in-memory mutation. Caller holds _lock."""
    global _state_dirty_generation
    _state_dirty_generation += 1


def _flush_state_if_dirty():
    """Persist one coherent snapshot without holding the request-path lock."""
    global _state_persisted_generation
    with _state_flush_lock:
        with _lock:
            generation = _state_dirty_generation
            if generation <= _state_persisted_generation:
                return False
            snapshot = _state_snapshot_locked()
        try:
            _persist_state(snapshot)
        except OSError as exc:
            logger.warning("Could not persist dashboard state: %s", exc)
            return False
        _state_persisted_generation = generation
        return True


def _state_writer():
    while not _state_shutdown.wait(STATE_FLUSH_INTERVAL):
        _flush_state_if_dirty()


def _shutdown_state_writer():
    _state_shutdown.set()
    _flush_state_if_dirty()


def _load_persisted_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        states = payload.get("bot_states", {})
        histories = payload.get("bot_history", {})
        if isinstance(states, dict):
            bot_states.update({bot_id: _sanitize_provider_events(state) for bot_id, state in states.items()})
        if isinstance(histories, dict):
            for bot_id, entries in histories.items():
                for entry in entries:
                    if isinstance(entry, dict):
                        _sanitize_provider_events(entry.get("data"))
                bot_history[bot_id].extend(entries[-MAX_HISTORY_PER_BOT:])
        logger.info("Restored %d bots from %s", len(bot_states), STATE_FILE)
    except FileNotFoundError:
        pass
    except (OSError, ValueError, TypeError) as exc:
        logger.warning("Could not restore dashboard state: %s", exc)


_load_persisted_state()
threading.Thread(target=_state_writer, name="dashboard-state-writer", daemon=True).start()
atexit.register(_shutdown_state_writer)

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


def _allowlisted_mapping(value, allowed_fields):
    """Copy only known fields from one public nested payload object."""
    if not isinstance(value, dict):
        return None
    return {key: value[key] for key in allowed_fields if key in value}


def _allowlisted_status_payload(data):
    """Drop unrecognized top-level and nested fields before persistence/publication."""
    filtered = {key: data[key] for key in _STATUS_FIELDS if key in data}

    for field, allowed, maximum in (
        ("positions", _POSITION_FIELDS, _MAX_POSITIONS),
        ("trades_history", _TRADE_FIELDS, _MAX_TRADES),
        ("events", _EVENT_FIELDS, _MAX_EVENTS),
    ):
        value = filtered.get(field)
        if not isinstance(value, list):
            continue
        filtered[field] = [item for item in (
            _allowlisted_mapping(entry, allowed) for entry in value[:maximum]
        ) if item is not None]

    for field, allowed in (
        ("capacity_warning", _CAPACITY_WARNING_FIELDS),
        ("sell_attempt", _SELL_ATTEMPT_FIELDS),
        ("sigil", _SIGIL_FIELDS),
    ):
        if field in filtered and filtered[field] is not None:
            filtered[field] = _allowlisted_mapping(filtered[field], allowed)

    return filtered


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

    data = _allowlisted_status_payload(data)
    _sanitize_provider_events(data)

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
        _mark_state_dirty_locked()

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
        _mark_state_dirty_locked()
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


@app.route("/api/eth-price", methods=["GET"])
def eth_price():
    """Return cached ETH prices in USD and CAD from CoinGecko."""
    global _eth_price_cache
    now = time.monotonic()
    with _eth_price_lock:
        cached_at, cached_prices = _eth_price_cache
        if cached_prices and now - cached_at < ETH_PRICE_CACHE_TTL:
            return jsonify({**cached_prices, "cached": True}), 200

        try:
            response = http_requests.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": "ethereum", "vs_currencies": "usd,cad"},
                headers={"Accept": "application/json"},
                timeout=ETH_PRICE_TIMEOUT,
            )
            response.raise_for_status()
            ethereum = response.json().get("ethereum", {})
            prices = {"usd": float(ethereum["usd"]), "cad": float(ethereum["cad"])}
            _eth_price_cache = (now, prices)
            return jsonify({**prices, "cached": False}), 200
        except (http_requests.RequestException, KeyError, TypeError, ValueError) as exc:
            if cached_prices:
                logger.warning("ETH price refresh failed; serving stale cache: %s", exc)
                return jsonify({**cached_prices, "cached": True, "stale": True}), 200
            logger.warning("ETH price lookup failed: %s", exc)
            return jsonify({"error": "ETH price temporarily unavailable"}), 503


def _dexscreener_pair_data(chain_id, token_address):
    """Return cached preferred-pair data, preserving stale data on API failure."""
    chain_slug = CHAIN_SLUGS.get(chain_id)
    if not chain_slug:
        raise ValueError(f"Unsupported chain_id: {chain_id}")
    cache_key = (chain_id, token_address.lower())
    now = time.monotonic()
    with _dexscreener_lock:
        cached = _dexscreener_pair_cache.get(cache_key)
    if cached and now - cached[0] < DEXSCREENER_CACHE_TTL:
        return {**cached[1], "cached": True, "stale": False}

    api_url = f"https://api.dexscreener.com/token-pairs/v1/{chain_slug}/{token_address}"
    try:
        response = http_requests.get(api_url, timeout=DEXSCREENER_TIMEOUT)
        response.raise_for_status()
        pairs = response.json()
        if not isinstance(pairs, list) or not pairs:
            raise LookupError("No Dexscreener pair found")

        def pair_rank(pair):
            quote_symbol = str(pair.get("quoteToken", {}).get("symbol", "")).upper()
            base_symbol = str(pair.get("baseToken", {}).get("symbol", "")).upper()
            is_weth = quote_symbol == "WETH" or base_symbol == "WETH"
            liquidity = float(pair.get("liquidity", {}).get("usd") or 0)
            return (is_weth, liquidity)

        selected = max(pairs, key=pair_rank)
        pair_address = str(selected.get("pairAddress", ""))
        if not pair_address:
            raise LookupError("Dexscreener pair response missing address")
        market_cap = selected.get("marketCap")
        fdv = selected.get("fdv")
        value = market_cap if market_cap is not None else fdv
        label = "Market Cap" if market_cap is not None else ("FDV" if fdv is not None else "Market Cap")
        try:
            value = float(value) if value is not None else None
        except (TypeError, ValueError):
            value = None
        raw_changes = selected.get("priceChange") if isinstance(selected.get("priceChange"), dict) else {}
        price_change = {}
        for window in ("m5", "h1", "h6", "h24"):
            try:
                price_change[window] = float(raw_changes[window])
            except (KeyError, TypeError, ValueError):
                price_change[window] = None
        data = {"pair_address": pair_address, "label": label, "value_usd": value,
                "price_change": price_change}
        with _dexscreener_lock:
            _dexscreener_pair_cache[cache_key] = (now, data)
        return {**data, "cached": False, "stale": False}
    except (http_requests.RequestException, ValueError, LookupError) as exc:
        if cached:
            logger.warning("Dexscreener refresh failed; serving stale pair data: %s", exc)
            return {**cached[1], "cached": True, "stale": True}
        raise


@app.route("/api/dexscreener/market-data", methods=["GET"])
def dexscreener_market_data():
    """Return batched cached market cap/FDV data for currently reported bots."""
    with _lock:
        reported = {
            bot_id: (state.get("chain_id"), str(state.get("token_address", "")).strip())
            for bot_id, state in bot_states.items()
        }
    token_bots = defaultdict(list)
    for bot_id, (chain_id, token_address) in reported.items():
        try:
            chain_id = int(chain_id)
        except (TypeError, ValueError):
            continue
        if chain_id in CHAIN_SLUGS and re.fullmatch(r"0x[0-9a-fA-F]{40}", token_address):
            token_bots[(chain_id, token_address.lower())].append(bot_id)

    results = {}
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(token_bots)))) as executor:
        futures = {
            executor.submit(_dexscreener_pair_data, chain, token): (chain, token)
            for chain, token in token_bots
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                data = future.result()
            except Exception as exc:
                logger.warning("Dexscreener market-data lookup failed for %s: %s", key, exc)
                continue
            for bot_id in token_bots[key]:
                results[bot_id] = data
    return jsonify({"bots": results}), 200


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
    if chain_id not in CHAIN_SLUGS:
        return jsonify({"error": f"Unsupported chain_id: {chain_id}"}), 400

    try:
        pair_data = _dexscreener_pair_data(chain_id, token_address)
    except LookupError:
        return jsonify({"error": "No Dexscreener pair found"}), 404
    except (http_requests.RequestException, ValueError) as exc:
        logger.warning("Dexscreener pair lookup failed: %s", exc)
        return jsonify({"error": "Dexscreener pair lookup failed"}), 502
    pair_address = pair_data["pair_address"]
    chain_slug = CHAIN_SLUGS[chain_id]

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


@app.errorhandler(413)
def request_too_large(e):
    return jsonify({"error": f"Request body exceeds {MAX_STATUS_REQUEST_BYTES} byte limit"}), 413


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
  .summary-item.needs-positions { background: #78350f; border-color: #f59e0b; color: #fef3c7; font-weight: 700; animation: capacity-pulse 1.5s ease-in-out infinite; }
  .summary-item.needs-positions .bot-names { color: #fbbf24; }
  @keyframes capacity-pulse { 0%, 100% { box-shadow: 0 0 0 0 rgba(245, 158, 11, 0.25); } 50% { box-shadow: 0 0 0 4px rgba(245, 158, 11, 0.12); } }
  .toolbar select, .toolbar input, .toolbar button { background: #1e293b; color: #e2e8f0; border: 1px solid #334155; border-radius: 0.35rem; padding: 0.45rem 0.6rem; }
  .filter-wrap { position: relative; display: inline-flex; }
  .filter-wrap input { padding-right: 2rem; width: 100%; }
  .clear-filter { position: absolute; right: 0.25rem; top: 50%; transform: translateY(-50%); border: 0 !important; background: transparent !important; padding: 0.25rem 0.45rem !important; color: #94a3b8 !important; font-size: 1rem; line-height: 1; display: none; }
  .chain-badge, .provider-badge, .group-badge { display: inline-block; color: #cbd5e1; background: #334155; border-radius: 9999px; padding: 0.1rem 0.4rem; font-size: 0.65rem; margin-left: 0.3rem; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(350px, 1fr)); gap: 1rem; }
  .card { background: #1e293b; border: 1px solid #334155; border-radius: 0.5rem; padding: 1.25rem; }
  .card.capacity-warning { border-color: #f59e0b; box-shadow: 0 0 0 1px rgba(245, 158, 11, 0.25); }
  .capacity-alert { background: #78350f; border: 1px solid #f59e0b; color: #fef3c7; border-radius: 0.35rem; padding: 0.6rem 0.7rem; margin-bottom: 0.75rem; font-size: 0.78rem; }
  .capacity-alert strong { color: #fbbf24; display: block; margin-bottom: 0.15rem; }
  .sell-attempt { display: flex; align-items: center; gap: 0.65rem; background: linear-gradient(90deg, rgba(14, 116, 144, 0.22), rgba(15, 23, 42, 0.3)); border: 1px solid #0e7490; color: #cffafe; border-radius: 0.35rem; padding: 0.6rem 0.7rem; margin-bottom: 0.75rem; font-size: 0.78rem; }
  .sell-attempt-dot { width: 0.55rem; height: 0.55rem; flex: 0 0 auto; border-radius: 50%; background: #22d3ee; box-shadow: 0 0 0 0 rgba(34, 211, 238, 0.4); animation: sell-pulse 1.7s ease-out infinite; }
  .sell-attempt-copy { min-width: 0; flex: 1; }
  .sell-attempt-copy strong { display: block; color: #67e8f9; font-size: 0.7rem; letter-spacing: 0.08em; margin-bottom: 0.1rem; }
  .sell-attempt-detail { color: #94a3b8; font-size: 0.7rem; white-space: nowrap; }
  @keyframes sell-pulse { 70%, 100% { box-shadow: 0 0 0 6px rgba(34, 211, 238, 0); } }
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
  details.market-movement { border-bottom: 1px solid #1e293b; }
  details.market-movement summary { display: flex; justify-content: space-between; padding: 0.35rem 0; cursor: pointer; list-style: none; font-size: 0.875rem; }
  details.market-movement summary::-webkit-details-marker { display: none; }
  details.market-movement summary .label { color: #94a3b8; }
  details.market-movement summary .label::after { content: ' ▸'; color: #64748b; font-size: 0.7rem; }
  details.market-movement[open] summary .label::after { content: ' ▾'; }
  details.market-movement summary .value { color: #f1f5f9; font-weight: 500; }
  details.market-movement .value.positive { color: #22c55e; }
  details.market-movement .value.negative { color: #ef4444; }
  .movement-breakdown { padding-left: 0.75rem; border-left: 1px solid #334155; margin-bottom: 0.25rem; }
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
  details.sigil-panel { margin-top: 0.75rem; }
  .sigil-stage { display: grid; place-items: center; min-height: 300px; margin-top: 0.5rem; border: 1px solid #334155; border-radius: 0.375rem; background: radial-gradient(circle at center, #172033 0, #0f172a 68%); overflow: hidden; }
  .sigil-stage[role="button"] { cursor: pointer; }
  .sigil-stage[role="button"]:focus-visible { outline: 2px solid #facc15; outline-offset: 2px; }
  .sigil-stage svg { width: min(100%, 360px); height: auto; filter: drop-shadow(0 0 8px rgba(250, 204, 21, 0.24)); }
  .sigil-animation-toggle { display: block; margin: 0.4rem auto 0; }
  .sigil-stage.animation-enabled .sigil-stroke-current { stroke-dasharray: 0.14 0.08; animation: sigil-current var(--sigil-draw-duration) linear calc(var(--sigil-clock-phase) + var(--sigil-seed-phase)) infinite; }
  .sigil-stage.animation-enabled .sigil-glyph { transform-origin: 128px 128px; transform-box: view-box; animation: sigil-turn var(--sigil-spin-duration) linear calc(var(--sigil-clock-phase) + var(--sigil-seed-phase)) infinite; }
  .sigil-stage.animation-enabled .sigil-rings { animation: sigil-breathe var(--sigil-breathe-duration) ease-in-out calc(var(--sigil-clock-phase) + var(--sigil-seed-phase)) infinite alternate; }
  .sigil-stage.animation-enabled .sigil-node { transform-box: fill-box; transform-origin: center; animation: sigil-node-pulse var(--sigil-pulse-duration) ease-in-out calc(var(--sigil-node-index) * 0.16s + var(--sigil-clock-phase) + var(--sigil-seed-phase)) infinite alternate; }
  .sigil-stage.animation-enabled:not(.is-visible) *,
  .sigil-motion-paused .sigil-stage.animation-enabled * { animation-play-state: paused !important; }
  @keyframes sigil-current { from { stroke-dashoffset: 0; } to { stroke-dashoffset: -0.22; } }
  @keyframes sigil-turn { to { transform: rotate(360deg); } }
  @keyframes sigil-breathe { from { transform: scale(0.985); opacity: 0.58; } to { transform: scale(1.015); opacity: 1; } }
  @keyframes sigil-node-pulse { from { opacity: 0.35; transform: scale(0.72); } to { opacity: 1; transform: scale(1.24); } }
  .sigil-meta { color: #64748b; font: 0.68rem monospace; text-align: center; margin: 0.35rem 0 0.55rem; letter-spacing: 0.08em; }
  @media (max-width: 640px), (pointer: coarse) {
    .sigil-stage svg { filter: none; }
  }
  .dex-chart { width: 100%; height: 520px; border: 1px solid #334155; border-radius: 0.375rem; margin-top: 0.5rem; background: #0f172a; }
  .trades { margin-top: 0.75rem; }
  .trade { display: grid; grid-template-columns: 3.5rem 1fr auto; gap: 0.5rem; background: #0f172a; border-radius: 0.25rem; padding: 0.45rem; margin-top: 0.35rem; font-size: 0.75rem; }
  .trade .buy { color: #22c55e; } .trade .sell { color: #ef4444; }
  .trade a { color: #f1f5f9; text-decoration: none; }
  .trade a:hover { text-decoration: underline; }
  .events { margin-top: 0.75rem; }
  .event { background: #0f172a; border-left: 3px solid #f59e0b; border-radius: 0.25rem; padding: 0.5rem 0.6rem; margin-top: 0.4rem; font-size: 0.75rem; }
  .event.error { border-left-color: #ef4444; }
  .event.success { border-left-color: #22c55e; }
  .event-header { display: flex; justify-content: space-between; gap: 0.5rem; color: #94a3b8; margin-bottom: 0.2rem; }
  .event-level { color: #fbbf24; font-weight: 700; text-transform: uppercase; }
  .event.error .event-level { color: #f87171; }
  .event.success .event-level { color: #4ade80; }
  .event-message { color: #e2e8f0; overflow-wrap: anywhere; word-break: break-word; }
  .event-code { color: #64748b; margin-top: 0.2rem; font-family: monospace; overflow-wrap: anywhere; }
  .event-tx { color: #f8fafc; text-decoration: none; }
  .event-tx:hover { text-decoration: underline; }
  .event-details { margin-top: 0.3rem; color: #64748b; }
  .event-details summary { cursor: pointer; user-select: none; }
  .event-raw { margin-top: 0.3rem; white-space: pre-wrap; overflow-wrap: anywhere; word-break: break-word; font-family: monospace; color: #94a3b8; }
  .currency-toggle { background: none; border: 1px solid #475569; color: #f1f5f9; border-radius: 0.25rem; padding: 0.1rem 0.35rem; cursor: pointer; }
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
    <select id="provider-filter"><option value="">All providers</option><option value="0x">0x</option><option value="lifi">LI.FI</option><option value="uniswap">Uniswap</option><option value="sushiswap">SushiSwap</option><option value="__unreported">Unreported</option></select>
    <select id="sort-bots"><option value="name">Name</option><option value="symbol">Symbol</option><option value="pnl">AVG P&amp;L</option><option value="top-position-pnl">Top position P&amp;L</option><option value="profit" selected>Session profit</option><option value="realized-profit">Realized profit</option><option value="treasury-sent">Treasury sent</option><option value="position-utilization">Position utilization</option><option value="eth-balance">ETH balance</option><option value="usdg-balance">USDG balance</option><option value="status">Status</option></select>
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
  const providerFilter = document.getElementById('provider-filter');
  const sortBots = document.getElementById('sort-bots');
  const sortDirection = document.getElementById('sort-direction');
  const notificationsButton = document.getElementById('notifications');
  const bots = {};
  const marketData = {};
  const openMarketMovements = new Set();
  const openMoreInfo = new Set();
  const openPositions = new Set();
  const openRawJson = new Set();
  const openCharts = new Set();
  // Sigils begin open. Remember only explicit closures so new bot
  // incarnations reveal their new working without requiring a click.
  const closedSigils = new Set();
  const openTrades = new Set();
  const openEvents = new Set();
  const rawJsonScroll = new Map();
  const notifiedOffline = new Set();
  const defaultSortDirections = {
    name: 'asc', pnl: 'desc', 'top-position-pnl': 'desc', profit: 'desc', 'realized-profit': 'desc', 'treasury-sent': 'desc',
    'position-utilization': 'desc', 'eth-balance': 'desc', 'usdg-balance': 'desc', status: 'asc'
  };
  let sortDirectionValue = defaultSortDirections.profit;
  let profitCurrency = localStorage.getItem('dashboard-profit-currency') || 'cad';
  let ethPrices = {};
  const chainMetadata = {
    1: { name: 'Ethereum', explorer: 'https://etherscan.io/address/' },
    8453: { name: 'Base', explorer: 'https://base.blockscout.com/address/' },
    4663: { name: 'Robinhood', explorer: 'https://robinhoodchain.blockscout.com/address/' },
  };

  let evtSource = null;
  let reconnectTimer = null;
  let connectionGeneration = 0;
  let reconnectDelay = 1000;
  const maxReconnectDelay = 30000;
  let viewportBusy = false;
  let viewportMotionAt = 0;
  let viewportIdleTimer = null;
  let renderPendingForViewport = false;
  let renderPendingForce = false;
  let touchInteractionActive = false;
  let marketDataTimer = null;
  const sigilMotionPreference = localStorage.getItem('dashboard-sigil-animation');
  let sigilAnimationEnabled = sigilMotionPreference === 'on' ||
    (sigilMotionPreference === null && !window.matchMedia('(prefers-reduced-motion: reduce)').matches);
  const sigilVisibilityObserver = 'IntersectionObserver' in window ? new IntersectionObserver(function(entries) {
    entries.forEach(function(entry) { entry.target.classList.toggle('is-visible', entry.isIntersecting); });
  }, { rootMargin: '80px 0px', threshold: 0.01 }) : null;

  function setSigilInteractionPaused(paused) {
    container.classList.toggle('sigil-motion-paused', Boolean(paused));
  }

  function markViewportBusy() {
    viewportBusy = true;
    setSigilInteractionPaused(true);
    viewportMotionAt = performance.now();
    if (viewportIdleTimer !== null) return;

    const finishWhenIdle = function() {
      if (touchInteractionActive) {
        viewportIdleTimer = setTimeout(finishWhenIdle, 180);
        return;
      }
      const remaining = 180 - (performance.now() - viewportMotionAt);
      if (remaining > 0) {
        viewportIdleTimer = setTimeout(finishWhenIdle, remaining);
        return;
      }
      viewportIdleTimer = null;
      viewportBusy = false;
      setSigilInteractionPaused(false);
      if (renderPendingForViewport) {
        const force = renderPendingForce;
        renderPendingForViewport = false;
        renderPendingForce = false;
        render(force);
      }
    };
    viewportIdleTimer = setTimeout(finishWhenIdle, 180);
  }

  function connect() {
    connectionGeneration += 1;
    const generation = connectionGeneration;
    if (reconnectTimer !== null) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (evtSource) evtSource.close();
    const source = new EventSource('/api/stream');
    evtSource = source;

    source.onopen = function() {
      if (generation !== connectionGeneration) return;
      dot.className = 'status-dot connected';
      connStatus.textContent = 'Live';
      reconnectDelay = 1000;
    };

    source.onerror = function() {
      if (generation !== connectionGeneration) return;
      dot.className = 'status-dot disconnected';
      connStatus.textContent = 'Reconnecting…';
      source.close();
      reconnectTimer = setTimeout(function() {
        reconnectTimer = null;
        connect();
      }, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 2, maxReconnectDelay);
    };

    source.addEventListener('snapshot', function(e) {
      if (generation !== connectionGeneration) return;
      const data = JSON.parse(e.data);
      if (data.bots) {
        Object.keys(data.bots).forEach(function(botId) {
          bots[botId] = data.bots[botId];
        });
        render();
        scheduleMarketDataFetch();
      }
    });

    source.addEventListener('update', function(e) {
      if (generation !== connectionGeneration) return;
      const entry = JSON.parse(e.data);
      bots[entry.bot_id] = entry.data || entry;
      render();
      scheduleMarketDataFetch();
    });

    source.addEventListener('remove', function(e) {
      if (generation !== connectionGeneration) return;
      const data = JSON.parse(e.data);
      if (data.bot_id && bots[data.bot_id]) {
        delete bots[data.bot_id];
        render();
      }
    });
  }

  function reconnectNow() {
    dot.className = 'status-dot disconnected';
    connStatus.textContent = 'Reconnecting…';
    reconnectDelay = 1000;
    connect();
  }

  function esc(str) {
    const div = document.createElement('div');
    div.textContent = String(str === null || str === undefined ? '' : str);
    return div.innerHTML;
  }

  function sigilSvg(sigil) {
    const seed = sigil && String(sigil.seed || '').toLowerCase();
    const key = sigil && String(sigil.key || '').toUpperCase();
    if (!/^[0-9a-f]{64}$/.test(seed) || !/^[B-DF-HJ-NP-TV-Z]{3,21}$/.test(key)) return '';
    const bytes = [];
    for (let i = 0; i < seed.length; i += 2) bytes.push(parseInt(seed.slice(i, i + 2), 16));
    const points = [];
    for (let i = 0; i < key.length; i++) {
      const letter = key.charCodeAt(i) - 65;
      const angle = ((letter / 26) * Math.PI * 2) - Math.PI / 2 + ((bytes[i] % 13) - 6) * 0.008;
      const radius = 50 + (bytes[(i + 13) % bytes.length] % 43);
      points.push([128 + Math.cos(angle) * radius, 128 + Math.sin(angle) * radius]);
    }
    let path = 'M ' + points.map(function(point) { return point[0].toFixed(1) + ' ' + point[1].toFixed(1); }).join(' L ');
    if (bytes[30] % 2) path += ' Z';
    const symmetry = 2 + (bytes[31] % 3);
    let currentStrokes = '';
    for (let turn = 0; turn < symmetry; turn++) {
      const transformedPath = ' pathLength="1" d="' + path + '" transform="rotate(' + ((360 / symmetry) * turn) + ' 128 128)"/>';
      currentStrokes += '<path class="sigil-stroke-current"' + transformedPath;
    }
    const first = points[0], last = points[points.length - 1];
    const satellites = points.filter(function(_, index) { return index % 3 === bytes[29] % 3; }).map(function(point, index) {
      return '<circle class="sigil-node" style="--sigil-node-index:' + index + '" cx="' + point[0].toFixed(1) + '" cy="' + point[1].toFixed(1) + '" r="2.2"/>';
    }).join('');
    const animationStyle = '--sigil-draw-duration:' + (5 + bytes[26] % 5) + 's;--sigil-spin-duration:' + (42 + bytes[27] % 39) +
      's;--sigil-breathe-duration:' + (4 + bytes[28] % 4) + 's;--sigil-pulse-duration:' + (2 + (bytes[29] % 15) / 10).toFixed(1) +
      's;--sigil-seed-phase:-' + (bytes[30] % 40) / 10 + 's;--sigil-clock-phase:0s';
    return '<svg viewBox="0 0 256 256" role="img" aria-label="Deterministic prosperity sigil" style="' + animationStyle + '">' +
      '<g class="sigil-glyph">' +
      '<g class="sigil-rings" fill="none" stroke="#facc15" stroke-width="2"><circle cx="128" cy="128" r="106" opacity="0.22"/><circle cx="128" cy="128" r="42" opacity="0.16"/></g>' +
      '<g fill="none" stroke="#facc15" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' + currentStrokes + '</g>' +
      '<g fill="#fde68a" stroke="#facc15">' + satellites + '<circle cx="' + first[0].toFixed(1) + '" cy="' + first[1].toFixed(1) + '" r="4" fill="none" stroke-width="2"/>' +
      '<path d="M ' + (last[0] - 4).toFixed(1) + ' ' + last[1].toFixed(1) + ' h 8 M ' + last[0].toFixed(1) + ' ' + (last[1] - 4).toFixed(1) + ' v 8" stroke-width="1.5"/></g>' +
      '<circle cx="128" cy="128" r="3" fill="#fff7cc"/></g></svg>';
  }

  function applySigilAnimationState() {
    container.querySelectorAll('.sigil-stage').forEach(function(stage) {
      stage.classList.toggle('animation-enabled', sigilAnimationEnabled);
      stage.setAttribute('aria-pressed', sigilAnimationEnabled ? 'true' : 'false');
    });
    container.querySelectorAll('.sigil-animation-toggle').forEach(function(button) {
      button.textContent = 'Animation: ' + (sigilAnimationEnabled ? 'On' : 'Off');
      button.setAttribute('aria-pressed', sigilAnimationEnabled ? 'true' : 'false');
    });
  }

  function toggleSigilAnimation() {
    sigilAnimationEnabled = !sigilAnimationEnabled;
    localStorage.setItem('dashboard-sigil-animation', sigilAnimationEnabled ? 'on' : 'off');
    applySigilAnimationState();
  }

  function wireSigilAnimation(panel, stage) {
    const svg = stage.querySelector('svg');
    if (svg) svg.style.setProperty('--sigil-clock-phase', '-' + (Date.now() / 1000).toFixed(3) + 's');
    if (stage.dataset.animationWired !== 'true') {
      stage.dataset.animationWired = 'true';
      stage.addEventListener('click', toggleSigilAnimation);
      stage.addEventListener('keydown', function(event) {
        if (event.key !== 'Enter' && event.key !== ' ') return;
        event.preventDefault();
        toggleSigilAnimation();
      });
    }
    const button = panel.querySelector('.sigil-animation-toggle');
    if (button && button.dataset.animationWired !== 'true') {
      button.dataset.animationWired = 'true';
      button.addEventListener('click', toggleSigilAnimation);
    }
    if (sigilVisibilityObserver) sigilVisibilityObserver.observe(stage);
    else stage.classList.add('is-visible');
    applySigilAnimationState();
  }

  function eventDisplay(message) {
    const raw = String(message || 'Unknown event');
    const normalized = raw.trim().toLowerCase();
    const providerResponse = normalized.startsWith('response: {') || normalized.startsWith('response: [');
    if ((/uniswap/i.test(raw) || /resourceNotFound/i.test(raw) || providerResponse) && /no quotes? available/i.test(raw)) {
      return { summary: 'Uniswap: no quote available' };
    }
    if (/request[_-]?id/i.test(raw) || providerResponse) {
      return { summary: 'Provider request failed' };
    }
    return { summary: raw };
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
    const needsPositions = Object.keys(bots).filter(function(id) {
      const state = bots[id];
      return Boolean(state.capacity_warning) && reportAge(state.received_at).status === 'running';
    });
    const needsPositionNames = needsPositions.map(function(id) { return bots[id].display_name || id; });
    const active = states.filter(function(d) { return reportAge(d.received_at).status === 'running'; }).length;
    const offline = states.filter(function(d) { return reportAge(d.received_at).status === 'offline'; }).length;
    const profit = states.reduce(function(total, d) { return total + (parseFloat(d.session_profit_eth) || 0); }, 0);
    const realizedProfit = states.reduce(function(total, d) { return total + (parseFloat(d.realized_profit_eth) || 0); }, 0);
    const usdgBalance = states.reduce(function(total, d) { return total + (parseFloat(d.usdg_balance) || 0); }, 0);
    const treasurySentUsdg = states.reduce(function(total, d) { return total + (parseFloat(d.treasury_sent_usdg) || 0); }, 0);
    const filled = states.reduce(function(total, d) { return total + (parseInt(d.filled_positions, 10) || 0); }, 0);
    const longestUptime = states.reduce(function(longest, d) { return Math.max(longest, Number(d.uptime_seconds) || 0); }, 0);
    const uptimeDays = Math.floor(longestUptime / 86400);
    const uptimeHours = Math.floor((longestUptime % 86400) / 3600);
    const uptimeMinutes = Math.floor((longestUptime % 3600) / 60);
    const uptimeText = uptimeDays > 0 ? uptimeDays + 'd ' + uptimeHours + 'h' : (uptimeHours > 0 ? uptimeHours + 'h ' + uptimeMinutes + 'm' : uptimeMinutes + 'm');
    const fiatRate = ethPrices[profitCurrency];
    const fiatCode = profitCurrency.toUpperCase();
    const fiatProfit = Number.isFinite(fiatRate) ? profit * fiatRate : null;
    const fiatRealizedProfit = Number.isFinite(fiatRate) ? realizedProfit * fiatRate : null;
    summaryBar.innerHTML = '<span class="summary-item">Active: ' + active + ' / ' + states.length + '</span>' +
      '<span class="summary-item">Offline: ' + offline + '</span>' +
      (needsPositions.length
        ? '<span class="summary-item needs-positions" aria-live="polite">⚑ Needs new positions: ' + needsPositions.length +
          ' <span class="bot-names">(' + needsPositionNames.map(esc).join(', ') + ')</span></span>'
        : '<span class="summary-item">Needs new positions: 0</span>') +
      '<span class="summary-item">Session profit: ' + (profit >= 0 ? '+' : '') + profit.toFixed(8) + ' ETH' +
      (fiatProfit === null ? '' : ' / ' + (fiatProfit >= 0 ? '+' : '') + new Intl.NumberFormat(undefined, { style: 'currency', currency: fiatCode }).format(fiatProfit)) +
      ' <button class="currency-toggle" type="button" data-currency-toggle>' + fiatCode + '</button></span>' +
      '<span class="summary-item">Realized profit: ' + (realizedProfit >= 0 ? '+' : '') + realizedProfit.toFixed(8) + ' ETH' +
      (fiatRealizedProfit === null ? '' : ' / ' + (fiatRealizedProfit >= 0 ? '+' : '') + new Intl.NumberFormat(undefined, { style: 'currency', currency: fiatCode }).format(fiatRealizedProfit)) + '</span>' +
      '<span class="summary-item">USDG: ' + usdgBalance.toFixed(2) + '</span>' +
      '<span class="summary-item">Treasury sent: ' + treasurySentUsdg.toFixed(2) + ' USDG</span>' +
      '<span class="summary-item">Filled positions: ' + filled + '</span>' +
      '<span class="summary-item">Longest uptime: ' + uptimeText + '</span>';
  }

  function fetchEthPrices() {
    fetch('/api/eth-price')
      .then(function(response) { if (!response.ok) throw new Error(response.status); return response.json(); })
      .then(function(data) {
        ethPrices = { usd: Number(data.usd), cad: Number(data.cad) };
        render(true);
      })
      .catch(function() {});
  }

  function shortenAddress(address) {
    const value = String(address || '');
    return value.length > 9 ? value.slice(0, 5) + '…' + value.slice(-3) : value;
  }

  function formatCompactUsd(value) {
    const amount = Number(value);
    if (!Number.isFinite(amount)) return '—';
    const absolute = Math.abs(amount);
    const units = [[1e12, 'T'], [1e9, 'B'], [1e6, 'M'], [1e3, 'K']];
    for (const unit of units) {
      if (absolute >= unit[0]) {
        const scaled = amount / unit[0];
        return '$' + scaled.toFixed(scaled >= 100 ? 0 : (scaled >= 10 ? 1 : 2)).replace(/\\.0+$|(\\.[0-9])0$/, '$1') + unit[1];
      }
    }
    return '$' + amount.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }

  function formatMovement(value) {
    const movement = Number(value);
    if (!Number.isFinite(movement)) return '—';
    return (movement >= 0 ? '+' : '') + movement.toFixed(2) + '%';
  }

  function setMovementValue(element, value) {
    if (!element) return;
    const movement = Number(value);
    element.textContent = formatMovement(value);
    element.classList.toggle('positive', Number.isFinite(movement) && movement >= 0);
    element.classList.toggle('negative', Number.isFinite(movement) && movement < 0);
  }

  function updateMarketDataNodes() {
    container.querySelectorAll('[data-market-key]').forEach(function(row) {
      const botId = decodeURIComponent(row.dataset.marketKey || '');
      const data = marketData[botId];
      if (!data || !Number.isFinite(Number(data.value_usd))) return;
      row.querySelector('.label').textContent = data.label === 'FDV' ? 'FDV' : 'Market Cap';
      row.querySelector('.value').textContent = formatCompactUsd(data.value_usd);
      row.classList.toggle('stale', Boolean(data.stale));
    });
    container.querySelectorAll('details.market-movement[data-market-movement-key]').forEach(function(panel) {
      const botId = decodeURIComponent(panel.dataset.marketMovementKey || '');
      const changes = (marketData[botId] || {}).price_change || {};
      setMovementValue(panel.querySelector('[data-market-window="h24"]'), changes.h24);
      setMovementValue(panel.querySelector('[data-market-window="m5"]'), changes.m5);
      setMovementValue(panel.querySelector('[data-market-window="h1"]'), changes.h1);
      setMovementValue(panel.querySelector('[data-market-window="h6"]'), changes.h6);
    });
  }

  function fetchMarketData() {
    marketDataTimer = null;
    if (!Object.keys(bots).length) return;
    fetch('/api/dexscreener/market-data')
      .then(function(response) { if (!response.ok) throw new Error(response.status); return response.json(); })
      .then(function(data) {
        Object.keys(data.bots || {}).forEach(function(botId) { marketData[botId] = data.bots[botId]; });
        // Mutate only these value nodes. Do not rebuild cards during scrolling or zooming.
        updateMarketDataNodes();
      })
      .catch(function() {});
  }

  function scheduleMarketDataFetch() {
    if (marketDataTimer !== null) return;
    marketDataTimer = setTimeout(fetchMarketData, 250);
  }

  function topPositionPnl(state) {
    const values = (state.positions || []).map(function(position) {
      return parseFloat(position.pnl);
    }).filter(Number.isFinite);
    return values.length ? Math.max.apply(null, values) : null;
  }

  function render(force) {
    // Replacing the full fleet DOM during a touch-scroll causes visible frame
    // drops. Status objects continue updating; coalesce their visual refresh
    // until the viewport has been still for a brief moment.
    if (viewportBusy) {
      renderPendingForViewport = true;
      renderPendingForce = renderPendingForce || Boolean(force);
      return;
    }
    renderPendingForViewport = false;
    renderPendingForce = false;

    // Preserve expansion state before live updates rebuild the cards.
    container.querySelectorAll('details.more-info[data-bot-key]').forEach(function(el) {
      if (el.open) openMoreInfo.add(el.dataset.botKey);
      else openMoreInfo.delete(el.dataset.botKey);
    });
    container.querySelectorAll('details.market-movement[data-market-movement-key]').forEach(function(el) {
      if (el.open) openMarketMovements.add(el.dataset.marketMovementKey);
      else openMarketMovements.delete(el.dataset.marketMovementKey);
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
    container.querySelectorAll('details.sigil-panel[data-sigil-key]').forEach(function(el) {
      if (el.open) closedSigils.delete(el.dataset.sigilKey);
      else closedSigils.add(el.dataset.sigilKey);
    });
    container.querySelectorAll('details.trades[data-trades-key]').forEach(function(el) {
      if (el.open) openTrades.add(el.dataset.tradesKey);
      else openTrades.delete(el.dataset.tradesKey);
    });
    container.querySelectorAll('details.events[data-events-key]').forEach(function(el) {
      if (el.open) openEvents.add(el.dataset.eventsKey);
      else openEvents.delete(el.dataset.eventsKey);
    });
    // Never replace a living chart or sigil during routine status events. In
    // particular, detaching an SVG makes browsers restart its CSS timeline.
    // The latest status remains in memory and renders after the panel closes
    // or the user explicitly changes a view control (which calls force).
    const hasOpenSigil = Boolean(container.querySelector('details.sigil-panel[open]'));
    if ((openCharts.size > 0 || hasOpenSigil) && !force) return;

    const query = botFilter.value.trim().toLowerCase();
    const wantedChain = chainFilter.value;
    const wantedProvider = providerFilter.value;
    const rank = { running: 0, stale: 1, offline: 2, unknown: 3 };
    const botIds = Object.keys(bots).filter(function(id) {
      const d = bots[id];
      const provider = String(d.swap_provider || '').toLowerCase();
      const haystack = [id, d.display_name, d.group, provider].join(' ').toLowerCase();
      const providerMatches = !wantedProvider ||
        (wantedProvider === '__unreported' ? !provider : provider === wantedProvider);
      return (!query || haystack.includes(query)) &&
        (!wantedChain || String(d.chain_id) === wantedChain) && providerMatches;
    }).sort(function(a, b) {
      const av = bots[a], bv = bots[b], mode = sortBots.value;
      let result;
      if (mode === 'symbol') {
        const aSymbol = String(av.token_symbol || a).toLowerCase();
        const bSymbol = String(bv.token_symbol || b).toLowerCase();
        result = aSymbol.localeCompare(bSymbol) || a.localeCompare(b);
      }
      else if (mode === 'name') {
        const aName = String(av.display_name || a).toLowerCase();
        const bName = String(bv.display_name || b).toLowerCase();
        result = aName.localeCompare(bName) || a.localeCompare(b);
      }
      else if (mode === 'pnl') result = (parseFloat(av.profit_percent) || 0) - (parseFloat(bv.profit_percent) || 0);
      else if (mode === 'top-position-pnl') {
        const aTop = topPositionPnl(av), bTop = topPositionPnl(bv);
        if (aTop === null && bTop === null) return a.localeCompare(b);
        if (aTop === null) return 1;
        if (bTop === null) return -1;
        result = aTop - bTop;
      }
      else if (mode === 'profit') result = (parseFloat(av.session_profit_eth) || 0) - (parseFloat(bv.session_profit_eth) || 0);
      else if (mode === 'realized-profit') result = (parseFloat(av.realized_profit_eth) || 0) - (parseFloat(bv.realized_profit_eth) || 0);
      else if (mode === 'treasury-sent') result = (parseFloat(av.treasury_sent_usdg) || 0) - (parseFloat(bv.treasury_sent_usdg) || 0);
      else if (mode === 'position-utilization') {
        const aMax = parseFloat(av.max_positions) || 0, bMax = parseFloat(bv.max_positions) || 0;
        const aUtilization = aMax > 0 ? (parseFloat(av.filled_positions) || 0) / aMax : 0;
        const bUtilization = bMax > 0 ? (parseFloat(bv.filled_positions) || 0) / bMax : 0;
        result = aUtilization - bUtilization;
      }
      else if (mode === 'eth-balance') result = (parseFloat(av.eth_balance) || 0) - (parseFloat(bv.eth_balance) || 0);
      else if (mode === 'usdg-balance') result = (parseFloat(av.usdg_balance) || 0) - (parseFloat(bv.usdg_balance) || 0);
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
      const sigilOpen = !closedSigils.has(botKey);
      html += '<div class="card' + (d.capacity_warning ? ' capacity-warning' : '') + '" data-bot-id="' + esc(botId) + '">';
      html += '<h2>Bot</h2>';
      const chain = chainMetadata[Number(d.chain_id)];
      html += '<div class="bot-id">' + esc(d.display_name || botId) + ' ' + statusBadge(status).replace('<span ', '<span data-inferred="' + (!d.status) + '" ') +
        (chain ? '<span class="chain-badge">' + esc(chain.name) + '</span>' : '') +
        (d.swap_provider ? '<span class="provider-badge">' + esc(String(d.swap_provider).toUpperCase()) + '</span>' : '') +
        (d.group ? '<span class="group-badge">' + esc(d.group) + '</span>' : '') + '</div>';

      if (d.capacity_warning) {
        const warningPnl = parseFloat(d.capacity_warning.highest_position_pnl);
        const warningThreshold = parseFloat(d.capacity_warning.buy_threshold);
        html += '<div class="capacity-alert"><strong>⚠ ADD POSITIONS</strong>' +
          'Buy point reached, but all ' + esc(d.capacity_warning.max_positions) + ' position slots are filled. ' +
          'Highest P&amp;L: ' + esc(Number.isFinite(warningPnl) ? warningPnl.toFixed(1) : '?') +
          '% · Buy point: ' + esc(Number.isFinite(warningThreshold) ? warningThreshold.toFixed(1) : '?') + '%</div>';
      }

      if (d.sell_attempt && d.sell_attempt.status === 'quote_below_minimum') {
        const quoted = parseFloat(d.sell_attempt.quoted_profit_eth);
        const minimum = parseFloat(d.sell_attempt.minimum_profit_eth);
        const detail = Number.isFinite(quoted) && Number.isFinite(minimum)
          ? '<span class="sell-attempt-detail" title="Quoted profit / minimum profit">' + esc(quoted.toFixed(6)) + ' / ' + esc(minimum.toFixed(6)) + ' ETH</span>'
          : '';
        html += '<div class="sell-attempt" role="status" aria-label="Sell attempted; quote is below minimum">' +
          '<span class="sell-attempt-dot" aria-hidden="true"></span>' +
          '<span class="sell-attempt-copy"><strong>SELL CHECK ACTIVE</strong>Waiting for minimum quote</span>' + detail + '</div>';
      }

      d.buys = d.buys ?? 0;
      d.sells = d.sells ?? 0;

      const metrics = [
        ['AVG P&L', 'profit_percent'],
        ['Session Profit', 'session_profit_eth'],
        ['Realized Profit', 'realized_profit_eth'],
        ['Filled / Max Positions', 'position_capacity'],
      ];
      const moreMetrics = [
        ['Price', 'price'],
        ['Buys', 'buys'], ['Sells', 'sells'],
        ['Realized Sells', 'realized_sales'], ['Profit Tracking Since', 'profit_tracking_started_at'],
        ['ETH Balance', 'eth_balance'], ['USDG Balance', 'usdg_balance'], ['Treasury Sent', 'treasury_sent_usdg'], ['Token Balance', 'token_balance'],
        ['Wallet', 'wallet_link'], ['Token', 'token_link'],
        ['RPC', 'rpc_status'], ['Polling', 'poll_interval_seconds'], ['Uptime', 'uptime_seconds'],
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
          } else if (key === 'session_profit_eth' || key === 'realized_profit_eth') {
            cls = parseFloat(val) >= 0 ? 'positive' : 'negative';
            val = (parseFloat(val) >= 0 ? '+' : '') + parseFloat(val).toFixed(8) + ' ETH';
          } else if (key === 'profit_tracking_started_at') {
            const timestamp = Date.parse(val);
            val = Number.isFinite(timestamp) ? new Date(timestamp).toLocaleString() : val;
          } else if (key === 'price') {
            const n = parseFloat(val);
            val = n.toFixed(10);
          } else if (key === 'uptime_seconds') {
            const s = parseInt(val);
            if (s < 60) val = s + 's';
            else if (s < 3600) val = Math.floor(s/60) + 'm ' + (s%60) + 's';
            else val = Math.floor(s/3600) + 'h ' + Math.floor((s%3600)/60) + 'm';
          } else if (key === 'poll_interval_seconds') {
            val = parseFloat(val) + 's';
          } else if (key === 'eth_balance' || key === 'usdg_balance' || key === 'treasury_sent_usdg' || key === 'token_balance') {
            val = parseFloat(val).toFixed(key === 'eth_balance' ? 4 : ((key === 'usdg_balance' || key === 'treasury_sent_usdg') ? 2 : 0));
          }
          const renderedValue = key === 'wallet_link' || key === 'token_link' ? val : esc(val);
          return '<div class="metric"><span class="label">' + esc(label) + '</span><span class="value ' + cls + '">' + renderedValue + '</span></div>';
        }
        return '';
      }

      const market = marketData[botId] || {};
      const marketLabel = market.label === 'FDV' ? 'FDV' : 'Market Cap';
      html += '<div class="metric market-data' + (market.stale ? ' stale' : '') + '" data-market-key="' + esc(botKey) + '"><span class="label">' +
        esc(marketLabel) + '</span><span class="value">' + esc(formatCompactUsd(market.value_usd)) + '</span></div>';
      const changes = market.price_change || {};
      const movementClass = function(value) {
        return Number.isFinite(Number(value)) ? (Number(value) >= 0 ? ' positive' : ' negative') : '';
      };
      html += '<details class="market-movement" data-market-movement-key="' + esc(botKey) + '"' + (openMarketMovements.has(botKey) ? ' open' : '') + '>' +
        '<summary><span class="label">Day</span><span class="value' + movementClass(changes.h24) + '" data-market-window="h24">' + esc(formatMovement(changes.h24)) + '</span></summary>' +
        '<div class="movement-breakdown">' +
        '<div class="metric"><span class="label">5m</span><span class="value' + movementClass(changes.m5) + '" data-market-window="m5">' + esc(formatMovement(changes.m5)) + '</span></div>' +
        '<div class="metric"><span class="label">1h</span><span class="value' + movementClass(changes.h1) + '" data-market-window="h1">' + esc(formatMovement(changes.h1)) + '</span></div>' +
        '<div class="metric"><span class="label">6h</span><span class="value' + movementClass(changes.h6) + '" data-market-window="h6">' + esc(formatMovement(changes.h6)) + '</span></div>' +
        '</div></details>';
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

      if (d.sigil && d.sigil.method === 'spare-wheel-v1') {
        html += '<details class="sigil-panel" data-sigil-key="' + esc(botKey) + '"' + (sigilOpen ? ' open' : '') + '><summary class="toggle-raw">Sigil</summary>';
        html += '<div class="sigil-stage" role="button" tabindex="0" aria-label="Toggle sigil animation" data-sigil-seed="' + esc(d.sigil.seed || '') + '"></div>';
        html += '<button class="toggle-raw sigil-animation-toggle" type="button">Animation: ' + (sigilAnimationEnabled ? 'On' : 'Off') + '</button>';
        html += '<div class="sigil-meta">' + esc(d.sigil.method) + ' · ' + esc(d.sigil.key || '') + '</div></details>';
      }

      if (d.events && d.events.length) {
        const recentEvents = d.events.slice().reverse();
        const errorCount = recentEvents.reduce(function(total, event) {
          return total + (event.level === 'error' ? (parseInt(event.count, 10) || 1) : 0);
        }, 0);
        html += '<details class="events" data-events-key="' + esc(botKey) + '"' + (openEvents.has(botKey) ? ' open' : '') + '><summary class="toggle-raw">Events (' + recentEvents.length + (errorCount ? ' · ' + errorCount + ' errors' : '') + ')</summary>';
        recentEvents.forEach(function(event) {
          const level = event.level === 'error' ? 'error' : (event.level === 'success' ? 'success' : 'warning');
          const repeats = parseInt(event.count, 10) || 1;
          const eventTime = event.timestamp ? new Date(event.timestamp).toLocaleString() : '';
          const display = eventDisplay(event.message);
          const eventTxUrl = chain && event.tx_hash ? chain.explorer.replace('/address/', '/tx/') + event.tx_hash : '';
          const eventTx = eventTxUrl ? ' · <a class="event-tx" href="' + esc(eventTxUrl) + '" target="_blank" rel="noopener noreferrer">tx ' + esc(shortenAddress(event.tx_hash)) + '</a>' : '';
          html += '<div class="event ' + level + '"><div class="event-header"><span class="event-level">' + esc(level) + (repeats > 1 ? ' ×' + repeats : '') + '</span><span>' + esc(eventTime) + '</span></div>' +
            '<div class="event-message">' + esc(display.summary) + '</div>' +
            '<div class="event-code">' + esc(event.code || 'unknown') + eventTx + '</div></div>';
        });
        html += '</details>';
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
    const preservedSigilStages = new Map();
    container.querySelectorAll('details.sigil-panel[data-sigil-key]').forEach(function(panel) {
      const stage = panel.querySelector('.sigil-stage[data-rendered="true"]');
      if (!stage) return;
      stage.remove();
      preservedSigilStages.set(panel.dataset.sigilKey, stage);
    });
    if (sigilVisibilityObserver) sigilVisibilityObserver.disconnect();
    container.innerHTML = html;
    container.querySelectorAll('details.sigil-panel[data-sigil-key]').forEach(function(panel) {
      const preservedStage = preservedSigilStages.get(panel.dataset.sigilKey);
      const placeholder = panel.querySelector('.sigil-stage');
      if (!preservedStage || !placeholder || preservedStage.dataset.sigilSeed !== placeholder.dataset.sigilSeed) return;
      placeholder.replaceWith(preservedStage);
    });
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
    container.querySelectorAll('details.sigil-panel').forEach(function(panel) {
      const drawSigil = function() {
        if (!panel.open) return;
        const stage = panel.querySelector('.sigil-stage');
        if (!stage) return;
        if (stage.dataset.rendered !== 'true') {
          const botId = decodeURIComponent(panel.dataset.sigilKey || '');
          const svg = bots[botId] ? sigilSvg(bots[botId].sigil) : '';
          stage.innerHTML = svg || '<span class="empty">Invalid sigil</span>';
          stage.dataset.rendered = 'true';
        }
        wireSigilAnimation(panel, stage);
      };
      panel.addEventListener('toggle', function() {
        if (panel.open) drawSigil();
        else render(true);
      });
      drawSigil();
    });
  }

  connect();
  window.addEventListener('scroll', markViewportBusy, { passive: true });
  window.addEventListener('touchmove', markViewportBusy, { passive: true });
  window.addEventListener('touchstart', function() {
    touchInteractionActive = true;
    markViewportBusy();
  }, { passive: true });
  const finishTouchInteraction = function(event) {
    if (event.touches && event.touches.length > 0) return;
    touchInteractionActive = false;
    markViewportBusy();
  };
  window.addEventListener('touchend', finishTouchInteraction, { passive: true });
  window.addEventListener('touchcancel', finishTouchInteraction, { passive: true });
  if (window.visualViewport) {
    window.visualViewport.addEventListener('scroll', markViewportBusy, { passive: true });
    window.visualViewport.addEventListener('resize', markViewportBusy, { passive: true });
  }
  fetchEthPrices();
  setInterval(fetchEthPrices, 60000);
  setInterval(fetchMarketData, 60000);
  summaryBar.addEventListener('click', function(event) {
    if (!event.target.closest('[data-currency-toggle]')) return;
    profitCurrency = profitCurrency === 'usd' ? 'cad' : 'usd';
    localStorage.setItem('dashboard-profit-currency', profitCurrency);
    render(true);
  });
  document.addEventListener('visibilitychange', function() {
    if (document.visibilityState === 'visible') {
      setSigilInteractionPaused(viewportBusy);
      reconnectNow();
      fetchEthPrices();
      fetchMarketData();
    } else setSigilInteractionPaused(true);
  });
  window.addEventListener('pageshow', function(event) {
    if (event.persisted) reconnectNow();
  });
  window.addEventListener('online', reconnectNow);
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
  providerFilter.addEventListener('input', function() { render(true); });
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
