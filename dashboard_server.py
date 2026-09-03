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
import gzip
import json
import logging
import os
import queue
import re
import threading
import time
import uuid
import zlib
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from functools import wraps
from urllib.parse import urlencode

import requests as http_requests
from flask import Flask, Response, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
from telegram_alerts import TelegramAlerts
from doom_scout import DoomScout

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
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_ALERT_STATE_FILE = os.environ.get("TELEGRAM_ALERT_STATE_FILE", "data/telegram_alert_state.json")
TELEGRAM_LOW_FUNDS_BUFFER_ETH = float(os.environ.get("TELEGRAM_LOW_FUNDS_BUFFER_ETH", "0.0005"))
TELEGRAM_UNBANKED_USDG_THRESHOLD = float(os.environ.get("TELEGRAM_UNBANKED_USDG_THRESHOLD", "10"))
TELEGRAM_DAILY_DIGEST_TIME = os.environ.get("TELEGRAM_DAILY_DIGEST_TIME", "13:00")
DOOM_SCOUT_STATE_FILE = os.environ.get("DOOM_SCOUT_STATE_FILE", "data/doom_scout.json")
DOOM_SCOUT_INTERVAL_SECONDS = int(os.environ.get("DOOM_SCOUT_INTERVAL_SECONDS", "900"))
UNISWAP_API_KEY = os.environ.get("UNISWAP_API_KEY", "")

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
    "price", "eth_balance", "gas_reserve_eth", "usdg_balance", "treasury_sent_usdg", "token_balance",
    "moonbag_balance", "estimated_moonbag_value_eth",
    "positions", "profit_percent", "session_profit_eth", "realized_profit_eth", "realized_profit_periods",
    "realized_sales", "profit_tracking_started_at", "buys", "sells",
    "filled_positions", "max_positions", "capacity_warning", "needs_gas", "funding_warning", "buy_attempt", "sell_attempt",
    "chain_id", "swap_provider", "taxed_token", "token_transfer_fee_percent",
    "token_tax_detection_source", "token_tax_detection_observations",
    "swap_slippage_percent", "token_symbol", "token_address", "wallet_address",
    "display_name", "group", "buy_point_percent", "sell_point_percent",
    "poll_interval_seconds", "trades_history", "events", "rpc_status", "sigil",
})
_POSITION_FIELDS = frozenset({"id", "buy_amount_token", "cost_basis", "pnl", "timestamp"})
_TRADE_FIELDS = frozenset({
    "timestamp", "side", "eth_amount", "token_amount", "price", "tx_hash",
    "profit_eth", "gas_fee_eth",
})
_EVENT_FIELDS = frozenset({
    "timestamp", "level", "code", "message", "count", "tx_hash",
    "source_amount", "source_asset", "usdg_amount",
})
_CAPACITY_WARNING_FIELDS = frozenset({"highest_position_pnl", "buy_threshold", "max_positions"})
_NEEDS_GAS_FIELDS = frozenset({"balance_eth", "reserve_eth", "shortfall_eth"})
_FUNDING_WARNING_FIELDS = frozenset({"asset", "trade_balance", "minimum_trade_balance", "available_slots", "reason"})
_BUY_ATTEMPT_FIELDS = frozenset({
    "status", "position_id", "quote_provider", "projected_gas_eth",
    "maximum_gas_eth", "gas_limit", "gas_price_wei", "buy_amount_eth",
    "available_slots", "phase",
})
_SELL_ATTEMPT_FIELDS = frozenset({
    "status", "position_id", "pnl_percent", "quoted_profit_eth", "minimum_profit_eth",
    "projected_gas_eth", "projected_net_profit_eth",
    "quote_provider", "previous_quote_provider", "quoted_return_eth",
    "previous_quoted_return_eth", "quote_divergence_percent",
    "observed_fee_percent", "matching_observations", "confirmations_required",
    "detected_fee_percent", "tracked_sell_amount_raw", "wallet_balance_raw", "deficit_raw",
})
_REALIZED_PERIOD_FIELDS = frozenset({"month", "week", "24h", "6h", "1h"})
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


def _append_vary(response, value):
    existing = [item.strip() for item in response.headers.get("Vary", "").split(",") if item.strip()]
    if value not in existing:
        existing.append(value)
    response.headers["Vary"] = ", ".join(existing)


def compress_regular_response(response):
    """Compress sizeable non-streaming responses for constrained clients."""
    accepts_gzip = request.accept_encodings["gzip"] > 0
    if (
        not accepts_gzip
        or response.status_code < 200
        or response.status_code in (204, 304)
        or response.is_streamed
        or response.headers.get("Content-Encoding")
    ):
        return response
    payload = response.get_data()
    if len(payload) < 1024:
        return response
    compressed = gzip.compress(payload, compresslevel=6)
    if len(compressed) >= len(payload):
        return response
    response.set_data(compressed)
    response.headers["Content-Encoding"] = "gzip"
    response.headers["Content-Length"] = str(len(compressed))
    _append_vary(response, "Accept-Encoding")
    return response


def _gzip_stream(chunks):
    """Gzip a streaming response while flushing every SSE message promptly."""
    compressor = zlib.compressobj(level=6, method=zlib.DEFLATED, wbits=31)
    try:
        for chunk in chunks:
            raw = chunk.encode("utf-8") if isinstance(chunk, str) else bytes(chunk)
            encoded = compressor.compress(raw) + compressor.flush(zlib.Z_SYNC_FLUSH)
            if encoded:
                yield encoded
    finally:
        tail = compressor.flush(zlib.Z_FINISH)
        if tail:
            yield tail

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------

app = Flask(__name__)
# Normal fleet reports are far smaller than this. A hard ceiling prevents a
# keyed (or accidental) client from making the public dashboard parse/store an
# unbounded JSON document.
app.config["MAX_CONTENT_LENGTH"] = MAX_STATUS_REQUEST_BYTES
CORS(app)
app.after_request(compress_regular_response)

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


def _telegram_state_snapshot():
    with _lock:
        return {bot_id: dict(state) for bot_id, state in bot_states.items()}


doom_scout = DoomScout(
    state_file=DOOM_SCOUT_STATE_FILE,
    interval_seconds=DOOM_SCOUT_INTERVAL_SECONDS,
    uniswap_api_key=UNISWAP_API_KEY,
)
telegram_alerts = TelegramAlerts(
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_ALERT_STATE_FILE,
    _telegram_state_snapshot,
    low_funds_buffer_eth=TELEGRAM_LOW_FUNDS_BUFFER_ETH,
    unbanked_usdg_threshold=TELEGRAM_UNBANKED_USDG_THRESHOLD,
    daily_digest_time=TELEGRAM_DAILY_DIGEST_TIME,
    scout=doom_scout,
)
doom_scout.notify = telegram_alerts.process_scout_transition
doom_scout.start()
atexit.register(doom_scout.close)
atexit.register(telegram_alerts.close)

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
        ("needs_gas", _NEEDS_GAS_FIELDS),
        ("funding_warning", _FUNDING_WARNING_FIELDS),
        ("buy_attempt", _BUY_ATTEMPT_FIELDS),
        ("sell_attempt", _SELL_ATTEMPT_FIELDS),
        ("sigil", _SIGIL_FIELDS),
        ("realized_profit_periods", _REALIZED_PERIOD_FIELDS),
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
        previous_state = bot_states.get(bot_id)
        bot_states[bot_id] = data
        bot_history[bot_id].append(entry)
        _mark_state_dirty_locked()

    _broadcast("update", entry)
    telegram_alerts.start()
    telegram_alerts.process_status(bot_id, previous_state, data)
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
    accepts_gzip = request.accept_encodings["gzip"] > 0
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

    response = Response(
        _gzip_stream(generate()) if accepts_gzip else generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
    if accepts_gzip:
        response.headers["Content-Encoding"] = "gzip"
        _append_vary(response, "Accept-Encoding")
    return response


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


@app.route("/api/scout", methods=["GET"])
def scout_snapshot():
    """Public, read-only scout cards; quote execution remains authenticated."""
    return jsonify(doom_scout.snapshot()), 200


@app.route("/api/scout/<address>/history", methods=["GET"])
def scout_history(address):
    try:
        return jsonify({"address": address, "history": doom_scout.history(address)}), 200
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/scout/assess", methods=["POST"])
@require_api_key
def scout_assess():
    data = request.get_json(silent=True) or {}
    try:
        report = doom_scout.assess(
            data.get("address"), chain_id=data.get("chain_id", 4663),
            budget_eth=data.get("budget_eth", 0.003), positions=data.get("positions", 4),
        )
        return jsonify(report), 200
    except (ValueError, TypeError) as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logger.warning("Scout assessment failed: %s", exc)
        return jsonify({"error": "Scout providers temporarily unavailable"}), 502


@app.route("/api/scout/watch", methods=["POST"])
@require_api_key
def scout_watch():
    data = request.get_json(silent=True) or {}
    try:
        item = doom_scout.watch(
            data.get("address"), data.get("label", ""), data.get("chain_id", 4663),
            data.get("budget_eth", 0.003), data.get("positions", 4),
        )
        return jsonify(item), 201
    except (ValueError, TypeError) as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/scout/watch/<address>", methods=["DELETE"])
@require_api_key
def scout_unwatch(address):
    try:
        return jsonify({"removed": doom_scout.unwatch(address)}), 200
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/scout/<address>", methods=["DELETE"])
@require_api_key
def scout_forget(address):
    try:
        return jsonify({"removed": doom_scout.forget(address)}), 200
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


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
  .connection-wrap { position: relative; }
  .connection-button { appearance: none; border: 0; background: none; color: inherit; font: inherit; cursor: pointer; }
  .connection-diagnostics { position: absolute; right: 0; top: calc(100% + 0.5rem); z-index: 30; min-width: 15rem; padding: 0.65rem 0.75rem; border: 1px solid #475569; border-radius: 0.4rem; background: #0f172a; color: #cbd5e1; box-shadow: 0 1rem 2rem rgba(0,0,0,.4); font-size: 0.72rem; line-height: 1.55; }
  .connection-diagnostics[hidden] { display: none; }
  .container { max-width: 1200px; margin: 2rem auto; padding: 0 1rem; }
  .summary-bar, .toolbar { display: flex; flex-wrap: wrap; gap: 0.6rem; margin-bottom: 1rem; }
  .summary-item { background: #1e293b; border: 1px solid #334155; border-radius: 0.4rem; padding: 0.55rem 0.75rem; font-size: 0.8rem; }
  .summary-detail { display: block; margin-top: 0.2rem; color: #94a3b8; font-size: 0.68rem; white-space: nowrap; }
  button.summary-item { color: inherit; font-family: inherit; text-align: left; cursor: pointer; }
  button.summary-item:hover, button.summary-item:focus-visible { border-color: #64748b; outline: none; }
  button.summary-item.status-summary { text-align: center; min-width: 5.5rem; }
  button.summary-item.status-summary.running { border-color: #166534; }
  button.summary-item.status-summary.stale { border-color: #a16207; }
  button.summary-item.status-summary.offline { border-color: #7f1d1d; }
  .realized-summary { min-width: 15rem; }
  .realized-first-line { display: flex; align-items: center; justify-content: space-between; gap: 0.5rem; }
  .realized-amount { appearance: none; border: 0; padding: 0; background: none; color: inherit; font: inherit; cursor: pointer; }
  .realized-period { appearance: none; -webkit-appearance: none; max-width: 5.5rem; border: 1px solid #475569; border-radius: 0.25rem; background: #0f172a; background-image: none; color: #f1f5f9; font: inherit; font-size: 0.68rem; text-align: center; padding: 0.12rem 0.3rem; }
  .summary-item.needs-positions { background: #78350f; border-color: #f59e0b; color: #fef3c7; font-weight: 700; animation: capacity-pulse 1.5s ease-in-out infinite; }
  .summary-item.needs-positions .bot-names { color: #fbbf24; }
  .summary-item.needs-gas { background: #7f1d1d; border-color: #ef4444; color: #fee2e2; font-weight: 700; animation: capacity-pulse 1.5s ease-in-out infinite; }
  .summary-item.needs-gas .bot-names { color: #fca5a5; }
  .summary-item.needs-funds { background: #4c1d95; border-color: #a78bfa; color: #ede9fe; font-weight: 700; animation: capacity-pulse 1.5s ease-in-out infinite; }
  .summary-item.needs-funds .bot-names { color: #c4b5fd; }
  .summary-item.sell-checks-active { background: #164e63; border-color: #22d3ee; color: #cffafe; font-weight: 700; }
  .summary-item.sell-checks-active .bot-names { color: #67e8f9; }
  .summary-item.buy-gas-blocked { background: #78350f; border-color: #f59e0b; color: #fef3c7; font-weight: 700; }
  .summary-item.buy-gas-blocked .bot-names { color: #fbbf24; }
  .needs-position-link { appearance: none; border: 0; padding: 0; background: none; color: inherit; font: inherit; font-weight: inherit; text-decoration: underline; text-underline-offset: 0.15rem; cursor: pointer; }
  .needs-position-link:hover, .needs-position-link:focus-visible { color: #fef3c7; outline: none; }
  .card:focus { outline: 2px solid #f59e0b; outline-offset: 3px; }
  @keyframes capacity-pulse { 0%, 100% { box-shadow: 0 0 0 0 rgba(245, 158, 11, 0.25); } 50% { box-shadow: 0 0 0 4px rgba(245, 158, 11, 0.12); } }
  .toolbar select, .toolbar input, .toolbar button { background: #1e293b; color: #e2e8f0; border: 1px solid #334155; border-radius: 0.35rem; padding: 0.45rem 0.6rem; }
  .toolbar button[aria-pressed="true"] { color: #fde68a; background: #713f12; border-color: #d97706; }
  .notification-wrap { position: relative; }
  .notification-menu { position: absolute; right: 0; top: calc(100% + 0.4rem); z-index: 20; width: min(19rem, calc(100vw - 2rem)); padding: 0.75rem; border: 1px solid #475569; border-radius: 0.45rem; background: #1e293b; box-shadow: 0 1rem 2.5rem rgba(0, 0, 0, 0.45); }
  .notification-menu[hidden] { display: none; }
  .notification-menu strong { display: block; margin-bottom: 0.45rem; font-size: 0.82rem; }
  .notification-option { display: flex; align-items: center; gap: 0.55rem; padding: 0.3rem 0; color: #cbd5e1; font-size: 0.75rem; }
  .notification-option input { accent-color: #22c55e; }
  .notification-enable { width: 100%; margin-top: 0.55rem; }
  .notification-note { display: block; margin-top: 0.45rem; color: #64748b; font-size: 0.66rem; line-height: 1.35; }
  @media (max-width: 600px) {
    .notification-menu {
      position: fixed;
      left: 1rem;
      right: 1rem;
      top: auto;
      bottom: calc(1rem + env(safe-area-inset-bottom, 0px));
      width: auto;
      max-height: calc(100dvh - 2rem - env(safe-area-inset-bottom, 0px));
      overflow-y: auto;
      overscroll-behavior: contain;
    }
  }
  .filter-wrap { position: relative; display: inline-flex; }
  .filter-wrap input { padding-right: 2rem; width: 100%; }
  .clear-filter { position: absolute; right: 0.25rem; top: 50%; transform: translateY(-50%); border: 0 !important; background: transparent !important; padding: 0.25rem 0.45rem !important; color: #94a3b8 !important; font-size: 1rem; line-height: 1; display: none; }
  .chain-badge, .provider-badge, .group-badge, .tax-badge { display: inline-block; color: #cbd5e1; background: #334155; border-radius: 9999px; padding: 0.1rem 0.4rem; font-size: 0.65rem; margin-left: 0.3rem; }
  .tax-badge { color: #fde68a; background: #713f12; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(350px, 1fr)); gap: 1rem; }
  .card { background: #1e293b; border: 1px solid #334155; border-radius: 0.5rem; padding: 1.25rem; contain: layout paint style; }
  .card.capacity-warning { border-color: #f59e0b; box-shadow: 0 0 0 1px rgba(245, 158, 11, 0.25); }
  .card.needs-gas { border-color: #ef4444; box-shadow: 0 0 0 1px rgba(239, 68, 68, 0.35); }
  .card.needs-funds { border-color: #a78bfa; box-shadow: 0 0 0 1px rgba(167, 139, 250, 0.35); }
  .card.balance-mismatch { border-color: #ef4444; box-shadow: 0 0 0 2px rgba(239, 68, 68, 0.35); }
  .balance-mismatch-alert { background: #7f1d1d; border: 1px solid #ef4444; color: #fee2e2; border-radius: 0.35rem; padding: 0.7rem; margin-bottom: 0.75rem; font-size: 0.78rem; }
  .balance-mismatch-alert strong { color: #fecaca; display: block; margin-bottom: 0.2rem; }
  .capacity-alert { background: #78350f; border: 1px solid #f59e0b; color: #fef3c7; border-radius: 0.35rem; padding: 0.6rem 0.7rem; margin-bottom: 0.75rem; font-size: 0.78rem; }
  .capacity-alert strong { color: #fbbf24; display: block; margin-bottom: 0.15rem; }
  .gas-alert { background: #7f1d1d; border: 1px solid #ef4444; color: #fee2e2; border-radius: 0.35rem; padding: 0.6rem 0.7rem; margin-bottom: 0.75rem; font-size: 0.78rem; }
  .gas-alert strong { color: #fca5a5; display: block; margin-bottom: 0.15rem; }
  .funding-alert { background: #4c1d95; border: 1px solid #a78bfa; color: #ede9fe; border-radius: 0.35rem; padding: 0.6rem 0.7rem; margin-bottom: 0.75rem; font-size: 0.78rem; }
  .funding-alert strong { color: #c4b5fd; display: block; margin-bottom: 0.15rem; }
  .sell-attempt { display: flex; align-items: center; gap: 0.65rem; background: linear-gradient(90deg, rgba(14, 116, 144, 0.22), rgba(15, 23, 42, 0.3)); border: 1px solid #0e7490; color: #cffafe; border-radius: 0.35rem; padding: 0.6rem 0.7rem; margin-bottom: 0.75rem; font-size: 0.78rem; }
  .sell-attempt-dot { width: 0.55rem; height: 0.55rem; flex: 0 0 auto; border-radius: 50%; background: #22d3ee; box-shadow: 0 0 0 0 rgba(34, 211, 238, 0.4); animation: sell-pulse 1.7s ease-out infinite; }
  .sell-attempt-copy { min-width: 0; flex: 1; }
  .sell-attempt-copy strong { display: block; color: #67e8f9; font-size: 0.7rem; letter-spacing: 0.08em; margin-bottom: 0.1rem; }
  .sell-attempt-detail { display: flex; flex: 0 0 auto; flex-direction: column; align-items: flex-end; color: #94a3b8; font-size: 0.7rem; white-space: nowrap; }
  .sell-attempt-provider { color: #64748b; font-size: 0.64rem; line-height: 1.2; text-transform: uppercase; }
  .buy-attempt { display: flex; align-items: center; gap: 0.65rem; background: linear-gradient(90deg, rgba(120, 53, 15, 0.32), rgba(15, 23, 42, 0.3)); border: 1px solid #d97706; color: #fef3c7; border-radius: 0.35rem; padding: 0.6rem 0.7rem; margin-bottom: 0.75rem; font-size: 0.78rem; }
  .buy-attempt strong { color: #fbbf24; display: block; font-size: 0.7rem; letter-spacing: 0.08em; margin-bottom: 0.1rem; }
  @keyframes sell-pulse { 70%, 100% { box-shadow: 0 0 0 6px rgba(34, 211, 238, 0); } }
  .card h2 { font-size: 1rem; color: #94a3b8; margin-bottom: 0.5rem; }
  .card .bot-id { font-size: 1.1rem; font-weight: 700; color: #f1f5f9; margin-bottom: 0.75rem; }
  .metric { display: flex; justify-content: space-between; padding: 0.35rem 0; border-bottom: 1px solid #1e293b; font-size: 0.875rem; }
  .metric:last-child { border-bottom: none; }
  .metric .label { color: #94a3b8; }
  .metric .value { color: #f1f5f9; font-weight: 500; }
  .metric .value a { color: inherit; text-decoration: none; }
  .metric .value a:hover { text-decoration: underline; }
  .address-value { display: inline-flex; align-items: center; gap: 0.3rem; }
  .copy-address { appearance: none; border: 0; padding: 0.05rem; background: transparent; color: #94a3b8; font: inherit; line-height: 1; cursor: pointer; }
  .copy-address:hover, .copy-address:focus-visible { color: #67e8f9; outline: none; }
  .copy-address.copied { color: #22c55e; }
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
  .chart-error { display: flex; align-items: center; justify-content: center; gap: 0.6rem; min-height: 8rem; margin-top: 0.5rem; border: 1px solid #7f1d1d; border-radius: 0.375rem; background: #1f1115; color: #fca5a5; font-size: 0.78rem; }
  .chart-retry { border: 1px solid #ef4444; border-radius: 0.3rem; padding: 0.3rem 0.55rem; background: #450a0a; color: #fecaca; font: inherit; cursor: pointer; }
  .chart-retry:hover, .chart-retry:focus-visible { border-color: #fca5a5; color: #fff; outline: none; }
  details.sigil-panel { margin-top: 0.75rem; }
  .sigil-stage { display: grid; place-items: center; min-height: 300px; margin-top: 0.5rem; border: 1px solid #334155; border-radius: 0.375rem; background: radial-gradient(circle at center, #172033 0, #0f172a 68%); overflow: hidden; contain: layout paint style; isolation: isolate; }
  .sigil-stage[role="button"] { cursor: pointer; }
  .sigil-stage[role="button"]:focus-visible { outline: 2px solid #facc15; outline-offset: 2px; }
  .sigil-stage svg { width: min(100%, 360px); height: auto; }
  .sigil-controls { display: flex; justify-content: center; gap: 0.4rem; }
  .sigil-animation-toggle, .sigil-view-large { margin-top: 0.4rem; }
  body.sigil-modal-open { overflow: hidden; }
  .sigil-modal { position: fixed; inset: 0; z-index: 1000; display: grid; place-items: center; padding: 1rem; background: rgba(2, 6, 23, 0.88); }
  .sigil-modal[hidden] { display: none; }
  .sigil-modal-content { position: relative; display: grid; place-items: center; width: min(94vw, calc(94vh - 2rem)); aspect-ratio: 1; border: 1px solid #475569; border-radius: 0.75rem; background: radial-gradient(circle at center, #172033 0, #0f172a 68%); box-shadow: 0 1.5rem 5rem rgba(0, 0, 0, 0.72); overflow: hidden; }
  .sigil-modal-stage { display: grid; place-items: center; width: 100%; height: 100%; }
  .sigil-modal-stage svg { width: 100%; height: 100%; max-width: none; }
  .sigil-modal-close { position: absolute; top: 0.65rem; right: 0.65rem; z-index: 1; width: 2.4rem; height: 2.4rem; border: 1px solid #64748b; border-radius: 999px; background: rgba(15, 23, 42, 0.88); color: #e2e8f0; font-size: 1.5rem; line-height: 1; cursor: pointer; }
  .sigil-modal-close:hover, .sigil-modal-close:focus-visible { border-color: #facc15; color: #facc15; outline: none; }
  body.history-modal-open { overflow: hidden; }
  .history-modal { position: fixed; inset: 0; z-index: 1001; display: grid; place-items: center; padding: 1rem; background: rgba(2, 6, 23, 0.88); }
  .history-modal[hidden] { display: none; }
  .history-modal-content { position: relative; width: min(94vw, 720px); max-height: min(86vh, 760px); display: flex; flex-direction: column; border: 1px solid #475569; border-radius: 0.75rem; background: #111827; box-shadow: 0 1.5rem 5rem rgba(0, 0, 0, 0.72); overflow: hidden; }
  .history-modal-header { padding: 1rem 3.6rem 0.8rem 1rem; border-bottom: 1px solid #334155; }
  .history-modal-title { margin: 0; color: #f8fafc; font-size: 1.05rem; }
  .history-modal-subtitle { margin-top: 0.25rem; color: #64748b; font-size: 0.75rem; }
  .history-modal-close { position: absolute; top: 0.65rem; right: 0.65rem; z-index: 1; width: 2.4rem; height: 2.4rem; border: 1px solid #64748b; border-radius: 999px; background: rgba(15, 23, 42, 0.88); color: #e2e8f0; font-size: 1.5rem; line-height: 1; cursor: pointer; }
  .history-modal-close:hover, .history-modal-close:focus-visible { border-color: #facc15; color: #facc15; outline: none; }
  .history-list { overflow-y: auto; overscroll-behavior: contain; padding: 0.5rem 1rem 1rem; }
  .history-row { display: grid; grid-template-columns: minmax(7rem, 1fr) minmax(9rem, 1.5fr) auto; gap: 0.75rem; align-items: center; padding: 0.7rem 0; border-bottom: 1px solid #253247; font-size: 0.8rem; }
  .history-row:last-child { border-bottom: 0; }
  .history-coin { appearance: none; border: 0; padding: 0; background: none; color: #f8fafc; font: inherit; font-weight: 700; text-align: left; text-decoration: underline; text-decoration-color: #475569; text-underline-offset: 0.18rem; overflow-wrap: anywhere; cursor: pointer; }
  .history-coin:hover, .history-coin:focus-visible { color: #facc15; text-decoration-color: #facc15; outline: none; }
  .history-detail { color: #cbd5e1; }
  .history-detail.positive { color: #4ade80; }
  .history-detail.negative { color: #f87171; }
  .history-gas { display: block; margin-top: 0.15rem; color: #94a3b8; font-size: 0.68rem; }
  .history-when { color: #64748b; text-align: right; white-space: nowrap; }
  .history-tx { display: block; margin-top: 0.2rem; color: #f8fafc; font-size: 0.7rem; text-decoration: none; }
  .history-tx:hover { text-decoration: underline; }
  .history-banking { margin-top: 0.45rem; padding: 0.4rem 0.5rem; border-left: 3px solid #22c55e; border-radius: 0.2rem; background: #10251d; color: #86efac; font-size: 0.75rem; }
  .history-banking.unmatched { border-left-color: #38bdf8; background: #102330; color: #7dd3fc; }
  .history-unbanked { margin-top: 0.35rem; color: #64748b; font-size: 0.72rem; }
  .history-empty { padding: 3rem 1rem; color: #64748b; text-align: center; }
  .history-summary-button { cursor: pointer; color: #e2e8f0; }
  .history-summary-button:hover, .history-summary-button:focus-visible { border-color: #64748b; color: #fff; outline: none; }
  @media (max-width: 560px) { .history-row { grid-template-columns: 1fr auto; } .history-detail { grid-column: 1 / -1; grid-row: 2; } .history-when { grid-column: 2; grid-row: 1; } }
  .sigil-stage.animation-enabled .sigil-stroke-current { stroke-dasharray: 0.14 0.08; animation: sigil-current var(--sigil-draw-duration) linear calc(var(--sigil-clock-phase) + var(--sigil-seed-phase)) infinite, sigil-glimmer var(--sigil-glimmer-duration) ease-in-out calc(var(--sigil-clock-phase) + var(--sigil-seed-phase)) infinite alternate; }
  .sigil-stage.animation-enabled .sigil-glyph { transform-origin: 128px 128px; transform-box: view-box; animation: sigil-turn var(--sigil-spin-duration) linear calc(var(--sigil-clock-phase) + var(--sigil-seed-phase)) infinite; }
  .sigil-stage.animation-enabled .sigil-rings { animation: sigil-breathe var(--sigil-breathe-duration) ease-in-out calc(var(--sigil-clock-phase) + var(--sigil-seed-phase)) infinite alternate; }
  .sigil-stage.animation-enabled .sigil-node { transform-box: fill-box; transform-origin: center; animation: sigil-node-pulse var(--sigil-pulse-duration) ease-in-out calc(var(--sigil-node-index) * 0.16s + var(--sigil-clock-phase) + var(--sigil-seed-phase)) infinite alternate; }
  .sigil-stage.animation-enabled:not(.is-visible) *,
  .sigil-motion-paused .sigil-stage.animation-enabled * { animation-play-state: paused !important; }
  @keyframes sigil-current { from { stroke-dashoffset: 0; } to { stroke-dashoffset: -0.22; } }
  @keyframes sigil-glimmer { from { opacity: 0.58; } to { opacity: 1; } }
  @keyframes sigil-turn { to { transform: rotate(360deg); } }
  @keyframes sigil-breathe { from { transform: scale(0.985); opacity: 0.58; } to { transform: scale(1.015); opacity: 1; } }
  @keyframes sigil-node-pulse { from { opacity: 0.35; transform: scale(0.72); } to { opacity: 1; transform: scale(1.24); } }
  .sigil-meta { color: #64748b; font: 0.68rem monospace; text-align: center; margin: 0.35rem 0 0.55rem; letter-spacing: 0.08em; }
  .dex-chart { width: 100%; height: 520px; border: 1px solid #334155; border-radius: 0.375rem; margin-top: 0.5rem; background: #0f172a; }
  .trades { margin-top: 0.75rem; }
  .trade { display: grid; grid-template-columns: 3.5rem 1fr auto auto; gap: 0.5rem; align-items: center; background: #0f172a; border-radius: 0.25rem; padding: 0.45rem; margin-top: 0.35rem; font-size: 0.75rem; }
  .trade .buy { color: #22c55e; } .trade .sell { color: #ef4444; }
  .trade-gas { color: #94a3b8; font-size: 0.66rem; white-space: nowrap; }
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
  .empty-clear { margin-top: 0.8rem; }
  .scout-panel { margin-bottom: 1rem; border: 1px solid #334155; border-radius: 0.55rem; background: #111827; overflow: hidden; }
  .scout-header { display: flex; align-items: center; justify-content: space-between; gap: 1rem; padding: 0.9rem; cursor: pointer; list-style: none; user-select: none; }
  .scout-header::-webkit-details-marker { display: none; }
  .scout-header::after { content: '▸'; color: #94a3b8; font-size: 0.9rem; transition: transform 0.15s ease; }
  .scout-panel[open] .scout-header::after { transform: rotate(90deg); }
  .scout-icon { display: grid; place-items: center; flex: 0 0 2rem; width: 2rem; height: 2rem; margin: -0.25rem 0; border: 1px solid #334155; border-radius: 0.45rem; background: #0f172a; font-size: 1.05rem; }
  .scout-heading { display: flex; align-items: baseline; gap: 0.65rem; min-width: 0; }
  .scout-title { margin: 0; font-size: 1rem; color: #f8fafc; font-weight: 700; }
  .scout-header span { color: #94a3b8; font-size: 0.72rem; }
  .scout-summary { margin-left: auto; white-space: nowrap; }
  .scout-body { padding: 0 0.9rem 0.9rem; border-top: 1px solid #1e293b; }
  .scout-note { margin: 0.65rem 0; color: #64748b; font-size: 0.7rem; }
  .scout-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 0.6rem; }
  .scout-card { padding: 0.7rem; border: 1px solid #334155; border-left-width: 4px; border-radius: 0.4rem; background: #0f172a; }
  .scout-card.pass { border-left-color: #22c55e; } .scout-card.caution { border-left-color: #eab308; } .scout-card.reject { border-left-color: #ef4444; }
  .scout-card-head { display: flex; justify-content: space-between; gap: 0.5rem; font-weight: 700; }
  .scout-score { color: #facc15; } .scout-meta { margin-top: 0.35rem; color: #94a3b8; font-size: 0.72rem; line-height: 1.45; }
  .scout-reason { margin-top: 0.35rem; color: #fca5a5; font-size: 0.68rem; overflow-wrap: anywhere; }
  .scout-warning { margin-top: 0.3rem; color: #fcd34d; font-size: 0.68rem; overflow-wrap: anywhere; }
  .scout-routes { margin-top: 0.45rem; padding-top: 0.4rem; border-top: 1px solid #1e293b; color: #94a3b8; font-size: 0.68rem; line-height: 1.45; }
  .scout-route-ok { color: #86efac; } .scout-route-bad { color: #fca5a5; }
  .scout-links { display: flex; gap: 0.65rem; margin-top: 0.45rem; }
  .scout-links a { color: #93c5fd; font-size: 0.68rem; text-decoration: none; }
  .scout-links a:hover { text-decoration: underline; }
  .scout-stale { color: #f87171; font-weight: 700; }
  @media (max-width: 640px) { .scout-heading > span { display: none; } .scout-summary { overflow: hidden; text-overflow: ellipsis; } }
</style>
</head>
<body>

<div class="header">
  <h1>⚡ Grid Bot Dashboard</h1>
  <div class="connection-wrap">
    <button class="connection-button" id="connection-button" type="button" aria-expanded="false" aria-controls="connection-diagnostics">
      <span class="status-dot disconnected" id="dot"></span>
      <span id="connection-status">Connecting…</span>
    </button>
    <div class="connection-diagnostics" id="connection-diagnostics" hidden>Waiting for the first live message…</div>
  </div>
</div>

<div class="container">
  <details class="scout-panel" id="scout-panel">
    <summary class="scout-header" aria-controls="scout-body">
      <span class="scout-icon" aria-hidden="true">🧭</span>
      <span class="scout-heading"><span class="scout-title" id="scout-title">DoomScout</span><span>read-only executable exit safety</span></span>
      <span class="scout-summary" id="scout-summary">Loading candidates…</span>
    </summary>
    <div class="scout-body" id="scout-body">
      <div class="scout-note" id="scout-note">Exact planned-size buy → sell checks · use /scout or /watch in Telegram</div>
      <div class="scout-grid" id="scout-grid"><div class="scout-meta">No candidates assessed yet.</div></div>
    </div>
  </details>
  <div class="summary-bar" id="summary-bar"></div>
  <div class="toolbar">
    <span class="filter-wrap"><input id="bot-filter" placeholder="Filter bots or groups"><button id="clear-filter" class="clear-filter" type="button" aria-label="Clear filter">×</button></span>
    <select id="chain-filter"><option value="">All chains</option><option value="4663">Robinhood</option><option value="8453">Base</option><option value="1">Ethereum</option></select>
    <select id="provider-filter"><option value="">All providers</option><option value="0x">0x</option><option value="lifi">LI.FI</option><option value="uniswap">Uniswap</option><option value="sushiswap">SushiSwap</option><option value="__unreported">Unreported</option></select>
    <button id="tax-filter" type="button" aria-pressed="false" title="Show only manually declared or auto-detected taxed tokens">Tax coins</button>
    <select id="sort-bots"><option value="name">Name</option><option value="symbol">Symbol</option><option value="estimated-value">Estimated value</option><option value="moonbag-value">Moonbag value</option><option value="next-buy-estimate">Next buy estimate</option><option value="needs-positions">Needs positions</option><option value="market-cap">Market Cap</option><option value="day-movement">Day Movement</option><option value="pnl">AVG P&amp;L</option><option value="top-position-pnl">Top position P&amp;L</option><option value="profit" selected>Session profit</option><option value="buys">Session buys</option><option value="sells">Session sells</option><option value="realized-profit">Realized profit</option><option value="treasury-sent">Treasury sent</option><option value="position-utilization">Position utilization</option><option value="eth-balance">ETH balance</option><option value="usdg-balance">USDG balance</option><option value="status">Status</option></select>
    <button id="sort-direction" type="button" title="Reverse sort direction">Descending ↓</button>
    <span class="notification-wrap"><button id="notifications" type="button" aria-haspopup="true" aria-expanded="false">Notifications</button>
      <div class="notification-menu" id="notification-menu" hidden>
        <strong>Browser notifications</strong>
        <label class="notification-option"><input type="checkbox" data-notification-type="sells"> Confirmed sells</label>
        <label class="notification-option"><input type="checkbox" data-notification-type="positions"> Needs new positions</label>
        <label class="notification-option"><input type="checkbox" data-notification-type="offline"> Bot offline</label>
        <label class="notification-option"><input type="checkbox" data-notification-type="recovered"> Bot recovered</label>
        <label class="notification-option"><input type="checkbox" data-notification-type="buys"> Confirmed buys</label>
        <label class="notification-option"><input type="checkbox" data-notification-type="stoploss"> Stop-loss sells</label>
        <label class="notification-option"><input type="checkbox" data-notification-type="treasury"> Treasury/banking success</label>
        <label class="notification-option"><input type="checkbox" data-notification-type="errors"> Persistent errors</label>
        <label class="notification-option"><input type="checkbox" data-notification-type="safety"> Balance safety faults</label>
        <button class="notification-enable" id="notification-enable" type="button">Enable browser notifications</button>
        <span class="notification-note" id="notification-note">Alerts work while this dashboard is open. Choices are saved in this browser.</span>
      </div>
    </span>
    <button id="reconnect-cards" type="button" title="Reconnect and refresh all bot cards" aria-label="Reconnect and refresh all bot cards">Refresh cards</button>
  </div>
  <div id="bots-container">
    <div class="empty" id="empty-state">
      <p>No bots reporting yet</p>
      <span>Waiting for status updates…</span>
      <button class="empty-clear" id="clear-all-filters" type="button" hidden>Clear all filters</button>
    </div>
  </div>
</div>

<div class="sigil-modal" id="sigil-modal" role="dialog" aria-modal="true" aria-label="Enlarged animated sigil" hidden>
  <div class="sigil-modal-content">
    <button class="sigil-modal-close" type="button" aria-label="Close enlarged sigil">×</button>
    <div class="sigil-stage sigil-modal-stage is-visible"></div>
  </div>
</div>

<div class="history-modal" id="history-modal" role="dialog" aria-modal="true" aria-labelledby="history-modal-title" hidden>
  <div class="history-modal-content">
    <button class="history-modal-close" type="button" aria-label="Close history">×</button>
    <div class="history-modal-header">
      <h2 class="history-modal-title" id="history-modal-title">Fleet history</h2>
      <div class="history-modal-subtitle" id="history-modal-subtitle"></div>
    </div>
    <div class="history-list" id="history-list"></div>
  </div>
</div>

<script>
(function() {
  const container = document.getElementById('bots-container');
  const emptyState = document.getElementById('empty-state');
  const dot = document.getElementById('dot');
  const connStatus = document.getElementById('connection-status');
  const connectionButton = document.getElementById('connection-button');
  const connectionDiagnostics = document.getElementById('connection-diagnostics');
  const summaryBar = document.getElementById('summary-bar');
  const scoutGrid = document.getElementById('scout-grid');
  const scoutSummary = document.getElementById('scout-summary');
  const scoutNote = document.getElementById('scout-note');
  const botFilter = document.getElementById('bot-filter');
  const clearFilter = document.getElementById('clear-filter');
  const clearAllFilters = document.getElementById('clear-all-filters');
  const chainFilter = document.getElementById('chain-filter');
  const providerFilter = document.getElementById('provider-filter');
  const taxFilter = document.getElementById('tax-filter');
  const sortBots = document.getElementById('sort-bots');
  const sortDirection = document.getElementById('sort-direction');
  const sigilModal = document.getElementById('sigil-modal');
  const sigilModalStage = sigilModal.querySelector('.sigil-modal-stage');
  const sigilModalClose = sigilModal.querySelector('.sigil-modal-close');
  let sigilModalReturnFocus = null;
  const historyModal = document.getElementById('history-modal');
  const historyModalTitle = document.getElementById('history-modal-title');
  const historyModalSubtitle = document.getElementById('history-modal-subtitle');
  const historyList = document.getElementById('history-list');
  const historyModalClose = historyModal.querySelector('.history-modal-close');
  let historyModalReturnFocus = null;
  let historyModalMode = 'history';
  let summaryBotIds = [];
  const notificationsButton = document.getElementById('notifications');
  const reconnectCardsButton = document.getElementById('reconnect-cards');
  const notificationMenu = document.getElementById('notification-menu');
  const notificationEnable = document.getElementById('notification-enable');
  const notificationNote = document.getElementById('notification-note');
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
  const notificationDefaults = { sells: true, positions: false, offline: false, recovered: false, buys: false, stoploss: false, treasury: false, errors: false, safety: true };
  let notificationPreferences = Object.assign({}, notificationDefaults);
  let notificationsMasterEnabled = localStorage.getItem('dashboard-notifications-enabled') === 'true';
  try { notificationPreferences = Object.assign(notificationPreferences, JSON.parse(localStorage.getItem('dashboard-notification-preferences') || '{}')); } catch (_) {}
  const defaultSortDirections = {
    name: 'asc', 'estimated-value': 'desc', 'moonbag-value': 'desc', 'next-buy-estimate': 'desc', 'needs-positions': 'desc', 'market-cap': 'desc', 'day-movement': 'desc', pnl: 'desc', 'top-position-pnl': 'desc', profit: 'desc', buys: 'desc', sells: 'desc', 'realized-profit': 'desc', 'treasury-sent': 'desc',
    'position-utilization': 'desc', 'eth-balance': 'desc', 'usdg-balance': 'desc', status: 'asc'
  };
  const storedSortMode = localStorage.getItem('dashboard-sort-mode');
  if (storedSortMode && sortBots.querySelector('option[value="' + CSS.escape(storedSortMode) + '"]')) sortBots.value = storedSortMode;
  const storedSortDirection = localStorage.getItem('dashboard-sort-direction');
  let sortDirectionValue = ['asc', 'desc'].includes(storedSortDirection) ? storedSortDirection : (defaultSortDirections[sortBots.value] || 'asc');
  const storedProfitCurrency = localStorage.getItem('dashboard-profit-currency');
  let profitCurrency = ['cad', 'usd'].includes(storedProfitCurrency) ? storedProfitCurrency : 'cad';
  const launchParams = new URLSearchParams(window.location.search);
  const launchedBot = (launchParams.get('bot') || '').trim();
  botFilter.value = launchedBot || localStorage.getItem('dashboard-bot-filter') || '';
  if (launchedBot && launchParams.get('chart') === '1') openCharts.add(encodeURIComponent(launchedBot));
  const storedChainFilter = localStorage.getItem('dashboard-chain-filter') || '';
  if (chainFilter.querySelector('option[value="' + CSS.escape(storedChainFilter) + '"]')) chainFilter.value = storedChainFilter;
  const storedProviderFilter = localStorage.getItem('dashboard-provider-filter') || '';
  if (providerFilter.querySelector('option[value="' + CSS.escape(storedProviderFilter) + '"]')) providerFilter.value = storedProviderFilter;
  let taxFilterEnabled = localStorage.getItem('dashboard-tax-filter') === 'true';
  function updateTaxFilterButton() {
    taxFilter.setAttribute('aria-pressed', String(taxFilterEnabled));
    taxFilter.textContent = taxFilterEnabled ? 'Tax coins only ✓' : 'Tax coins';
  }
  updateTaxFilterButton();
  clearFilter.style.display = botFilter.value ? 'block' : 'none';
  const storedRealizedProfitUnit = localStorage.getItem('dashboard-realized-profit-unit');
  let realizedProfitUnit = ['eth', 'cad', 'usd'].includes(storedRealizedProfitUnit) ? storedRealizedProfitUnit : 'eth';
  const storedRealizedPeriod = localStorage.getItem('dashboard-realized-profit-period');
  let realizedProfitPeriod = ['all', 'month', 'week', '24h', '6h', '1h'].includes(storedRealizedPeriod) ? storedRealizedPeriod : 'all';
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
  let reconnectCount = 0;
  let lastLiveMessageAt = null;
  let lastSnapshotAt = null;
  const maxReconnectDelay = 30000;
  let viewportBusy = false;
  let viewportMotionAt = 0;
  let viewportIdleTimer = null;
  let renderPendingForViewport = false;
  let renderPendingForce = false;
  let touchInteractionActive = false;
  let marketDataTimer = null;
  let marketDataFetchInFlight = false;
  let marketDataLastRequestedAt = 0;
  let marketDataSignature = '';
  let routineRenderTimer = null;
  let routineRenderIdleCallback = null;
  const pendingChangedBotIds = new Set();
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

  function browserNotificationsEnabled() {
    return notificationsMasterEnabled && 'Notification' in window && Notification.permission === 'granted';
  }

  function sendBrowserNotification(title, body, tag, url) {
    if (!browserNotificationsEnabled()) return;
    const notice = new Notification(title, { body: body, tag: tag });
    notice.onclick = function() {
      window.focus();
      if (url) window.open(url, '_blank', 'noopener');
      notice.close();
    };
  }

  function tradeIdentity(trade) {
    return String(trade.tx_hash || [trade.timestamp, trade.side, trade.eth_amount, trade.token_amount].join(':'));
  }

  function sellHistoryIdentity(state) {
    if (!state) return '';
    const sells = (state.trades_history || []).filter(function(trade) {
      return String(trade.side || '').toLowerCase() === 'sell';
    }).map(tradeIdentity);
    const banking = (state.events || []).filter(function(event) {
      return event.level === 'success' && event.code === 'usdg_banked';
    }).map(function(event) {
      return [event.timestamp, event.tx_hash, event.source_amount, event.usdg_amount].join(':');
    });
    return sells.join('|') + '//' + banking.join('|');
  }

  function processBotNotifications(botId, previous, next) {
    if (!browserNotificationsEnabled() || !previous || !next) return;
    const name = next.display_name || botId;
    const symbol = next.token_symbol ? ' · ' + next.token_symbol : '';
    const chain = chainMetadata[parseInt(next.chain_id, 10)];
    const previousTrades = new Set((previous.trades_history || []).map(tradeIdentity));
    (next.trades_history || []).forEach(function(trade) {
      const identity = tradeIdentity(trade);
      if (previousTrades.has(identity)) return;
      const side = String(trade.side || '').toLowerCase();
      const profit = parseFloat(trade.profit_eth);
      const isStopLoss = side === 'sell' && Number.isFinite(profit) && profit < 0;
      const txUrl = chain && trade.tx_hash ? chain.explorer.replace('/address/', '/tx/') + trade.tx_hash : '';
      if (isStopLoss && notificationPreferences.stoploss) {
        sendBrowserNotification('Stop-loss sell · ' + name, (Number.isFinite(profit) ? profit.toFixed(8) + ' ETH profit' : 'Confirmed') + symbol, 'stoploss:' + identity, txUrl);
      } else if (side === 'sell' && notificationPreferences.sells) {
        sendBrowserNotification('Sell confirmed · ' + name, (Number.isFinite(profit) ? (profit >= 0 ? '+' : '') + profit.toFixed(8) + ' ETH profit' : parseFloat(trade.eth_amount || 0).toFixed(8) + ' ETH received') + symbol, 'sell:' + identity, txUrl);
      } else if (side === 'buy' && notificationPreferences.buys) {
        sendBrowserNotification('Buy confirmed · ' + name, parseFloat(trade.eth_amount || 0).toFixed(8) + ' ETH' + symbol, 'buy:' + identity, txUrl);
      }
    });
    if (notificationPreferences.positions && !previous.capacity_warning && next.capacity_warning) {
      sendBrowserNotification('Needs new positions · ' + name, 'All ' + (next.capacity_warning.max_positions || next.max_positions || '') + ' position slots are filled' + symbol, 'positions:' + botId);
    }
    const previousMismatch = previous.sell_attempt && previous.sell_attempt.status === 'position_balance_mismatch';
    const nextMismatch = next.sell_attempt && next.sell_attempt.status === 'position_balance_mismatch';
    if (notificationPreferences.safety && !previousMismatch && nextMismatch) {
      const deficit = String(next.sell_attempt.deficit_raw || '?');
      sendBrowserNotification('BALANCE MISMATCH · ' + name, 'Sell blocked for position #' + String(next.sell_attempt.position_id || '?') + ' · raw deficit ' + deficit + symbol, 'balance-mismatch:' + botId);
    } else if (notificationPreferences.safety && previousMismatch && !nextMismatch) {
      sendBrowserNotification('Balance reconciled · ' + name, 'Position accounting matches the wallet again' + symbol, 'balance-recovered:' + botId);
    }
    const previousAge = reportAge(previous.received_at).status;
    if (notificationPreferences.recovered && previousAge === 'offline' && reportAge(next.received_at).status === 'running') {
      sendBrowserNotification('Bot recovered · ' + name, 'Status reports resumed' + symbol, 'recovered:' + botId);
    }
    const previousTreasury = parseFloat(previous.treasury_sent_usdg) || 0;
    const nextTreasury = parseFloat(next.treasury_sent_usdg) || 0;
    if (notificationPreferences.treasury && nextTreasury > previousTreasury) {
      sendBrowserNotification('Treasury transfer · ' + name, '+' + (nextTreasury - previousTreasury).toFixed(2) + ' USDG confirmed', 'treasury:' + botId + ':' + nextTreasury);
    }
    if (notificationPreferences.treasury) {
      const previousBankingEvents = new Set((previous.events || []).map(function(event) { return String(event.code || '') + ':' + String(event.timestamp || '') + ':' + String(event.tx_hash || ''); }));
      (next.events || []).forEach(function(event) {
        if (event.level !== 'success' || event.code !== 'usdg_banked') return;
        const key = String(event.code) + ':' + String(event.timestamp || '') + ':' + String(event.tx_hash || '');
        if (previousBankingEvents.has(key)) return;
        const txUrl = chain && event.tx_hash ? chain.explorer.replace('/address/', '/tx/') + event.tx_hash : '';
        sendBrowserNotification('Profit banked · ' + name, String(event.message || 'USDG banking confirmed'), 'banking:' + botId + ':' + key, txUrl);
      });
    }
    if (notificationPreferences.errors) {
      const previousEvents = new Map((previous.events || []).map(function(event) { return [String(event.code || '') + ':' + String(event.timestamp || ''), parseInt(event.count, 10) || 1]; }));
      (next.events || []).forEach(function(event) {
        if (event.level !== 'error') return;
        const key = String(event.code || '') + ':' + String(event.timestamp || '');
        const count = parseInt(event.count, 10) || 1;
        if (count >= 3 && (previousEvents.get(key) || 0) < 3) sendBrowserNotification('Persistent error · ' + name, String(event.message || event.code || 'Repeated bot error'), 'error:' + botId + ':' + key);
      });
    }
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
      reconnectCardsButton.disabled = false;
      reconnectCardsButton.textContent = 'Refresh cards';
      reconnectDelay = 1000;
    };

    source.onerror = function() {
      if (generation !== connectionGeneration) return;
      reconnectCount += 1;
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
      lastLiveMessageAt = Date.now();
      lastSnapshotAt = lastLiveMessageAt;
      const data = JSON.parse(e.data);
      if (data.bots) {
        Object.keys(bots).forEach(function(botId) { delete bots[botId]; });
        Object.keys(data.bots).forEach(function(botId) {
          bots[botId] = data.bots[botId];
        });
        render();
        scheduleMarketDataFetch();
      }
    });

    source.addEventListener('update', function(e) {
      if (generation !== connectionGeneration) return;
      lastLiveMessageAt = Date.now();
      const entry = JSON.parse(e.data);
      const nextState = entry.data || entry;
      const previousState = bots[entry.bot_id];
      const historyChanged = sellHistoryIdentity(previousState) !== sellHistoryIdentity(nextState);
      processBotNotifications(entry.bot_id, previousState, nextState);
      bots[entry.bot_id] = nextState;
      if (historyChanged && !historyModal.hidden && historyModalMode === 'history' && summaryBotIds.includes(entry.bot_id)) {
        openFleetHistory(null, true);
      }
      scheduleRoutineRender(entry.bot_id);
      scheduleMarketDataFetch();
    });

    source.addEventListener('remove', function(e) {
      if (generation !== connectionGeneration) return;
      lastLiveMessageAt = Date.now();
      const data = JSON.parse(e.data);
      if (data.bot_id && bots[data.bot_id]) {
        delete bots[data.bot_id];
        render();
      }
    });
  }

  function reconnectNow() {
    reconnectCount += 1;
    dot.className = 'status-dot disconnected';
    connStatus.textContent = 'Reconnecting…';
    reconnectDelay = 1000;
    connect();
  }

  function updateConnectionDiagnostics() {
    const age = function(timestamp) {
      if (!timestamp) return 'never';
      const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
      return seconds < 60 ? seconds + 's ago' : Math.floor(seconds / 60) + 'm ago';
    };
    connectionDiagnostics.textContent = 'Last live message: ' + age(lastLiveMessageAt) + '\\n' +
      'Last full snapshot: ' + age(lastSnapshotAt) + '\\n' +
      'Manual/automatic reconnects: ' + reconnectCount + '\\n' +
      'Cards in memory: ' + Object.keys(bots).length;
    connectionDiagnostics.style.whiteSpace = 'pre-line';
  }

  function refreshCardsFromApi() {
    return fetch('/api/bots', { cache: 'no-store' })
      .then(function(response) {
        if (!response.ok) throw new Error('Card refresh failed: ' + response.status);
        return response.json();
      })
      .then(function(data) {
        lastLiveMessageAt = Date.now();
        lastSnapshotAt = lastLiveMessageAt;
        const nextBots = {};
        Object.keys(data.bots || {}).forEach(function(botId) {
          const entry = data.bots[botId];
          if (entry && entry.state) nextBots[botId] = entry.state;
        });
        Object.keys(bots).forEach(function(botId) { delete bots[botId]; });
        Object.keys(nextBots).forEach(function(botId) { bots[botId] = nextBots[botId]; });
        render(true);
        scheduleMarketDataFetch();
      });
  }

  reconnectCardsButton.addEventListener('click', function() {
    reconnectCardsButton.disabled = true;
    reconnectCardsButton.textContent = 'Refreshing…';
    reconnectNow();
    refreshCardsFromApi()
      .catch(function() {})
      .finally(function() {
        reconnectCardsButton.disabled = false;
        reconnectCardsButton.textContent = 'Refresh cards';
      });
    fetchEthPrices();
    fetchMarketData();
  });

  function esc(str) {
    const div = document.createElement('div');
    div.textContent = String(str === null || str === undefined ? '' : str);
    return div.innerHTML;
  }

  function formatTokenAmount(value) {
    const amount = parseFloat(value || 0);
    if (!Number.isFinite(amount) || amount === 0) return '0';
    const magnitude = Math.abs(amount);
    const decimals = magnitude >= 1000 ? 2 : magnitude >= 1 ? 6 : magnitude >= 0.01 ? 8 : 12;
    return amount.toFixed(decimals).replace(/\\.?0+$/, '');
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
    const animationStyle = '--sigil-draw-duration:' + (5 + bytes[26] % 5) + 's;--sigil-glimmer-duration:' + (3 + bytes[25] % 4) + 's;--sigil-spin-duration:' + (42 + bytes[27] % 39) +
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
    document.querySelectorAll('.sigil-stage').forEach(function(stage) {
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

  function closeSigilModal() {
    if (sigilModal.hidden) return;
    sigilModal.hidden = true;
    sigilModalStage.replaceChildren();
    document.body.classList.remove('sigil-modal-open');
    if (sigilModalReturnFocus && sigilModalReturnFocus.isConnected) sigilModalReturnFocus.focus();
    sigilModalReturnFocus = null;
  }

  function openSigilModal(sigil, trigger) {
    const svg = sigilSvg(sigil);
    if (!svg) return;
    sigilModalReturnFocus = trigger;
    sigilModalStage.innerHTML = svg;
    const modalSvg = sigilModalStage.querySelector('svg');
    if (modalSvg) modalSvg.style.setProperty('--sigil-clock-phase', '-' + (Date.now() / 1000).toFixed(3) + 's');
    sigilModal.hidden = false;
    document.body.classList.add('sigil-modal-open');
    applySigilAnimationState();
    sigilModalClose.focus();
  }

  function historyTimestamp(value) {
    const parsed = Date.parse(value || '');
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function historyAgo(value) {
    const timestamp = historyTimestamp(value);
    if (!timestamp) return 'time unknown';
    const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
    if (seconds < 60) return seconds + 's ago';
    if (seconds < 3600) return Math.floor(seconds / 60) + 'm ago';
    if (seconds < 86400) return Math.floor(seconds / 3600) + 'h ago';
    if (seconds < 604800) return Math.floor(seconds / 86400) + 'd ago';
    return new Date(timestamp).toLocaleDateString();
  }

  function historyTxUrl(state, txHash) {
    const chain = chainMetadata[parseInt(state.chain_id, 10)];
    return chain && txHash ? chain.explorer.replace('/address/', '/tx/') + txHash : '';
  }

  function closeHistoryModal() {
    if (historyModal.hidden) return;
    historyModal.hidden = true;
    historyList.replaceChildren();
    document.body.classList.remove('history-modal-open');
    if (historyModalReturnFocus && historyModalReturnFocus.isConnected) historyModalReturnFocus.focus();
    historyModalReturnFocus = null;
    historyModalMode = 'history';
  }

  function openFleetHistory(trigger, refreshOnly) {
    const preservedScrollTop = historyList.scrollTop;
    const entries = [];
    summaryBotIds.forEach(function(botId) {
      const state = bots[botId];
      if (!state) return;
      const coin = state.token_symbol || state.display_name || botId;
      const sells = (state.trades_history || []).filter(function(trade) {
        return String(trade.side || '').toLowerCase() === 'sell';
      }).map(function(trade) {
        return { state: state, botId: botId, coin: coin, timestamp: trade.timestamp, txHash: trade.tx_hash, trade: trade, banking: null };
      });
      const bankingEvents = (state.events || []).filter(function(event) {
        return event.level === 'success' && event.code === 'usdg_banked';
      }).sort(function(a, b) { return historyTimestamp(a.timestamp) - historyTimestamp(b.timestamp); });
      const usedBanking = new Set();
      sells.forEach(function(sell) {
        const sellTime = historyTimestamp(sell.timestamp);
        let bestIndex = -1;
        let bestDelay = Infinity;
        bankingEvents.forEach(function(event, index) {
          if (usedBanking.has(index)) return;
          const delay = historyTimestamp(event.timestamp) - sellTime;
          if (delay >= 0 && delay <= 10 * 60 * 1000 && delay < bestDelay) {
            bestIndex = index;
            bestDelay = delay;
          }
        });
        if (bestIndex >= 0) {
          sell.banking = bankingEvents[bestIndex];
          usedBanking.add(bestIndex);
        }
        entries.push(sell);
      });
      bankingEvents.forEach(function(event, index) {
        if (!usedBanking.has(index)) entries.push({ state: state, botId: botId, coin: coin, timestamp: event.timestamp, event: event, unmatchedBanking: true });
      });
    });
    entries.sort(function(a, b) { return historyTimestamp(b.timestamp) - historyTimestamp(a.timestamp); });
    historyModalTitle.textContent = 'Sell history';
    historyModalSubtitle.textContent = entries.length + ' retained entr' + (entries.length === 1 ? 'y' : 'ies') + ' across ' + summaryBotIds.length + ' displayed bot' + (summaryBotIds.length === 1 ? '' : 's');
    if (!entries.length) {
      historyList.innerHTML = '<div class="history-empty">No retained sells or successful banking events yet.</div>';
    } else {
      historyList.innerHTML = entries.map(function(entry) {
        if (entry.unmatchedBanking) {
          const event = entry.event;
          const sourceAmount = parseFloat(event.source_amount);
          const usdgAmount = parseFloat(event.usdg_amount);
          const bankingTxUrl = historyTxUrl(entry.state, event.tx_hash);
          const bankingDetail = (Number.isFinite(sourceAmount) ? sourceAmount.toFixed(8) + ' ' + esc(event.source_asset || 'ETH') + ' → ' : '') + (Number.isFinite(usdgAmount) ? usdgAmount.toFixed(2) + ' USDG' : esc(event.message || 'Banking confirmed'));
          return '<div class="history-row"><button class="history-coin" type="button" data-history-focus-bot="' + esc(entry.botId) + '">' + esc(entry.coin) + '</button><div class="history-detail"><div class="history-banking unmatched">🏦 Banking confirmed · ' + bankingDetail + (bankingTxUrl ? '<a class="history-tx" href="' + esc(bankingTxUrl) + '" target="_blank" rel="noopener">Banking transaction ↗</a>' : '') + '</div></div>' +
            '<div class="history-when" title="' + esc(entry.timestamp || '') + '">' + esc(historyAgo(entry.timestamp)) + '</div></div>';
        }
        const txUrl = historyTxUrl(entry.state, entry.txHash);
        const profit = parseFloat(entry.trade.profit_eth);
        const received = parseFloat(entry.trade.eth_amount) || 0;
        const detail = Number.isFinite(profit) ? (profit >= 0 ? '+' : '') + profit.toFixed(8) + ' ETH profit' : received.toFixed(8) + ' ETH received';
        const gasFee = parseFloat(entry.trade.gas_fee_eth);
        const gasHtml = Number.isFinite(gasFee) ? '<span class="history-gas">⛽ ' + esc(gasFee.toFixed(8)) + ' ETH</span>' : '';
        const detailClass = Number.isFinite(profit) ? (profit >= 0 ? ' positive' : ' negative') : '';
        let bankingHtml = '<div class="history-unbanked">No successful banking recorded for this sell</div>';
        if (entry.banking) {
          const sourceAmount = parseFloat(entry.banking.source_amount);
          const usdgAmount = parseFloat(entry.banking.usdg_amount);
          const bankingTxUrl = historyTxUrl(entry.state, entry.banking.tx_hash);
          const bankingDetail = (Number.isFinite(sourceAmount) ? sourceAmount.toFixed(8) + ' ' + esc(entry.banking.source_asset || 'ETH') + ' → ' : '') + (Number.isFinite(usdgAmount) ? usdgAmount.toFixed(2) + ' USDG' : esc(entry.banking.message || 'Banking confirmed'));
          bankingHtml = '<div class="history-banking">🏦 Banked · ' + bankingDetail + ' · ' + esc(historyAgo(entry.banking.timestamp)) + (bankingTxUrl ? '<a class="history-tx" href="' + esc(bankingTxUrl) + '" target="_blank" rel="noopener">Banking transaction ↗</a>' : '') + '</div>';
        }
        return '<div class="history-row"><button class="history-coin" type="button" data-history-focus-bot="' + esc(entry.botId) + '">' + esc(entry.coin) + '</button>' +
          '<div class="history-detail' + detailClass + '">' + detail + gasHtml + (txUrl ? '<a class="history-tx" href="' + esc(txUrl) + '" target="_blank" rel="noopener">Sell transaction ↗</a>' : '') + bankingHtml + '</div>' +
          '<div class="history-when" title="' + esc(entry.timestamp || '') + '">' + esc(historyAgo(entry.timestamp)) + '</div></div>';
      }).join('');
    }
    if (refreshOnly) {
      historyList.scrollTop = preservedScrollTop;
      return;
    }
    historyModalMode = 'history';
    historyModalReturnFocus = trigger;
    historyModal.hidden = false;
    document.body.classList.add('history-modal-open');
    historyModalClose.focus();
  }

  function openStatusList(wantedStatus, trigger, refreshOnly) {
    const preservedScrollTop = historyList.scrollTop;
    const entries = summaryBotIds.map(function(botId) {
      const state = bots[botId];
      const age = reportAge(state && state.received_at);
      return { botId: botId, state: state, age: age };
    }).filter(function(entry) {
      return entry.state && entry.age.status === wantedStatus;
    }).sort(function(a, b) {
      const aName = String(a.state.token_symbol || a.state.display_name || a.botId);
      const bName = String(b.state.token_symbol || b.state.display_name || b.botId);
      return aName.localeCompare(bName);
    });
    const labels = { running: 'Active bots', stale: 'Stale bots', offline: 'Offline bots' };
    historyModalTitle.textContent = labels[wantedStatus] || 'Bots';
    historyModalSubtitle.textContent = entries.length + ' bot' + (entries.length === 1 ? '' : 's') +
      ' in the current filtered view · select a coin to jump to its card';
    historyList.innerHTML = entries.length ? entries.map(function(entry) {
      const state = entry.state;
      const coin = state.token_symbol || state.display_name || entry.botId;
      const chain = chainMetadata[parseInt(state.chain_id, 10)];
      const provider = state.swap_provider ? String(state.swap_provider) : 'provider unreported';
      const positions = (parseInt(state.filled_positions, 10) || 0) + '/' + (parseInt(state.max_positions, 10) || 0) + ' positions';
      const detail = [chain ? chain.name : 'Unknown chain', provider, positions].join(' · ');
      return '<div class="history-row"><button class="history-coin" type="button" data-history-focus-bot="' + esc(entry.botId) + '">' + esc(coin) + '</button>' +
        '<div class="history-detail">' + esc(detail) + '</div>' +
        '<div class="history-when" title="' + esc(state.received_at || '') + '">' + esc(entry.age.text) + '</div></div>';
    }).join('') : '<div class="history-empty">No ' + esc(wantedStatus) + ' bots in the current view.</div>';
    historyModalMode = 'status:' + wantedStatus;
    if (refreshOnly) {
      historyList.scrollTop = preservedScrollTop;
      return;
    }
    historyModalReturnFocus = trigger;
    historyModal.hidden = false;
    document.body.classList.add('history-modal-open');
    historyModalClose.focus();
  }

  function refreshOpenHistoryModal() {
    if (historyModal.hidden) return;
    if (historyModalMode === 'history') openFleetHistory(null, true);
    else if (historyModalMode.startsWith('status:')) openStatusList(historyModalMode.slice(7), null, true);
  }

  function focusBotCard(botId) {
    let card = Array.from(container.querySelectorAll('.card[data-bot-id]')).find(function(candidate) {
      return candidate.dataset.botId === botId;
    });
    if (!card && bots[botId]) {
      botFilter.value = '';
      chainFilter.value = '';
      providerFilter.value = '';
      taxFilterEnabled = false;
      localStorage.setItem('dashboard-bot-filter', '');
      localStorage.setItem('dashboard-chain-filter', '');
      localStorage.setItem('dashboard-provider-filter', '');
      localStorage.setItem('dashboard-tax-filter', 'false');
      updateTaxFilterButton();
      clearFilter.style.display = 'none';
      render(true);
      card = Array.from(container.querySelectorAll('.card[data-bot-id]')).find(function(candidate) {
        return candidate.dataset.botId === botId;
      });
    }
    if (card) {
      card.scrollIntoView({ behavior: 'smooth', block: 'start' });
      card.focus({ preventScroll: true });
    }
  }

  sigilModalClose.addEventListener('click', closeSigilModal);
  sigilModal.addEventListener('click', function(event) {
    if (event.target === sigilModal) closeSigilModal();
  });
  historyModalClose.addEventListener('click', closeHistoryModal);
  historyModal.addEventListener('click', function(event) {
    const focusButton = event.target.closest('[data-history-focus-bot]');
    if (focusButton) {
      const botId = focusButton.dataset.historyFocusBot;
      closeHistoryModal();
      focusBotCard(botId);
      return;
    }
    if (event.target === historyModal) closeHistoryModal();
  });
  document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape' && !sigilModal.hidden) closeSigilModal();
    if (event.key === 'Escape' && !historyModal.hidden) closeHistoryModal();
  });

  function wireSigilAnimation(panel, stage) {
    const svg = stage.querySelector('svg');
    if (svg && stage.dataset.clockSynced !== 'true') {
      svg.style.setProperty('--sigil-clock-phase', '-' + (Date.now() / 1000).toFixed(3) + 's');
      stage.dataset.clockSynced = 'true';
    }
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
    const viewButton = panel.querySelector('.sigil-view-large');
    if (viewButton && viewButton.dataset.viewWired !== 'true') {
      viewButton.dataset.viewWired = 'true';
      viewButton.addEventListener('click', function() {
        const botId = decodeURIComponent(panel.dataset.sigilKey || '');
        if (bots[botId]) openSigilModal(bots[botId].sigil, viewButton);
      });
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

  function reportAge(receivedAt, precisionSeconds) {
    const timestamp = Date.parse(receivedAt || '');
    if (!Number.isFinite(timestamp)) return { status: 'unknown', text: 'unknown' };
    const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
    const displaySeconds = precisionSeconds > 1
      ? Math.floor(seconds / precisionSeconds) * precisionSeconds
      : seconds;
    let text;
    if (displaySeconds < 60) text = displaySeconds + 's ago';
    else if (displaySeconds < 3600) text = Math.floor(displaySeconds / 60) + 'm ' + (displaySeconds % 60) + 's ago';
    else text = Math.floor(displaySeconds / 3600) + 'h ' + Math.floor((displaySeconds % 3600) / 60) + 'm ago';
    return { status: seconds < 120 ? 'running' : (seconds < 300 ? 'stale' : 'offline'), text: text };
  }

  function refreshReportAges() {
    updateConnectionDiagnostics();
    const animationVisible = Boolean(document.querySelector('.sigil-stage.animation-enabled.is-visible'));
    const precisionSeconds = animationVisible ? 10 : 1;
    container.querySelectorAll('[data-received-at]').forEach(function(el) {
      const age = reportAge(el.dataset.receivedAt, precisionSeconds);
      const nextText = 'Updated ' + age.text;
      if (el.textContent !== nextText) el.textContent = nextText;
      const badge = el.closest('.card').querySelector('.badge');
      if (badge && badge.dataset.inferred === 'true') {
        const nextClassName = 'badge ' + age.status;
        if (badge.className !== nextClassName) badge.className = nextClassName;
        if (badge.textContent !== age.status) badge.textContent = age.status;
      }
      const card = el.closest('.card');
      const botId = card ? card.dataset.botId : '';
      if (age.status === 'offline' && botId && notificationPreferences.offline && browserNotificationsEnabled() && !notifiedOffline.has(botId)) {
        notifiedOffline.add(botId);
        sendBrowserNotification(botId + ' is offline', 'Last report ' + age.text, 'offline:' + botId);
      } else if (age.status === 'running') notifiedOffline.delete(botId);
    });
    window.setTimeout(refreshReportAges, animationVisible ? 10000 : 1000);
  }

  function needsGasState(state) {
    const reported = state && state.needs_gas;
    const balance = parseFloat(state && state.eth_balance !== undefined ? state.eth_balance : reported && reported.balance_eth);
    const reserve = parseFloat(state && state.gas_reserve_eth !== undefined ? state.gas_reserve_eth : reported && reported.reserve_eth);
    const warningThreshold = reserve * 0.5;
    if (!Number.isFinite(balance) || !Number.isFinite(reserve) || reserve <= 0 || balance >= warningThreshold) return null;
    return {
      balance_eth: balance,
      reserve_eth: reserve,
      warning_threshold_eth: warningThreshold,
      shortfall_eth: warningThreshold - balance,
    };
  }

  function updateSummary(botIds) {
    summaryBotIds = botIds.slice();
    const states = botIds.map(function(id) { return bots[id]; });
    const needsPositions = Object.keys(bots).filter(function(id) {
      const state = bots[id];
      return Boolean(state.capacity_warning) && reportAge(state.received_at).status === 'running';
    });
    const needsGas = Object.keys(bots).filter(function(id) {
      const state = bots[id];
      return Boolean(needsGasState(state)) && reportAge(state.received_at).status === 'running';
    });
    const needsFunds = Object.keys(bots).filter(function(id) {
      const state = bots[id];
      return Boolean(state.funding_warning) && reportAge(state.received_at).status === 'running';
    });
    const activeSellChecks = Object.keys(bots).filter(function(id) {
      const state = bots[id];
      return Boolean(state.sell_attempt) && reportAge(state.received_at).status === 'running';
    });
    const buyGasBlocked = Object.keys(bots).filter(function(id) {
      const state = bots[id];
      return Boolean(state.buy_attempt && state.buy_attempt.status === 'projected_gas_above_cap') && reportAge(state.received_at).status === 'running';
    });
    const active = states.filter(function(d) { return reportAge(d.received_at).status === 'running'; }).length;
    const stale = states.filter(function(d) { return reportAge(d.received_at).status === 'stale'; }).length;
    const offline = states.filter(function(d) { return reportAge(d.received_at).status === 'offline'; }).length;
    const profit = states.reduce(function(total, d) { return total + (parseFloat(d.session_profit_eth) || 0); }, 0);
    const allRealizedProfit = states.reduce(function(total, d) { return total + (parseFloat(d.realized_profit_eth) || 0); }, 0);
    const realizedProfit = realizedProfitPeriod === 'all' ? allRealizedProfit : states.reduce(function(total, d) {
      return total + realizedProfitForPeriod(d, realizedProfitPeriod);
    }, 0);
    const totalEthBalance = states.reduce(function(total, d) { return total + (parseFloat(d.eth_balance) || 0); }, 0);
    const trackingTimestamps = states.map(function(d) { return Date.parse(d.profit_tracking_started_at || ''); }).filter(Number.isFinite);
    const oldestTrackingAt = trackingTimestamps.length ? Math.min.apply(null, trackingTimestamps) : null;
    const trackingElapsedHours = oldestTrackingAt === null ? null : Math.max(0, (Date.now() - oldestTrackingAt) / 3600000);
    const trackingAgeDays = trackingElapsedHours === null ? 0 : Math.floor(trackingElapsedHours / 24);
    const trackingAgeHours = trackingElapsedHours === null ? 0 : Math.floor(trackingElapsedHours % 24);
    const trackingAgeText = trackingElapsedHours === null ? '' :
      (trackingAgeDays > 0 ? trackingAgeDays + 'd ' + trackingAgeHours + 'h ago' :
        (trackingAgeHours > 0 ? trackingAgeHours + 'h ago' : '<1h ago'));
    const realizedPeriodHours = { month: 720, week: 168, '24h': 24, '6h': 6, '1h': 1 }[realizedProfitPeriod];
    const realizedAverageHours = realizedProfitPeriod === 'all' ? trackingElapsedHours :
      (trackingElapsedHours === null ? realizedPeriodHours : Math.min(realizedPeriodHours, trackingElapsedHours));
    const realizedDailyAverage = realizedAverageHours > 0 ? realizedProfit / (realizedAverageHours / 24) : null;
    const realizedHourlyAverage = realizedAverageHours > 0 ? realizedProfit / realizedAverageHours : null;
    const realizedPeriodLabel = realizedProfitPeriod !== 'all' && trackingElapsedHours !== null && trackingElapsedHours < realizedPeriodHours
      ? 'Since ' + trackingAgeText
      : { all: 'Since ' + trackingAgeText, month: 'Since 30d ago', week: 'Since 7d ago', '24h': 'Since 24h ago', '6h': 'Since 6h ago', '1h': 'Since 1h ago' }[realizedProfitPeriod];
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
    const usdgCadValue = Number.isFinite(ethPrices.cad) && Number.isFinite(ethPrices.usd) && ethPrices.usd > 0
      ? usdgBalance * ethPrices.cad / ethPrices.usd : null;
    const fiatProfit = Number.isFinite(fiatRate) ? profit * fiatRate : null;
    const fleetBagValue = states.reduce(function(total, d) {
      const value = estimatedBagValue(d, profitCurrency);
      return value === null ? total : total + value;
    }, 0);
    const fleetBagValueAvailable = Number.isFinite(fiatRate) && Number.isFinite(ethPrices.usd);
    const totalEthFiat = Number.isFinite(fiatRate) ? totalEthBalance * fiatRate : null;
    const realizedUnitCode = realizedProfitUnit.toUpperCase();
    const realizedUnitRate = realizedProfitUnit === 'eth' ? 1 : ethPrices[realizedProfitUnit];
    const formatRealizedAmount = function(valueEth, includeUnit) {
      if (!Number.isFinite(realizedUnitRate)) return '—' + (includeUnit ? ' ' + realizedUnitCode : '');
      const value = valueEth * realizedUnitRate;
      if (realizedProfitUnit === 'eth') return (value >= 0 ? '+' : '') + value.toFixed(8) + (includeUnit ? ' ETH' : '');
      const formatted = new Intl.NumberFormat(undefined, { style: 'currency', currency: realizedUnitCode, minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(value);
      return (value >= 0 ? '+' : '') + formatted + (includeUnit ? ' ' + realizedUnitCode : '');
    };
    const nextRealizedProfitUnit = { eth: 'CAD', cad: 'USD', usd: 'ETH' }[realizedProfitUnit];
      const nextSummaryHtml = (buyGasBlocked.length
        ? '<span class="summary-item buy-gas-blocked" aria-live="polite">● Buy gas blocked: ' + buyGasBlocked.length +
          ' <span class="bot-names">(' + buyGasBlocked.map(function(id) {
            return '<button class="needs-position-link" type="button" data-focus-bot="' + esc(id) + '">' + esc(bots[id].token_symbol || bots[id].display_name || id) + '</button>';
          }).join(', ') + ')</span></span>'
        : '') +
      (activeSellChecks.length
        ? '<span class="summary-item sell-checks-active" aria-live="polite">● Sell checks active: ' + activeSellChecks.length +
          ' <span class="bot-names">(' + activeSellChecks.map(function(id) {
            return '<button class="needs-position-link" type="button" data-focus-bot="' + esc(id) + '">' + esc(bots[id].token_symbol || bots[id].display_name || id) + '</button>';
          }).join(', ') + ')</span></span>'
        : '') +
      (needsGas.length
        ? '<span class="summary-item needs-gas" aria-live="polite">⛽ Needs gas: ' + needsGas.length +
          ' <span class="bot-names">(' + needsGas.map(function(id) {
            return '<button class="needs-position-link" type="button" data-focus-bot="' + esc(id) + '">' + esc(bots[id].token_symbol || bots[id].display_name || id) + '</button>';
          }).join(', ') + ')</span></span>'
        : '') +
      (needsFunds.length
        ? '<span class="summary-item needs-funds" aria-live="polite">💸 Needs funds: ' + needsFunds.length +
          ' <span class="bot-names">(' + needsFunds.map(function(id) {
            return '<button class="needs-position-link" type="button" data-focus-bot="' + esc(id) + '">' + esc(bots[id].token_symbol || bots[id].display_name || id) + '</button>';
          }).join(', ') + ')</span></span>'
        : '') +
      (needsPositions.length
        ? '<span class="summary-item needs-positions" aria-live="polite">⚑ Needs new positions: ' + needsPositions.length +
          ' <span class="bot-names">(' + needsPositions.map(function(id) {
            return '<button class="needs-position-link" type="button" data-focus-bot="' + esc(id) + '">' + esc(bots[id].display_name || id) + '</button>';
          }).join(', ') + ')</span></span>'
        : '') +
      '<button class="summary-item status-summary running" type="button" data-status-list="running" title="List active bots">Active: ' + active + ' / ' + states.length + '</button>' +
      '<button class="summary-item status-summary stale" type="button" data-status-list="stale" title="List stale bots">Stale: ' + stale + '</button>' +
      '<button class="summary-item status-summary offline" type="button" data-status-list="offline" title="List offline bots">Offline: ' + offline + '</button>' +
      '<span class="summary-item" title="Estimated from current balances and spot prices; liquidation value may be lower">Estimated fleet value: ' +
      (fleetBagValueAvailable ? formatBagValue(fleetBagValue, profitCurrency) : '—') +
      ' <button class="currency-toggle" type="button" data-bag-currency-toggle>' + fiatCode + '</button>' +
      '<span class="summary-detail">ETH + USDG + tokens</span></span>' +
      '<span class="summary-item">Session profit: ' + (profit >= 0 ? '+' : '') + profit.toFixed(8) + ' ETH' +
      (fiatProfit === null ? '' : ' / ' + (fiatProfit >= 0 ? '+' : '') + new Intl.NumberFormat(undefined, { style: 'currency', currency: fiatCode }).format(fiatProfit)) +
      ' <button class="currency-toggle" type="button" data-currency-toggle>' + fiatCode + '</button></span>' +
      '<span class="summary-item realized-summary"><span class="realized-first-line"><button class="realized-amount" type="button" data-realized-unit-toggle aria-label="Cycle realized profit units, currently ' + realizedUnitCode + '" title="Click to show ' + nextRealizedProfitUnit + '">Realized profit: ' + formatRealizedAmount(realizedProfit, true) + '</button>' +
      '<select class="realized-period" data-realized-period aria-label="Realized profit period">' +
      [['all', 'All'], ['month', 'Month'], ['week', 'Week'], ['24h', '24 hr'], ['6h', '6 hr'], ['1h', '1 hr']].map(function(option) { return '<option value="' + option[0] + '"' + (realizedProfitPeriod === option[0] ? ' selected' : '') + '>' + option[1] + '</option>'; }).join('') + '</select></span>' +
      (realizedDailyAverage === null ? '' : '<span class="summary-detail" title="Fleet realized profit divided by the selected period">' + realizedPeriodLabel + ' · avg ' +
        formatRealizedAmount(realizedDailyAverage, false) + '/day · ' +
        formatRealizedAmount(realizedHourlyAverage, false) + '/hr</span>') + '</span>' +
      '<button class="summary-item" type="button" data-bag-currency-toggle title="Combined ETH balance across the displayed bots">Total ETH: ' +
      totalEthBalance.toFixed(8) + ' ETH' + (totalEthFiat === null ? '' : '<span class="summary-detail">' +
      formatBagValue(totalEthFiat, profitCurrency) + ' ' + fiatCode + ' · tap for ' + (profitCurrency === 'usd' ? 'CAD' : 'USD') + '</span>') + '</button>' +
      '<span class="summary-item">USDG: ' + usdgBalance.toFixed(2) +
      (usdgCadValue === null ? '' : '<span class="summary-detail">' + formatBagValue(usdgCadValue, 'cad') + ' CAD</span>') + '</span>' +
      '<span class="summary-item">Treasury sent: ' + treasurySentUsdg.toFixed(2) + ' USDG</span>' +
      '<span class="summary-item">Filled positions: ' + filled + '</span>' +
      '<span class="summary-item">Longest uptime: ' + uptimeText + '</span>' +
      '<button class="summary-item history-summary-button" type="button" data-fleet-history>Sell history</button>';
    const periodSelectorOpen = document.activeElement && document.activeElement.matches('[data-realized-period]');
    if (!periodSelectorOpen && summaryBar.innerHTML !== nextSummaryHtml) summaryBar.innerHTML = nextSummaryHtml;
    refreshOpenHistoryModal();
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

  function copyText(value) {
    if (navigator.clipboard && window.isSecureContext) return navigator.clipboard.writeText(value);
    return new Promise(function(resolve, reject) {
      const input = document.createElement('textarea');
      input.value = value;
      input.setAttribute('readonly', '');
      input.style.position = 'fixed';
      input.style.opacity = '0';
      document.body.appendChild(input);
      input.select();
      try {
        if (!document.execCommand('copy')) throw new Error('copy command failed');
        resolve();
      } catch (error) {
        reject(error);
      } finally {
        input.remove();
      }
    });
  }

  function addressValue(address, url, label) {
    if (!address) return null;
    const copyButton = '<button class="copy-address" type="button" data-copy-address="' + esc(address) + '" aria-label="Copy ' + esc(label) + ' address" title="Copy ' + esc(label) + ' address">📋</button>';
    const addressLink = url
      ? '<a href="' + esc(url) + '" target="_blank" rel="noopener noreferrer" title="' + esc(address) + '">' + esc(shortenAddress(address)) + '</a>'
      : '<span title="' + esc(address) + '">' + esc(shortenAddress(address)) + '</span>';
    return '<span class="address-value">' + copyButton + addressLink + '</span>';
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

  function estimatedBagValue(d, currency) {
    const ethRate = Number(ethPrices[currency]);
    const usdRate = Number(ethPrices.usd);
    const ethBalance = Number(d.eth_balance) || 0;
    const tokenBalance = Number(d.token_balance) || 0;
    const tokenPriceEth = Number(d.price) || 0;
    const usdgBalance = Number(d.usdg_balance) || 0;
    if (!Number.isFinite(ethRate) || !Number.isFinite(usdRate) || usdRate <= 0) return null;
    const cryptoValue = (ethBalance + tokenBalance * tokenPriceEth) * ethRate;
    const usdgValue = currency === 'usd' ? usdgBalance : usdgBalance * ethRate / usdRate;
    return cryptoValue + usdgValue;
  }

  function estimatedNextBuy(d) {
    const unfilled = Math.max(0, (parseInt(d.max_positions, 10) || 0) - (parseInt(d.filled_positions, 10) || 0));
    const reserve = parseFloat(d.gas_reserve_eth);
    if (unfilled <= 0 || !Number.isFinite(reserve)) return null;
    return Math.max(0, (parseFloat(d.eth_balance) || 0) - reserve) / unfilled;
  }

  function formatBagValue(value, currency) {
    if (!Number.isFinite(value)) return '—';
    return new Intl.NumberFormat(undefined, {
      style: 'currency', currency: currency.toUpperCase(),
      minimumFractionDigits: 2, maximumFractionDigits: 2
    }).format(value);
  }

  function realizedProfitForPeriod(d, period) {
    if (period === 'all') return parseFloat(d.realized_profit_eth) || 0;
    const hours = { month: 720, week: 168, '24h': 24, '6h': 6, '1h': 1 }[period];
    const cutoff = Date.now() - hours * 3600000;
    const receivedAt = Date.parse(d.received_at || '');
    if (Number.isFinite(receivedAt) && receivedAt <= cutoff) return 0;
    const reported = parseFloat((d.realized_profit_periods || {})[period]);
    if (Number.isFinite(reported)) return reported;
    const trackingStart = Date.parse(d.profit_tracking_started_at || '');
    if (Number.isFinite(trackingStart) && trackingStart >= cutoff) return parseFloat(d.realized_profit_eth) || 0;
    return (d.trades_history || []).reduce(function(total, trade) {
      const timestamp = Date.parse(trade.timestamp || '');
      return trade.side === 'sell' && Number.isFinite(timestamp) && timestamp >= cutoff
        ? total + (parseFloat(trade.profit_eth) || 0) : total;
    }, 0);
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
    if (!Object.keys(bots).length || marketDataFetchInFlight) return;
    marketDataFetchInFlight = true;
    marketDataLastRequestedAt = Date.now();
    fetch('/api/dexscreener/market-data')
      .then(function(response) { if (!response.ok) throw new Error(response.status); return response.json(); })
      .then(function(data) {
        const nextSignature = JSON.stringify(data.bots || {});
        if (nextSignature === marketDataSignature) return;
        marketDataSignature = nextSignature;
        Object.keys(data.bots || {}).forEach(function(botId) { marketData[botId] = data.bots[botId]; });
        // Market-cap sorting must reorder after the asynchronous values arrive.
        // Other modes mutate only value nodes to avoid unnecessary card rebuilds.
        if (sortBots.value === 'market-cap' || sortBots.value === 'day-movement') render(true);
        else updateMarketDataNodes();
      })
      .catch(function() {})
      .finally(function() { marketDataFetchInFlight = false; });
  }

  function scheduleMarketDataFetch() {
    if (marketDataTimer !== null) return;
    const minimumGap = 15000;
    const delay = Math.max(250, minimumGap - (Date.now() - marketDataLastRequestedAt));
    marketDataTimer = setTimeout(fetchMarketData, delay);
  }

  function topPositionPnl(state) {
    const values = (state.positions || []).map(function(position) {
      return parseFloat(position.pnl);
    }).filter(Number.isFinite);
    return values.length ? Math.max.apply(null, values) : null;
  }

  function scheduleRoutineRender(botId) {
    if (botId) pendingChangedBotIds.add(botId);
    if (routineRenderTimer !== null || routineRenderIdleCallback !== null) return;
    routineRenderTimer = setTimeout(function() {
      routineRenderTimer = null;
      const flush = function() {
        routineRenderIdleCallback = null;
        const changedBotIds = new Set(pendingChangedBotIds);
        pendingChangedBotIds.clear();
        render(false, changedBotIds);
      };
      if ('requestIdleCallback' in window && document.querySelector('.sigil-stage.animation-enabled.is-visible')) {
        routineRenderIdleCallback = window.requestIdleCallback(flush, { timeout: 1200 });
      } else {
        flush();
      }
    }, 750);
  }

  function livePanelKey(element) {
    if (!element || element.nodeType !== Node.ELEMENT_NODE) return '';
    if (element.matches('details.chart-panel[open][data-chart-key]')) return 'chart:' + element.dataset.chartKey;
    if (element.matches('details.sigil-panel[open][data-sigil-key]')) return 'sigil:' + element.dataset.sigilKey;
    return '';
  }

  function preserveRuntimeAttribute(element, name) {
    if (name === 'style' && element.matches('.card[data-bot-id]')) return true;
    return name === 'src' && element.matches('iframe.dex-chart') ||
      name === 'data-chart-wired' || name === 'data-sigil-wired' ||
      name === 'data-animation-wired' || name === 'data-view-wired' ||
      name === 'data-rendered' || name === 'data-clock-synced';
  }

  function morphNode(currentNode, freshNode) {
    if (currentNode.isEqualNode(freshNode) || livePanelKey(currentNode)) return currentNode;
    if (currentNode.nodeType !== freshNode.nodeType ||
        (currentNode.nodeType === Node.ELEMENT_NODE && currentNode.tagName !== freshNode.tagName)) {
      currentNode.replaceWith(freshNode);
      return freshNode;
    }
    if (currentNode.nodeType === Node.TEXT_NODE) {
      if (currentNode.nodeValue !== freshNode.nodeValue) currentNode.nodeValue = freshNode.nodeValue;
      return currentNode;
    }
    Array.from(currentNode.attributes).forEach(function(attribute) {
      if (!freshNode.hasAttribute(attribute.name) && !preserveRuntimeAttribute(currentNode, attribute.name)) {
        currentNode.removeAttribute(attribute.name);
      }
    });
    Array.from(freshNode.attributes).forEach(function(attribute) {
      if (preserveRuntimeAttribute(currentNode, attribute.name)) return;
      if (currentNode.getAttribute(attribute.name) !== attribute.value) currentNode.setAttribute(attribute.name, attribute.value);
    });
    const currentChildren = Array.from(currentNode.childNodes);
    const freshChildren = Array.from(freshNode.childNodes);
    const sharedLength = Math.min(currentChildren.length, freshChildren.length);
    for (let index = 0; index < sharedLength; index++) morphNode(currentChildren[index], freshChildren[index]);
    for (let index = currentChildren.length - 1; index >= freshChildren.length; index--) currentChildren[index].remove();
    for (let index = sharedLength; index < freshChildren.length; index++) currentNode.appendChild(freshChildren[index]);
    return currentNode;
  }

  function morphRange(currentNodes, freshNodes, boundary) {
    const sharedLength = Math.min(currentNodes.length, freshNodes.length);
    for (let index = 0; index < sharedLength; index++) morphNode(currentNodes[index], freshNodes[index]);
    for (let index = currentNodes.length - 1; index >= freshNodes.length; index--) currentNodes[index].remove();
    for (let index = sharedLength; index < freshNodes.length; index++) boundary.parentNode.insertBefore(freshNodes[index], boundary);
  }

  function updateCardAroundLivePanels(currentCard, freshCard) {
    const livePanels = Array.from(currentCard.children).filter(function(child) { return Boolean(livePanelKey(child)); });
    if (!livePanels.length) {
      morphNode(currentCard, freshCard);
      return;
    }
    const freshChildren = Array.from(freshCard.children);
    const freshIndexes = livePanels.map(function(livePanel) {
      const key = livePanelKey(livePanel);
      return freshChildren.findIndex(function(child) { return livePanelKey(child) === key; });
    });
    if (freshIndexes.some(function(index) { return index < 0; })) return;
    let freshStart = 0;
    let oldStart = currentCard.firstChild;
    livePanels.forEach(function(livePanel, panelIndex) {
      const freshIndex = freshIndexes[panelIndex];
      const currentSegment = [];
      let oldNode = oldStart;
      while (oldNode && oldNode !== livePanel) {
        currentSegment.push(oldNode);
        oldNode = oldNode.nextSibling;
      }
      morphRange(currentSegment, freshChildren.slice(freshStart, freshIndex), livePanel);
      oldStart = livePanel.nextSibling;
      freshStart = freshIndex + 1;
    });
    const trailingCurrent = [];
    let oldNode = oldStart;
    while (oldNode) {
      trailingCurrent.push(oldNode);
      oldNode = oldNode.nextSibling;
    }
    const trailingBoundary = document.createComment('card-end');
    currentCard.appendChild(trailingBoundary);
    morphRange(trailingCurrent, freshChildren.slice(freshStart), trailingBoundary);
    trailingBoundary.remove();
    if (currentCard.className !== freshCard.className) currentCard.className = freshCard.className;
  }

  function updateGridPreservingLivePanels(html, changedBotIds) {
    const currentGrid = container.querySelector('.grid');
    if (!currentGrid || !container.querySelector('details.chart-panel[open], details.sigil-panel[open]')) return false;
    const template = document.createElement('template');
    template.innerHTML = html;
    const freshGrid = template.content.querySelector('.grid');
    if (!freshGrid) return false;
    currentGrid.hidden = false;
    const freshCards = new Map();
    freshGrid.querySelectorAll(':scope > .card[data-bot-id]').forEach(function(card) { freshCards.set(card.dataset.botId, card); });
    const currentCards = new Map();
    currentGrid.querySelectorAll(':scope > .card[data-bot-id]').forEach(function(card) { currentCards.set(card.dataset.botId, card); });
    const visualOrder = new Map();
    freshGrid.querySelectorAll(':scope > .card[data-bot-id]').forEach(function(card, index) { visualOrder.set(card.dataset.botId, index); });
    currentCards.forEach(function(card, botId) {
      if (!visualOrder.has(botId)) return;
      const nextOrder = String(visualOrder.get(botId));
      if (card.style.order !== nextOrder) card.style.order = nextOrder;
    });
    const botIdsToUpdate = changedBotIds && changedBotIds.size
      ? changedBotIds
      : new Set(Array.from(currentCards.keys()).concat(Array.from(freshCards.keys())));
    botIdsToUpdate.forEach(function(botId) {
      const card = currentCards.get(botId);
      const freshCard = freshCards.get(botId);
      if (!card) {
        if (freshCards.has(botId)) {
          const newCard = freshCards.get(botId);
          newCard.style.order = String(visualOrder.get(botId));
          currentGrid.appendChild(newCard);
        }
        return;
      }
      if (!freshCard) {
        if (card.querySelector('details.chart-panel[open], details.sigil-panel[open]')) card.hidden = true;
        else card.remove();
        return;
      }
      card.hidden = false;
      updateCardAroundLivePanels(card, freshCard);
    });
    return true;
  }

  function render(force, changedBotIds) {
    if (force && routineRenderTimer !== null) {
      clearTimeout(routineRenderTimer);
      routineRenderTimer = null;
      pendingChangedBotIds.clear();
    }
    if (force && routineRenderIdleCallback !== null) {
      window.cancelIdleCallback(routineRenderIdleCallback);
      routineRenderIdleCallback = null;
      pendingChangedBotIds.clear();
    }
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
    const query = botFilter.value.trim().toLowerCase();
    const wantedChain = chainFilter.value;
    const wantedProvider = providerFilter.value;
    const rank = { running: 0, stale: 1, offline: 2, unknown: 3 };
    const botIds = Object.keys(bots).filter(function(id) {
      const d = bots[id];
      const provider = String(d.swap_provider || '').toLowerCase();
      const haystack = [id, d.display_name, d.token_symbol, d.group, provider].join(' ').toLowerCase();
      const providerMatches = !wantedProvider ||
        (wantedProvider === '__unreported' ? !provider : provider === wantedProvider);
      const taxSource = String(d.token_tax_detection_source || '').toLowerCase();
      const isTaxedToken = d.taxed_token === true ||
        ['manual', 'declared', 'auto-detected'].includes(taxSource);
      return (!query || haystack.includes(query)) &&
        (!wantedChain || String(d.chain_id) === wantedChain) && providerMatches &&
        (!taxFilterEnabled || isTaxedToken);
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
      else if (mode === 'estimated-value') {
        const aValue = estimatedBagValue(av, profitCurrency);
        const bValue = estimatedBagValue(bv, profitCurrency);
        if (aValue === null && bValue === null) return a.localeCompare(b);
        if (aValue === null) return 1;
        if (bValue === null) return -1;
        result = aValue - bValue;
      }
      else if (mode === 'moonbag-value') {
        const aRaw = av.estimated_moonbag_value_eth, bRaw = bv.estimated_moonbag_value_eth;
        const aValue = Number(aRaw), bValue = Number(bRaw);
        const aKnown = aRaw !== null && aRaw !== '' && Number.isFinite(aValue);
        const bKnown = bRaw !== null && bRaw !== '' && Number.isFinite(bValue);
        if (!aKnown && !bKnown) return a.localeCompare(b);
        if (!aKnown) return 1;
        if (!bKnown) return -1;
        result = aValue - bValue;
      }
      else if (mode === 'next-buy-estimate') {
        const aValue = estimatedNextBuy(av), bValue = estimatedNextBuy(bv);
        if (aValue === null && bValue === null) return a.localeCompare(b);
        if (aValue === null) return 1;
        if (bValue === null) return -1;
        result = aValue - bValue;
      }
      else if (mode === 'needs-positions') {
        result = Number(Boolean(av.capacity_warning)) - Number(Boolean(bv.capacity_warning));
      }
      else if (mode === 'market-cap') {
        const aRaw = (marketData[a] || {}).value_usd, bRaw = (marketData[b] || {}).value_usd;
        const aValue = Number(aRaw), bValue = Number(bRaw);
        const aKnown = aRaw !== null && aRaw !== '' && Number.isFinite(aValue);
        const bKnown = bRaw !== null && bRaw !== '' && Number.isFinite(bValue);
        if (!aKnown && !bKnown) return a.localeCompare(b);
        if (!aKnown) return 1;
        if (!bKnown) return -1;
        result = aValue - bValue;
      }
      else if (mode === 'day-movement') {
        const aRaw = ((marketData[a] || {}).price_change || {}).h24;
        const bRaw = ((marketData[b] || {}).price_change || {}).h24;
        const aValue = Number(aRaw), bValue = Number(bRaw);
        const aKnown = aRaw !== null && aRaw !== '' && Number.isFinite(aValue);
        const bKnown = bRaw !== null && bRaw !== '' && Number.isFinite(bValue);
        if (!aKnown && !bKnown) return a.localeCompare(b);
        if (!aKnown) return 1;
        if (!bKnown) return -1;
        result = aValue - bValue;
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
      else if (mode === 'buys') result = (parseInt(av.buys, 10) || 0) - (parseInt(bv.buys, 10) || 0);
      else if (mode === 'sells') result = (parseInt(av.sells, 10) || 0) - (parseInt(bv.sells, 10) || 0);
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
      const filtered = Object.keys(bots).length > 0 && Boolean(query || wantedChain || wantedProvider || taxFilterEnabled);
      emptyState.querySelector('p').textContent = filtered ? 'No bots match your filters' : 'No bots reporting yet';
      emptyState.querySelector('span').textContent = filtered ? 'Clear the active filters to show the fleet.' : 'Waiting for status updates…';
      clearAllFilters.hidden = !filtered;
      const liveGrid = container.querySelector('.grid');
      if (liveGrid && container.querySelector('details.chart-panel[open], details.sigil-panel[open]')) {
        liveGrid.hidden = true;
        if (!emptyState.isConnected) container.appendChild(emptyState);
        emptyState.style.display = '';
        return;
      }
      container.innerHTML = '';
      container.appendChild(emptyState);
      emptyState.style.display = '';
      return;
    }
    emptyState.style.display = 'none';

    let html = '<div class="grid">';
    botIds.forEach(function(botId) {
      const d = bots[botId];
      if (changedBotIds && changedBotIds.size && !changedBotIds.has(botId)) {
        html += '<div class="card" data-bot-id="' + esc(botId) + '" tabindex="-1"></div>';
        return;
      }
      const age = reportAge(d.received_at);
      const status = d.status || age.status;
      const botKey = encodeURIComponent(botId);
      const moreOpen = openMoreInfo.has(botKey);
      const positionsOpen = openPositions.has(botKey);
      const rawOpen = openRawJson.has(botKey);
      const chartOpen = openCharts.has(botKey);
      const sigilOpen = !closedSigils.has(botKey);
      const hasBalanceMismatch = d.sell_attempt && d.sell_attempt.status === 'position_balance_mismatch';
      const gasWarning = needsGasState(d);
      html += '<div class="card' + (d.capacity_warning ? ' capacity-warning' : '') + (gasWarning ? ' needs-gas' : '') + (d.funding_warning ? ' needs-funds' : '') + (hasBalanceMismatch ? ' balance-mismatch' : '') + '" data-bot-id="' + esc(botId) + '" tabindex="-1">';
      html += '<h2>Bot</h2>';
      const chain = chainMetadata[Number(d.chain_id)];
      const taxFee = parseFloat(d.token_transfer_fee_percent);
      const taxSource = String(d.token_tax_detection_source || 'none');
      const taxBadge = d.taxed_token && Number.isFinite(taxFee) && taxFee > 0
        ? '<span class="tax-badge" title="' + esc(taxSource === 'auto-detected' ? 'Runtime auto-detected transfer fee' : 'Declared transfer fee') + '">' +
          (taxSource === 'auto-detected' ? 'AUTO TAX ' : 'TAX ') + esc(taxFee.toFixed(1)) + '%</span>'
        : '';
      html += '<div class="bot-id">' + esc(d.display_name || botId) + ' ' + statusBadge(status).replace('<span ', '<span data-inferred="' + (!d.status) + '" ') +
        (chain ? '<span class="chain-badge">' + esc(chain.name) + '</span>' : '') +
        (d.swap_provider ? '<span class="provider-badge">' + esc(String(d.swap_provider).toUpperCase()) + '</span>' : '') +
        taxBadge +
        (d.group ? '<span class="group-badge">' + esc(d.group) + '</span>' : '') + '</div>';

      if (d.capacity_warning) {
        const warningPnl = parseFloat(d.capacity_warning.highest_position_pnl);
        const warningThreshold = parseFloat(d.capacity_warning.buy_threshold);
        html += '<div class="capacity-alert"><strong>⚠ ADD POSITIONS</strong>' +
          'Buy point reached, but all ' + esc(d.capacity_warning.max_positions) + ' position slots are filled. ' +
          'Highest P&amp;L: ' + esc(Number.isFinite(warningPnl) ? warningPnl.toFixed(1) : '?') +
          '% · Buy point: ' + esc(Number.isFinite(warningThreshold) ? warningThreshold.toFixed(1) : '?') + '%</div>';
      }

      if (gasWarning) {
        const gasBalance = parseFloat(gasWarning.balance_eth);
        const gasReserve = parseFloat(gasWarning.reserve_eth);
        const gasThreshold = parseFloat(gasWarning.warning_threshold_eth);
        const gasShortfall = parseFloat(gasWarning.shortfall_eth);
        html += '<div class="gas-alert" role="alert"><strong>⛽ NEEDS GAS — TRADES MAY FAIL</strong>' +
          'ETH balance: ' + esc(Number.isFinite(gasBalance) ? gasBalance.toFixed(8) : '?') +
          ' · Reserve: ' + esc(Number.isFinite(gasReserve) ? gasReserve.toFixed(8) : '?') +
          ' · Warning below: ' + esc(Number.isFinite(gasThreshold) ? gasThreshold.toFixed(8) : '?') +
          ' · Shortfall: ' + esc(Number.isFinite(gasShortfall) ? gasShortfall.toFixed(8) : '?') + ' ETH</div>';
      }

      if (d.funding_warning) {
        const fundingBalance = parseFloat(d.funding_warning.trade_balance);
        const fundingMinimum = parseFloat(d.funding_warning.minimum_trade_balance);
        html += '<div class="funding-alert" role="alert"><strong>💸 BUY BLOCKED — NEEDS TRADING FUNDS</strong>' +
          esc(d.funding_warning.asset || 'ETH') + ' available after gas reserve: ' +
          esc(Number.isFinite(fundingBalance) ? fundingBalance.toFixed(8) : '?') +
          ' · Minimum: ' + esc(Number.isFinite(fundingMinimum) ? fundingMinimum.toFixed(8) : '?') +
          ' · Open slots: ' + esc(d.funding_warning.available_slots ?? '?') + '</div>';
      }

      if (d.buy_attempt && d.buy_attempt.status === 'projected_gas_above_cap') {
        const attempt = d.buy_attempt;
        const projected = parseFloat(attempt.projected_gas_eth);
        const maximum = parseFloat(attempt.maximum_gas_eth);
        const amount = parseFloat(attempt.buy_amount_eth);
        html += '<div class="buy-attempt" role="status" aria-label="Buy attempted but blocked by projected gas fee">' +
          '<span class="sell-attempt-dot" aria-hidden="true"></span>' +
          '<span class="sell-attempt-copy"><strong>BUY ATTEMPT — GAS CAP BLOCKED</strong>' +
          'Projected fee ' + esc(Number.isFinite(projected) ? projected.toFixed(8) : '?') +
          ' ETH exceeds cap ' + esc(Number.isFinite(maximum) ? maximum.toFixed(8) : '?') +
          ' ETH · source ' + esc(attempt.quote_provider || '?') +
          (Number.isFinite(amount) ? ' · buy ' + esc(amount.toFixed(8)) + ' ETH' : '') +
          (attempt.position_id ? ' · position #' + esc(attempt.position_id) : '') +
          '</span></div>';
      }

      if (hasBalanceMismatch) {
        const mismatch = d.sell_attempt;
        html += '<div class="balance-mismatch-alert" role="alert"><strong>🚨 POSITION BALANCE MISMATCH — SELL BLOCKED</strong>' +
          'Position #' + esc(mismatch.position_id || '?') + ' tracks ' + esc(mismatch.tracked_sell_amount_raw || '?') +
          ' raw units, but the wallet has ' + esc(mismatch.wallet_balance_raw || '?') +
          ' · deficit ' + esc(mismatch.deficit_raw || '?') + '. Reconcile position accounting before restarting sales.</div>';
      }

      if (d.sell_attempt && d.sell_attempt.status === 'quote_below_minimum') {
        const reportedNet = parseFloat(d.sell_attempt.projected_net_profit_eth);
        const quoted = parseFloat(d.sell_attempt.quoted_profit_eth);
        const projectedGas = parseFloat(d.sell_attempt.projected_gas_eth);
        const net = Number.isFinite(reportedNet)
          ? reportedNet
          : (Number.isFinite(quoted) && Number.isFinite(projectedGas) ? quoted - projectedGas : quoted);
        const minimum = parseFloat(d.sell_attempt.minimum_profit_eth);
        const provider = d.sell_attempt.quote_provider
          ? '<span class="sell-attempt-provider">' + esc(d.sell_attempt.quote_provider) + '</span>'
          : '';
        const detail = Number.isFinite(net) && Number.isFinite(minimum)
          ? '<span class="sell-attempt-detail" title="Projected net profit after sell gas / minimum profit"><span>' + esc(net.toFixed(6)) + ' / ' + esc(minimum.toFixed(6)) + ' ETH net</span>' + provider + '</span>'
          : '';
        html += '<div class="sell-attempt" role="status" aria-label="Sell attempted; quote is below minimum">' +
          '<span class="sell-attempt-dot" aria-hidden="true"></span>' +
          '<span class="sell-attempt-copy"><strong>SELL CHECK ACTIVE</strong>Waiting for minimum quote</span>' + detail + '</div>';
      }

      if (d.sell_attempt && (d.sell_attempt.status === 'quote_provider_disagreement' || d.sell_attempt.status === 'quote_provider_changed')) {
        const attempt = d.sell_attempt;
        const disagreement = attempt.status === 'quote_provider_disagreement';
        html += '<div class="sell-attempt" role="alert" aria-label="Sell blocked while quote provider changes">' +
          '<span class="sell-attempt-dot" aria-hidden="true"></span>' +
          '<span class="sell-attempt-copy"><strong>' + (disagreement ? 'QUOTE DISAGREEMENT — SELL BLOCKED' : 'QUOTE PROVIDER CHANGED — CONFIRMING') + '</strong>' +
          esc(attempt.previous_quote_provider || '?') + ' → ' + esc(attempt.quote_provider || '?') +
          ' · ' + esc(attempt.quote_divergence_percent ?? '?') + '% difference</span></div>';
      }

      d.buys = d.buys ?? 0;
      d.sells = d.sells ?? 0;

      const metrics = [
        ['Estimated Bag Value', 'estimated_bag_value'],
        ['AVG P&L', 'profit_percent'],
        ['Session Profit', 'session_profit_eth'],
        ['Realized Profit', 'realized_profit_eth'],
        ['Filled / Max Positions', 'position_capacity'],
      ];
      const moreMetrics = [
        ['Price', 'price'],
        ['Buy Point', 'buy_point_percent'], ['Sell Point', 'sell_point_percent'],
        ['Buys', 'buys'], ['Sells', 'sells'],
        ['Realized Sells', 'realized_sales'], ['Profit Tracking Since', 'profit_tracking_started_at'],
        ['Next Buy Est.', 'next_buy_estimated_eth'], ['Gas Reserve', 'gas_reserve_eth'],
        ['ETH Balance', 'eth_balance'], ['USDG Balance', 'usdg_balance'], ['Treasury Sent', 'treasury_sent_usdg'], ['Token Balance', 'token_balance'],
        ['Est. Moonbag Value', 'estimated_moonbag_value_eth'],
        ['Wallet', 'wallet_link'], ['Contract', 'token_link'],
        ['RPC', 'rpc_status'], ['Polling', 'poll_interval_seconds'], ['Uptime', 'uptime_seconds'],
      ];

      d.wallet_link = addressValue(d.wallet_address, d.wallet_address && chain ? chain.explorer + d.wallet_address : '', 'wallet');
      d.token_link = addressValue(d.token_address, d.token_address && chain ? chain.explorer + d.token_address : '', 'contract');

      d.position_capacity = (d.filled_positions !== undefined && d.max_positions !== undefined)
        ? d.filled_positions + ' / ' + d.max_positions
        : null;
      d.next_buy_estimated_eth = estimatedNextBuy(d);
      d.estimated_bag_value = estimatedBagValue(d, profitCurrency);

      function renderMetric(pair) {
        const label = pair[0], key = pair[1];
        if (d[key] !== undefined && d[key] !== null) {
          let val = d[key];
          let cls = '';
          if (key === 'profit_percent') {
            cls = parseFloat(val) >= 0 ? 'positive' : 'negative';
            val = (parseFloat(val) >= 0 ? '+' : '') + parseFloat(val).toFixed(2) + '%';
          } else if (key === 'buy_point_percent' || key === 'sell_point_percent') {
            const point = parseFloat(val);
            cls = key === 'buy_point_percent' ? 'negative' : 'positive';
            val = (point >= 0 ? '+' : '') + point.toFixed(2).replace(/\\.00$/, '') + '%';
          } else if (key === 'estimated_bag_value') {
            val = '<button class="currency-toggle" type="button" data-bag-currency-toggle title="Estimated from current balances and spot prices; liquidation value may be lower">' +
              esc(formatBagValue(Number(val), profitCurrency)) + ' ' + esc(profitCurrency.toUpperCase()) + '</button>';
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
          } else if (key === 'next_buy_estimated_eth' || key === 'gas_reserve_eth' || key === 'estimated_moonbag_value_eth') {
            val = parseFloat(val).toFixed(5).replace(/\\.?0+$/, '') + ' ETH';
          } else if (key === 'eth_balance' || key === 'usdg_balance' || key === 'treasury_sent_usdg' || key === 'token_balance') {
            val = parseFloat(val).toFixed(key === 'eth_balance' ? 4 : ((key === 'usdg_balance' || key === 'treasury_sent_usdg') ? 2 : 0));
          }
          const renderedValue = key === 'wallet_link' || key === 'token_link' || key === 'estimated_bag_value' ? val : esc(val);
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
        '<summary><span class="label">24h</span><span class="value' + movementClass(changes.h24) + '" data-market-window="h24">' + esc(formatMovement(changes.h24)) + '</span></summary>' +
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
        html += '<div class="sigil-controls"><button class="toggle-raw sigil-animation-toggle" type="button">Animation: ' + (sigilAnimationEnabled ? 'On' : 'Off') + '</button>';
        html += '<button class="toggle-raw sigil-view-large" type="button" aria-label="View sigil enlarged">View Large</button></div>';
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
          const gasFee = parseFloat(trade.gas_fee_eth);
          const gasHtml = Number.isFinite(gasFee) ? '<small class="trade-gas">⛽ ' + esc(gasFee.toFixed(8)) + ' ETH</small>' : '';
          html += '<div class="trade"><strong class="' + esc(trade.side) + '">' + esc(String(trade.side).toUpperCase()) + '</strong>' +
            '<span>' + esc(parseFloat(trade.eth_amount || 0).toFixed(8)) + ' ETH · ' + esc(formatTokenAmount(trade.token_amount)) + ' tokens</span>' +
            gasHtml + (txUrl ? '<a href="' + esc(txUrl) + '" target="_blank" rel="noopener noreferrer">tx</a>' : '') + '</div>';
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
          html += 'Amount: ' + esc(formatTokenAmount(pos.buy_amount_token)) + ' | ';
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
    const preservedLivePanels = updateGridPreservingLivePanels(html, force ? null : changedBotIds);
    if (!preservedLivePanels) {
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
    }
    container.querySelectorAll('pre[data-raw-scroll-key]').forEach(function(el) {
      el.scrollTop = rawJsonScroll.get(el.dataset.rawScrollKey) || 0;
    });
    container.querySelectorAll('details.chart-panel').forEach(function(panel) {
      const loadChart = function() {
        if (!panel.open) return;
        const frame = panel.querySelector('iframe.dex-chart');
        if (!frame || frame.src || frame.dataset.loading === 'true') return;
        frame.dataset.loading = 'true';
        fetch(frame.dataset.resolver)
          .then(function(response) {
            if (!response.ok) throw new Error('Chart lookup failed: ' + response.status);
            return response.json();
          })
          .then(function(data) {
            frame.dataset.loading = 'false';
            frame.src = data.chart_url;
          })
          .catch(function(error) {
            const resolver = frame.dataset.resolver;
            const errorState = document.createElement('div');
            errorState.className = 'chart-error';
            const message = document.createElement('span');
            message.textContent = error.message;
            const retry = document.createElement('button');
            retry.type = 'button';
            retry.className = 'chart-retry';
            retry.textContent = 'Retry';
            retry.addEventListener('click', function() {
              const replacement = document.createElement('iframe');
              replacement.className = 'dex-chart';
              replacement.loading = 'lazy';
              replacement.dataset.resolver = resolver;
              replacement.title = 'Dexscreener chart';
              errorState.replaceWith(replacement);
              loadChart();
            });
            errorState.append(message, retry);
            frame.replaceWith(errorState);
          });
      };
      if (panel.dataset.chartWired !== 'true') {
        panel.dataset.chartWired = 'true';
        panel.addEventListener('toggle', loadChart);
      }
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
      if (panel.dataset.sigilWired !== 'true') {
        panel.dataset.sigilWired = 'true';
        panel.addEventListener('toggle', drawSigil);
      }
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
    const statusButton = event.target.closest('[data-status-list]');
    if (statusButton) {
      openStatusList(statusButton.dataset.statusList, statusButton);
      return;
    }
    const historyButton = event.target.closest('[data-fleet-history]');
    if (historyButton) {
      openFleetHistory(historyButton);
      return;
    }
    const focusLink = event.target.closest('[data-focus-bot]');
    if (focusLink) {
      focusBotCard(focusLink.dataset.focusBot);
      return;
    }
    if (event.target.closest('[data-realized-unit-toggle]')) {
      realizedProfitUnit = { eth: 'cad', cad: 'usd', usd: 'eth' }[realizedProfitUnit];
      localStorage.setItem('dashboard-realized-profit-unit', realizedProfitUnit);
      render(true);
      return;
    }
    if (!event.target.closest('[data-currency-toggle], [data-bag-currency-toggle]')) return;
    profitCurrency = profitCurrency === 'usd' ? 'cad' : 'usd';
    localStorage.setItem('dashboard-profit-currency', profitCurrency);
    render(true);
  });
  summaryBar.addEventListener('change', function(event) {
    const selector = event.target.closest('[data-realized-period]');
    if (!selector) return;
    realizedProfitPeriod = selector.value;
    localStorage.setItem('dashboard-realized-profit-period', realizedProfitPeriod);
    selector.blur();
    render(true);
  });
  container.addEventListener('click', function(event) {
    const copyButton = event.target.closest('[data-copy-address]');
    if (copyButton) {
      copyText(copyButton.dataset.copyAddress).then(function() {
        copyButton.textContent = '✓';
        copyButton.classList.add('copied');
        copyButton.title = 'Copied';
        window.setTimeout(function() {
          if (!copyButton.isConnected) return;
          copyButton.textContent = '📋';
          copyButton.classList.remove('copied');
          copyButton.title = 'Copy address';
        }, 1200);
      }).catch(function() {
        copyButton.textContent = '!';
        copyButton.title = 'Copy failed';
      });
      return;
    }
    if (!event.target.closest('[data-bag-currency-toggle]')) return;
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
    localStorage.setItem('dashboard-bot-filter', botFilter.value);
    clearFilter.style.display = botFilter.value ? 'block' : 'none';
    render(true);
  });
  clearFilter.addEventListener('click', function() {
    botFilter.value = '';
    localStorage.setItem('dashboard-bot-filter', '');
    clearFilter.style.display = 'none';
    botFilter.focus();
    render(true);
  });
  clearAllFilters.addEventListener('click', function() {
    botFilter.value = '';
    chainFilter.value = '';
    providerFilter.value = '';
    taxFilterEnabled = false;
    localStorage.setItem('dashboard-bot-filter', '');
    localStorage.setItem('dashboard-chain-filter', '');
    localStorage.setItem('dashboard-provider-filter', '');
    localStorage.setItem('dashboard-tax-filter', 'false');
    updateTaxFilterButton();
    clearFilter.style.display = 'none';
    render(true);
  });
  connectionButton.addEventListener('click', function() {
    const opening = connectionDiagnostics.hidden;
    connectionDiagnostics.hidden = !opening;
    connectionButton.setAttribute('aria-expanded', String(opening));
    updateConnectionDiagnostics();
  });
  chainFilter.addEventListener('input', function() {
    localStorage.setItem('dashboard-chain-filter', chainFilter.value);
    render(true);
  });
  providerFilter.addEventListener('input', function() {
    localStorage.setItem('dashboard-provider-filter', providerFilter.value);
    render(true);
  });
  taxFilter.addEventListener('click', function() {
    taxFilterEnabled = !taxFilterEnabled;
    localStorage.setItem('dashboard-tax-filter', String(taxFilterEnabled));
    updateTaxFilterButton();
    render(true);
  });
  function updateSortDirectionButton() {
    sortDirection.textContent = sortDirectionValue === 'asc' ? 'Ascending ↑' : 'Descending ↓';
  }
  sortBots.addEventListener('change', function() {
    sortDirectionValue = defaultSortDirections[sortBots.value] || 'asc';
    localStorage.setItem('dashboard-sort-mode', sortBots.value);
    localStorage.setItem('dashboard-sort-direction', sortDirectionValue);
    updateSortDirectionButton();
    render(true);
  });
  sortDirection.addEventListener('click', function() {
    sortDirectionValue = sortDirectionValue === 'asc' ? 'desc' : 'asc';
    localStorage.setItem('dashboard-sort-direction', sortDirectionValue);
    updateSortDirectionButton();
    render(true);
  });
  updateSortDirectionButton();
  function updateNotificationControls() {
    notificationMenu.querySelectorAll('[data-notification-type]').forEach(function(input) {
      input.checked = Boolean(notificationPreferences[input.dataset.notificationType]);
    });
    const enabled = browserNotificationsEnabled();
    notificationsButton.textContent = enabled ? 'Notifications: On' : 'Notifications';
    notificationEnable.textContent = enabled ? 'Disable browser notifications' : 'Enable browser notifications';
  }
  function setNotificationMessage(message) {
    notificationNote.textContent = message;
  }
  function notificationBlockedMessage() {
    if (/Android/i.test(navigator.userAgent)) {
      return 'Notifications are blocked. Check both Android Settings › Apps › Chrome › Notifications and Chrome’s permission for doomdash.ca, then reload.';
    }
    return 'Notifications are blocked. Allow them in this browser’s site settings, then reload.';
  }
  function renderScout(snapshot) {
    const reports = Array.isArray(snapshot && snapshot.reports) ? snapshot.reports.slice() : [];
    const watchlist = Array.isArray(snapshot && snapshot.watchlist) ? snapshot.watchlist : [];
    const watched = new Map(watchlist.map(item => [String(item.address || '').toLowerCase(), item]));
    reports.sort((a, b) => Number(b.score || 0) - Number(a.score || 0));
    const counts = reports.reduce(function(out, report) {
      const verdict = ['pass', 'caution', 'reject'].includes(report.verdict) ? report.verdict : 'reject';
      out[verdict] += 1;
      return out;
    }, {pass: 0, caution: 0, reject: 0});
    scoutSummary.textContent = reports.length ?
      counts.pass + ' pass · ' + counts.caution + ' caution · ' + counts.reject + ' reject · ' + watchlist.length + ' watched' :
      'No candidates yet';
    const intervalMinutes = Math.max(1, Math.round(Number(snapshot && snapshot.interval_seconds || 900) / 60));
    const providerStatus = snapshot && snapshot.provider_status || {};
    scoutNote.textContent = 'Exact planned-size buy → sell checks · watched tokens rescan every ' + intervalMinutes +
      'm · Sushi ' + (providerStatus.sushiswap === 'configured' ? 'ready' : 'unavailable') +
      ' · Uniswap ' + (providerStatus.uniswap === 'configured' ? 'ready' : 'not configured');
    if (!reports.length) {
      scoutGrid.innerHTML = '<div class="scout-meta">No candidates assessed yet. Use <strong>/scout 0xTOKEN</strong> in Telegram.</div>';
      return;
    }
    scoutGrid.innerHTML = reports.map(function(report) {
      const market = report.market || {};
      const verdict = ['pass', 'caution', 'reject'].includes(report.verdict) ? report.verdict : 'reject';
      const reasons = Array.isArray(report.reasons) ? report.reasons : [];
      const warnings = Array.isArray(report.warnings) ? report.warnings : [];
      const providers = report.providers || {};
      const watchedItem = watched.get(String(report.address || '').toLowerCase());
      const recovery = report.best_recovery_percent == null ? 'no exit' : Number(report.best_recovery_percent).toFixed(1) + '% recovery';
      const assessed = report.assessed_at ? new Date(report.assessed_at).toLocaleString() : 'never';
      const ageMs = report.assessed_at ? Date.now() - new Date(report.assessed_at).getTime() : Infinity;
      const stale = ageMs > Math.max(1800000, Number(snapshot && snapshot.interval_seconds || 900) * 2000);
      const marketAge = market.age_hours == null ? 'age unknown' : Number(market.age_hours) < 48 ? Number(market.age_hours).toFixed(1) + 'h old' : (Number(market.age_hours) / 24).toFixed(1) + 'd old';
      const change = Number(market.price_change_h24 || 0);
      const routeHtml = ['sushiswap', 'uniswap'].map(function(name) {
        const route = providers[name] || {};
        const ok = Boolean(route.sell_success);
        const detail = ok ? Number(route.recovery_percent || 0).toFixed(1) + '% recovery' : esc(route.error || (route.buy_success ? 'no sell route' : 'no buy route'));
        return '<div class="' + (ok ? 'scout-route-ok' : 'scout-route-bad') + '">' + (ok ? '✓ ' : '× ') + esc(name === 'sushiswap' ? 'Sushi' : 'Uniswap') + ': ' + detail + '</div>';
      }).join('');
      const chartUrl = market.url && String(market.url).startsWith('https://') ? market.url : '';
      const explorerBase = (chainMetadata[Number(report.chain_id)] || {}).explorer || '';
      return '<article class="scout-card ' + verdict + '">' +
        '<div class="scout-card-head"><span>' + esc(market.symbol || String(report.address || '').slice(0, 10)) + ' · ' + esc(verdict.toUpperCase()) + (watchedItem ? ' 👁' : '') + '</span><span class="scout-score">' + Number(report.score || 0) + '/100</span></div>' +
        (market.name ? '<div class="scout-meta">' + esc(market.name) + '</div>' : '') +
        '<div class="scout-meta">' + esc(recovery) + ' · ' + Number(report.sell_provider_count || 0) + ' sell route(s)<br>' +
        '$' + Number(market.liquidity_usd || 0).toLocaleString() + ' liquidity · $' + Number(market.volume_h24 || 0).toLocaleString() + ' volume<br>' +
        esc(marketAge) + ' · 24h ' + (change >= 0 ? '+' : '') + change.toFixed(1) + '% · ' + Number(report.budget_eth || 0) + ' ETH / ' + Number(report.positions || 0) + ' positions<br>' +
        'Assessed ' + esc(assessed) + (stale ? ' · <span class="scout-stale">STALE</span>' : '') + '</div>' +
        '<div class="scout-routes">' + routeHtml + '</div>' +
        (reasons.length ? '<div class="scout-reason">' + esc(reasons.join(' · ').replaceAll('_', ' ').toLowerCase()) + '</div>' : '') +
        (warnings.length ? '<div class="scout-warning">' + esc(warnings.join(' · ')) + '</div>' : '') +
        '<div class="scout-links">' + (chartUrl ? '<a href="' + esc(chartUrl) + '" target="_blank" rel="noopener noreferrer">Chart ↗</a>' : '') +
        (explorerBase ? '<a href="' + esc(explorerBase + report.address) + '" target="_blank" rel="noopener noreferrer">Contract ↗</a>' : '') + '</div>' +
        '</article>';
    }).join('');
  }
  function refreshScout() {
    fetch('/api/scout', {cache: 'no-store'}).then(function(response) {
      if (!response.ok) throw new Error('scout unavailable');
      return response.json();
    }).then(renderScout).catch(function() {
      scoutGrid.innerHTML = '<div class="scout-meta">Scout data is temporarily unavailable.</div>';
    });
  }
  notificationsButton.addEventListener('click', function() {
    const opening = notificationMenu.hidden;
    notificationMenu.hidden = !opening;
    notificationsButton.setAttribute('aria-expanded', String(opening));
  });
  notificationMenu.addEventListener('click', function(event) { event.stopPropagation(); });
  notificationMenu.addEventListener('change', function(event) {
    const input = event.target.closest('[data-notification-type]');
    if (!input) return;
    notificationPreferences[input.dataset.notificationType] = input.checked;
    localStorage.setItem('dashboard-notification-preferences', JSON.stringify(notificationPreferences));
  });
  notificationEnable.addEventListener('click', async function() {
    if (browserNotificationsEnabled()) {
      notificationsMasterEnabled = false;
      localStorage.setItem('dashboard-notifications-enabled', 'false');
      notifiedOffline.clear();
      updateNotificationControls();
      setNotificationMessage('Browser alerts are off. Your category choices are still saved.');
      return;
    }
    if (!window.isSecureContext) { setNotificationMessage('Notifications require HTTPS.'); return; }
    if (!('Notification' in window)) {
      setNotificationMessage('This browser does not expose notifications. On iPhone, add doomdash.ca to the Home Screen and open it there.');
      return;
    }
    if (Notification.permission === 'denied') {
      setNotificationMessage(notificationBlockedMessage());
      return;
    }
    notificationEnable.disabled = true;
    notificationEnable.textContent = 'Requesting permission\u2026';
    try {
      const permission = await Notification.requestPermission();
      notificationsMasterEnabled = permission === 'granted';
      localStorage.setItem('dashboard-notifications-enabled', String(notificationsMasterEnabled));
      updateNotificationControls();
      setNotificationMessage(permission === 'granted'
        ? 'Browser alerts are on. Choices are saved in this browser.'
        : notificationBlockedMessage());
    } catch (_) {
      notificationsMasterEnabled = false;
      localStorage.setItem('dashboard-notifications-enabled', 'false');
      updateNotificationControls();
      setNotificationMessage('The browser could not open its permission prompt. On iPhone, add doomdash.ca to the Home Screen and open it there.');
    } finally {
      notificationEnable.disabled = false;
    }
  });
  document.addEventListener('click', function(event) {
    if (notificationMenu.hidden || event.target.closest('.notification-wrap')) return;
    notificationMenu.hidden = true;
    notificationsButton.setAttribute('aria-expanded', 'false');
  });
  updateNotificationControls();
  refreshScout();
  setInterval(refreshScout, 60000);
  refreshReportAges();
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
