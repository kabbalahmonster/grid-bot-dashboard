"""Server-side Telegram alerts with durable deduplication and chat commands."""

import json
import logging
import os
import threading
from collections import deque
from datetime import datetime, timezone

import requests


DEFAULT_PREFERENCES = {
    "sells": True,
    "stoploss": True,
    "positions": True,
    "offline": True,
    "recovered": False,
    "buys": False,
    "treasury": False,
    "errors": False,
}
CHAIN_EXPLORERS = {
    1: "https://etherscan.io/tx/",
    8453: "https://base.blockscout.com/tx/",
    4663: "https://robinhoodchain.blockscout.com/tx/",
}


class TelegramAlerts:
    def __init__(self, token, chat_id, state_file, state_provider, offline_seconds=300, request_timeout=10):
        self.token = str(token or "").strip()
        self.chat_id = str(chat_id or "").strip()
        self.state_file = state_file
        self.state_provider = state_provider
        self.offline_seconds = offline_seconds
        self.request_timeout = request_timeout
        self.log = logging.getLogger("dashboard.telegram")
        self.enabled = bool(self.token and self.chat_id)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._preferences = dict(DEFAULT_PREFERENCES)
        self._seen = deque(maxlen=5000)
        self._seen_set = set()
        self._offline_notified = set()
        self._update_offset = 0
        self._load()
        self._thread = None
 
    def start(self):
        """Start command polling once; called by the authenticated status path."""
        if self.enabled and self._thread is None:
            self._baseline_existing_offline_states()
            self._thread = threading.Thread(target=self._run, name="telegram-alerts", daemon=True)
            self._thread.start()

    def _baseline_existing_offline_states(self):
        """Do not replay pre-existing offline conditions when alerts start/restart."""
        now = datetime.now(timezone.utc)
        existing = set()
        for bot_id, state in self.state_provider().items():
            try:
                received = datetime.fromisoformat(str(state.get("received_at", "")).replace("Z", "+00:00"))
                if (now - received).total_seconds() >= self.offline_seconds:
                    existing.add(bot_id)
            except (TypeError, ValueError):
                continue
        if existing:
            with self._lock:
                self._offline_notified.update(existing)
                self._save_locked()

    def _load(self):
        try:
            with open(self.state_file, "r") as handle:
                state = json.load(handle)
            self._preferences.update(state.get("preferences", {}))
            self._seen = deque(state.get("seen", [])[-5000:], maxlen=5000)
            self._seen_set = set(self._seen)
            self._offline_notified = set(state.get("offline_notified", []))
            self._update_offset = int(state.get("update_offset", 0))
        except (FileNotFoundError, TypeError, ValueError, json.JSONDecodeError):
            pass

    def _save_locked(self):
        directory = os.path.dirname(self.state_file)
        if directory:
            os.makedirs(directory, exist_ok=True)
        temporary = self.state_file + ".tmp"
        with open(temporary, "w") as handle:
            json.dump({
                "preferences": self._preferences,
                "seen": list(self._seen),
                "offline_notified": sorted(self._offline_notified),
                "update_offset": self._update_offset,
            }, handle, indent=2)
        os.replace(temporary, self.state_file)

    def _remember(self, identity):
        with self._lock:
            if identity in self._seen_set:
                return False
            if len(self._seen) == self._seen.maxlen:
                self._seen_set.discard(self._seen[0])
            self._seen.append(identity)
            self._seen_set.add(identity)
            self._save_locked()
            return True

    def _wanted(self, category):
        with self._lock:
            return bool(self._preferences.get(category))

    def send(self, text, disable_preview=True):
        if not self.enabled:
            return False
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "disable_web_page_preview": disable_preview,
                },
                timeout=self.request_timeout,
            )
            response.raise_for_status()
            return True
        except requests.RequestException as exc:
            self.log.warning("Telegram send failed: %s", exc)
            return False

    @staticmethod
    def _trade_id(trade):
        return str(trade.get("tx_hash") or ":".join(str(trade.get(key, "")) for key in (
            "timestamp", "side", "eth_amount", "token_amount"
        )))

    @staticmethod
    def _event_id(event):
        return ":".join(str(event.get(key, "")) for key in ("code", "timestamp", "tx_hash"))

    @staticmethod
    def _name(bot_id, state):
        symbol = state.get("token_symbol")
        name = state.get("display_name") or bot_id
        return f"{name} · {symbol}" if symbol and symbol not in name else name

    @staticmethod
    def _tx_url(state, tx_hash):
        try:
            prefix = CHAIN_EXPLORERS.get(int(state.get("chain_id")))
        except (TypeError, ValueError):
            prefix = None
        return prefix + tx_hash if prefix and tx_hash else ""

    def process_status(self, bot_id, previous, current):
        if not self.enabled or not previous:
            return
        name = self._name(bot_id, current)
        previous_trades = {self._trade_id(trade) for trade in previous.get("trades_history", [])}
        for trade in current.get("trades_history", []):
            trade_id = self._trade_id(trade)
            if trade_id in previous_trades:
                continue
            side = str(trade.get("side", "")).lower()
            try:
                profit = float(trade.get("profit_eth"))
            except (TypeError, ValueError):
                profit = None
            category = "stoploss" if side == "sell" and profit is not None and profit < 0 else side + "s"
            if category not in ("sells", "stoploss", "buys") or not self._wanted(category):
                continue
            identity = f"trade:{category}:{bot_id}:{trade_id}"
            if not self._remember(identity):
                continue
            if side == "buy":
                amount = float(trade.get("eth_amount") or 0)
                message = f"🟢 Buy confirmed · {name}\n{amount:.8f} ETH"
            else:
                icon = "🔻" if category == "stoploss" else "💰"
                detail = f"{profit:+.8f} ETH profit" if profit is not None else f"{float(trade.get('eth_amount') or 0):.8f} ETH received"
                message = f"{icon} {'Stop-loss sell' if category == 'stoploss' else 'Sell confirmed'} · {name}\n{detail}"
            tx_url = self._tx_url(current, str(trade.get("tx_hash") or ""))
            self.send(message + (f"\n{tx_url}" if tx_url else ""))

        if self._wanted("positions") and not previous.get("capacity_warning") and current.get("capacity_warning"):
            identity = f"positions:{bot_id}:{current.get('received_at', '')}"
            if self._remember(identity):
                maximum = current["capacity_warning"].get("max_positions") or current.get("max_positions") or "all"
                self.send(f"⚑ Needs new positions · {name}\nAll {maximum} position slots are filled")

        previous_treasury = float(previous.get("treasury_sent_usdg") or 0)
        current_treasury = float(current.get("treasury_sent_usdg") or 0)
        if self._wanted("treasury") and current_treasury > previous_treasury:
            identity = f"treasury:{bot_id}:{current_treasury}"
            if self._remember(identity):
                self.send(f"🏦 Treasury transfer · {name}\n+{current_treasury - previous_treasury:.2f} USDG confirmed")

        previous_events = {self._event_id(event): int(event.get("count") or 1) for event in previous.get("events", [])}
        for event in current.get("events", []):
            event_id = self._event_id(event)
            if self._wanted("treasury") and event.get("level") == "success" and event.get("code") == "usdg_banked" and event_id not in previous_events:
                if self._remember(f"banking:{bot_id}:{event_id}"):
                    self.send(f"🏦 Profit banked · {name}\n{event.get('message') or 'USDG banking confirmed'}")
            count = int(event.get("count") or 1)
            if self._wanted("errors") and event.get("level") == "error" and count >= 3 and previous_events.get(event_id, 0) < 3:
                if self._remember(f"error:{bot_id}:{event_id}"):
                    self.send(f"🚨 Persistent error · {name}\n{event.get('message') or event.get('code') or 'Repeated bot error'}")

        recovered = False
        with self._lock:
            if bot_id in self._offline_notified:
                self._offline_notified.remove(bot_id)
                self._save_locked()
                recovered = True
        if recovered and self._wanted("recovered"):
            self.send(f"✅ Bot recovered · {name}\nStatus reports resumed")

    def scan_offline(self):
        if not self.enabled or not self._wanted("offline"):
            return
        now = datetime.now(timezone.utc)
        states = self.state_provider()
        for bot_id, state in states.items():
            try:
                received = datetime.fromisoformat(str(state.get("received_at", "")).replace("Z", "+00:00"))
                offline = (now - received).total_seconds() >= self.offline_seconds
            except (TypeError, ValueError):
                offline = False
            if not offline:
                continue
            with self._lock:
                if bot_id in self._offline_notified:
                    continue
                self._offline_notified.add(bot_id)
                self._save_locked()
            self.send(f"🔴 Bot offline · {self._name(bot_id, state)}\nNo status report for 5 minutes")

    def _preferences_text(self):
        with self._lock:
            lines = [f"{'✅' if enabled else '⬜'} {name}" for name, enabled in self._preferences.items()]
        return "DoomDash alerts\n\n" + "\n".join(lines) + "\n\nUse /alerts <type> on|off, or /test."

    def _handle_command(self, message):
        chat_id = str((message.get("chat") or {}).get("id", ""))
        if chat_id != self.chat_id:
            return
        text = str(message.get("text") or "").strip().lower()
        parts = text.split()
        command = parts[0].split("@")[0] if parts else ""
        if command in ("/start", "/alerts") and len(parts) == 1:
            self.send(self._preferences_text())
            return
        if command == "/test":
            self.send("🦊 DoomDash Telegram alerts are online.")
            return
        if len(parts) == 3 and command == "/alerts" and parts[1] in DEFAULT_PREFERENCES and parts[2] in ("on", "off"):
            with self._lock:
                self._preferences[parts[1]] = parts[2] == "on"
                self._save_locked()
            self.send(self._preferences_text())
            return
        if command == "/alerts":
            self.send("Unknown alert setting. Use /alerts to see valid types.")

    def _poll_commands(self):
        response = requests.get(
            f"https://api.telegram.org/bot{self.token}/getUpdates",
            params={"offset": self._update_offset, "timeout": 20, "allowed_updates": json.dumps(["message"])},
            timeout=25,
        )
        response.raise_for_status()
        for update in response.json().get("result", []):
            self._handle_command(update.get("message") or {})
            with self._lock:
                self._update_offset = max(self._update_offset, int(update["update_id"]) + 1)
                self._save_locked()

    def _run(self):
        while not self._stop.is_set():
            try:
                self.scan_offline()
                self._poll_commands()
            except requests.RequestException as exc:
                self.log.warning("Telegram polling failed: %s", exc)
                self._stop.wait(5)

    def close(self):
        self._stop.set()
