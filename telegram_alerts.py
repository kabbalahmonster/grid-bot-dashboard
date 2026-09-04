"""Server-side Telegram alerts with durable deduplication and chat commands."""

import json
import hashlib
import logging
import os
import re
import threading
from io import BytesIO
from collections import deque
from datetime import datetime, timedelta, timezone

import requests

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:  # Recaps remain optional until dependencies are installed.
    Image = ImageDraw = ImageFont = None


DEFAULT_PREFERENCES = {
    "sells": True,
    "stoploss": True,
    "positions": True,
    "offline": True,
    "recovered": False,
    "buys": False,
    "treasury": False,
    "errors": False,
    "funds": True,
    "safety": True,
    "fun": True,
}
CHAIN_EXPLORERS = {
    1: "https://etherscan.io/tx/",
    8453: "https://base.blockscout.com/tx/",
    4663: "https://robinhoodchain.blockscout.com/tx/",
}


class TelegramAlerts:
    def __init__(self, token, chat_id, state_file, state_provider, offline_seconds=300, request_timeout=10,
                 dashboard_url="https://doomdash.ca", low_funds_buffer_eth=0.0005,
                 unbanked_usdg_threshold=10.0, daily_digest_time="13:00", scout=None):
        self.token = str(token or "").strip()
        self.chat_id = str(chat_id or "").strip()
        self.state_file = state_file
        self.state_provider = state_provider
        self.offline_seconds = offline_seconds
        self.request_timeout = request_timeout
        self.dashboard_url = dashboard_url.rstrip("/")
        self.low_funds_buffer_eth = max(0.0, float(low_funds_buffer_eth))
        self.unbanked_usdg_threshold = max(0.0, float(unbanked_usdg_threshold))
        self.daily_digest_time = str(daily_digest_time or "").strip()
        self.scout = scout
        self.log = logging.getLogger("dashboard.telegram")
        self.enabled = bool(self.token and self.chat_id)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._preferences = dict(DEFAULT_PREFERENCES)
        self._seen = deque(maxlen=5000)
        self._seen_set = set()
        self._offline_notified = set()
        self._low_funds_notified = set()
        self._unbanked_usdg_notified = set()
        self._last_digest_date = None
        self._update_offset = 0
        self._muted_until = None
        self._muted_bots = {}
        self._achievement_state = {}
        self._leader = None
        self._rivalry_state = {}
        self._load()
        self._thread = None
 
    def start(self):
        """Start command polling once; called by the authenticated status path."""
        if not self.enabled:
            return
        self._baseline_existing_offline_states()
        with self._lock:
            if self._thread is not None:
                return
            thread = threading.Thread(target=self._run, name="telegram-alerts", daemon=True)
            self._thread = thread
        thread.start()

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
            self._low_funds_notified = set(state.get("low_funds_notified", []))
            self._unbanked_usdg_notified = set(state.get("unbanked_usdg_notified", []))
            self._last_digest_date = state.get("last_digest_date")
            self._update_offset = int(state.get("update_offset", 0))
            muted_until = state.get("muted_until")
            self._muted_until = datetime.fromisoformat(muted_until) if muted_until else None
            self._muted_bots = dict(state.get("muted_bots", {}))
            self._achievement_state = dict(state.get("achievement_state", {}))
            self._leader = state.get("leader")
            self._rivalry_state = dict(state.get("rivalry_state", {}))
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
                "low_funds_notified": sorted(self._low_funds_notified),
                "unbanked_usdg_notified": sorted(self._unbanked_usdg_notified),
                "last_digest_date": self._last_digest_date,
                "update_offset": self._update_offset,
                "muted_until": self._muted_until.isoformat() if self._muted_until else None,
                "muted_bots": self._muted_bots,
                "achievement_state": self._achievement_state,
                "leader": self._leader,
                "rivalry_state": self._rivalry_state,
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

    def _is_muted(self, bot_id=None):
        with self._lock:
            now = datetime.now(timezone.utc)
            if self._muted_until is not None and self._muted_until > now:
                return True
            if bot_id:
                try:
                    return datetime.fromisoformat(self._muted_bots.get(bot_id, "")) > now
                except ValueError:
                    pass
            return False

    def send(self, text, disable_preview=True, reply_markup=None, force=False, bot_id=None):
        if not self.enabled:
            return False
        if not force and self._is_muted(bot_id):
            return False
        try:
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "disable_web_page_preview": disable_preview,
            }
            if reply_markup:
                payload["reply_markup"] = reply_markup
            response = requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json=payload,
                timeout=self.request_timeout,
            )
            response.raise_for_status()
            return True
        except requests.RequestException as exc:
            self.log.warning("Telegram send failed: %s", exc)
            return False

    def send_photo(self, image, caption, reply_markup=None, force=False):
        if not self.enabled or (not force and self._is_muted()):
            return False
        try:
            image.seek(0)
            data = {"chat_id": self.chat_id, "caption": caption}
            if reply_markup:
                data["reply_markup"] = json.dumps(reply_markup)
            response = requests.post(
                f"https://api.telegram.org/bot{self.token}/sendPhoto",
                data=data, files={"photo": ("doomdash-recap.png", image, "image/png")},
                timeout=self.request_timeout,
            )
            response.raise_for_status()
            return True
        except requests.RequestException as exc:
            self.log.warning("Telegram recap send failed: %s", exc)
            return False

    def _alert_buttons(self, tx_url="", bot_id="", category=""):
        row = []
        if tx_url:
            row.append({"text": "View transaction ↗", "url": tx_url})
        target = self.dashboard_url
        if bot_id:
            target += f"?bot={requests.utils.quote(bot_id)}&chart=1"
        row.append({"text": "Open bot + chart ↗" if bot_id else "Open DoomDash ↗", "url": target})
        rows = [row]
        controls = []
        if bot_id:
            controls.append({"text": "Mute bot 6h", "callback_data": f"mutebot:{bot_id}"})
        if category in DEFAULT_PREFERENCES:
            controls.append({"text": f"Disable {category}", "callback_data": f"disable:{category}"})
        if controls:
            rows.append(controls)
        return {"inline_keyboard": rows}

    @staticmethod
    def _sell_stats(state):
        sells = [trade for trade in state.get("trades_history", [])
                 if str(trade.get("side", "")).lower() == "sell"]
        wins = sum(float(trade.get("profit_eth") or 0) > 0 for trade in sells)
        return len(sells), wins

    def _fun_buttons(self, bot_id, category, tx_url=""):
        return self._alert_buttons(tx_url, bot_id=bot_id, category=category)

    def _drama(self, bot_id, profit):
        if not self._wanted("fun"):
            return ""
        magnitude = abs(profit)
        if profit >= 0.01:
            choices = (
                "ABSOLUTE CINEMA.", "THE MACHINE DEMANDS APPLAUSE.", "CAPITAL HAS BEEN SUMMONED.",
                "A vulgar display of grid power.", "Somewhere, a bear has deleted the app.",
                "Profit so loud it violated local ordinances.", "The machine ate first and left no crumbs.",
                "The chart has been defeated in ritual combat.",
            )
        elif profit >= 0.003:
            choices = (
                "A juicy little extraction.", "Another victim of the grid.", "The fox approves.",
                "A modest heist, tastefully executed.", "Booked, banked, and emotionally unavailable.",
                "The grid found loose change in the market's couch.", "The candles have paid tribute.",
                "Clean work from a deeply unserious machine.",
            )
        elif profit <= -0.01:
            choices = (
                "A financially educational event.", "We have angered the chart gods.", "That candle chose violence.",
                "The market has submitted an invoice.", "This episode was directed by volatility.",
                "An expensive lesson in candle-based betrayal.", "The grid has entered its flop era.",
                "Please respect the privacy of the recently liquid.",
            )
        elif profit < 0:
            choices = (
                "A small blood offering.", "Character development.", "The market collected rent.",
                "A tiny tax paid to the chaos department.", "Barely a wound; dramatically a betrayal.",
                "The chart took a snack, not the whole lunch.", "Minor loss, major theatrical response.",
                "A paper cut in the ledger of destiny.",
            )
        else:
            return ""
        digest = hashlib.sha256(f"{bot_id}:{profit:.12f}".encode()).digest()[0]
        return choices[digest % len(choices)] if magnitude else ""

    @staticmethod
    def _deterministic_choice(key, choices):
        """Choose varied copy without making alerts or tests depend on randomness."""
        digest = hashlib.sha256(str(key).encode()).digest()
        return choices[int.from_bytes(digest[:4], "big") % len(choices)]

    def _achievement_messages(self, bot_id, previous, current):
        if not self._wanted("fun"):
            return []
        before_sells, before_wins = self._sell_stats(previous)
        saved = self._achievement_state.get(bot_id, {})
        old_total = int(saved.get("sells", before_sells))
        old_wins = int(saved.get("wins", before_wins))
        previous_ids = {self._trade_id(trade) for trade in previous.get("trades_history", [])}
        new_sells = [trade for trade in current.get("trades_history", [])
                     if str(trade.get("side", "")).lower() == "sell" and self._trade_id(trade) not in previous_ids]
        sells = old_total + len(new_sells)
        wins = old_wins + sum(float(trade.get("profit_eth") or 0) > 0 for trade in new_sells)
        messages = []
        if old_total == 0 < sells:
            messages.append("FIRST BLOOD · first confirmed sell")
        for milestone in (10, 25, 50, 100, 250, 500, 1000):
            if old_total < milestone <= sells:
                messages.append(f"{milestone} confirmed sells")
        if old_wins < 10 <= wins:
            messages.append("10 profitable sells")
        chronological = sorted(
            (trade for trade in current.get("trades_history", [])
             if str(trade.get("side", "")).lower() == "sell"),
            key=lambda trade: str(trade.get("timestamp", "")), reverse=True,
        )
        streak = 0
        for trade in chronological:
            if float(trade.get("profit_eth") or 0) <= 0:
                break
            streak += 1
        previous_streak = int(self._achievement_state.get(bot_id, {}).get("win_streak", 0))
        if streak in (3, 5, 10) and previous_streak < streak:
            messages.append(f"{streak}-sell profit streak")
        with self._lock:
            self._achievement_state[bot_id] = {"win_streak": streak, "sells": sells, "wins": wins}
            self._save_locked()
        if len(messages) <= 1:
            return messages

        # One status update can cross several counters at once (for example the
        # tenth sell can also be the tenth profitable sell and complete a
        # ten-win streak). Celebrate the event once instead of sending a stack
        # of nearly identical Telegram cards.
        if messages == ["10 confirmed sells", "10 profitable sells", "10-sell profit streak"]:
            return ["PERFECT TEN · 10 confirmed sells, all profitable"]
        return [" · ".join(messages)]

    def _rivalry_message(self, event, winner, loser, winner_score, loser_score, key):
        templates = {
            "rematch": (
                "{winner} reclaimed the 24h crown from {loser}. Same feud, fresh paperwork.",
                "{winner} took the crown back from {loser}. The rematch clause has been invoked.",
                "{winner} answered {loser}'s coup and returned to first. This rivalry has lore now.",
            ),
            "return": (
                "{winner} returned to the 24h throne, removing {loser} from the furniture.",
                "Former champion {winner} is back on top. {loser}'s reign enters the archives.",
                "{winner} completed the comeback and repossessed the crown from {loser}.",
                "{winner} remembered being champion and made it {loser}'s problem.",
            ),
            "profitline_coup": (
                "{winner} crossed into profit and took {loser}'s crown in the same motion.",
                "{winner} escaped the red and immediately deposed {loser}. Ambition is a disease.",
                "{winner} surfaced above zero holding {loser}'s crown. Efficient and deeply rude.",
                "{winner} found green territory; {loser} found out through this notification.",
            ),
            "underwater_pageant": (
                "{winner} passed {loser} to become the fleet's least underwater aristocrat.",
                "{winner} now leads the red kingdom. {loser} has sunk to a less prestigious depth.",
                "{winner} claimed first while everyone is underwater. A crown is a crown, darling.",
                "{winner} is losing the least, which legally counts as royalty today. Sorry, {loser}.",
            ),
            "collapse": (
                "{loser}'s lead collapsed and {winner} inherited the 24h crown. Brutal succession.",
                "{loser} dropped the crown; {winner} caught it before it hit the floor.",
                "The {loser} reign buckled. {winner} now controls the leaderboard.",
            ),
            "upset": (
                "{winner} came from behind to depose {loser}. The models demand a recount.",
                "Upset: {winner} erased the deficit and took {loser}'s crown.",
                "{winner} flipped the table on {loser} and left wearing the crown.",
            ),
            "narrow": (
                "{winner} edged past {loser} by a whisker. The crown is being held with tweezers.",
                "Photo finish: {winner} slipped ahead of {loser} for the 24h crown.",
                "{winner} leads {loser} by pocket lint. Technically, that still buys a crown.",
                "{winner} won the crown on a margin thin enough to qualify as gossip.",
            ),
            "dominant": (
                "{winner} seized the 24h crown from {loser} and opened daylight behind it.",
                "{winner} didn't just pass {loser}; it installed a moat around first place.",
                "{winner} took the throne from {loser} with an indecently large lead.",
                "{winner} took first from {loser} with enough daylight to install solar panels.",
            ),
            "crown_change": (
                "{winner} stole the 24h crown from {loser}. The leaderboard has become personal.",
                "New ruler: {winner} passed {loser} and claimed the 24h throne.",
                "{winner} has overthrown {loser}. Fleet politics remain deeply unserious.",
                "{loser}'s reign is over; {winner} now wears the 24h crown.",
                "{winner} overtook {loser}. Please update the tiny fleet history books.",
                "{winner} passed {loser} and immediately changed the locks on first place.",
                "{winner} staged a bloodless but extremely theatrical coup against {loser}.",
                "{winner} took the lead from {loser}. Sportsmanship was considered and rejected.",
            ),
        }
        body = self._deterministic_choice(key, templates[event]).format(winner=winner, loser=loser)
        margin = max(0.0, winner_score - loser_score)
        return (f"⚔️ Fleet rivalry · {event.replace('_', ' ')}\n{body}\n"
                f"24h realized: {winner} {winner_score:+.8f} ETH · {loser} {loser_score:+.8f} ETH\n"
                f"Lead: {margin:.8f} ETH")

    def _scan_rivalry(self, now=None):
        if not self._wanted("fun"):
            return
        now = now or datetime.now(timezone.utc)
        states = self.state_provider()
        if len(states) < 2:
            return
        ranked = sorted(states.items(), key=lambda item: self._realized_for_period(item[1], "24h"), reverse=True)
        leader_id, leader_state = ranked[0]
        scores = {bot_id: self._realized_for_period(state, "24h") for bot_id, state in states.items()}
        old_leader = self._leader
        prior_scores = dict(self._rivalry_state.get("scores", {}))
        crown_counts = dict(self._rivalry_state.get("crown_counts", {}))
        pair_counts = dict(self._rivalry_state.get("pair_counts", {}))
        with self._lock:
            self._leader = leader_id
            self._rivalry_state["scores"] = scores
            if not old_leader:
                crown_counts.setdefault(leader_id, 1)
                self._rivalry_state["crown_counts"] = crown_counts
            self._save_locked()
        if old_leader and old_leader != leader_id:
            old_state = states.get(old_leader, {})
            pair_key = ":".join(sorted((old_leader, leader_id)))
            winner_score, loser_score = scores[leader_id], scores.get(old_leader, 0.0)
            scale = max(abs(winner_score), abs(loser_score), 0.00000001)
            gap = winner_score - loser_score
            if pair_counts.get(pair_key, 0) > 0:
                event = "rematch"
            elif crown_counts.get(leader_id, 0) > 0:
                event = "return"
            elif winner_score > 0 >= loser_score:
                event = "profitline_coup"
            elif winner_score <= 0:
                event = "underwater_pageant"
            elif old_leader in prior_scores and loser_score < float(prior_scores[old_leader]):
                event = "collapse"
            elif leader_id in prior_scores and float(prior_scores[leader_id]) < float(prior_scores.get(old_leader, 0)):
                event = "upset"
            elif gap <= scale * 0.1:
                event = "narrow"
            elif gap >= scale * 0.75:
                event = "dominant"
            else:
                event = "crown_change"
            identity = f"rivalry:{old_leader}:{leader_id}:{now.date()}"
            message = self._rivalry_message(
                event, self._name(leader_id, leader_state), self._name(old_leader, old_state),
                winner_score, loser_score, identity,
            )
            if self._remember(identity):
                self.send(message, reply_markup=self._fun_buttons(leader_id, "fun"), bot_id=leader_id)
                with self._lock:
                    crown_counts[leader_id] = int(crown_counts.get(leader_id, 0)) + 1
                    pair_counts[pair_key] = int(pair_counts.get(pair_key, 0)) + 1
                    self._rivalry_state["crown_counts"] = crown_counts
                    self._rivalry_state["pair_counts"] = pair_counts
                    self._save_locked()

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
        if not self.enabled:
            return
        name = self._name(bot_id, current)
        if not previous:
            attempt = current.get("sell_attempt") or {}
            if self._wanted("safety") and attempt.get("status") == "position_balance_mismatch":
                identity = f"balance-mismatch:{bot_id}:{attempt.get('position_id')}:{attempt.get('deficit_raw')}"
                if self._remember(identity):
                    self.send(
                        f"🚨 POSITION BALANCE MISMATCH · {name}\n"
                        f"Sell blocked for position #{attempt.get('position_id', '?')}\n"
                        f"Tracked: {attempt.get('tracked_sell_amount_raw', '?')} raw\n"
                        f"Wallet: {attempt.get('wallet_balance_raw', '?')} raw\n"
                        f"Deficit: {attempt.get('deficit_raw', '?')} raw",
                        reply_markup=self._alert_buttons(),
                    )
            return
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
                drama = self._drama(bot_id, profit or 0)
                if drama:
                    message += f"\n{drama}"
            tx_url = self._tx_url(current, str(trade.get("tx_hash") or ""))
            self.send(message, reply_markup=self._fun_buttons(bot_id, category, tx_url), bot_id=bot_id)

        for achievement in self._achievement_messages(bot_id, previous, current):
            identity = f"achievement:{bot_id}:{achievement}"
            if self._remember(identity):
                self.send(f"🏆 Achievement unlocked · {name}\n{achievement}",
                          reply_markup=self._fun_buttons(bot_id, "fun"), bot_id=bot_id)
        self._scan_rivalry()

        if self._wanted("positions") and not previous.get("capacity_warning") and current.get("capacity_warning"):
            identity = f"positions:{bot_id}:{current.get('received_at', '')}"
            if self._remember(identity):
                maximum = current["capacity_warning"].get("max_positions") or current.get("max_positions") or "all"
                self.send(f"⚑ Needs new positions · {name}\nAll {maximum} position slots are filled",
                          reply_markup=self._alert_buttons())

        previous_attempt = previous.get("sell_attempt") or {}
        current_attempt = current.get("sell_attempt") or {}
        previous_mismatch = previous_attempt.get("status") == "position_balance_mismatch"
        current_mismatch = current_attempt.get("status") == "position_balance_mismatch"
        if self._wanted("safety") and current_mismatch and not previous_mismatch:
            identity = f"balance-mismatch:{bot_id}:{current_attempt.get('position_id')}:{current_attempt.get('deficit_raw')}"
            if self._remember(identity):
                self.send(
                    f"🚨 POSITION BALANCE MISMATCH · {name}\n"
                    f"Sell blocked for position #{current_attempt.get('position_id', '?')}\n"
                    f"Tracked: {current_attempt.get('tracked_sell_amount_raw', '?')} raw\n"
                    f"Wallet: {current_attempt.get('wallet_balance_raw', '?')} raw\n"
                    f"Deficit: {current_attempt.get('deficit_raw', '?')} raw",
                    reply_markup=self._alert_buttons(),
                )
        elif self._wanted("safety") and previous_mismatch and not current_mismatch:
            identity = f"balance-recovered:{bot_id}:{previous_attempt.get('position_id')}:{previous_attempt.get('deficit_raw')}"
            if self._remember(identity):
                self.send(f"✅ Balance reconciled · {name}\nPosition accounting matches the wallet again",
                          reply_markup=self._alert_buttons())

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
                    tx_url = self._tx_url(current, str(event.get("tx_hash") or ""))
                    self.send(
                        f"🏦 Profit banked · {name}\n"
                        f"{event.get('message') or 'USDG banking confirmed'}",
                        reply_markup=self._alert_buttons(tx_url),
                    )
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
            self.send(f"🔴 Bot offline · {self._name(bot_id, state)}\nNo status report for 5 minutes",
                      reply_markup=self._alert_buttons())

    def scan_funds(self):
        if not self.enabled or not self._wanted("funds"):
            return
        for bot_id, state in self.state_provider().items():
            issue = self._funds_issue(state)
            usdg = float(state.get("usdg_balance") or 0)
            with self._lock:
                before = (bot_id in self._low_funds_notified, bot_id in self._unbanked_usdg_notified)
                notify_low = bool(issue and bot_id not in self._low_funds_notified)
                notify_usdg = bool(self.unbanked_usdg_threshold > 0 and
                                   usdg >= self.unbanked_usdg_threshold and
                                   bot_id not in self._unbanked_usdg_notified)
                if notify_low:
                    self._low_funds_notified.add(bot_id)
                elif not issue:
                    self._low_funds_notified.discard(bot_id)
                if notify_usdg:
                    self._unbanked_usdg_notified.add(bot_id)
                elif usdg < self.unbanked_usdg_threshold * 0.5:
                    self._unbanked_usdg_notified.discard(bot_id)
                after = (bot_id in self._low_funds_notified, bot_id in self._unbanked_usdg_notified)
                if before != after:
                    self._save_locked()
            if notify_low:
                balance, reserve, _ = issue
                self.send(f"⛽ Low ETH · {self._name(bot_id, state)}\n{balance:.8f} ETH remaining · reserve {reserve:.8f}",
                          reply_markup=self._alert_buttons())
            if notify_usdg:
                self.send(f"🏦 USDG awaiting banking · {self._name(bot_id, state)}\n{usdg:.2f} USDG in bot wallet",
                          reply_markup=self._alert_buttons())

    def _daily_digest_text(self):
        states = self.state_provider()
        trades = self._recent_trades("24h")
        values = [(self._name(bot_id, state), self._realized_for_period(state, "24h"))
                  for bot_id, state in states.items()]
        ranked = sorted(values, key=lambda item: item[1], reverse=True)
        crypto_value = sum(float(state.get("eth_balance") or 0) +
                           float(state.get("token_balance") or 0) * float(state.get("price") or 0)
                           for state in states.values())
        usdg = sum(float(state.get("usdg_balance") or 0) for state in states.values())
        buys = sum(str(trade.get("side", "")).lower() == "buy" for _, _, trade in trades)
        sells = sum(str(trade.get("side", "")).lower() == "sell" for _, _, trade in trades)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        banked = 0.0
        for state in states.values():
            for event in state.get("events", []):
                if event.get("code") != "usdg_banked":
                    continue
                try:
                    stamp = datetime.fromisoformat(str(event.get("timestamp", "")).replace("Z", "+00:00"))
                    if stamp >= cutoff:
                        banked += float(event.get("usdg_amount") or 0)
                except (TypeError, ValueError):
                    continue
        lines = ["🌅 DoomDash daily digest", "",
                 f"Fleet crypto value: ~{crypto_value:.8f} ETH + {usdg:.2f} USDG",
                 f"24h realized: {sum(value for _, value in values):+.8f} ETH",
                 f"Trades: {len(trades)} · {buys} buys · {sells} sells",
                 f"Treasury banked: {banked:.2f} USDG"]
        if ranked:
            lines.extend((f"Best: {ranked[0][0]} · {ranked[0][1]:+.8f} ETH",
                          f"Worst: {ranked[-1][0]} · {ranked[-1][1]:+.8f} ETH"))
        lines.extend(("", self._attention_text(), "", self._oracle_text()))
        return "\n".join(lines)

    def scan_daily_digest(self, now=None):
        now = now or datetime.now(timezone.utc)
        match = re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", self.daily_digest_time)
        if not self.enabled or not match or (now.hour, now.minute) < (int(match.group(1)), int(match.group(2))):
            return
        day = now.date().isoformat()
        with self._lock:
            if self._last_digest_date == day:
                return
            self._last_digest_date = day
            self._save_locked()
        self.send(self._daily_digest_text(), reply_markup=self._alert_buttons())

    def _settings_markup(self):
        with self._lock:
            preferences = dict(self._preferences)
        buttons = []
        names = list(DEFAULT_PREFERENCES)
        for index in range(0, len(names), 2):
            row = []
            for name in names[index:index + 2]:
                row.append({
                    "text": f"{'✅' if preferences[name] else '⬜'} {name}",
                    "callback_data": f"toggle:{name}",
                })
            buttons.append(row)
        buttons.append([{"text": "Open DoomDash ↗", "url": self.dashboard_url}])
        return {"inline_keyboard": buttons}

    def _preferences_text(self):
        with self._lock:
            lines = [f"{'✅' if enabled else '⬜'} {name}" for name, enabled in self._preferences.items()]
        mute = ""
        with self._lock:
            if self._muted_until and self._muted_until > datetime.now(timezone.utc):
                mute = f"\n\n🔕 Muted until {self._muted_until.strftime('%Y-%m-%d %H:%M UTC')}"
        return "DoomDash alerts\n\n" + "\n".join(lines) + mute + "\n\nTap a button or use /alerts <type> on|off."

    @staticmethod
    def _age_seconds(state):
        try:
            received = datetime.fromisoformat(str(state.get("received_at", "")).replace("Z", "+00:00"))
            return max(0, (datetime.now(timezone.utc) - received).total_seconds())
        except (TypeError, ValueError):
            return float("inf")

    def _fleet_status_text(self):
        states = self.state_provider()
        running = sum(self._age_seconds(state) < self.offline_seconds for state in states.values())
        needs = [self._name(bot_id, state) for bot_id, state in states.items() if state.get("capacity_warning")]
        eth = sum(float(state.get("eth_balance") or 0) for state in states.values())
        usdg = sum(float(state.get("usdg_balance") or 0) for state in states.values())
        session = sum(float(state.get("session_profit_eth") or 0) for state in states.values())
        realized = sum(float(state.get("realized_profit_eth") or 0) for state in states.values())
        return (
            "⚡ DoomDash fleet\n\n"
            f"🟢 Running: {running}/{len(states)}\n"
            f"⚑ Needs positions: {len(needs)}" + ((" · " + ", ".join(needs)) if needs else "") + "\n"
            f"Ξ Total ETH: {eth:.8f}\n"
            f"💵 USDG: {usdg:.2f}\n"
            f"📈 Session: {session:+.8f} ETH\n"
            f"🏆 Realized: {realized:+.8f} ETH"
        )

    @staticmethod
    def _period_hours(period):
        return {"1h": 1, "6h": 6, "24h": 24, "week": 168, "month": 720}.get(period)

    def _realized_for_period(self, state, period):
        if period == "all":
            return float(state.get("realized_profit_eth") or 0)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self._period_hours(period))
        try:
            received_at = datetime.fromisoformat(str(state.get("received_at", "")).replace("Z", "+00:00"))
            if received_at <= cutoff:
                return 0.0
        except (TypeError, ValueError):
            pass
        reported = (state.get("realized_profit_periods") or {}).get(period)
        try:
            return float(reported)
        except (TypeError, ValueError):
            pass
        total = 0.0
        for trade in state.get("trades_history", []):
            try:
                stamp = datetime.fromisoformat(str(trade.get("timestamp", "")).replace("Z", "+00:00"))
            except (TypeError, ValueError):
                continue
            if trade.get("side") == "sell" and stamp >= cutoff:
                total += float(trade.get("profit_eth") or 0)
        return total

    def _profit_text(self, period):
        values = [(self._name(bot_id, state), self._realized_for_period(state, period))
                  for bot_id, state in self.state_provider().items()]
        ranked = sorted(values, key=lambda item: item[1], reverse=True)
        lines = [f"📈 Realized profit · {period}", "", f"Fleet: {sum(value for _, value in values):+.8f} ETH"]
        if ranked:
            lines.extend((f"Best: {ranked[0][0]} · {ranked[0][1]:+.8f} ETH",
                          f"Worst: {ranked[-1][0]} · {ranked[-1][1]:+.8f} ETH"))
        return "\n".join(lines)

    def _leaderboard_text(self, mode="24h"):
        states = self.state_provider()
        rows = []
        if mode == "winrate":
            for bot_id, state in states.items():
                sells, wins = self._sell_stats(state)
                rows.append((bot_id, state, wins / sells if sells else -1, f"{wins}/{sells} · {(100 * wins / sells):.0f}%" if sells else "no sells"))
            title = "Win rate · recent history"
        elif mode == "treasury":
            for bot_id, state in states.items():
                value = float(state.get("treasury_sent_usdg") or 0)
                rows.append((bot_id, state, value, f"{value:.2f} USDG"))
            title = "Treasury contribution · all time"
        else:
            for bot_id, state in states.items():
                value = self._realized_for_period(state, mode)
                rows.append((bot_id, state, value, f"{value:+.8f} ETH"))
            title = f"Realized profit · {mode}"
        rows.sort(key=lambda row: row[2], reverse=True)
        medals = ("🥇", "🥈", "🥉")
        lines = [f"🏁 DoomDash leaderboard\n{title}", ""]
        for index, (bot_id, state, _, display) in enumerate(rows[:10]):
            rank = medals[index] if index < 3 else f"{index + 1}."
            lines.append(f"{rank} {self._name(bot_id, state)} · {display}")
        return "\n".join(lines) if rows else "No bots on the leaderboard yet."

    def _oracle_text(self):
        states = self.state_provider()
        if not states:
            return "🔮 The Doom Oracle sees only an empty fleet. Feed the machine."
        ranked = sorted(states.items(), key=lambda item: self._realized_for_period(item[1], "24h"), reverse=True)
        leader_id, leader = ranked[0]
        total = sum(self._realized_for_period(state, "24h") for state in states.values())
        sigil = str((leader.get("sigil") or {}).get("seed") or leader_id)
        sigil_key = str((leader.get("sigil") or {}).get("key") or "UNMARKED")
        fortunes = (
            "The grid hums. Take clean profit and do not become emotionally attached to a candle.",
            "Volatility approaches wearing cheap perfume. Keep the gas tanks fed.",
            "The treasury desires tribute. The chart desires humiliation. Only one gets paid.",
            "A green candle is not a personality. Let the bots remain disciplined.",
            "The fox sees opportunity, but also several extremely suspicious wicks.",
            "Today favors patience, sharp exits, and refusing to marry the bags.",
        )
        index = hashlib.sha256(f"{datetime.now(timezone.utc).date()}:{sigil}".encode()).digest()[0] % len(fortunes)
        mood = "feral prosperity" if total > 0 else "character development" if total < 0 else "ominous neutrality"
        return (f"🔮 Daily Doom Oracle\nFleet mood: {mood}\nChosen vessel: {self._name(leader_id, leader)}\n"
                f"Sigil of the day: {sigil_key}\n\n"
                f"{fortunes[index]}")

    @staticmethod
    def _font(size, bold=False):
        if ImageFont is None:
            return None
        paths = (["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                  "/usr/share/fonts/google-noto-vf/NotoSans[wght].ttf"] if bold else
                 ["/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                  "/usr/share/fonts/google-noto-vf/NotoSans[wght].ttf"])
        for path in paths:
            try:
                return ImageFont.truetype(path, size)
            except OSError:
                continue
        return ImageFont.load_default()

    def _recap_image(self, period):
        if Image is None:
            return None
        states = self.state_provider()
        ranked = sorted(states.items(), key=lambda item: self._realized_for_period(item[1], period), reverse=True)
        trades = self._recent_trades(period)
        realized = sum(self._realized_for_period(state, period) for state in states.values())
        treasury = sum(float(state.get("treasury_sent_usdg") or 0) for state in states.values())
        image = Image.new("RGB", (1200, 675), "#07111f")
        draw = ImageDraw.Draw(image)
        draw.rectangle((0, 0, 1200, 16), fill="#facc15")
        draw.text((70, 55), "DOOMDASH", font=self._font(64, True), fill="#f8fafc")
        draw.text((73, 130), f"{period.upper()} FLEET RECAP", font=self._font(28, True), fill="#facc15")
        profit_color = "#22c55e" if realized >= 0 else "#ef4444"
        draw.text((70, 220), f"{realized:+.8f} ETH", font=self._font(52, True), fill=profit_color)
        draw.text((73, 285), f"REALIZED · {len(trades)} TRADES · {treasury:.2f} USDG TREASURY", font=self._font(21), fill="#94a3b8")
        draw.text((700, 65), "LEADERBOARD", font=self._font(28, True), fill="#f8fafc")
        for index, (bot_id, state) in enumerate(ranked[:5]):
            value = self._realized_for_period(state, period)
            y = 125 + index * 72
            draw.text((700, y), f"{index + 1}", font=self._font(24, True), fill="#facc15")
            draw.text((750, y), self._name(bot_id, state)[:22], font=self._font(24, True), fill="#e2e8f0")
            draw.text((750, y + 31), f"{value:+.8f} ETH", font=self._font(18), fill="#94a3b8")
        winner = self._name(ranked[0][0], ranked[0][1]) if ranked else "THE VOID"
        draw.text((70, 410), "MVP", font=self._font(22, True), fill="#facc15")
        draw.text((70, 450), winner, font=self._font(42, True), fill="#f8fafc")
        draw.text((70, 610), "doomdash.ca  ·  THE GRID NEVER SLEEPS", font=self._font(18, True), fill="#64748b")
        output = BytesIO()
        image.save(output, "PNG", optimize=True)
        output.seek(0)
        return output

    def _recent_trades(self, period):
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self._period_hours(period))
        trades = []
        for bot_id, state in self.state_provider().items():
            for trade in state.get("trades_history", []):
                try:
                    stamp = datetime.fromisoformat(str(trade.get("timestamp", "")).replace("Z", "+00:00"))
                except (TypeError, ValueError):
                    continue
                if stamp >= cutoff:
                    trades.append((stamp, self._name(bot_id, state), trade))
        return sorted(trades, key=lambda item: item[0], reverse=True)

    def _trades_text(self, period):
        trades = self._recent_trades(period)
        buys = sum(str(trade.get("side", "")).lower() == "buy" for _, _, trade in trades)
        sells = sum(str(trade.get("side", "")).lower() == "sell" for _, _, trade in trades)
        profit = sum(float(trade.get("profit_eth") or 0) for _, _, trade in trades
                     if str(trade.get("side", "")).lower() == "sell")
        lines = [f"🔁 Trades · {period}", "", f"{len(trades)} total · {buys} buys · {sells} sells",
                 f"Realized: {profit:+.8f} ETH"]
        for stamp, name, trade in trades[:10]:
            side = str(trade.get("side", "trade")).upper()
            amount = float(trade.get("profit_eth") or 0) if side == "SELL" else float(trade.get("eth_amount") or 0)
            suffix = "profit" if side == "SELL" else "spent"
            lines.append(f"• {stamp.strftime('%H:%M')} {name} · {side} · {amount:+.8f} ETH {suffix}")
        if len(trades) > 10:
            lines.append(f"…and {len(trades) - 10} more")
        return "\n".join(lines)

    def _funds_issue(self, state):
        try:
            balance = float(state.get("eth_balance"))
            reserve = float(state.get("gas_reserve_eth"))
        except (TypeError, ValueError):
            return None
        threshold = reserve + self.low_funds_buffer_eth
        return (balance, reserve, threshold) if balance <= threshold else None

    def _attention_text(self):
        sections = {"offline": [], "stale": [], "positions": [], "sells": [], "safety": [], "errors": [], "funds": []}
        for bot_id, state in self.state_provider().items():
            name = self._name(bot_id, state)
            age = self._age_seconds(state)
            if age >= self.offline_seconds:
                sections["offline"].append(name)
            elif age >= 120:
                sections["stale"].append(name)
            if state.get("capacity_warning"):
                sections["positions"].append(name)
            if state.get("sell_attempt"):
                sections["sells"].append(name)
            if (state.get("sell_attempt") or {}).get("status") == "position_balance_mismatch":
                sections["safety"].append(name)
            if self._funds_issue(state):
                sections["funds"].append(name)
            repeated = [event for event in state.get("events", [])
                        if event.get("level") == "error" and int(event.get("count") or 1) >= 3]
            if repeated or str(state.get("rpc_status", "ok")).lower() not in ("", "ok"):
                sections["errors"].append(name)
        labels = (("offline", "🔴 Offline"), ("stale", "🟠 Stale"), ("safety", "🚨 Balance mismatch"), ("positions", "⚑ Needs positions"),
                  ("sells", "🔎 Sell checks"), ("errors", "🚨 Errors/RPC"), ("funds", "⛽ Low funds"))
        lines = ["🚦 DoomDash attention", ""]
        for key, label in labels:
            if sections[key]:
                lines.append(f"{label}: {len(sections[key])} · {', '.join(sections[key])}")
        if len(lines) == 2:
            lines.append("✅ Nothing needs attention.")
        return "\n".join(lines)

    def _needs_text(self):
        needs = [(bot_id, state) for bot_id, state in self.state_provider().items() if state.get("capacity_warning")]
        if not needs:
            return "✅ No bots currently need new positions."
        lines = ["⚑ Needs new positions"]
        for bot_id, state in needs:
            maximum = (state.get("capacity_warning") or {}).get("max_positions") or state.get("max_positions") or "?"
            lines.append(f"• {self._name(bot_id, state)} · {maximum} slots full")
        return "\n".join(lines)

    def _bot_text(self, query):
        query = query.strip().lower()
        matches = []
        for bot_id, state in self.state_provider().items():
            terms = (bot_id, state.get("display_name", ""), state.get("token_symbol", ""))
            if any(query == str(term).lower() for term in terms):
                matches.append((bot_id, state))
        if not matches:
            return f"No bot found for “{query}”."
        bot_id, state = matches[0]
        age = self._age_seconds(state)
        status = "running" if age < self.offline_seconds else "offline"
        filled = int(state.get("filled_positions") or 0)
        maximum = int(state.get("max_positions") or 0)
        return (
            f"🤖 {self._name(bot_id, state)}\n\n"
            f"Status: {status}\n"
            f"Positions: {filled}/{maximum}\n"
            f"ETH: {float(state.get('eth_balance') or 0):.8f}\n"
            f"USDG: {float(state.get('usdg_balance') or 0):.2f}\n"
            f"Session: {float(state.get('session_profit_eth') or 0):+.8f} ETH\n"
            f"Realized: {float(state.get('realized_profit_eth') or 0):+.8f} ETH\n"
            f"24h move: {state.get('token_symbol') or 'token'} market data is on DoomDash"
        )

    def _handle_command(self, message):
        chat_id = str((message.get("chat") or {}).get("id", ""))
        if chat_id != self.chat_id:
            return
        text = str(message.get("text") or "").strip().lower()
        parts = text.split()
        command = parts[0].split("@")[0] if parts else ""
        if command == "/scout":
            if self.scout is None:
                self.send("DoomScout is unavailable.", force=True)
            elif len(parts) < 2:
                self.send("Use /scout 0xTOKEN [budget_eth] [positions].", force=True)
            else:
                try:
                    budget = float(parts[2]) if len(parts) >= 3 else 0.003
                    positions = int(parts[3]) if len(parts) >= 4 else 4
                    self.send("🔭 Scouting executable exits now…", force=True)
                    self.send(self._scout_text(self.scout.assess(parts[1], budget_eth=budget, positions=positions)), force=True)
                except (ValueError, TypeError) as exc:
                    self.send(f"Scout rejected that request: {exc}", force=True)
                except Exception as exc:
                    self.log.warning("Scout command failed: %s", exc)
                    self.send("Scout providers are temporarily unavailable.", force=True)
            return
        if command == "/watch":
            if self.scout is None or len(parts) < 2:
                self.send("Use /watch 0xTOKEN [label].", force=True)
            else:
                try:
                    item = self.scout.watch(parts[1], " ".join(parts[2:]))
                    report = self.scout.assess(item["address"], budget_eth=item["budget_eth"], positions=item["positions"])
                    self.send("👁 Added to DoomScout watchlist.\n\n" + self._scout_text(report), force=True)
                except (ValueError, TypeError) as exc:
                    self.send(f"Could not watch token: {exc}", force=True)
            return
        if command == "/unwatch":
            if self.scout is None or len(parts) != 2:
                self.send("Use /unwatch 0xTOKEN.", force=True)
            else:
                try:
                    removed = self.scout.unwatch(parts[1])
                    self.send("Removed from DoomScout." if removed else "That token was not watched.", force=True)
                except ValueError as exc:
                    self.send(str(exc), force=True)
            return
        if command == "/forget":
            if self.scout is None or len(parts) != 2:
                self.send("Use /forget 0xTOKEN.", force=True)
            else:
                try:
                    removed = self.scout.forget(parts[1])
                    self.send("Candidate and retained Scout history removed." if removed else "Scout did not know that token.", force=True)
                except ValueError as exc:
                    self.send(str(exc), force=True)
            return
        if command == "/candidates":
            self.send(self._candidates_text(), force=True)
            return
        if command == "/discover":
            if self.scout is None:
                self.send("DoomScout is unavailable.", force=True)
            else:
                try:
                    found = self.scout.discover(10)
                    lines = ["🛰 Recent Robinhood Chain token profiles (unscored)"]
                    for item in found:
                        symbol = item.get("symbol") or "Unknown token"
                        name = item.get("name") or ""
                        title = f"{symbol} — {name}" if name and name.lower() != symbol.lower() else symbol
                        details = []
                        if item.get("liquidity_usd") is not None:
                            details.append(f"liq ${item['liquidity_usd']:,.0f}")
                        if item.get("volume_h24") is not None:
                            details.append(f"24h vol ${item['volume_h24']:,.0f}")
                        if item.get("price_change_h24") is not None:
                            details.append(f"24h {item['price_change_h24']:+.1f}%")
                        if item.get("age_hours") is not None:
                            age = item["age_hours"]
                            details.append(f"age {age / 24:.1f}d" if age >= 24 else f"age {age:.1f}h")
                        lines.append(f"\n• {title}\n  {item['address']}" +
                                     (f"\n  {' · '.join(details)}" if details else " · no market data"))
                    self.send("\n".join(lines) if found else "No recent Robinhood token profiles found.", force=True)
                except Exception as exc:
                    self.log.warning("Scout discovery failed: %s", exc)
                    self.send("Discovery feed is temporarily unavailable.", force=True)
            return
        if command in ("/start", "/alerts") and len(parts) == 1:
            self.send(self._preferences_text(), reply_markup=self._settings_markup(), force=True)
            return
        if command == "/test":
            self.send("🦊 DoomDash Telegram alerts are online.", reply_markup=self._alert_buttons(), force=True)
            return
        if command == "/status":
            self.send(self._fleet_status_text(), reply_markup=self._alert_buttons(), force=True)
            return
        if command == "/needs":
            self.send(self._needs_text(), reply_markup=self._alert_buttons(), force=True)
            return
        if command == "/attention":
            self.send(self._attention_text(), reply_markup=self._alert_buttons(), force=True)
            return
        if command == "/profit":
            period = parts[1] if len(parts) == 2 else "24h"
            if period not in ("1h", "6h", "24h", "week", "month", "all"):
                self.send("Use /profit 1h, 6h, 24h, week, month, or all.", force=True)
            else:
                self.send(self._profit_text(period), reply_markup=self._alert_buttons(), force=True)
            return
        if command == "/trades":
            period = parts[1] if len(parts) == 2 else "24h"
            if period not in ("1h", "6h", "24h", "week", "month"):
                self.send("Use /trades 1h, 6h, 24h, week, or month.", force=True)
            else:
                self.send(self._trades_text(period), reply_markup=self._alert_buttons(), force=True)
            return
        if command == "/digest":
            self.send(self._daily_digest_text(), reply_markup=self._alert_buttons(), force=True)
            return
        if command == "/oracle":
            self.send(self._oracle_text(), reply_markup=self._alert_buttons(), force=True)
            return
        if command == "/leaderboard":
            mode = parts[1] if len(parts) == 2 else "24h"
            if mode not in ("24h", "week", "month", "all", "winrate", "treasury"):
                self.send("Use /leaderboard 24h, week, month, all, winrate, or treasury.", force=True)
            else:
                self.send(self._leaderboard_text(mode), reply_markup=self._alert_buttons(), force=True)
            return
        if command == "/recap":
            period = parts[1] if len(parts) == 2 else "week"
            if period not in ("24h", "week", "month"):
                self.send("Use /recap 24h, week, or month.", force=True)
                return
            image = self._recap_image(period)
            if image is None:
                self.send("Recap image support is unavailable until Pillow is installed.", force=True)
            else:
                self.send_photo(image, f"🦊 DoomDash {period} fleet recap", self._alert_buttons(), force=True)
            return
        if command == "/bot" and len(parts) >= 2:
            self.send(self._bot_text(" ".join(parts[1:])), reply_markup=self._alert_buttons(), force=True)
            return
        if command == "/mute" and len(parts) == 2:
            match = re.fullmatch(r"(1|6|12|24)h", parts[1])
            if not match:
                self.send("Use /mute 1h, /mute 6h, /mute 12h, or /mute 24h.", force=True)
                return
            with self._lock:
                self._muted_until = datetime.now(timezone.utc) + timedelta(hours=int(match.group(1)))
                self._save_locked()
            self.send(f"🔕 Alerts muted for {match.group(1)}h. Commands still work.", force=True)
            return
        if command == "/unmute":
            with self._lock:
                self._muted_until = None
                self._save_locked()
            self.send("🔔 Alerts unmuted.", force=True)
            return
        if command == "/help":
            self.send(
                "DoomDash commands\n\n/status — fleet snapshot\n/needs — bots needing positions\n"
                "/attention — exceptions\n/profit [period] — realized performance\n"
                "/trades [period] — recent activity\n/digest — daily report now\n/bot <name> — one bot\n"
                "/leaderboard [mode] — fleet rankings\n/recap [period] — share card\n/oracle — daily fleet omen\n"
                "/scout <contract> [budget] [positions] — executable exit test\n"
                "/watch <contract> [label] — monitor candidate\n/unwatch <contract> — stop rescans\n/forget <contract> — remove candidate and history\n/candidates — ranked watchlist\n"
                "/discover — recent unscored token profiles\n"
                "/alerts — alert toggles\n/mute 1h|6h|12h|24h\n"
                "/unmute\n/test",
                reply_markup=self._alert_buttons(), force=True,
            )
            return
        if len(parts) == 3 and command == "/alerts" and parts[1] in DEFAULT_PREFERENCES and parts[2] in ("on", "off"):
            with self._lock:
                self._preferences[parts[1]] = parts[2] == "on"
                self._save_locked()
            self.send(self._preferences_text(), reply_markup=self._settings_markup(), force=True)
            return
        if command == "/alerts":
            self.send("Unknown alert setting. Use /alerts to see valid types.", force=True)

    def _handle_callback(self, callback):
        callback_id = callback.get("id")
        message = callback.get("message") or {}
        chat_id = str((message.get("chat") or {}).get("id", ""))
        data = str(callback.get("data") or "")
        if chat_id != self.chat_id or ":" not in data:
            return
        action, value = data.split(":", 1)
        answer = "Updated"
        edit_settings = False
        if action == "toggle" and value in DEFAULT_PREFERENCES:
            with self._lock:
                self._preferences[value] = not self._preferences[value]
                enabled = self._preferences[value]
                self._save_locked()
            answer = f"{value}: {'on' if enabled else 'off'}"
            edit_settings = True
        elif action == "disable" and value in DEFAULT_PREFERENCES:
            with self._lock:
                self._preferences[value] = False
                self._save_locked()
            answer = f"{value} alerts disabled"
        elif action == "mutebot" and re.fullmatch(r"[A-Za-z0-9._-]+", value):
            with self._lock:
                self._muted_bots[value] = (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat()
                self._save_locked()
            answer = f"{value} muted for 6h"
        else:
            return
        try:
            requests.post(
                f"https://api.telegram.org/bot{self.token}/answerCallbackQuery",
                json={"callback_query_id": callback_id, "text": answer},
                timeout=self.request_timeout,
            ).raise_for_status()
            if edit_settings:
                requests.post(
                    f"https://api.telegram.org/bot{self.token}/editMessageText",
                    json={
                        "chat_id": self.chat_id,
                        "message_id": message.get("message_id"),
                        "text": self._preferences_text(),
                        "reply_markup": self._settings_markup(),
                    },
                    timeout=self.request_timeout,
                ).raise_for_status()
        except requests.RequestException as exc:
            self.log.warning("Telegram settings update failed: %s", exc)

    def _poll_commands(self):
        response = requests.get(
            f"https://api.telegram.org/bot{self.token}/getUpdates",
            params={"offset": self._update_offset, "timeout": 20, "allowed_updates": json.dumps(["message", "callback_query"])},
            timeout=25,
        )
        response.raise_for_status()
        for update in response.json().get("result", []):
            self._handle_command(update.get("message") or {})
            self._handle_callback(update.get("callback_query") or {})
            with self._lock:
                self._update_offset = max(self._update_offset, int(update["update_id"]) + 1)
                self._save_locked()

    def _register_commands(self):
        commands = [
            ("status", "Fleet snapshot"), ("attention", "Operational exceptions"),
            ("profit", "Realized profit by period"), ("trades", "Recent trades by period"),
            ("digest", "Daily report now"), ("leaderboard", "Fleet rankings"),
            ("recap", "Shareable fleet recap"), ("oracle", "Daily fleet omen"),
            ("needs", "Bots needing positions"),
            ("scout", "Test a token's executable exits"), ("watch", "Watch a candidate"),
            ("unwatch", "Stop watching a candidate"), ("forget", "Remove a Scout candidate"),
            ("candidates", "Ranked scout watchlist"),
            ("discover", "Recent unscored token profiles"),
            ("bot", "Inspect one bot"), ("alerts", "Alert toggles"),
            ("mute", "Mute alerts"), ("unmute", "Unmute alerts"), ("help", "Command help"),
        ]
        response = requests.post(
            f"https://api.telegram.org/bot{self.token}/setMyCommands",
            json={"commands": [{"command": command, "description": description} for command, description in commands]},
            timeout=self.request_timeout,
        )
        response.raise_for_status()

    @staticmethod
    def _scout_text(report):
        verdict = str(report.get("verdict", "unknown")).upper()
        icon = {"PASS": "🟢", "CAUTION": "🟡", "REJECT": "🔴"}.get(verdict, "⚪")
        market = report.get("market") or {}
        providers = report.get("providers") or {}
        routes = []
        for name, route in providers.items():
            recovery = route.get("recovery_percent")
            routes.append(f"{name.title()}: {recovery:.2f}% recovery" if recovery is not None else f"{name.title()}: no round trip")
        reasons = report.get("reasons") or []
        reason_text = ", ".join(str(x).replace("_", " ").lower() for x in reasons) or "none"
        return (
            f"{icon} DoomScout: {verdict} · {report.get('score', 0)}/100\n"
            f"{market.get('symbol') or 'TOKEN'} · budget {float(report.get('budget_eth') or 0):.6f} ETH\n"
            f"Liquidity: ${float(market.get('liquidity_usd') or 0):,.0f} · 24h volume: ${float(market.get('volume_h24') or 0):,.0f}\n"
            + "\n".join(routes) + f"\nProvider redundancy: {report.get('sell_provider_count', 0)}\nReasons: {reason_text}"
        )

    def _candidates_text(self):
        if self.scout is None:
            return "DoomScout is unavailable."
        reports = sorted(self.scout.snapshot().get("reports", []), key=lambda item: item.get("score", 0), reverse=True)
        if not reports:
            return "🔭 DoomScout has no assessed candidates. Use /watch 0xTOKEN."
        lines = ["🔭 DoomScout candidates"]
        for report in reports[:12]:
            market = report.get("market") or {}
            icon = {"pass": "🟢", "caution": "🟡", "reject": "🔴"}.get(report.get("verdict"), "⚪")
            lines.append(f"{icon} {market.get('symbol') or report.get('address', '')[:8]} — {report.get('score', 0)}/100 · {report.get('best_recovery_percent') or 0:.1f}% recovery")
        return "\n".join(lines)

    def process_scout_transition(self, previous, current):
        """Send one meaningful alert when a watched candidate changes class."""
        if not self._preferences.get("safety", True):
            return
        market = current.get("market") or {}
        self.send(
            f"🔭 DoomScout changed its mind about {market.get('symbol') or current.get('address', '')[:10]}: "
            f"{str(previous.get('verdict')).upper()} → {str(current.get('verdict')).upper()}\n\n"
            + self._scout_text(current),
            force=False,
        )

    def _run(self):
        try:
            self._register_commands()
        except requests.RequestException as exc:
            self.log.warning("Telegram command registration failed: %s", exc)
        while not self._stop.is_set():
            try:
                self.scan_offline()
                self.scan_funds()
                self.scan_daily_digest()
                self._poll_commands()
            except requests.RequestException as exc:
                self.log.warning("Telegram polling failed: %s", exc)
                self._stop.wait(5)

    def close(self):
        self._stop.set()
