import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from telegram_alerts import TelegramAlerts


class TestTelegramAlerts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.states = {}
        self.alerts = TelegramAlerts(
            "", "7045629589", str(Path(self.temporary.name) / "alerts.json"),
            lambda: self.states, offline_seconds=300,
        )
        self.alerts.enabled = True
        self.alerts.send = Mock(return_value=True)

    def tearDown(self):
        self.alerts.close()
        self.temporary.cleanup()

    def test_new_sell_is_alerted_once_and_persistently_deduplicated(self):
        trade = {"side": "sell", "tx_hash": "0xabc", "profit_eth": 0.001, "eth_amount": 0.01}
        previous = {"trades_history": []}
        current = {"display_name": "CASHCAT", "trades_history": [trade], "chain_id": 8453}

        self.alerts.process_status("cashcat", previous, current)
        self.alerts.process_status("cashcat", previous, current)

        self.assertEqual(self.alerts.send.call_count, 2)
        sell_call = next(call for call in self.alerts.send.call_args_list if "Sell confirmed" in call.args[0])
        self.assertIn("Sell confirmed · CASHCAT", sell_call.args[0])
        self.assertEqual(
            sell_call.kwargs["reply_markup"]["inline_keyboard"][0][0]["url"],
            "https://base.blockscout.com/tx/0xabc",
        )

    def test_stop_loss_uses_high_priority_category(self):
        current = {"trades_history": [{"side": "sell", "tx_hash": "0xloss", "profit_eth": -0.002}]}
        self.alerts.process_status("loser", {"trades_history": []}, current)
        self.assertTrue(any("Stop-loss sell" in call.args[0] for call in self.alerts.send.call_args_list))

    def test_profit_banked_alert_includes_transaction_button(self):
        self.alerts._preferences["treasury"] = True
        event = {
            "timestamp": "2026-08-26T03:00:00+00:00",
            "level": "success",
            "code": "usdg_banked",
            "message": "Banked 1.25 USDG",
            "tx_hash": "0xbank",
        }
        previous = {"events": []}
        current = {"events": [event], "chain_id": 8453}

        self.alerts.process_status("banker", previous, current)

        self.assertIn("Profit banked", self.alerts.send.call_args.args[0])
        buttons = self.alerts.send.call_args.kwargs["reply_markup"]["inline_keyboard"][0]
        self.assertEqual(buttons[0]["text"], "View transaction ↗")
        self.assertEqual(buttons[0]["url"], "https://base.blockscout.com/tx/0xbank")

    def test_balance_mismatch_alerts_immediately_once_and_recovers(self):
        mismatch = {
            "status": "position_balance_mismatch", "position_id": "1",
            "tracked_sell_amount_raw": 1000, "wallet_balance_raw": 400, "deficit_raw": 600,
        }
        current = {"display_name": "BOW", "sell_attempt": mismatch}

        self.alerts.process_status("bow", {}, current)
        self.alerts.process_status("bow", {}, current)
        self.assertEqual(self.alerts.send.call_count, 1)
        self.assertIn("POSITION BALANCE MISMATCH · BOW", self.alerts.send.call_args.args[0])
        self.assertIn("Deficit: 600 raw", self.alerts.send.call_args.args[0])

        self.alerts.process_status("bow", current, {"display_name": "BOW"})
        self.assertEqual(self.alerts.send.call_count, 2)
        self.assertIn("Balance reconciled · BOW", self.alerts.send.call_args.args[0])

    def test_offline_and_recovery_are_edge_triggered(self):
        old = (datetime.now(timezone.utc) - timedelta(minutes=6)).isoformat()
        self.states["bot-1"] = {"display_name": "OLDIE", "received_at": old}
        self.alerts.scan_offline()
        self.alerts.scan_offline()
        self.assertEqual(self.alerts.send.call_count, 1)
        self.assertIn("Bot offline", self.alerts.send.call_args.args[0])

        self.alerts._preferences["recovered"] = True
        self.alerts.process_status("bot-1", self.states["bot-1"], {"display_name": "OLDIE", "trades_history": []})
        self.assertEqual(self.alerts.send.call_count, 2)
        self.assertIn("Bot recovered", self.alerts.send.call_args.args[0])

    def test_preexisting_offline_bots_are_baselined_without_alerting(self):
        old = (datetime.now(timezone.utc) - timedelta(minutes=6)).isoformat()
        self.states["already-offline"] = {"received_at": old}
        self.alerts._baseline_existing_offline_states()
        self.assertIn("already-offline", self.alerts._offline_notified)
        self.alerts.send.assert_not_called()

    def test_concurrent_start_creates_only_one_poller(self):
        self.alerts._run = Mock()
        callers = [threading.Thread(target=self.alerts.start) for _ in range(8)]
        for caller in callers:
            caller.start()
        for caller in callers:
            caller.join()
        if self.alerts._thread:
            self.alerts._thread.join()
        self.assertEqual(self.alerts._run.call_count, 1)

    def test_alert_command_updates_durable_preference(self):
        self.alerts._handle_command({"chat": {"id": 7045629589}, "text": "/alerts buys on"})
        self.assertTrue(self.alerts._preferences["buys"])
        reloaded = TelegramAlerts("", "7045629589", self.alerts.state_file, lambda: {})
        try:
            self.assertTrue(reloaded._preferences["buys"])
        finally:
            reloaded.close()

    def test_commands_from_other_chats_are_ignored(self):
        self.alerts._handle_command({"chat": {"id": 123}, "text": "/alerts buys on"})
        self.assertFalse(self.alerts._preferences["buys"])
        self.alerts.send.assert_not_called()

    def test_status_needs_and_bot_commands_use_live_fleet_state(self):
        self.states["cashcat"] = {
            "display_name": "CASHCAT", "token_symbol": "CASHCAT",
            "received_at": datetime.now(timezone.utc).isoformat(),
            "eth_balance": 0.25, "usdg_balance": 50, "session_profit_eth": 0.01,
            "realized_profit_eth": 0.02, "filled_positions": 4, "max_positions": 5,
            "capacity_warning": {"max_positions": 5},
        }
        self.alerts._handle_command({"chat": {"id": 7045629589}, "text": "/status"})
        self.assertIn("Running: 1/1", self.alerts.send.call_args.args[0])
        self.alerts._handle_command({"chat": {"id": 7045629589}, "text": "/needs"})
        self.assertIn("CASHCAT · 5 slots full", self.alerts.send.call_args.args[0])
        self.alerts._handle_command({"chat": {"id": 7045629589}, "text": "/bot CASHCAT"})
        self.assertIn("Positions: 4/5", self.alerts.send.call_args.args[0])

    def test_attention_reports_operational_exceptions(self):
        self.states["bot-1"] = {
            "display_name": "DRAMA", "received_at": datetime.now(timezone.utc).isoformat(),
            "capacity_warning": {"max_positions": 5}, "sell_attempt": {"status": "checking"},
            "eth_balance": 0.0006, "gas_reserve_eth": 0.0002,
            "events": [{"level": "error", "count": 3, "message": "nope"}],
        }
        self.alerts._handle_command({"chat": {"id": 7045629589}, "text": "/attention"})
        text = self.alerts.send.call_args.args[0]
        self.assertIn("Needs positions: 1 · DRAMA", text)
        self.assertIn("Sell checks: 1 · DRAMA", text)
        self.assertIn("Errors/RPC: 1 · DRAMA", text)
        self.assertIn("Low funds: 1 · DRAMA", text)

    def test_profit_and_trade_commands_use_requested_period(self):
        now = datetime.now(timezone.utc).isoformat()
        self.states["winner"] = {
            "realized_profit_periods": {"24h": 0.012},
            "trades_history": [{"timestamp": now, "side": "sell", "profit_eth": 0.012}],
        }
        self.alerts._handle_command({"chat": {"id": 7045629589}, "text": "/profit 24h"})
        self.assertIn("Fleet: +0.01200000 ETH", self.alerts.send.call_args.args[0])
        self.alerts._handle_command({"chat": {"id": 7045629589}, "text": "/trades 24h"})
        self.assertIn("1 total · 0 buys · 1 sells", self.alerts.send.call_args.args[0])

    def test_low_funds_and_unbanked_usdg_alerts_are_edge_triggered(self):
        self.states["poor"] = {
            "eth_balance": 0.0006, "gas_reserve_eth": 0.0002, "usdg_balance": 12,
        }
        self.alerts.scan_funds()
        self.alerts.scan_funds()
        self.assertEqual(self.alerts.send.call_count, 2)
        messages = [call.args[0] for call in self.alerts.send.call_args_list]
        self.assertTrue(any("Low ETH" in message for message in messages))
        self.assertTrue(any("USDG awaiting banking" in message for message in messages))

    def test_daily_digest_runs_once_after_configured_utc_time(self):
        self.alerts.daily_digest_time = "13:00"
        self.states["bot"] = {"eth_balance": 1, "token_balance": 2, "price": 0.5}
        before = datetime(2026, 8, 27, 12, 59, tzinfo=timezone.utc)
        after = datetime(2026, 8, 27, 13, 0, tzinfo=timezone.utc)
        self.alerts.scan_daily_digest(before)
        self.alerts.send.assert_not_called()
        self.alerts.scan_daily_digest(after)
        self.alerts.scan_daily_digest(after + timedelta(hours=1))
        self.assertEqual(self.alerts.send.call_count, 1)
        self.assertIn("Fleet crypto value: ~2.00000000 ETH", self.alerts.send.call_args.args[0])

    def test_mute_and_unmute_are_durable(self):
        self.alerts._handle_command({"chat": {"id": 7045629589}, "text": "/mute 6h"})
        self.assertIsNotNone(self.alerts._muted_until)
        self.alerts._handle_command({"chat": {"id": 7045629589}, "text": "/unmute"})
        self.assertIsNone(self.alerts._muted_until)

    @patch("telegram_alerts.requests.post")
    def test_inline_callback_toggles_category(self, post):
        post.return_value.raise_for_status.return_value = None
        self.alerts._handle_callback({
            "id": "callback-1", "data": "toggle:buys",
            "message": {"chat": {"id": 7045629589}, "message_id": 12},
        })
        self.assertTrue(self.alerts._preferences["buys"])
        self.assertEqual(post.call_count, 2)

    @patch("telegram_alerts.requests.post")
    def test_registers_new_commands_with_telegram(self, post):
        post.return_value.raise_for_status.return_value = None
        self.alerts._register_commands()
        commands = post.call_args.kwargs["json"]["commands"]
        self.assertIn({"command": "attention", "description": "Operational exceptions"}, commands)
        self.assertIn({"command": "digest", "description": "Daily report now"}, commands)
        self.assertIn({"command": "leaderboard", "description": "Fleet rankings"}, commands)
        self.assertIn({"command": "recap", "description": "Shareable fleet recap"}, commands)

    def test_leaderboard_supports_profit_winrate_and_treasury(self):
        self.states.update({
            "winner": {"realized_profit_periods": {"24h": 0.02}, "treasury_sent_usdg": 5,
                       "trades_history": [{"side": "sell", "profit_eth": 0.02}]},
            "other": {"realized_profit_periods": {"24h": -0.01}, "treasury_sent_usdg": 10,
                      "trades_history": [{"side": "sell", "profit_eth": -0.01}]},
        })
        self.assertIn("🥇 winner · +0.02000000 ETH", self.alerts._leaderboard_text("24h"))
        self.assertIn("1/1 · 100%", self.alerts._leaderboard_text("winrate"))
        self.assertIn("🥇 other · 10.00 USDG", self.alerts._leaderboard_text("treasury"))

    def test_bounded_profit_period_zeroes_report_older_than_entire_window(self):
        stale = {
            "received_at": "2020-01-01T00:00:00+00:00",
            "realized_profit_eth": 1.0,
            "realized_profit_periods": {"24h": 0.25},
        }
        self.assertEqual(self.alerts._realized_for_period(stale, "24h"), 0.0)
        self.assertEqual(self.alerts._realized_for_period(stale, "all"), 1.0)

    def test_oracle_is_deterministic_for_the_day(self):
        self.states["fox"] = {"realized_profit_periods": {"24h": 0.01}, "sigil": {"seed": "ab" * 32}}
        self.assertEqual(self.alerts._oracle_text(), self.alerts._oracle_text())
        self.assertIn("feral prosperity", self.alerts._oracle_text())

    def test_recap_command_sends_png(self):
        if self.alerts._recap_image("week") is None:
            self.skipTest("Pillow unavailable")
        self.alerts.send_photo = Mock(return_value=True)
        self.states["winner"] = {"realized_profit_periods": {"week": 0.01}}
        self.alerts._handle_command({"chat": {"id": 7045629589}, "text": "/recap week"})
        image = self.alerts.send_photo.call_args.args[0]
        self.assertEqual(image.read(8), b"\x89PNG\r\n\x1a\n")

    @patch("telegram_alerts.requests.post")
    def test_inline_callback_can_mute_one_bot(self, post):
        post.return_value.raise_for_status.return_value = None
        self.alerts._handle_callback({
            "id": "callback-2", "data": "mutebot:lemon",
            "message": {"chat": {"id": 7045629589}, "message_id": 13},
        })
        self.assertTrue(self.alerts._is_muted("lemon"))
        self.assertFalse(self.alerts._is_muted("printer"))


if __name__ == "__main__":
    unittest.main()
