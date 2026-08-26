import tempfile
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

        self.assertEqual(self.alerts.send.call_count, 1)
        self.assertIn("Sell confirmed · CASHCAT", self.alerts.send.call_args.args[0])
        self.assertEqual(
            self.alerts.send.call_args.kwargs["reply_markup"]["inline_keyboard"][0][0]["url"],
            "https://base.blockscout.com/tx/0xabc",
        )

    def test_stop_loss_uses_high_priority_category(self):
        current = {"trades_history": [{"side": "sell", "tx_hash": "0xloss", "profit_eth": -0.002}]}
        self.alerts.process_status("loser", {"trades_history": []}, current)
        self.assertIn("Stop-loss sell", self.alerts.send.call_args.args[0])

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


if __name__ == "__main__":
    unittest.main()
