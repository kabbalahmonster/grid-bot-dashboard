import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

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
        self.assertIn("https://base.blockscout.com/tx/0xabc", self.alerts.send.call_args.args[0])

    def test_stop_loss_uses_high_priority_category(self):
        current = {"trades_history": [{"side": "sell", "tx_hash": "0xloss", "profit_eth": -0.002}]}
        self.alerts.process_status("loser", {"trades_history": []}, current)
        self.assertIn("Stop-loss sell", self.alerts.send.call_args.args[0])

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


if __name__ == "__main__":
    unittest.main()
