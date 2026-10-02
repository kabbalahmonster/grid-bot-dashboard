import json
import subprocess
import unittest

import dashboard_test_env  # noqa: F401  (must precede application import)
import dashboard_server as server


class TestStrategyModeRendering(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    def _present(self, values):
        body = self.client.get("/").get_data(as_text=True)
        start = body.index("  function strategyPresentation(")
        end = body.index("\n  function reportAge(", start)
        script = body[start:end]
        result = subprocess.run(
            [
                "node",
                "-e",
                script + "\nconsole.log(JSON.stringify(" + json.dumps(values)
                + ".map(strategyPresentation)));",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return json.loads(result.stdout)

    def test_compact_labels_cover_all_modes_and_legacy_payloads(self):
        rendered = self._present([
            {"strategy_mode": "grid"},
            {"strategy_mode": "gridless_threshold"},
            {"strategy_mode": "drawdown_ladder", "strategy_spacing": "linear"},
            {"strategy_mode": "drawdown_ladder", "strategy_spacing": "log"},
            {"strategy_mode": "survivor", "strategy_spacing": "log"},
            {},
        ])

        self.assertEqual([item["label"] for item in rendered], [
            "GRID",
            "GRIDLESS · THRESHOLD",
            "DRAWDOWN · LINEAR",
            "DRAWDOWN · LOG",
            "SURVIVOR · LOG",
            "LEGACY / UNKNOWN",
        ])

    def test_old_drawdown_payload_is_inferred_and_ladder_summary_is_compact(self):
        rendered = self._present([{
            "entry_allocation_mode": "drawdown_ladder",
            "drawdown_ladder": {
                "spacing": "log",
                "levels_funded": 17,
                "levels_total": 50,
                "levels_open": 4,
                "terminal_drawdown_percent": 95,
                "reserved_eth": 0.043,
            },
        }])[0]

        self.assertEqual(rendered["label"], "DRAWDOWN · LOG")
        self.assertEqual(
            rendered["ladderSummary"],
            "17/50 funded · 4 open · 95% terminal · 0.043 ETH reserved",
        )

    def test_card_uses_one_strategy_badge_and_more_info_details(self):
        body = self.client.get("/").get_data(as_text=True)

        self.assertIn('class="strategy-mode-badge', body)
        self.assertIn("strategyBadge + pnlModeBadge", body)
        self.assertIn("['Strategy', 'strategy_display']", body)
        self.assertIn("['Ladder', 'ladder_summary']", body)
        self.assertIn("['Next Ladder Buy', 'next_ladder_buy_price']", body)
        self.assertIn("['Next Leading Buy', 'next_leading_buy_price']", body)
        self.assertIn("['Leading Buy Trigger', 'leading_buy_trigger_percent']", body)


if __name__ == "__main__":
    unittest.main()
