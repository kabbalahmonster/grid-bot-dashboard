import json
import subprocess
import unittest

import dashboard_test_env  # noqa: F401  (must precede application import)
import dashboard_server as server


class TestRapidCycleDashboard(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()

    def _render(self, rapid):
        body = self.client.get("/").get_data(as_text=True)
        start = body.index("  function renderRapidCycle(")
        end = body.index("\n  function reportAge(", start)
        script = (
            "function esc(value) { return String(value); }\n"
            "function reportAge() { return {text: '5s ago'}; }\n"
            + body[start:end]
        )
        result = subprocess.run(
            [
                "node",
                "-e",
                script + "\nconsole.log(renderRapidCycle(" + json.dumps(rapid) + "));",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout

    def test_shadow_payload_is_strictly_allowlisted(self):
        payload = {
            "mode": "shadow",
            "status": "open",
            "principal_eth": 0.001,
            "entry_drop_percent": 2.0,
            "min_profit_percent": 1.0,
            "min_buy_samples": 3,
            "lookback_samples": 12,
            "buy_sample_count": 3,
            "completed_cycle_count": 4,
            "last_observation": {
                "direction": "sell",
                "block_number": 123,
                "provider": "abyss",
                "settlement": "native",
                "observed_at": "2026-11-08T03:50:00+00:00",
                "projected_profit_percent": 0.42,
                "projected_gas_eth": 0.00001,
                "calldata": "drop-me",
            },
            "position": {
                "kind": "paper",
                "opened_at": "2026-11-08T03:49:00+00:00",
                "token_amount": 2100.5,
                "cost_eth": 0.00101,
                "buy_provider": "uniswap",
                "buy_settlement": "weth",
                "wallet_address": "drop-me",
            },
            "private_config": "drop-me",
        }

        clean = server._allowlisted_status_payload({"rapid_cycle": payload})["rapid_cycle"]

        self.assertEqual(clean["mode"], "shadow")
        self.assertEqual(clean["last_observation"]["provider"], "abyss")
        self.assertEqual(clean["position"]["kind"], "paper")
        self.assertNotIn("calldata", clean["last_observation"])
        self.assertNotIn("wallet_address", clean["position"])
        self.assertNotIn("private_config", clean)

    def test_invalid_mode_or_status_drops_the_entire_card(self):
        self.assertEqual(
            server._allowlisted_status_payload({
                "rapid_cycle": {"mode": "execute", "status": "open"}
            }),
            {},
        )
        self.assertEqual(
            server._allowlisted_status_payload({
                "rapid_cycle": {"mode": "shadow", "status": "corrupt"}
            }),
            {},
        )

    def test_shadow_flat_card_shows_paper_badge_and_entry_progress(self):
        html = self._render({
            "mode": "shadow",
            "status": "flat",
            "principal_eth": 0.001,
            "entry_drop_percent": 2,
            "min_buy_samples": 3,
            "buy_sample_count": 2,
            "completed_cycle_count": 0,
            "last_observation": {
                "direction": "buy",
                "provider": "uniswap",
                "settlement": "native",
                "drop_from_peak_percent": 1.25,
                "observed_at": "2026-11-08T03:50:00+00:00",
            },
        })

        self.assertIn("SHADOW · PAPER ONLY", html)
        self.assertIn("Scanning executable entries", html)
        self.assertIn(">2 / 3</strong>", html)
        self.assertIn(">1.25% / 2.00%</strong>", html)
        self.assertIn("UNISWAP / NATIVE", html)

    def test_open_card_shows_paper_position_and_projected_exit(self):
        html = self._render({
            "mode": "shadow",
            "status": "open",
            "principal_eth": 0.001,
            "min_profit_percent": 1,
            "completed_cycle_count": 2,
            "position": {
                "kind": "paper",
                "cost_eth": 0.00101,
                "token_amount": 2100.5,
            },
            "last_observation": {
                "direction": "sell",
                "provider": "sushiswap",
                "settlement": "weth",
                "projected_profit_percent": 0.42,
            },
        })

        self.assertIn("Paper position open", html)
        self.assertIn("Paper Cost basis", html)
        self.assertIn("+0.42% / +1.00%", html)
        self.assertIn("paper amount: 2,100.5 tokens", html)

    def test_main_card_renders_rapid_cycle_without_affecting_legacy_payloads(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("html += renderRapidCycle(d.rapid_cycle);", body)
        self.assertEqual(self._render(None).strip(), "")


if __name__ == "__main__":
    unittest.main()
