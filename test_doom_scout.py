import tempfile
import unittest
from unittest.mock import Mock

from doom_scout import DoomScout, score_assessment


TOKEN = "0x" + "12" * 20


class TestDoomScout(unittest.TestCase):
    def test_wth_style_exit_is_rejected(self):
        result = score_assessment(
            {"liquidity_usd": 20_000, "volume_h24": 5_000, "age_hours": 48, "eth_usd": 4_000},
            {"uniswap": {"sell_success": True, "recovery_percent": 53},
             "sushiswap": {"sell_success": False, "recovery_percent": None}}, 0.003,
        )
        self.assertEqual(result["verdict"], "reject")
        self.assertIn("ROUND_TRIP_RECOVERY_BELOW_85_PERCENT", result["reasons"])

    def test_good_single_provider_is_caution_not_pass(self):
        result = score_assessment(
            {"liquidity_usd": 150_000, "volume_h24": 40_000, "age_hours": 72, "eth_usd": 4000},
            {"sushiswap": {"sell_success": True, "recovery_percent": 98},
             "uniswap": {"sell_success": False, "recovery_percent": None}},
            0.003,
        )
        self.assertEqual(result["verdict"], "caution")
        self.assertIn("NO_PROVIDER_REDUNDANCY", result["reasons"])
        self.assertIn("NO_PROVIDER_REDUNDANCY", result["reasons"])

    def test_deep_redundant_coin_passes(self):
        result = score_assessment(
            {"liquidity_usd": 100_000, "volume_h24": 50_000, "age_hours": 240, "eth_usd": 4_000},
            {"uniswap": {"sell_success": True, "recovery_percent": 96},
             "sushiswap": {"sell_success": True, "recovery_percent": 95}}, 0.003,
        )
        self.assertEqual(result["verdict"], "pass")
        self.assertGreaterEqual(result["score"], 75)

    def test_watchlist_is_durable_and_validated(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/scout.json"
            scout = DoomScout(path)
            scout.watch(TOKEN, "LEMON", budget_eth=0.004, positions=4)
            restored = DoomScout(path)
            self.assertEqual(restored.snapshot()["watchlist"][0]["label"], "LEMON")
            with self.assertRaises(ValueError):
                restored.watch("not-an-address")

    def test_assessment_round_trips_each_provider(self):
        scout = DoomScout(state_file="/dev/null", uniswap_api_key="x")
        scout._market = Mock(return_value={"symbol": "OK", "liquidity_usd": 100_000,
                                          "volume_h24": 10_000, "age_hours": 100})
        scout._eth_usd = Mock(return_value=4_000)
        scout._provider_roundtrip = Mock(return_value={"sell_success": True, "recovery_percent": 95})
        report = scout.assess(TOKEN, persist=False)
        self.assertEqual(report["verdict"], "pass")
        self.assertEqual(scout._provider_roundtrip.call_count, 2)


if __name__ == "__main__":
    unittest.main()
