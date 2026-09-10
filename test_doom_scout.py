import os
import tempfile
import unittest
from unittest.mock import Mock

from doom_scout import DoomScout, score_assessment


TOKEN = "0x" + "12" * 20


def runtime(*selectors, opcodes=b""):
    return "0x" + (b"".join(b"\x63" + bytes.fromhex(selector) for selector in selectors) + opcodes).hex()


class TestDoomScout(unittest.TestCase):
    def test_discovery_enriches_sparse_profiles(self):
        scout = DoomScout(state_file="/dev/null")
        profile_response = Mock()
        profile_response.json.return_value = [{"chainId": "robinhood", "tokenAddress": TOKEN}]
        market_response = Mock()
        market_response.json.return_value = [{
            "baseToken": {"address": TOKEN, "symbol": "FOX", "name": "Nullfox"},
            "quoteToken": {"address": "0x" + "34" * 20, "symbol": "WETH"},
            "liquidity": {"usd": 12000}, "volume": {"h24": 3400},
            "priceChange": {"h24": 6.5}, "pairCreatedAt": 1,
        }]
        scout.http.get = Mock(side_effect=[profile_response, market_response])
        found = scout.discover(10)
        self.assertEqual(found[0]["symbol"], "FOX")
        self.assertEqual(found[0]["name"], "Nullfox")
        self.assertEqual(found[0]["liquidity_usd"], 12000)

    def test_wth_style_exit_is_rejected(self):
        result = score_assessment(
            {"liquidity_usd": 20_000, "volume_h24": 5_000, "age_hours": 48, "eth_usd": 4_000},
            {"uniswap": {"sell_success": True, "recovery_percent": 53},
             "sushiswap": {"sell_success": False, "recovery_percent": None}}, 0.003,
        )
        self.assertEqual(result["verdict"], "reject")
        self.assertIn("ROUND_TRIP_RECOVERY_BELOW_85_PERCENT", result["reasons"])

    def test_good_single_provider_is_caution_not_reject(self):
        result = score_assessment(
            {"liquidity_usd": 150_000, "volume_h24": 40_000, "age_hours": 72, "eth_usd": 4000},
            {"sushiswap": {"sell_success": True, "recovery_percent": 98},
             "uniswap": {"sell_success": False, "recovery_percent": None}},
            0.003,
        )
        self.assertEqual(result["verdict"], "caution")
        self.assertEqual(result["score"], 85)
        self.assertIn("NO_PROVIDER_REDUNDANCY", result["reasons"])

    def test_single_provider_recovery_is_graded(self):
        market = {"liquidity_usd": 150_000, "volume_h24": 40_000,
                  "age_hours": 72, "eth_usd": 4000}
        strong = score_assessment(
            market, {"sushiswap": {"sell_success": True, "recovery_percent": 98},
                     "uniswap": {"sell_success": False}}, 0.003,
        )
        thinner = score_assessment(
            market, {"sushiswap": {"sell_success": True, "recovery_percent": 92.1},
                     "uniswap": {"sell_success": False}}, 0.003,
        )
        self.assertEqual(strong["score"], 85)
        self.assertEqual(thinner["score"], 79)
        self.assertEqual(thinner["verdict"], "caution")

    def test_deep_redundant_coin_passes(self):
        result = score_assessment(
            {"liquidity_usd": 100_000, "volume_h24": 50_000, "age_hours": 240, "eth_usd": 4_000},
            {"uniswap": {"sell_success": True, "recovery_percent": 96},
             "sushiswap": {"sell_success": True, "recovery_percent": 95}}, 0.003,
        )
        self.assertEqual(result["verdict"], "pass")
        self.assertGreaterEqual(result["score"], 75)

    def test_tx_origin_contract_is_hard_rejected(self):
        result = score_assessment(
            {"liquidity_usd": 100_000, "volume_h24": 50_000, "age_hours": 240, "eth_usd": 4_000},
            {"uniswap": {"sell_success": True, "recovery_percent": 96},
             "sushiswap": {"sell_success": True, "recovery_percent": 95}}, 0.003,
            {"flags": ["TX_ORIGIN_LOGIC"]},
        )
        self.assertEqual(result["verdict"], "reject")
        self.assertIn("TX_ORIGIN_DEPENDENT_TOKEN_LOGIC", result["reasons"])

    def test_active_owner_tax_template_is_hard_rejected(self):
        result = score_assessment(
            {"liquidity_usd": 100_000, "volume_h24": 50_000, "age_hours": 240, "eth_usd": 4_000},
            {"uniswap": {"sell_success": True, "recovery_percent": 96},
             "sushiswap": {"sell_success": True, "recovery_percent": 95}}, 0.003,
            {"flags": ["ACTIVE_OWNER_TAX_TOKEN"]},
        )
        self.assertEqual(result["verdict"], "reject")
        self.assertIn("ACTIVE_OWNER_CONTROLS_TAX_TOKEN", result["reasons"])

    def test_watchlist_is_durable_and_validated(self):
        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/scout.json"
            scout = DoomScout(path)
            scout._require_token_contract = Mock(return_value={"status": "contract", "bytecode_present": True})
            scout.watch(TOKEN, "LEMON", budget_eth=0.004, positions=4)
            restored = DoomScout(path)
            self.assertEqual(restored.snapshot()["watchlist"][0]["label"], "LEMON")

    def test_forget_removes_watch_report_and_history(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = os.path.join(directory, "scout.json")
            scout = DoomScout(state_file=state_file)
            scout._require_token_contract = Mock(return_value={"status": "contract", "bytecode_present": True})
            scout.watch(TOKEN, "LEMON")
            scout._reports[TOKEN.lower()] = {"address": TOKEN}
            scout._history[TOKEN.lower()] = [{"score": 85}]
            scout._save_locked()

            self.assertTrue(scout.forget(TOKEN))
            restored = DoomScout(state_file=state_file)
            self.assertEqual(restored.snapshot()["watchlist"], [])
            self.assertEqual(restored.snapshot()["reports"], [])
            self.assertEqual(restored.history(TOKEN), [])
            with self.assertRaises(ValueError):
                restored.watch("not-an-address")

    def test_snapshot_exposes_safe_runtime_metadata(self):
        scout = DoomScout(state_file="/dev/null", interval_seconds=900, uniswap_api_key="x")
        snapshot = scout.snapshot()
        self.assertEqual(snapshot["interval_seconds"], 900)
        self.assertEqual(snapshot["provider_status"]["sushiswap"], "configured")
        self.assertEqual(snapshot["provider_status"]["uniswap"], "configured")
        self.assertNotIn("uniswap_api_key", snapshot)

    def test_snapshot_reports_missing_optional_uniswap_provider(self):
        snapshot = DoomScout(state_file="/dev/null").snapshot()
        self.assertEqual(snapshot["provider_status"]["uniswap"], "not_configured")

    def test_assessment_round_trips_each_provider(self):
        scout = DoomScout(state_file="/dev/null", uniswap_api_key="x")
        scout._require_token_contract = Mock(return_value={"status": "contract", "bytecode_present": True})
        scout._market = Mock(return_value={"symbol": "OK", "liquidity_usd": 100_000,
                                          "volume_h24": 10_000, "age_hours": 100})
        scout._eth_usd = Mock(return_value=4_000)
        scout._provider_roundtrip = Mock(return_value={"sell_success": True, "recovery_percent": 95})
        report = scout.assess(TOKEN, persist=False)
        self.assertEqual(report["verdict"], "pass")
        self.assertEqual(scout._provider_roundtrip.call_count, 2)

    def test_uniswap_quote_uses_safe_user_agent(self):
        scout = DoomScout(state_file="/dev/null", uniswap_api_key="x")
        response = Mock(status_code=200, headers={})
        response.json.return_value = {"quote": {"output": {"amount": "123"}}}
        scout.http.post = Mock(return_value=response)
        self.assertEqual(scout._uniswap_quote(TOKEN, "0x" + "34" * 20, 100, 4663), 123)
        headers = scout.http.post.call_args.kwargs["headers"]
        self.assertEqual(headers["User-Agent"], "curl/8.0")
        self.assertEqual(headers["Accept"], "application/json")

    def test_wallet_address_is_rejected_before_provider_calls(self):
        scout = DoomScout(state_file="/dev/null")
        response = Mock()
        response.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": "0x"}
        scout.http.post = Mock(return_value=response)
        with self.assertRaisesRegex(ValueError, "wallet/EOA"):
            scout.assess(TOKEN, persist=False)

    def test_contract_security_detects_astro_style_controls(self):
        scout = DoomScout(state_file="/dev/null")
        owner = "34" * 20
        code = runtime(*(
            "8da5cb5b", "2dc0562d", "8a8c523c", "8f3fa860",
            "8c0b5e22", "8f3d6e04", "40b0c56e", "d8454a82",
        ), opcodes=b"\x32\x00")
        scout._rpc_call = Mock(return_value="0x" + "0" * 24 + owner)
        security = scout._contract_security(TOKEN, 4663, code)
        self.assertTrue(security["owner_active"])
        self.assertTrue(security["tax_template"])
        self.assertIn("ACTIVE_OWNER_TAX_TOKEN", security["flags"])
        self.assertIn("TX_ORIGIN_LOGIC", security["flags"])
        self.assertEqual(security["bytecode_size"], (len(code) - 2) // 2)

    def test_push_data_does_not_create_false_dangerous_opcode_flags(self):
        scout = DoomScout(state_file="/dev/null")
        # ORIGIN/DELEGATECALL/SELFDESTRUCT occur only inside PUSH3 data.
        code = "0x6232f4ff00"
        security = scout._contract_security(TOKEN, 4663, code)
        self.assertNotIn("TX_ORIGIN_LOGIC", security["flags"])
        self.assertNotIn("DELEGATECALL", security["flags"])
        self.assertNotIn("SELFDESTRUCT", security["flags"])

    def test_uniswap_probes_specific_protocols_after_default_no_route(self):
        scout = DoomScout(state_file="/dev/null", uniswap_api_key="x")
        no_route = Mock(status_code=404, headers={})
        no_route.json.return_value = {"error": "NoRoute"}
        v4 = Mock(status_code=200, headers={})
        v4.json.return_value = {"quote": {"output": {"amount": "456"}}}
        scout.http.post = Mock(side_effect=[no_route, v4])
        self.assertEqual(scout._uniswap_quote(TOKEN, "0x" + "34" * 20, 100, 4663), 456)
        self.assertNotIn("protocols", scout.http.post.call_args_list[0].kwargs["json"])
        self.assertEqual(scout.http.post.call_args_list[1].kwargs["json"]["protocols"], ["V4"])
        self.assertEqual(scout._last_uniswap_protocol, "V4")

    def test_uniswap_quote_surfaces_gateway_error_and_request_id(self):
        scout = DoomScout(state_file="/dev/null", uniswap_api_key="x")
        response = Mock(status_code=409, headers={"x-request-id": "trace-1"})
        response.json.return_value = {"error": "packet failure"}
        scout.http.post = Mock(return_value=response)
        with self.assertRaisesRegex(LookupError, "packet failure; request_id=trace-1"):
            scout._uniswap_quote(TOKEN, "0x" + "34" * 20, 100, 4663)


if __name__ == "__main__":
    unittest.main()
