import unittest

from dashboard_server import _allowlisted_status_payload


class TestStatusPayloadAllowlist(unittest.TestCase):
    def test_drops_unknown_top_level_and_nested_fields(self):
        payload = {
            "bot_id": "test-bot",
            "token_symbol": "TENDIES",
            "eth_balance": 1.0,
            "moonbag_balance": 20.0,
            "estimated_moonbag_value_eth": 0.0012,
            "gas_reserve_eth": 0.0005,
            "buy_point_percent": -14.0,
            "sell_point_percent": 10.0,
            "taxed_token": True,
            "token_transfer_fee_percent": 3.0,
            "token_tax_detection_source": "auto-detected",
            "token_tax_detection_observations": 2,
            "swap_slippage_percent": 5.0,
            "private_config": "do-not-persist",
            "positions": [{"id": "1", "pnl": 5.0, "private_note": "nope"}],
            "trades_history": [{"side": "buy", "tx_hash": "0xabc", "gas_fee_eth": 0.00001234, "api_key": "nope"}],
            "events": [{"level": "warning", "message": "safe", "provider_response": "nope"}],
            "capacity_warning": {"max_positions": 10, "internal_reason": "nope"},
            "sell_attempt": {
                "status": "quote_below_minimum",
                "quoted_profit_eth": 0.000206,
                "projected_gas_eth": 0.000122,
                "projected_net_profit_eth": 0.000084,
                "minimum_profit_eth": 0.0001,
                "observed_fee_percent": 2.9999,
                "matching_observations": 2,
                "confirmations_required": 2,
                "detected_fee_percent": 3.0,
                "trace": "nope",
            },
            "sigil": {"version": 1, "method": "spare-wheel-v1", "key": "PRSTY", "seed": "ab" * 32, "intent": "private"},
        }

        self.assertEqual(
            _allowlisted_status_payload(payload),
            {
                "bot_id": "test-bot",
                "token_symbol": "TENDIES",
                "eth_balance": 1.0,
                "moonbag_balance": 20.0,
                "estimated_moonbag_value_eth": 0.0012,
                "gas_reserve_eth": 0.0005,
                "buy_point_percent": -14.0,
                "sell_point_percent": 10.0,
                "taxed_token": True,
                "token_transfer_fee_percent": 3.0,
                "token_tax_detection_source": "auto-detected",
                "token_tax_detection_observations": 2,
                "swap_slippage_percent": 5.0,
                "positions": [{"id": "1", "pnl": 5.0}],
                "trades_history": [{"side": "buy", "tx_hash": "0xabc", "gas_fee_eth": 0.00001234}],
                "events": [{"level": "warning", "message": "safe"}],
                "capacity_warning": {"max_positions": 10},
                "sell_attempt": {
                    "status": "quote_below_minimum",
                    "quoted_profit_eth": 0.000206,
                    "projected_gas_eth": 0.000122,
                    "projected_net_profit_eth": 0.000084,
                    "minimum_profit_eth": 0.0001,
                    "observed_fee_percent": 2.9999,
                    "matching_observations": 2,
                    "confirmations_required": 2,
                    "detected_fee_percent": 3.0,
                },
                "sigil": {"version": 1, "method": "spare-wheel-v1", "key": "PRSTY", "seed": "ab" * 32},
            },
        )

    def test_caps_nested_public_history(self):
        payload = {
            "bot_id": "test-bot",
            "events": [{"message": str(index)} for index in range(55)],
        }
        result = _allowlisted_status_payload(payload)
        self.assertEqual(len(result["events"]), 50)
        self.assertEqual(result["events"][-1], {"message": "49"})


if __name__ == "__main__":
    unittest.main()
