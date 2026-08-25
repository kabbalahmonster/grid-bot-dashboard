import unittest

from dashboard_server import _allowlisted_status_payload


class TestStatusPayloadAllowlist(unittest.TestCase):
    def test_drops_unknown_top_level_and_nested_fields(self):
        payload = {
            "bot_id": "test-bot",
            "token_symbol": "TENDIES",
            "eth_balance": 1.0,
            "gas_reserve_eth": 0.0005,
            "private_config": "do-not-persist",
            "positions": [{"id": "1", "pnl": 5.0, "private_note": "nope"}],
            "trades_history": [{"side": "buy", "tx_hash": "0xabc", "api_key": "nope"}],
            "events": [{"level": "warning", "message": "safe", "provider_response": "nope"}],
            "capacity_warning": {"max_positions": 10, "internal_reason": "nope"},
            "sell_attempt": {"status": "quote_below_minimum", "trace": "nope"},
            "sigil": {"version": 1, "method": "spare-wheel-v1", "key": "PRSTY", "seed": "ab" * 32, "intent": "private"},
        }

        self.assertEqual(
            _allowlisted_status_payload(payload),
            {
                "bot_id": "test-bot",
                "token_symbol": "TENDIES",
                "eth_balance": 1.0,
                "gas_reserve_eth": 0.0005,
                "positions": [{"id": "1", "pnl": 5.0}],
                "trades_history": [{"side": "buy", "tx_hash": "0xabc"}],
                "events": [{"level": "warning", "message": "safe"}],
                "capacity_warning": {"max_positions": 10},
                "sell_attempt": {"status": "quote_below_minimum"},
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
