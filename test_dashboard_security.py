import unittest

import dashboard_test_env  # noqa: F401  (must precede application import)
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
            "pnl_polling_mode": "bidirectional",
            "pnl_legacy_triggers": False,
            "pnl_focus_side": "legacy",
            "pnl_focus_reason": "triggered",
            "pnl_focus_directions": "sell",
            "pnl_trigger_mode": "minimum_profit",
            "taxed_token": True,
            "token_transfer_fee_percent": 3.0,
            "token_tax_detection_source": "auto-detected",
            "token_tax_detection_observations": 2,
            "swap_slippage_percent": 5.0,
            "private_config": "do-not-persist",
            "positions": [{
                "id": "1", "pnl": 5.0, "buy_pnl": 4.2, "sell_pnl": 1.7,
                "legacy_pnl": 3.1,
                "buy_quote_at": "2026-09-16T03:00:00+00:00",
                "sell_quote_at": "2026-09-16T03:00:20+00:00",
                "legacy_quote_at": "2026-09-16T03:00:40+00:00",
                "buy_quote_provider": "uniswap", "sell_quote_provider": "sushiswap",
                "legacy_quote_provider": "uniswap",
                "sell_quote_source_position_id": "1",
                "buy_projected_gas_eth": 0.00001,
                "sell_projected_gas_eth": 0.00002,
                "sell_quote_basis": "exact_sell_quote_extrapolated_net",
                "legacy_quote_basis": "legacy_gross_0.001_buy_quote",
                "private_note": "nope",
            }],
            "trades_history": [{"side": "buy", "tx_hash": "0xabc", "gas_fee_eth": 0.00001234, "api_key": "nope"}],
            "events": [{"level": "warning", "message": "safe", "provider_response": "nope"}],
            "capacity_warning": {"max_positions": 10, "internal_reason": "nope"},
            "sell_attempt": {
                "status": "quote_below_minimum",
                "quoted_profit_eth": 0.000206,
                "projected_gas_eth": 0.000122,
                "projected_net_profit_eth": 0.000084,
                "minimum_profit_eth": 0.0001,
                "quote_provider": "sushiswap",
                "previous_quote_provider": "uniswap",
                "quoted_return_eth": 0.003206,
                "previous_quoted_return_eth": 0.00362,
                "quote_divergence_percent": 11.44,
                "observed_fee_percent": 2.9999,
                "matching_observations": 2,
                "confirmations_required": 2,
                "detected_fee_percent": 3.0,
                "trace": "nope",
            },
            "buy_attempt": {
                "status": "projected_gas_above_cap",
                "quote_provider": "sushiswap",
                "projected_gas_eth": 0.00008,
                "maximum_gas_eth": 0.00004,
                "gas_limit": 200000,
                "gas_price_wei": 400000000,
                "buy_amount_eth": 0.003,
                "position_id": "4",
                "phase": "prepared_quote",
                "secret": "nope",
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
                "pnl_polling_mode": "bidirectional",
                "pnl_legacy_triggers": False,
                "pnl_focus_side": "legacy",
                "pnl_focus_reason": "triggered",
                "pnl_focus_directions": "sell",
                "pnl_trigger_mode": "minimum_profit",
                "taxed_token": True,
                "token_transfer_fee_percent": 3.0,
                "token_tax_detection_source": "auto-detected",
                "token_tax_detection_observations": 2,
                "swap_slippage_percent": 5.0,
                "positions": [{
                    "id": "1", "pnl": 5.0, "buy_pnl": 4.2, "sell_pnl": 1.7,
                    "legacy_pnl": 3.1,
                    "buy_quote_at": "2026-09-16T03:00:00+00:00",
                    "sell_quote_at": "2026-09-16T03:00:20+00:00",
                    "legacy_quote_at": "2026-09-16T03:00:40+00:00",
                    "buy_quote_provider": "uniswap", "sell_quote_provider": "sushiswap",
                    "legacy_quote_provider": "uniswap",
                    "sell_quote_source_position_id": "1",
                    "buy_projected_gas_eth": 0.00001,
                    "sell_projected_gas_eth": 0.00002,
                    "sell_quote_basis": "exact_sell_quote_extrapolated_net",
                    "legacy_quote_basis": "legacy_gross_0.001_buy_quote",
                }],
                "trades_history": [{"side": "buy", "tx_hash": "0xabc", "gas_fee_eth": 0.00001234}],
                "events": [{"level": "warning", "message": "safe"}],
                "capacity_warning": {"max_positions": 10},
                "sell_attempt": {
                    "status": "quote_below_minimum",
                    "quoted_profit_eth": 0.000206,
                    "projected_gas_eth": 0.000122,
                    "projected_net_profit_eth": 0.000084,
                    "minimum_profit_eth": 0.0001,
                    "quote_provider": "sushiswap",
                    "previous_quote_provider": "uniswap",
                    "quoted_return_eth": 0.003206,
                    "previous_quoted_return_eth": 0.00362,
                    "quote_divergence_percent": 11.44,
                    "observed_fee_percent": 2.9999,
                    "matching_observations": 2,
                    "confirmations_required": 2,
                    "detected_fee_percent": 3.0,
                },
                "buy_attempt": {
                    "status": "projected_gas_above_cap",
                    "quote_provider": "sushiswap",
                    "projected_gas_eth": 0.00008,
                    "maximum_gas_eth": 0.00004,
                    "gas_limit": 200000,
                    "gas_price_wei": 400000000,
                    "buy_amount_eth": 0.003,
                    "position_id": "4",
                    "phase": "prepared_quote",
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

    def test_strategy_and_drawdown_summary_are_strictly_allowlisted(self):
        payload = {
            "strategy_mode": "drawdown_ladder",
            "strategy_spacing": "log",
            "entry_allocation_mode": "drawdown_ladder",
            "drawdown_ladder": {
                "id": "ladder-1",
                "status": "active",
                "spacing": "log",
                "terminal_drawdown_percent": 95.0,
                "levels_total": 50,
                "levels_funded": 17,
                "levels_open": 4,
                "levels_reserved": 13,
                "reserved_eth": 0.043,
                "next_level_amount_eth": 0.003,
                "state_version": 4,
                "mode": "survivor",
                "reanchor_count": 3,
                "last_reanchor_at": 1790950000.0,
                "leading_edge_pending": False,
                "leading_edge_open": True,
                "next_level_price": 0.00000123,
                "next_leading_edge_price": 0.00000145,
                "leading_edge_runway_prices": [0.00000145, 0.00000149, 0.00000153],
                "leading_edge_trigger_percent": 2.5,
                "private_note": "drop me",
            },
        }
        result = _allowlisted_status_payload(payload)
        self.assertEqual(result["strategy_mode"], "drawdown_ladder")
        self.assertEqual(result["strategy_spacing"], "log")
        self.assertEqual(result["entry_allocation_mode"], "drawdown_ladder")
        self.assertEqual(result["drawdown_ladder"], {
            "id": "ladder-1",
            "status": "active",
            "spacing": "log",
            "terminal_drawdown_percent": 95.0,
            "levels_total": 50,
            "levels_funded": 17,
            "levels_open": 4,
            "levels_reserved": 13,
            "reserved_eth": 0.043,
            "next_level_amount_eth": 0.003,
            "state_version": 4,
            "mode": "survivor",
            "reanchor_count": 3,
            "last_reanchor_at": 1790950000.0,
            "leading_edge_pending": False,
            "leading_edge_open": True,
            "next_level_price": 0.00000123,
            "next_leading_edge_price": 0.00000145,
            "leading_edge_runway_prices": [0.00000145, 0.00000149, 0.00000153],
            "leading_edge_trigger_percent": 2.5,
        })
        self.assertEqual(
            _allowlisted_status_payload({"strategy_mode": "survivor"}),
            {"strategy_mode": "survivor"},
        )
        self.assertEqual(
            _allowlisted_status_payload({"entry_allocation_mode": "survivor"}),
            {"entry_allocation_mode": "survivor"},
        )

    def test_invalid_leading_edge_runway_is_dropped(self):
        result = _allowlisted_status_payload({
            "drawdown_ladder": {
                "leading_edge_runway_prices": [1, 2, 3, 4],
            },
        })
        self.assertNotIn(
            "leading_edge_runway_prices", result["drawdown_ladder"]
        )

    def test_invalid_strategy_values_are_dropped_for_legacy_safety(self):
        result = _allowlisted_status_payload({
            "strategy_mode": "execute_everything",
            "strategy_spacing": "spiral",
            "entry_allocation_mode": "surprise",
            "drawdown_ladder": "not-an-object",
        })
        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
