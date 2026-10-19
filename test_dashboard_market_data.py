import unittest
from unittest.mock import Mock, patch

import requests

import dashboard_test_env  # noqa: F401  (must precede application import)
import dashboard_server


TOKEN = "0x" + "1" * 40
PAIR = "0x" + "2" * 40
OVERRIDE_PAIR = "0x" + "9" * 40


def response(pairs):
    result = Mock()
    result.raise_for_status.return_value = None
    result.json.return_value = pairs
    return result


class TestDexscreenerMarketData(unittest.TestCase):
    def setUp(self):
        with dashboard_server._dexscreener_lock:
            dashboard_server._dexscreener_pair_cache.clear()
        with dashboard_server._lock:
            dashboard_server.bot_states.clear()
        self.client = dashboard_server.app.test_client()

    @patch("dashboard_server.http_requests.get")
    def test_prefers_active_eth_pair_and_true_market_cap(self, get):
        get.return_value = response([
            {"pairAddress": "0x" + "3" * 40, "quoteToken": {"symbol": "USDC"},
             "liquidity": {"usd": 999999}, "volume": {"h24": 999999}, "marketCap": 900},
            {"pairAddress": PAIR, "quoteToken": {"symbol": "ETH"},
             "baseToken": {"address": TOKEN}, "priceUsd": "1",
             "liquidity": {"usd": 10, "base": 5}, "volume": {"h24": 20},
             "marketCap": 1234567, "fdv": 7654321,
             "priceChange": {"m5": -0.5, "h1": 1.25, "h6": -3, "h24": 8.43}},
        ])
        data = dashboard_server._dexscreener_pair_data(8453, TOKEN)
        self.assertEqual(data["pair_address"], PAIR)
        self.assertEqual(data["label"], "Market Cap")
        self.assertEqual(data["value_usd"], 1234567.0)
        self.assertEqual(data["price_change"], {"m5": -0.5, "h1": 1.25, "h6": -3.0, "h24": 8.43})
        self.assertEqual(data["pair_selection"], "automatic")

    @patch("dashboard_server.http_requests.get")
    def test_explicit_pair_override_wins_over_automatic_ranking(self, get):
        get.return_value = response([
            {"pairAddress": PAIR, "quoteToken": {"symbol": "ETH"},
             "liquidity": {"usd": 1000}, "volume": {"h24": 1000},
             "marketCap": 111},
            {"pairAddress": OVERRIDE_PAIR, "quoteToken": {"symbol": "WETH"},
             "liquidity": {"usd": 10}, "volume": {"h24": 10},
             "marketCap": 222},
        ])

        data = dashboard_server._dexscreener_pair_data(
            8453, TOKEN, OVERRIDE_PAIR.upper().replace("0X", "0x")
        )

        self.assertEqual(data["pair_address"], OVERRIDE_PAIR)
        self.assertEqual(data["value_usd"], 222.0)
        self.assertEqual(data["pair_selection"], "override")

    @patch("dashboard_server.http_requests.get")
    def test_unknown_explicit_pair_fails_closed(self, get):
        get.return_value = response([
            {"pairAddress": PAIR, "quoteToken": {"symbol": "ETH"},
             "liquidity": {"usd": 1000}, "volume": {"h24": 1000}},
        ])

        with self.assertRaisesRegex(LookupError, "not returned for token"):
            dashboard_server._dexscreener_pair_data(8453, TOKEN, OVERRIDE_PAIR)

    @patch("dashboard_server.http_requests.get")
    def test_rejects_bogus_high_liquidity_pair_with_incoherent_reserves(self, get):
        real_pair = "0x" + "4" * 40
        bogus_pair = "0x" + "5" * 40
        get.return_value = response([
            {"pairAddress": real_pair, "baseToken": {"address": TOKEN},
             "quoteToken": {"symbol": "ETH"}, "priceUsd": "0.002",
             "liquidity": {"usd": 120000, "base": 30000000},
             "volume": {"h24": 600000}, "marketCap": 1200000},
            {"pairAddress": bogus_pair, "baseToken": {"address": TOKEN},
             "quoteToken": {"symbol": "ETH"}, "priceUsd": "2.47",
             "liquidity": {"usd": 34000000, "base": 13765000},
             "volume": {"h24": 7}, "marketCap": 1340000000},
        ])
        data = dashboard_server._dexscreener_pair_data(4663, TOKEN)
        self.assertEqual(data["pair_address"], real_pair)
        self.assertEqual(data["value_usd"], 1200000.0)

    @patch("dashboard_server.http_requests.get")
    def test_fdv_is_explicit_fallback(self, get):
        get.return_value = response([
            {"pairAddress": PAIR, "baseToken": {"symbol": "TOKEN"},
             "quoteToken": {"symbol": "WETH"}, "liquidity": {"usd": 10}, "fdv": 4200000}
        ])
        data = dashboard_server._dexscreener_pair_data(4663, TOKEN)
        self.assertEqual(data["label"], "FDV")
        self.assertEqual(data["value_usd"], 4200000.0)

    @patch("dashboard_server.http_requests.get")
    def test_stale_cache_survives_api_failure(self, get):
        get.return_value = response([
            {"pairAddress": PAIR, "quoteToken": {"symbol": "WETH"},
             "liquidity": {"usd": 10}, "marketCap": 1000}
        ])
        dashboard_server._dexscreener_pair_data(8453, TOKEN)
        with dashboard_server._dexscreener_lock:
            key = (8453, TOKEN.lower(), "")
            _, cached = dashboard_server._dexscreener_pair_cache[key]
            dashboard_server._dexscreener_pair_cache[key] = (0, cached)
        get.side_effect = requests.ConnectionError("offline")
        data = dashboard_server._dexscreener_pair_data(8453, TOKEN)
        self.assertTrue(data["stale"])
        self.assertEqual(data["value_usd"], 1000.0)

    @patch("dashboard_server.http_requests.get")
    def test_batched_endpoint_deduplicates_identical_tokens(self, get):
        get.return_value = response([
            {"pairAddress": PAIR, "quoteToken": {"symbol": "WETH"},
             "liquidity": {"usd": 10}, "marketCap": 777}
        ])
        with dashboard_server._lock:
            dashboard_server.bot_states.update({
                "alpha": {"chain_id": 8453, "token_address": TOKEN},
                "beta": {"chain_id": 8453, "token_address": TOKEN},
                "invalid": {"chain_id": 999, "token_address": TOKEN},
            })
        result = self.client.get("/api/dexscreener/market-data")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(set(result.get_json()["bots"]), {"alpha", "beta"})
        self.assertEqual(get.call_count, 1)

    @patch("dashboard_server.http_requests.get")
    def test_batched_endpoint_isolates_pair_overrides(self, get):
        get.return_value = response([
            {"pairAddress": PAIR, "quoteToken": {"symbol": "ETH"},
             "liquidity": {"usd": 1000}, "volume": {"h24": 1000}},
            {"pairAddress": OVERRIDE_PAIR, "quoteToken": {"symbol": "WETH"},
             "liquidity": {"usd": 10}, "volume": {"h24": 10}},
        ])
        with dashboard_server._lock:
            dashboard_server.bot_states.update({
                "automatic": {"chain_id": 8453, "token_address": TOKEN},
                "overridden": {
                    "chain_id": 8453,
                    "token_address": TOKEN,
                    "dexscreener_pair_address": OVERRIDE_PAIR,
                },
            })

        result = self.client.get("/api/dexscreener/market-data")
        bots = result.get_json()["bots"]

        self.assertEqual(bots["automatic"]["pair_address"], PAIR)
        self.assertEqual(bots["overridden"]["pair_address"], OVERRIDE_PAIR)
        self.assertEqual(get.call_count, 2)

    @patch("dashboard_server.http_requests.get")
    def test_chart_url_uses_explicit_pair_override(self, get):
        get.return_value = response([
            {"pairAddress": PAIR, "quoteToken": {"symbol": "ETH"}},
            {"pairAddress": OVERRIDE_PAIR, "quoteToken": {"symbol": "WETH"}},
        ])

        result = self.client.get(
            "/api/dexscreener/chart-url",
            query_string={
                "chain_id": 8453,
                "token_address": TOKEN,
                "pair_address": OVERRIDE_PAIR,
            },
        )

        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.get_json()["pair_address"], OVERRIDE_PAIR)
        self.assertIn(OVERRIDE_PAIR, result.get_json()["chart_url"])

    def test_market_cap_row_precedes_average_pnl(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertLess(body.index("data-market-key"), body.index("metrics.forEach"))
        self.assertIn("fetchMarketData", body)
        self.assertIn("setInterval(fetchMarketData, 60000)", body)
        self.assertIn('data-market-window="h24"', body)
        self.assertIn('data-market-window="m5"', body)
        self.assertIn('data-market-window="h1"', body)
        self.assertIn('data-market-window="h6"', body)
        self.assertIn("openMarketMovements", body)

    def test_open_chart_refreshes_when_preferred_pair_changes(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("const expectedPair", body)
        self.assertIn("frame.dataset.pairAddress", body)
        self.assertIn("loadedPair === expectedPair", body)
        self.assertIn("frame.src = data.chart_url", body)
        self.assertIn("chartParams.set('pair_address'", body)

    def test_more_info_shows_estimated_moonbag_value(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("['Est. Moonbag Value', 'estimated_moonbag_value_eth']", body)
        self.assertIn("key === 'estimated_moonbag_value_eth'", body)

    @patch("dashboard_server.http_requests.get")
    def test_missing_or_invalid_movements_are_null(self, get):
        get.return_value = response([
            {"pairAddress": PAIR, "quoteToken": {"symbol": "WETH"},
             "liquidity": {"usd": 10}, "marketCap": 1000,
             "priceChange": {"m5": "bad", "h24": None}}
        ])
        data = dashboard_server._dexscreener_pair_data(8453, TOKEN)
        self.assertEqual(data["price_change"], {"m5": None, "h1": None, "h6": None, "h24": None})


if __name__ == "__main__":
    unittest.main()
