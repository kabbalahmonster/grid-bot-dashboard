import copy
import json
import subprocess
import unittest
from unittest.mock import patch

import dashboard_server as server


def comparison(direction="buy"):
    row = dict(provider="uniswap", settlement="native", validation_level="quote_only",
               quoted_output_raw="12345678901234567890", output_floor_raw="12000000000000000000",
               gas_components_wei=dict(swap="10", approval="0", wrap="0", unwrap="0"),
               projected_total_gas_wei="10", gas_basis="conservative_budget_not_simulated",
               slippage_fraction=0.01, tax_fraction=0, approval_assumption="none",
               projected_net_score="1.234E+20", score_unit="output_raw_per_eth_total_cost",
               rejections=[], execution_eligible=False)
    return dict(mode="shadow", direction=direction, status="hypothetical_only", candidates=[row],
                selected_hypothetical_winner=dict(provider="uniswap", settlement="native"),
                runner_up_delta="0", elapsed_ms=12.3,
                observation_timing="after_execution_attempt_with_pre_operation_budget")


class TestRouteComparison(unittest.TestCase):
    def clean(self, value, direction="buy"):
        return server._allowlisted_status_payload({direction + "_attempt": {"status": "checking", "route_comparison": value}})[direction + "_attempt"]

    def test_exact_round_trip_and_legacy_fields(self):
        for direction in ("buy", "sell"):
            value = comparison(direction)
            self.assertEqual(self.clean(value, direction), {"status": "checking", "route_comparison": value})
        self.assertEqual(self.clean(None), {"status": "checking"})

    def test_fixed_depth_and_bounds(self):
        value = comparison()
        row = value["candidates"][0]
        for obj in (value, row, row["gas_components_wei"], value["selected_hypothetical_winner"]):
            obj.update(address="0x" + "a" * 40, calldata="0xdead", credentials={"secret": "opaque"}, provider_error="opaque", raw_response={"x": "opaque"})
        row["rejections"] = ["native_reserve"] * 30
        value["candidates"] *= 10
        result = self.clean(value)["route_comparison"]
        self.assertEqual(len(result["candidates"]), 8)
        self.assertEqual(len(result["candidates"][0]["rejections"]), 8)
        self.assertNotIn("opaque", json.dumps(result))
        self.assertNotIn("0x", json.dumps(result))
        self.assertIsNone(result["selected_hypothetical_winner"])

    def test_all_supported_providers_survive_sanitization(self):
        value = comparison("sell")
        value["candidates"] = []
        for provider in ("uniswap", "sushiswap", "umbra", "lifi"):
            for settlement in ("native", "weth"):
                row = copy.deepcopy(comparison("sell")["candidates"][0])
                row.update(provider=provider, settlement=settlement)
                value["candidates"].append(row)
        value["selected_hypothetical_winner"] = {"provider": "lifi", "settlement": "native"}

        clean = self.clean(value, "sell")["route_comparison"]
        self.assertEqual(len(clean["candidates"]), 8)
        self.assertEqual(
            {(row["provider"], row["settlement"]) for row in clean["candidates"]},
            {(provider, settlement) for provider in ("uniswap", "sushiswap", "umbra", "lifi")
             for settlement in ("native", "weth")},
        )
        self.assertEqual(clean["selected_hypothetical_winner"],
                         {"provider": "lifi", "settlement": "native"})

    def test_invalid_values_never_survive(self):
        for field, invalids in {
            "quoted_output_raw": [True, 12, "-1", str(2**256), "9" * 1000, {"secret": "x"}, "0x123"],
            "projected_net_score": ["NaN", "Infinity", "1e999", "<script>", [], True],
            "slippage_fraction": [True, -1, 1, float("nan"), float("inf"), "0.1"],
            "approval_assumption": ["api-secret", {}, "0x" + "a" * 40],
        }.items():
            for invalid in invalids:
                with self.subTest(field=field, invalid=invalid):
                    value = comparison()
                    value["candidates"][0][field] = invalid
                    self.assertNotIn(field, self.clean(value)["route_comparison"]["candidates"][0])
        for invalid in (True, -1, 3600001, float("inf"), "12"):
            value = comparison()
            value["elapsed_ms"] = invalid
            self.assertNotIn("elapsed_ms", self.clean(value)["route_comparison"])

    def test_invalid_identity_and_execution(self):
        for key, invalid in (("provider", "secret"), ("settlement", {}), ("validation_level", "simulated"), ("execution_eligible", True)):
            value = comparison()
            value["candidates"][0][key] = invalid
            result = self.clean(value)["route_comparison"]
            self.assertEqual(result["candidates"], [])
            self.assertIsNone(result["selected_hypothetical_winner"])
        for key, invalid in (("mode", "live"), ("direction", "sell"), ("status", {}), ("candidates", {})):
            value = comparison()
            value[key] = invalid
            self.assertNotIn("route_comparison", self.clean(value))

    def test_failure_and_null_cases(self):
        for status in ("no_eligible_candidate", "observation_failed"):
            value = comparison()
            value.update(status=status, candidates=[], selected_hypothetical_winner=None, runner_up_delta=None)
            self.assertEqual(self.clean(value)["route_comparison"], value)
        value = comparison()
        value["candidates"][0].update(quoted_output_raw=None, projected_net_score=None, rejections=["unknown secret"])
        result = self.clean(value)["route_comparison"]
        self.assertEqual(result["candidates"][0]["validation_level"], "rejected")
        self.assertIsNone(result["selected_hypothetical_winner"])

    def test_not_sampled_candidate_is_preserved_and_rendered(self):
        value = comparison("sell")
        row = value["candidates"][0]
        row.update(validation_level="rejected", quoted_output_raw=None,
                   projected_net_score=None, rejections=["observation_deadline"],
                   candidate_outcome="not_sampled",
                   gas_price_currentness="unknown")
        value.update(status="no_eligible_candidate", selected_hypothetical_winner=None,
                     runner_up_delta=None)

        clean = self.clean(value, "sell")["route_comparison"]
        self.assertEqual(clean["candidates"][0]["candidate_outcome"], "not_sampled")

        html = server.DASHBOARD_HTML
        start = html.index("  function renderRouteComparison(")
        end = html.index("\n  function ", html.index("  function esc(", start) + 4)
        script = """const document = {createElement: () => ({textContent: '',
          get innerHTML() { return this.textContent.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'); }
        })};\n""" + html[start:end]
        output = subprocess.run(["node", "-e", script + '\nconsole.log(renderRouteComparison(' + json.dumps(clean) + '));'],
                                capture_output=True, text=True, check=True)
        self.assertIn("Not sampled before observation deadline", output.stdout)

    def test_observation_timeout_is_preserved_and_rendered_as_not_sampled(self):
        value = comparison("sell")
        row = value["candidates"][0]
        row.update(validation_level="rejected", quoted_output_raw=None,
                   projected_net_score=None, rejections=["observation_timeout"],
                   quote_failure_kind="observation_timeout",
                   gas_price_currentness="unknown")
        value.update(status="no_eligible_candidate", selected_hypothetical_winner=None,
                     runner_up_delta=None)

        clean = self.clean(value, "sell")["route_comparison"]
        self.assertEqual(clean["candidates"][0]["rejections"], ["observation_timeout"])
        self.assertEqual(clean["candidates"][0]["quote_failure_kind"], "observation_timeout")

        html = server.DASHBOARD_HTML
        start = html.index("  function renderRouteComparison(")
        end = html.index("\n  function ", html.index("  function esc(", start) + 4)
        script = """const document = {createElement: () => ({textContent: '',
          get innerHTML() { return this.textContent.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'); }
        })};\n""" + html[start:end]
        output = subprocess.run(["node", "-e", script + '\nconsole.log(renderRouteComparison(' + json.dumps(clean) + '));'],
                                capture_output=True, text=True, check=True)
        self.assertIn("Quote timed out within observation deadline", output.stdout)

    def test_actual_javascript_rendering(self):
        html = server.DASHBOARD_HTML
        start = html.index("  function renderRouteComparison(")
        end = html.index("\n  function ", html.index("  function esc(", start) + 4)
        script = html[start:end]
        # Minimal DOM text-node serialization for the existing browser esc helper.
        script = """const document = {createElement: () => ({textContent: '',
          get innerHTML() { return this.textContent.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'); }
        })};\n""" + script
        values = [comparison(), None]
        for status in ("no_eligible_candidate", "observation_failed"):
            value = comparison()
            value.update(status=status, candidates=[], selected_hypothetical_winner=None, runner_up_delta=None)
            values.append(value)
        malicious = comparison()
        malicious["candidates"][0]["provider"] = "<script>alert(1)</script>"
        values.append(malicious)
        output = subprocess.run(["node", "-e", script + '\nconsole.log(JSON.stringify(' + json.dumps(values) + '.map(renderRouteComparison)));'], capture_output=True, text=True, check=True)
        rendered = json.loads(output.stdout)
        for text in ("SHADOW ROUTE COMPARISON", "did not choose or affect the live trade", "Hypothetical winner: uniswap / native", "quote_only", "12345678901234567890", "approval 0", "total 10", "Normalized score", "Runner-up delta", "12.3 ms"):
            self.assertIn(text, rendered[0])
        self.assertEqual(rendered[1], "")
        self.assertIn("No eligible candidate", rendered[2])
        self.assertIn("Observation failed", rendered[3])
        self.assertNotIn("<script>", rendered[4])
        self.assertIn("&lt;script&gt;", rendered[4])
        self.assertIn("renderRouteComparison(sellRouteComparison, botKey)", html)

    def test_renderer_tolerates_restored_malformed_rejections(self):
        html = server.DASHBOARD_HTML
        start = html.index("  function renderRouteComparison(")
        end = html.index("\n  function ", html.index("  function esc(", start) + 4)
        script = """const document = {createElement: () => ({textContent: '',
          get innerHTML() { return this.textContent; }
        })};\n""" + html[start:end]
        value = comparison()
        value["candidates"][0]["rejections"] = "corrupt legacy state"
        result = subprocess.run(["node", "-e", script + "\nrenderRouteComparison(" + json.dumps(value) + ");"],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_ingest_security_and_transient_lifecycle(self):
        client = server.app.test_client()
        headers = {"X-API-Key": "route-test-key"}
        payload = {"bot_id": "route-test", "buy_attempt": {"route_comparison": comparison()}}
        with patch.object(server, "API_KEY", "route-test-key"), patch.object(server, "bot_states", {}), patch.object(server, "bot_history", copy.deepcopy(server.bot_history)), patch.object(server, "_mark_state_dirty_locked"), patch.object(server, "_broadcast"), patch.object(server, "_is_rate_limited", return_value=False), patch.object(server.telegram_alerts, "start"), patch.object(server.telegram_alerts, "process_status"):
            self.assertEqual(client.post("/api/status", json=payload).status_code, 401)
            self.assertEqual(client.post("/api/status", json=payload, headers=headers).status_code, 200)
            self.assertIn("route_comparison", server.bot_states["route-test"]["buy_attempt"])
            self.assertEqual(client.post("/api/status", json={"bot_id": "route-test"}, headers=headers).status_code, 200)
            self.assertNotIn("buy_attempt", server.bot_states["route-test"])
            payload["buy_attempt"]["route_comparison"]["secret"] = "0x" + "a" * 64
            self.assertEqual(client.post("/api/status", json=payload, headers=headers).status_code, 400)
            payload["buy_attempt"]["route_comparison"]["secret"] = "x" * server.MAX_STATUS_REQUEST_BYTES
            self.assertEqual(client.post("/api/status", json=payload, headers=headers).status_code, 413)

    # ------------------------------------------------------------------
    # Allowlist extension: provider gas, fresh gas price, human-readable
    # totals, and the new approval_assumption taxonomy.
    # ------------------------------------------------------------------

    def test_provider_gas_estimate_allowlist(self):
        """provider_gas_estimate is a non-negative int passed through verbatim."""
        for raw, expected in [(180000, 180000), (0, 0), (500000, 500000)]:
            with self.subTest(raw=raw):
                value = comparison()
                value["candidates"][0]["provider_gas_estimate"] = raw
                row = self.clean(value)["route_comparison"]["candidates"][0]
                self.assertEqual(row.get("provider_gas_estimate"), expected)
        for invalid in (-1, True, "abc", [], None, "9" * 1000, 2 ** 256):
            with self.subTest(invalid=invalid):
                value = comparison()
                value["candidates"][0]["provider_gas_estimate"] = invalid
                row = self.clean(value)["route_comparison"]["candidates"][0]
                self.assertNotIn("provider_gas_estimate", row)

    def test_effective_gas_price_wei_allowlist(self):
        """effective_gas_price_wei is a non-negative int (raw wei, bounded)."""
        for raw, expected in [(1_000_000, 1_000_000), (5_000_000_000, 5_000_000_000), (0, 0)]:
            with self.subTest(raw=raw):
                value = comparison()
                value["candidates"][0]["effective_gas_price_wei"] = raw
                row = self.clean(value)["route_comparison"]["candidates"][0]
                self.assertEqual(row.get("effective_gas_price_wei"), expected)
        for invalid in (-1, True, "<script>", "1e999", [], None, 2 ** 256):
            with self.subTest(invalid=invalid):
                value = comparison()
                value["candidates"][0]["effective_gas_price_wei"] = invalid
                row = self.clean(value)["route_comparison"]["candidates"][0]
                self.assertNotIn("effective_gas_price_wei", row)

    def test_gas_total_eth_allowlist(self):
        """gas_total_eth is a non-negative float in reasonable range."""
        for raw, expected in [(0.0001, 0.0001), (0.0, 0.0), (0.5, 0.5)]:
            with self.subTest(raw=raw):
                value = comparison()
                value["candidates"][0]["gas_total_eth"] = raw
                row = self.clean(value)["route_comparison"]["candidates"][0]
                self.assertEqual(row.get("gas_total_eth"), expected)
        for invalid in (-0.1, float("nan"), float("inf"), True, "<x>", []):
            with self.subTest(invalid=invalid):
                value = comparison()
                value["candidates"][0]["gas_total_eth"] = invalid
                row = self.clean(value)["route_comparison"]["candidates"][0]
                self.assertNotIn("gas_total_eth", row)

    def test_output_floor_human_allowlist(self):
        """output_floor_human is a non-negative float (human-readable token units)."""
        for raw, expected in [(12.5, 12.5), (0.0, 0.0), (3.14, 3.14)]:
            with self.subTest(raw=raw):
                value = comparison()
                value["candidates"][0]["output_floor_human"] = raw
                row = self.clean(value)["route_comparison"]["candidates"][0]
                self.assertEqual(row.get("output_floor_human"), expected)
        for invalid in (-1, float("nan"), float("inf"), True, "<x>", []):
            with self.subTest(invalid=invalid):
                value = comparison()
                value["candidates"][0]["output_floor_human"] = invalid
                row = self.clean(value)["route_comparison"]["candidates"][0]
                self.assertNotIn("output_floor_human", row)

    def test_quoted_output_human_allowlist(self):
        """quoted_output_human is a non-negative float."""
        value = comparison()
        value["candidates"][0]["quoted_output_human"] = 21.0
        row = self.clean(value)["route_comparison"]["candidates"][0]
        self.assertEqual(row.get("quoted_output_human"), 21.0)
        # Invalid types are dropped.
        value["candidates"][0]["quoted_output_human"] = -1
        row = self.clean(value)["route_comparison"]["candidates"][0]
        self.assertNotIn("quoted_output_human", row)

    def test_extended_gas_basis_enum(self):
        """gas_basis accepts the new 'provider_estimate' label too."""
        for label in ("local_estimate", "provider_estimate", "conservative_direction_fallback", "conservative_budget_not_simulated", "skipped"):
            with self.subTest(label=label):
                value = comparison()
                value["candidates"][0]["gas_basis"] = label
                row = self.clean(value)["route_comparison"]["candidates"][0]
                self.assertEqual(row.get("gas_basis"), label)
        # Unknown labels are dropped.
        value = comparison()
        value["candidates"][0]["gas_basis"] = "<script>alert(1)</script>"
        row = self.clean(value)["route_comparison"]["candidates"][0]
        self.assertNotIn("gas_basis", row)

    def test_extended_approval_assumption_enum(self):
        """approval_assumption accepts the new 'existing_allowance_covers' label too."""
        for label in ("existing_allowance_covers", "reset_and_exact_approval_budget", "none"):
            with self.subTest(label=label):
                value = comparison()
                value["candidates"][0]["approval_assumption"] = label
                row = self.clean(value)["route_comparison"]["candidates"][0]
                self.assertEqual(row.get("approval_assumption"), label)
        value = comparison()
        value["candidates"][0]["approval_assumption"] = "opaque-secret"
        row = self.clean(value)["route_comparison"]["candidates"][0]
        self.assertNotIn("approval_assumption", row)

    def test_legacy_payload_unchanged(self):
        """Bots running the old payload shape still render without new fields."""
        value = comparison()  # legacy fixture: no new fields present
        result = self.clean(value)["route_comparison"]
        # The new fields simply aren't there; nothing else breaks.
        row = result["candidates"][0]
        for new_field in ("provider_gas_estimate", "effective_gas_price_wei",
                          "gas_total_eth", "output_floor_human", "quoted_output_human"):
            self.assertNotIn(new_field, row)
        # Legacy fields still present.
        self.assertIn("quoted_output_raw", row)
        self.assertIn("projected_total_gas_wei", row)
        self.assertEqual(result["selected_hypothetical_winner"],
                         {"provider": "uniswap", "settlement": "native"})

    def test_renderer_shows_provider_gas_and_total_eth(self):
        """The JS renderer emits the new gas/price/cost fields when present."""
        html = server.DASHBOARD_HTML
        start = html.index("  function renderRouteComparison(")
        end = html.index("\n  function ", html.index("  function esc(", start) + 4)
        script = html[start:end]
        script = """const document = {createElement: () => ({textContent: '',
          get innerHTML() { return this.textContent.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'); }
        })};
""" + script
        enriched = comparison()
        enriched["candidates"][0].update(
            provider_gas_estimate=180000,
            effective_gas_price_wei=2000000,
            gas_total_eth=0.00042,
            output_floor_human=12.5,
            quoted_output_human=21.0,
        )
        out = subprocess.run(["node", "-e", script + "\nconsole.log(JSON.stringify(renderRouteComparison(" + json.dumps(enriched) + ")));"],
                             capture_output=True, text=True, check=True)
        rendered = json.loads(out.stdout)
        for needle in ("180000", "2000000", "0.000420", "12.5", "21"):
            with self.subTest(needle=needle):
                self.assertIn(needle, rendered, f"renderer missing {needle!r}: {rendered!r}")

    def test_failure_context_and_gas_currentness_are_allowlisted(self):
        value = comparison()
        row = value["candidates"][0]
        row.update(rejections=["provider_quote_failed"], quote_failure_kind="provider_quote_failed",
                   gas_price_currentness="fresh", gas_price_age_seconds=4.5)
        clean = self.clean(value)["route_comparison"]["candidates"][0]
        self.assertEqual(clean["quote_failure_kind"], "provider_quote_failed")
        self.assertEqual(clean["gas_price_currentness"], "fresh")
        self.assertEqual(clean["gas_price_age_seconds"], 4.5)
        for key, invalid in (("quote_failure_kind", "<script>"),
                             ("gas_price_currentness", "secret"),
                             ("gas_price_age_seconds", -1)):
            value = comparison()
            value["candidates"][0][key] = invalid
            self.assertNotIn(key, self.clean(value)["route_comparison"]["candidates"][0])

    def test_renderer_explains_missing_quotes_and_summarizes_status(self):
        html = server.DASHBOARD_HTML
        start = html.index("  function renderRouteComparison(")
        end = html.index("\n  function ", html.index("  function esc(", start) + 4)
        script = """const document = {createElement: () => ({textContent: '',
          get innerHTML() { return this.textContent.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'); }
        })};\n""" + html[start:end]
        value = comparison()
        value["candidates"][0].update(quoted_output_raw=None, output_floor_raw=None,
            projected_net_score=None, rejections=["provider_quote_failed"],
            quote_failure_kind="provider_quote_failed", gas_price_currentness="fresh",
            gas_price_age_seconds=4.5)
        output = subprocess.run(["node", "-e", script + "\nconsole.log(renderRouteComparison(" + json.dumps(value) + "));"],
                                capture_output=True, text=True, check=True).stdout
        self.assertIn("No provider quote available", output)
        self.assertIn("Provider quote failed", output)
        self.assertIn("fresh", output)
        self.assertIn("4.5s old", output)
        self.assertIn("1 candidate", output)

    def test_live_tournament_profit_fields_and_final_are_allowlisted(self):
        value = comparison("sell")
        value.update(mode="execution_preflight", status="completed",
                     observation_timing="parallel_pre_execution",
                     final={"tx_hash": "0x" + "a" * 64, "received_eth": 0.0023,
                            "gas_fee_eth": 0.00005, "profit_eth": 0.0002,
                            "profit_percent": 9.52})
        value["candidates"][0].update(
            score_unit="net_return_after_all_projected_gas_wei", protocol="V4",
            projected_profit_wei="200000000000000", projected_profit_eth=0.0002,
            projected_profit_percent=9.52, sold_cost_wei="2100000000000000",
            minimum_return_wei="2205000000000000", minimum_return_eth=0.002205,
            minimum_profit_percent=5.0,
        )
        clean = self.clean(value, "sell")["route_comparison"]
        self.assertEqual(clean["mode"], "execution_preflight")
        self.assertEqual(clean["candidates"][0]["projected_profit_percent"], 9.52)
        self.assertEqual(clean["candidates"][0]["minimum_profit_percent"], 5.0)
        self.assertEqual(clean["final"]["tx_hash"], "0x" + "a" * 64)

    def test_live_tournament_renderer_has_rank_crown_expansion_and_blockscout(self):
        html = server.DASHBOARD_HTML
        for needle in ("tournament-scoreboard", "tournament-contestant", "👑",
                       "Estimated return / minimum", "robinhoodchain.blockscout.com/tx/",
                       "Active tournaments", ">Tx ↗</a>",
                       ".tournament-final a { color: inherit; }"):
            self.assertIn(needle, html)
        self.assertNotIn(">View transaction ↗</a>", html)

    def test_live_tournament_renderer_has_buy_copy_and_sell_target(self):
        html = server.DASHBOARD_HTML
        for needle in ("BUY ROUTE BATTLE", "SELL ROUTE TOURNAMENT",
                       "Best acquisition route selected", "Quoted tokens:",
                       "Conservative receive floor:", "target +"):
            self.assertIn(needle, html)
        self.assertIn("timedOut ? 'timed out'", html)

    def test_sell_tournament_suppresses_redundant_active_sell_check(self):
        html = server.DASHBOARD_HTML
        self.assertIn("const sellRouteComparison = tournamentForDisplay(d.sell_attempt?.route_comparison, botKey);", html)
        self.assertIn("const tournamentOwnsSellStatus = Boolean(", html)
        self.assertIn("if (!tournamentOwnsSellStatus && d.sell_attempt && d.sell_attempt.status === 'quote_below_minimum')", html)
        self.assertIn("if (!tournamentOwnsSellStatus && d.sell_attempt && (d.sell_attempt.status === 'quote_provider_disagreement' || d.sell_attempt.status === 'quote_provider_changed'))", html)
        self.assertIn("renderRouteComparison(sellRouteComparison, botKey)", html)

    def test_completed_tournament_lingers_once_then_expires_and_active_replaces_it(self):
        html = server.DASHBOARD_HTML
        start = html.index("  function tournamentForDisplay(")
        end = html.index("\n  function renderRouteComparison(", start)
        helper = html[start:end]
        script = """
let now = 0;
Date.now = () => now;
const completedTournamentDisplays = new Map();
const completedTournamentLingerMs = 60000;
""" + helper + """
const done = {mode: 'execution_preflight', direction: 'sell', status: 'completed', final: {tx_hash: '0xabc'}};
const active = {mode: 'execution_preflight', direction: 'sell', status: 'preflight_candidate_selected'};
const results = [];
results.push(Boolean(tournamentForDisplay(done, 'BOT')));
now = 59000; results.push(Boolean(tournamentForDisplay(done, 'BOT')));
now = 60000; results.push(Boolean(tournamentForDisplay(done, 'BOT')));
results.push(tournamentForDisplay(active, 'BOT') === active);
results.push(Boolean(tournamentForDisplay(done, 'BOT')));
console.log(JSON.stringify(results));
"""
        output = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=True).stdout
        self.assertEqual(json.loads(output), [True, True, False, True, True])
        self.assertIn("setInterval(function() {", html)
        self.assertIn("expiredBotIds.add(display.botId)", html)


if __name__ == "__main__":
    unittest.main()
