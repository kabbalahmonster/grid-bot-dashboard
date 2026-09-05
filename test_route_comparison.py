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
        self.assertEqual(len(result["candidates"]), 4)
        self.assertEqual(len(result["candidates"][0]["rejections"]), 8)
        self.assertNotIn("opaque", json.dumps(result))
        self.assertNotIn("0x", json.dumps(result))
        self.assertIsNone(result["selected_hypothetical_winner"])

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
        self.assertIn("renderRouteComparison(d.sell_attempt?.route_comparison)", html)

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


if __name__ == "__main__":
    unittest.main()
