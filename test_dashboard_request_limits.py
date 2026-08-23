import unittest

import dashboard_server


class TestDashboardRequestLimits(unittest.TestCase):
    def setUp(self):
        self.client = dashboard_server.app.test_client()

    def test_status_body_limit_returns_json_413(self):
        original_api_key = dashboard_server.API_KEY
        dashboard_server.API_KEY = "test-key"
        try:
            response = self.client.post(
                "/api/status",
                data=b"x" * (dashboard_server.MAX_STATUS_REQUEST_BYTES + 1),
                content_type="application/json",
                headers={"X-API-Key": "test-key"},
            )
        finally:
            dashboard_server.API_KEY = original_api_key
        self.assertEqual(response.status_code, 413)
        self.assertIn("byte limit", response.get_json()["error"])

    def test_dashboard_offers_symbol_sort(self):
        response = self.client.get("/")
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn('<option value="symbol">Symbol</option>', body)
        self.assertIn("mode === 'symbol'", body)

    def test_sigil_animation_is_gated_and_user_controllable(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("sigil-current", body)
        self.assertIn("sigil-glimmer", body)
        self.assertIn("infinite alternate", body)
        self.assertIn("sigil-turn", body)
        self.assertIn("sigil-node-pulse", body)
        self.assertIn('class="sigil-glyph"', body)
        self.assertIn("Date.now() / 1000", body)
        self.assertIn("hasOpenSigil", body)
        self.assertNotIn("stroke-dashoffset: -0.22; opacity:", body)
        self.assertNotIn("sigil-stroke-base", body)
        self.assertIn("dashboard-sigil-animation", body)
        self.assertIn("prefers-reduced-motion: reduce", body)
        self.assertIn("IntersectionObserver", body)
        self.assertIn("setSigilInteractionPaused(true)", body)
        self.assertIn("Animation: ' + (sigilAnimationEnabled ? 'On' : 'Off')", body)
        self.assertIn('aria-label="Toggle sigil animation"', body)
        self.assertIn('pathLength="1"', body)
        self.assertIn("const preservedSigilStages = new Map()", body)
        self.assertIn("placeholder.replaceWith(preservedStage)", body)

    def test_sigil_theater_mode_is_accessible_and_viewport_fitted(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('id="sigil-modal" role="dialog" aria-modal="true"', body)
        self.assertIn('aria-label="Close enlarged sigil"', body)
        self.assertIn('class="toggle-raw sigil-view-large"', body)
        self.assertIn("openSigilModal(bots[botId].sigil, viewButton)", body)
        self.assertIn("event.key === 'Escape'", body)
        self.assertIn("event.target === sigilModal", body)
        self.assertIn("body.sigil-modal-open { overflow: hidden; }", body)


if __name__ == "__main__":
    unittest.main()
