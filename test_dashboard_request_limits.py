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

    def test_dashboard_offers_market_cap_sort(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('<option value="market-cap">Market Cap</option>', body)
        self.assertIn("mode === 'market-cap'", body)
        self.assertIn("(marketData[a] || {}).value_usd", body)
        self.assertIn("if (sortBots.value === 'market-cap' || sortBots.value === 'day-movement') render(true)", body)

    def test_dashboard_offers_day_movement_sort(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('<option value="day-movement">Day Movement</option>', body)
        self.assertIn("mode === 'day-movement'", body)
        self.assertIn("((marketData[a] || {}).price_change || {}).h24", body)
        self.assertIn("sortBots.value === 'day-movement'", body)

    def test_closing_live_panels_never_forces_a_grid_render(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("panel.addEventListener('toggle', loadChart)", body)
        self.assertIn("panel.addEventListener('toggle', drawSigil)", body)
        self.assertNotIn("else if (!container.querySelector('details.chart-panel[open]')) render(true)", body)
        self.assertNotIn("else if (!container.querySelector('details.sigil-panel[open]')) render(true)", body)

    def test_live_panels_are_preserved_while_surrounding_card_data_updates(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("updateGridPreservingLivePanels", body)
        self.assertIn("updateCardAroundLivePanels", body)
        self.assertIn("function morphNode(currentNode, freshNode)", body)
        self.assertIn("function morphRange(currentNodes, freshNodes, boundary)", body)
        self.assertIn("updateGridPreservingLivePanels(html, force ? null : changedBotIds)", body)
        self.assertIn("details.chart-panel[open], details.sigil-panel[open]", body)
        self.assertIn("morphRange(currentSegment, freshChildren.slice(freshStart, freshIndex), livePanel)", body)
        self.assertIn("morphRange(trailingCurrent, freshChildren.slice(freshStart), trailingBoundary)", body)
        self.assertIn("panel.dataset.chartWired", body)
        self.assertIn("panel.dataset.sigilWired", body)
        self.assertIn("scheduleRoutineRender(entry.bot_id)", body)
        self.assertIn("const pendingChangedBotIds = new Set()", body)
        self.assertIn("render(false, changedBotIds)", body)
        self.assertIn("updateGridPreservingLivePanels(html, changedBotIds)", body)
        self.assertIn("const card = currentCards.get(botId)", body)
        self.assertIn("stage.dataset.clockSynced", body)
        self.assertIn("const visualOrder = new Map()", body)
        self.assertIn("card.style.order = nextOrder", body)
        self.assertIn("card.hidden = true", body)
        self.assertIn("requestIdleCallback(flush, { timeout: 1200 })", body)
        self.assertIn("routineRenderTimer !== null || routineRenderIdleCallback !== null", body)
        self.assertIn("if (card.style.order !== nextOrder)", body)
        self.assertIn("contain: layout paint style", body)
        self.assertIn("currentNode.isEqualNode(freshNode)", body)
        self.assertIn("const minimumGap = 15000", body)
        self.assertIn("nextSignature === marketDataSignature", body)
        self.assertIn("marketDataFetchInFlight", body)
        self.assertIn("animationVisible ? 10000 : 1000", body)
        self.assertIn("if (el.textContent !== nextText)", body)

    def test_sigil_animation_is_gated_and_user_controllable(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("sigil-current", body)
        self.assertIn("sigil-glimmer", body)
        self.assertIn("infinite alternate", body)
        self.assertIn("sigil-turn", body)
        self.assertIn("sigil-node-pulse", body)
        self.assertIn('class="sigil-glyph"', body)
        self.assertIn("Date.now() / 1000", body)
        self.assertIn("updateGridPreservingLivePanels", body)
        self.assertNotIn("stroke-dashoffset: -0.22; opacity:", body)
        self.assertNotIn("sigil-stroke-base", body)
        self.assertIn("dashboard-sigil-animation", body)
        self.assertIn("prefers-reduced-motion: reduce", body)
        self.assertIn("IntersectionObserver", body)
        self.assertIn("setSigilInteractionPaused(true)", body)
        self.assertIn("Animation: ' + (sigilAnimationEnabled ? 'On' : 'Off')", body)
        self.assertIn('aria-label="Toggle sigil animation"', body)
        self.assertIn('pathLength="1"', body)
        self.assertIn("updateCardAroundLivePanels", body)
        self.assertNotIn("drop-shadow", body)

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
