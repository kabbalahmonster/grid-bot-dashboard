import gzip
import unittest

import dashboard_server


class TestDashboardRequestLimits(unittest.TestCase):
    def setUp(self):
        self.client = dashboard_server.app.test_client()

    def test_token_amounts_use_adaptive_precision(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("function formatTokenAmount(value)", body)
        self.assertIn("formatTokenAmount(pos.buy_amount_token)", body)
        self.assertIn("formatTokenAmount(trade.token_amount)", body)
        self.assertNotIn("parseFloat(pos.buy_amount_token || 0).toFixed(0)", body)

    def test_trade_histories_show_confirmed_gas_fee_when_available(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("trade.gas_fee_eth", body)
        self.assertIn('class="trade-gas">⛽ ', body)
        self.assertIn('class="history-gas">⛽ ', body)

    def test_scout_panel_is_collapsed_by_default_with_route_diagnostics(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('<details class="scout-panel" id="scout-panel">', body)
        self.assertNotIn('<details class="scout-panel" id="scout-panel" open>', body)
        self.assertIn('class="scout-routes"', body)
        self.assertIn("route.recovery_percent", body)
        self.assertIn("scout-summary", body)
        self.assertIn('class="scout-icon"', body)
        self.assertIn('aria-hidden="true">🧭</span>', body)

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

    def test_large_dashboard_response_is_gzipped_when_supported(self):
        plain = self.client.get("/")
        compressed = self.client.get("/", headers={"Accept-Encoding": "gzip"})
        self.assertNotIn("Content-Encoding", plain.headers)
        self.assertEqual(compressed.headers["Content-Encoding"], "gzip")
        self.assertIn("Accept-Encoding", compressed.headers["Vary"])
        self.assertEqual(gzip.decompress(compressed.data), plain.data)
        self.assertLess(len(compressed.data), len(plain.data) // 2)

    def test_sse_stream_gzip_flushes_each_message(self):
        chunks = list(dashboard_server._gzip_stream(["event: one\ndata: 1\n\n", "event: two\ndata: 2\n\n"]))
        self.assertEqual(gzip.decompress(b"".join(chunks)), b"event: one\ndata: 1\n\nevent: two\ndata: 2\n\n")
        self.assertGreaterEqual(len(chunks), 2)

    def test_dashboard_offers_symbol_sort(self):
        response = self.client.get("/")
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn('<option value="symbol">Symbol</option>', body)
        self.assertIn("mode === 'symbol'", body)

    def test_dashboard_offers_moonbag_value_sort(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('<option value="moonbag-value">Moonbag value</option>', body)
        self.assertIn("mode === 'moonbag-value'", body)
        self.assertIn("av.estimated_moonbag_value_eth", body)
        self.assertIn("'moonbag-value': 'desc'", body)

    def test_dashboard_offers_market_cap_sort(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('<option value="market-cap">Market Cap</option>', body)
        self.assertIn("mode === 'market-cap'", body)
        self.assertIn("(marketData[a] || {}).value_usd", body)
        self.assertIn("if (sortBots.value === 'market-cap' || sortBots.value === 'day-movement') render(true)", body)

    def test_dashboard_offers_day_movement_sort(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('<option value="day-movement">Day Movement</option>', body)
        self.assertIn('<summary><span class="label">24h</span>', body)
        self.assertIn("mode === 'day-movement'", body)
        self.assertIn("((marketData[a] || {}).price_change || {}).h24", body)
        self.assertIn("sortBots.value === 'day-movement'", body)

    def test_dashboard_offers_session_trade_count_sorts(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('<option value="buys">Session buys</option>', body)
        self.assertIn('<option value="sells">Session sells</option>', body)
        self.assertIn("mode === 'buys'", body)
        self.assertIn("parseInt(av.buys, 10)", body)
        self.assertIn("mode === 'sells'", body)
        self.assertIn("parseInt(av.sells, 10)", body)
        self.assertIn("buys: 'desc', sells: 'desc'", body)

    def test_toolbar_and_currency_settings_persist_locally(self):
        body = self.client.get("/").get_data(as_text=True)
        for key in (
            "dashboard-bot-filter", "dashboard-chain-filter", "dashboard-provider-filter",
            "dashboard-tax-filter",
            "dashboard-sort-mode", "dashboard-sort-direction", "dashboard-profit-currency",
            "dashboard-realized-profit-unit", "dashboard-realized-profit-period",
            "dashboard-sigil-animation", "dashboard-notification-preferences",
        ):
            self.assertIn(key, body)
        self.assertIn("['asc', 'desc'].includes(storedSortDirection)", body)
        self.assertIn("['cad', 'usd'].includes(storedProfitCurrency)", body)

    def test_summary_shows_realized_profit_age_and_averages(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("Date.parse(d.profit_tracking_started_at || '')", body)
        self.assertIn("realizedProfit / (realizedAverageHours / 24)", body)
        self.assertIn("realizedProfit / realizedAverageHours", body)
        self.assertIn("Math.min(realizedPeriodHours, trackingElapsedHours)", body)
        self.assertIn("trackingElapsedHours < realizedPeriodHours", body)
        self.assertIn("all: 'Since ' + trackingAgeText", body)
        self.assertIn("'/day · '", body)
        self.assertIn("'/hr</span>'", body)
        self.assertIn("Fleet realized profit divided by the selected period", body)
        self.assertIn("function realizedProfitForPeriod(d, period)", body)
        self.assertIn("d.trades_history || []", body)

    def test_realized_profit_summary_cycles_units(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("dashboard-realized-profit-unit", body)
        self.assertIn("['eth', 'cad', 'usd'].includes(storedRealizedProfitUnit)", body)
        self.assertIn('data-realized-unit-toggle', body)
        self.assertIn("{ eth: 'cad', cad: 'usd', usd: 'eth' }[realizedProfitUnit]", body)
        self.assertIn("formatRealizedAmount(realizedProfit, true)", body)
        self.assertIn("formatRealizedAmount(realizedDailyAverage, false)", body)
        self.assertIn("formatRealizedAmount(realizedHourlyAverage, false)", body)
        self.assertIn("minimumFractionDigits: 2, maximumFractionDigits: 2", body)
        self.assertIn("button.summary-item:hover, button.summary-item:focus-visible", body)
        self.assertIn("dashboard-realized-profit-period", body)
        self.assertIn("data-realized-period", body)
        for label in ("All", "Month", "Week", "24 hr", "6 hr", "1 hr"):
            self.assertIn("'" + label + "'", body)
        self.assertIn("document.activeElement.matches('[data-realized-period]')", body)
        self.assertIn("if (!periodSelectorOpen && summaryBar.innerHTML !== nextSummaryHtml)", body)
        self.assertIn("'1h': 'Since 1h ago'", body)
        self.assertIn("selector.blur();", body)
        self.assertIn(".realized-period { appearance: none; -webkit-appearance: none;", body)

    def test_fleet_history_summary_dialogs_use_retained_status_data(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('data-fleet-history>Sell history</button>', body)
        self.assertIn('id="history-modal" role="dialog" aria-modal="true"', body)
        self.assertIn("String(trade.side || '').toLowerCase() === 'sell'", body)
        self.assertIn("event.code === 'usdg_banked'", body)
        self.assertIn("entries.sort(function(a, b)", body)
        self.assertIn("delay <= 10 * 60 * 1000", body)
        self.assertIn("No successful banking recorded for this sell", body)
        self.assertIn("Banking transaction ↗", body)
        self.assertIn("refreshOpenHistoryModal();", body)
        self.assertIn("historyList.scrollTop = preservedScrollTop", body)
        self.assertIn("function sellHistoryIdentity(state)", body)
        self.assertIn("const historyChanged = sellHistoryIdentity(previousState) !== sellHistoryIdentity(nextState)", body)
        self.assertIn("historyChanged && !historyModal.hidden && historyModalMode === 'history' && summaryBotIds.includes(entry.bot_id)", body)
        self.assertIn('data-history-focus-bot="', body)
        self.assertIn("focusButton.dataset.historyFocusBot", body)
        self.assertIn("card.scrollIntoView({ behavior: 'smooth', block: 'start' })", body)

    def test_auto_detected_token_tax_is_visible_on_card(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("AUTO TAX ", body)
        self.assertIn("Runtime auto-detected transfer fee", body)
        self.assertIn("token_tax_detection_source", body)

    def test_estimated_bag_values_use_existing_balances_and_fiat_rates(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("function estimatedBagValue(d, currency)", body)
        self.assertIn("ethBalance + tokenBalance * tokenPriceEth", body)
        self.assertIn("usdgBalance * ethRate / usdRate", body)
        self.assertIn("Estimated fleet value:", body)
        self.assertIn("data-bag-currency-toggle>' + fiatCode + '</button>", body)
        self.assertIn("usdgBalance * ethPrices.cad / ethPrices.usd", body)
        self.assertIn("formatBagValue(usdgCadValue, 'cad') + ' CAD", body)
        self.assertLess(body.index("Estimated fleet value:"), body.index("Session profit:"))
        self.assertIn("['Estimated Bag Value', 'estimated_bag_value']", body)
        self.assertIn("data-bag-currency-toggle", body)
        self.assertIn("liquidation value may be lower", body)
        self.assertIn('<option value="estimated-value">Estimated value</option>', body)
        self.assertIn("mode === 'estimated-value'", body)
        self.assertIn("estimatedBagValue(av, profitCurrency)", body)
        self.assertIn("'estimated-value': 'desc'", body)
        self.assertLess(body.index("Needs new positions:"), body.index("Active:"))
        self.assertIn("Total ETH: ' +", body)
        self.assertIn("totalEthBalance.toFixed(8) + ' ETH'", body)
        self.assertIn("totalEthBalance * fiatRate", body)
        self.assertIn("Combined ETH balance across the displayed bots", body)
        self.assertLess(body.index("formatRealizedAmount(realizedHourlyAverage, false)"), body.index("Total ETH: ' +"))

    def test_needs_positions_names_focus_their_bot_cards(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('data-focus-bot="', body)
        self.assertIn('tabindex="-1"', body)
        self.assertIn("function focusBotCard(botId)", body)
        self.assertIn("focusBotCard(focusLink.dataset.focusBot)", body)
        self.assertIn("card.scrollIntoView({ behavior: 'smooth', block: 'start' })", body)
        self.assertIn("card.focus({ preventScroll: true })", body)
        self.assertIn(".card:focus { outline: 2px solid #f59e0b", body)

    def test_active_stale_and_offline_summaries_open_linked_bot_lists(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('data-status-list="running"', body)
        self.assertIn('data-status-list="stale"', body)
        self.assertIn('data-status-list="offline"', body)
        self.assertIn("function openStatusList(wantedStatus, trigger, refreshOnly)", body)
        self.assertIn("select a coin to jump to its card", body)
        self.assertIn("data-history-focus-bot=", body)
        self.assertIn("function focusBotCard(botId)", body)
        self.assertIn("refreshOpenHistoryModal();", body)
        self.assertIn("chain ? chain.name : 'Unknown chain'", body)
        self.assertIn("provider unreported", body)
        self.assertIn("positions].join(' · ')", body)
        self.assertIn("taxFilterEnabled = false;", body)
        self.assertIn("body.history-modal-open { overflow: hidden; }", body)

    def test_dashboard_surfaces_structured_needs_gas_warning(self):
        body = self.client.get("/").get_data(as_text=True)

        self.assertIn("function needsGasState(state)", body)
        self.assertIn("const warningThreshold = reserve * 0.5", body)
        self.assertIn("balance >= warningThreshold", body)
        self.assertIn("Boolean(needsGasState(state))", body)
        self.assertIn("⛽ Needs gas:", body)
        self.assertIn("⛽ NEEDS GAS — TRADES MAY FAIL", body)
        self.assertIn("Warning below:", body)
        self.assertIn("gasWarning.shortfall_eth", body)

    def test_dashboard_surfaces_buy_blocked_funding_warning(self):
        body = self.client.get("/").get_data(as_text=True)

        self.assertIn("Boolean(state.funding_warning)", body)
        self.assertIn("💸 Needs funds:", body)
        self.assertIn("💸 BUY BLOCKED — NEEDS TRADING FUNDS", body)
        self.assertIn("d.funding_warning.minimum_trade_balance", body)

    def test_sell_checks_summary_lists_and_focuses_running_bots(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("const activeSellChecks = Object.keys(bots).filter", body)
        self.assertIn("Boolean(state.sell_attempt)", body)
        self.assertIn("Sell checks active: ' + activeSellChecks.length", body)
        self.assertIn("bots[id].token_symbol || bots[id].display_name || id", body)
        self.assertIn("data-focus-bot=", body)
        self.assertLess(body.index("Sell checks active:"), body.index("Needs new positions:"))
        self.assertLess(body.index("Sell checks active:"), body.index("Active: ' + active"))
        self.assertIn(".summary-item.sell-checks-active", body)
        self.assertNotIn('Sell checks active: 0</span>', body)
        self.assertNotIn('Needs new positions: 0</span>', body)

    def test_sell_check_displays_projected_net_profit_after_gas(self):
        body = self.client.get("/").get_data(as_text=True)

        self.assertIn("d.sell_attempt.projected_net_profit_eth", body)
        self.assertIn("d.sell_attempt.projected_gas_eth", body)
        self.assertIn("quoted - projectedGas", body)
        self.assertIn("Projected net profit after sell gas / minimum profit", body)
        self.assertIn("ETH net", body)
        self.assertIn("d.sell_attempt.quote_provider", body)
        self.assertIn('class="sell-attempt-provider"', body)
        self.assertIn("QUOTE DISAGREEMENT — SELL BLOCKED", body)
        self.assertIn("quote_divergence_percent", body)

    def test_buy_gas_block_summary_and_card_show_fee_cap_and_provider(self):
        body = self.client.get("/").get_data(as_text=True)

        self.assertIn("const buyGasBlocked = Object.keys(bots).filter", body)
        self.assertIn("Buy gas blocked: ' + buyGasBlocked.length", body)
        self.assertIn("BUY ATTEMPT — GAS CAP BLOCKED", body)
        self.assertIn("attempt.projected_gas_eth", body)
        self.assertIn("attempt.maximum_gas_eth", body)
        self.assertIn("attempt.quote_provider", body)

    def test_more_info_shows_estimated_next_buy_and_gas_reserve(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("['Buy Point', 'buy_point_percent']", body)
        self.assertIn("['Sell Point', 'sell_point_percent']", body)
        self.assertIn("['Next Buy Est.', 'next_buy_estimated_eth']", body)
        self.assertIn("['Gas Reserve', 'gas_reserve_eth']", body)
        self.assertIn("function estimatedNextBuy(d)", body)
        self.assertIn("(parseFloat(d.eth_balance) || 0) - reserve", body)
        self.assertIn("toFixed(5)", body)

    def test_dashboard_offers_operational_next_buy_and_capacity_sorts(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('<option value="next-buy-estimate">Next buy estimate</option>', body)
        self.assertIn("mode === 'next-buy-estimate'", body)
        self.assertIn("estimatedNextBuy(av)", body)
        self.assertIn("'next-buy-estimate': 'desc'", body)
        self.assertIn('<option value="needs-positions">Needs positions</option>', body)
        self.assertIn("mode === 'needs-positions'", body)
        self.assertIn("Number(Boolean(av.capacity_warning))", body)

    def test_browser_notification_types_are_optional_and_deduplicated(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('class="notification-menu" id="notification-menu" hidden', body)
        self.assertIn("@media (max-width: 600px)", body)
        self.assertIn("bottom: calc(1rem + env(safe-area-inset-bottom, 0px))", body)
        self.assertIn("max-height: calc(100dvh - 2rem - env(safe-area-inset-bottom, 0px))", body)
        for alert_type in ("sells", "positions", "offline", "recovered", "buys", "stoploss", "treasury", "errors", "safety"):
            self.assertIn(f'data-notification-type="{alert_type}"', body)
        self.assertIn("const notificationDefaults = { sells: true, positions: false, offline: false", body)
        self.assertIn("dashboard-notification-preferences", body)
        self.assertIn("dashboard-notifications-enabled", body)
        self.assertIn("notificationsMasterEnabled && 'Notification' in window", body)
        self.assertIn("processBotNotifications(entry.bot_id, previousState, nextState)", body)
        self.assertIn("const previousTrades = new Set", body)
        self.assertIn("previous.capacity_warning && next.capacity_warning", body)
        self.assertIn("previousAge === 'offline'", body)
        self.assertIn("nextTreasury > previousTreasury", body)
        self.assertIn("event.code !== 'usdg_banked'", body)
        self.assertIn("count >= 3 && (previousEvents.get(key) || 0) < 3", body)
        self.assertIn("POSITION BALANCE MISMATCH — SELL BLOCKED", body)
        self.assertIn("!previousMismatch && nextMismatch", body)
        self.assertIn("previousMismatch && !nextMismatch", body)
        self.assertIn("Notification.requestPermission()", body)
        self.assertIn('id="notification-note"', body)
        self.assertIn("Notification.permission === 'denied'", body)
        self.assertIn('add doomdash.ca to the Home Screen', body)

    def test_dashboard_can_manually_reconnect_and_refresh_cards(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('id="reconnect-cards"', body)
        self.assertIn("reconnectCardsButton.addEventListener('click'", body)
        self.assertIn("reconnectCardsButton.textContent = 'Refreshing…'", body)
        self.assertIn("reconnectNow();", body)

        self.assertIn("fetch('/api/bots', { cache: 'no-store' })", body)
        self.assertIn("if (entry && entry.state) nextBots[botId] = entry.state", body)
        self.assertIn("refreshCardsFromApi()", body)

    def test_scout_renderer_uses_dashboard_chain_metadata(self):
        response = self.client.get("/")
        body = response.get_data(as_text=True)

        self.assertIn("(chainMetadata[Number(report.chain_id)] || {}).explorer", body)
        self.assertNotIn("CHAIN_INFO[Number(report.chain_id)]", body)
        self.assertIn("render(true);", body)
        self.assertIn("fetchEthPrices();", body)
        self.assertIn("fetchMarketData();", body)
        self.assertIn("Object.keys(bots).forEach(function(botId) { delete bots[botId]; });", body)

    def test_empty_filtered_fleet_offers_clear_all_filters(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('id="clear-all-filters"', body)
        self.assertIn("'No bots match your filters'", body)
        self.assertIn("clearAllFilters.addEventListener('click'", body)
        self.assertIn("chainFilter.value = '';", body)
        self.assertIn("providerFilter.value = '';", body)
        self.assertIn("taxFilterEnabled = false;", body)

    def test_tax_coin_filter_includes_declared_and_auto_detected_tokens(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('id="tax-filter" type="button" aria-pressed="false"', body)
        self.assertIn("['manual', 'declared', 'auto-detected'].includes(taxSource)", body)
        self.assertIn("d.taxed_token === true", body)
        self.assertIn("!taxFilterEnabled || isTaxedToken", body)
        self.assertIn("Tax coins only ✓", body)

    def test_connection_status_exposes_live_diagnostics(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('id="connection-diagnostics"', body)
        self.assertIn("Last live message:", body)
        self.assertIn("Last full snapshot:", body)
        self.assertIn("Manual/automatic reconnects:", body)
        self.assertIn("Cards in memory:", body)

    def test_closing_live_panels_never_forces_a_grid_render(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("panel.addEventListener('toggle', loadChart)", body)
        self.assertIn("panel.addEventListener('toggle', drawSigil)", body)
        self.assertNotIn("else if (!container.querySelector('details.chart-panel[open]')) render(true)", body)
        self.assertNotIn("else if (!container.querySelector('details.sigil-panel[open]')) render(true)", body)

    def test_chart_lookup_failure_offers_retry(self):
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn("retry.textContent = 'Retry'", body)
        self.assertIn("replacement.dataset.resolver = resolver", body)
        self.assertIn("errorState.replaceWith(replacement)", body)
        self.assertIn("frame.dataset.loading === 'true'", body)
        self.assertIn(".chart-retry", body)

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
        self.assertIn("!changedBotIds.has(botId)", body)
        self.assertIn("summaryBar.innerHTML !== nextSummaryHtml", body)

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
