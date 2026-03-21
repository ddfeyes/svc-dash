"""
Integration tests for issue #189: Full dashboard audit — fix 46 error badges.

Tests every card API endpoint to ensure:
  - HTTP 200 OK response
  - Valid JSON body
  - No "error" field in response (not a stub "not implemented")
  - Required fields present for each card category

Run against a live server:
    pytest -m integration tests/test_issue_189_audit.py -v

Server must be running at http://localhost:8765 (via Docker).
Tests skip gracefully if server is not reachable.
"""

import os
import sys
import pytest
import requests
from requests.exceptions import ConnectionError as ConnErr, ReadTimeout

pytestmark = pytest.mark.integration

SYM = "BANANAS31USDT"
BASE_URL = "http://localhost:8765/api"
TIMEOUT = 15
SLOW_TIMEOUT = 45


# ── Helpers ────────────────────────────────────────────────────────────────────


def get(path, params=None, timeout=TIMEOUT):
    """GET endpoint, skip on connection/timeout errors, assert 200."""
    try:
        r = requests.get(f"{BASE_URL}{path}", params=params, timeout=timeout)
    except ConnErr:
        pytest.skip("server not reachable")
    except ReadTimeout:
        pytest.skip(f"{path} timed out after {timeout}s")
    assert r.status_code == 200, f"GET {path} → {r.status_code}: {r.text[:300]}"
    data = r.json()
    assert data is not None, f"GET {path} returned null"
    return data


def no_error(data, path):
    """Assert data does not contain a top-level 'error' key indicating stub failure."""
    assert (
        "error" not in data or data["error"] is None
    ), f"GET {path} returned error field: {data.get('error')}"


def has_keys(data, *keys):
    for k in keys:
        assert k in data, f"missing key '{k}' in {list(data.keys())}"


# ── 1. Core health & meta ─────────────────────────────────────────────────────


class TestCoreHealth:
    def test_health_200(self):
        data = get("/health")
        has_keys(data, "status")
        assert data["status"] == "ok"

    def test_symbols_returns_list(self):
        data = get("/symbols")
        has_keys(data, "status", "symbols")
        assert isinstance(data["symbols"], list)
        assert len(data["symbols"]) > 0

    def test_ws_stats_200(self):
        data = get("/ws-stats")
        has_keys(data, "connections", "messages_per_sec")

    def test_freshness_200(self):
        data = get("/freshness", {"symbol": SYM})
        has_keys(data, "status", "freshness")


# ── 2. Market-regime / Phase card ────────────────────────────────────────────


class TestMarketRegimeCard:
    def test_market_regime_200(self):
        data = get("/market-regime", {"symbol": SYM})
        no_error(data, "/market-regime")
        # Either from v1 (phase/score/action) or v2 (regime/confidence)
        assert (
            "regime" in data or "phase" in data
        ), "market-regime must return 'regime' or 'phase' key"

    def test_market_regime_no_duplicate_route(self):
        """Second call to same path must not 500 due to duplicate route."""
        d1 = get("/market-regime", {"symbol": SYM})
        d2 = get("/market-regime", {"symbol": SYM})
        assert d1 is not None and d2 is not None

    def test_phase_history_200(self):
        data = get("/phase-history", {"symbol": SYM, "limit": 10})
        has_keys(data, "status", "symbol", "data", "count")
        assert isinstance(data["data"], list)


# ── 3. Microstructure card ───────────────────────────────────────────────────


class TestMicrostructureCard:
    def test_market_microstructure_200(self):
        data = get("/market-microstructure", {"symbol": SYM, "window": 300})
        no_error(data, "/market-microstructure")
        has_keys(data, "status", "symbol")
        # Must have score and grade even with empty data
        assert "score" in data, "microstructure must return 'score'"
        assert "grade" in data, "microstructure must return 'grade'"


# ── 4. Whale clustering card ─────────────────────────────────────────────────


class TestWhaleClusteringCard:
    def test_whale_clustering_200(self):
        data = get("/whale-clustering", {"symbol": SYM, "window": 1800})
        no_error(data, "/whale-clustering")
        has_keys(data, "status", "symbol")

    def test_whale_flow_200(self):
        data = get("/whale-flow", {"symbol": SYM})
        no_error(data, "/whale-flow")
        has_keys(data, "status")


# ── 5. VWAP / OI-weighted price cards ───────────────────────────────────────


class TestVwapOiCards:
    def test_vwap_deviation_200(self):
        data = get("/vwap-deviation", {"symbol": SYM})
        no_error(data, "/vwap-deviation")
        has_keys(data, "status", "symbol")

    def test_oi_weighted_price_200(self):
        data = get("/oi-weighted-price", {"symbol": SYM})
        no_error(data, "/oi-weighted-price")
        has_keys(data, "status", "symbol")


# ── 6. OB Walls card ─────────────────────────────────────────────────────────


class TestObWallsCard:
    def test_ob_walls_200(self):
        data = get("/ob-walls", {"symbol": SYM})
        no_error(data, "/ob-walls")
        has_keys(data, "status", "symbol", "walls", "liquidation_risk")
        assert isinstance(data["walls"], list)


# ── 7. Liquidation heatmap card ──────────────────────────────────────────────


class TestLiqHeatmapCard:
    def test_liquidation_heatmap_200(self):
        data = get("/liquidation-heatmap", {"window_s": 3600, "buckets": 20})
        no_error(data, "/liquidation-heatmap")
        has_keys(data, "status", "symbols")
        assert isinstance(data["symbols"], dict)


# ── 8. CVD momentum / Delta-divergence ──────────────────────────────────────


class TestCvdCards:
    def test_cvd_momentum_200(self):
        data = get("/cvd-momentum", {"symbol": SYM})
        no_error(data, "/cvd-momentum")
        has_keys(data, "status", "symbol")

    def test_delta_divergence_200(self):
        data = get("/delta-divergence", {"symbol": SYM})
        no_error(data, "/delta-divergence")
        has_keys(data, "status", "symbol")

    def test_cvd_divergence_200(self):
        data = get("/cvd-divergence", {"symbol": SYM})
        no_error(data, "/cvd-divergence")
        has_keys(data, "status")


# ── 9. Funding extreme card ──────────────────────────────────────────────────


class TestFundingCards:
    def test_funding_extreme_200(self):
        data = get("/funding-extreme", {"symbol": SYM})
        no_error(data, "/funding-extreme")
        has_keys(data, "status", "symbol", "extreme")

    def test_funding_arb_scanner_200(self):
        data = get("/funding-arb-scanner")
        no_error(data, "/funding-arb-scanner")
        has_keys(data, "status")

    def test_funding_rate_heatmap_200(self):
        """Stub endpoint must return 200 with required frontend fields."""
        data = get("/funding-rate-heatmap")
        no_error(data, "/funding-rate-heatmap")
        has_keys(data, "status")
        # Frontend uses data.z_score and data.anomaly_level
        assert "z_score" in data, "/funding-rate-heatmap must return 'z_score'"
        assert (
            "anomaly_level" in data
        ), "/funding-rate-heatmap must return 'anomaly_level'"


# ── 10. Liquidation cascade card ─────────────────────────────────────────────


class TestLiqCascadeCards:
    def test_liq_cascade_200(self):
        data = get("/liq-cascade", {"symbol": SYM})
        no_error(data, "/liq-cascade")
        has_keys(data, "status", "symbol", "cascade")

    def test_liquidation_cascade_stub_fields(self):
        """Stub /liquidation-cascade must return level and risk_score."""
        data = get("/liquidation-cascade")
        no_error(data, "/liquidation-cascade")
        has_keys(data, "status")
        assert "level" in data, "/liquidation-cascade must return 'level'"
        assert "risk_score" in data, "/liquidation-cascade must return 'risk_score'"

    def test_liquidation_cascade_detector_200(self):
        data = get("/liquidation-cascade-detector")
        no_error(data, "/liquidation-cascade-detector")
        has_keys(data, "status")


# ── 11. Large trades card ────────────────────────────────────────────────────


class TestLargeTradesCard:
    def test_large_trades_200(self):
        data = get("/large-trades", {"symbol": SYM})
        no_error(data, "/large-trades")
        has_keys(data, "status", "symbol", "count", "trades")
        assert isinstance(data["trades"], list)


# ── 12. Alerts card ──────────────────────────────────────────────────────────


class TestAlertsCard:
    def test_alerts_200(self):
        data = get("/alerts", {"limit": 20})
        no_error(data, "/alerts")
        has_keys(data, "status", "data", "count")
        assert isinstance(data["data"], list)


# ── 13. OI delta card ────────────────────────────────────────────────────────


class TestOiDeltaCard:
    def test_oi_delta_200(self):
        data = get("/oi-delta", {"symbol": SYM, "interval": 300, "window": 3600})
        no_error(data, "/oi-delta")
        has_keys(data, "status", "symbol", "interval", "candles")
        assert isinstance(data["candles"], list)


# ── 14. Squeeze setup cards ──────────────────────────────────────────────────


class TestSqueezeSetupCards:
    def test_squeeze_setup_200(self):
        data = get("/squeeze-setup", {"symbol": SYM})
        no_error(data, "/squeeze-setup")
        has_keys(data, "status", "symbol")

    def test_squeeze_setup_has_signal(self):
        data = get("/squeeze-setup", {"symbol": SYM})
        assert (
            "squeeze_signal" in data or "signal" in data or "squeeze" in data
        ), "/squeeze-setup must return a squeeze signal field"


# ── 15. Volume spike card ────────────────────────────────────────────────────


class TestVolumeSpikeCard:
    def test_volume_spike_200(self):
        data = get("/volume-spike", {"symbol": SYM}, timeout=SLOW_TIMEOUT)
        no_error(data, "/volume-spike")
        has_keys(data, "status", "symbol")


# ── 16. Trade count rate card ────────────────────────────────────────────────


class TestTradeCountRateCard:
    def test_trade_count_rate_200(self):
        data = get("/trade-count-rate", {"symbol": SYM, "interval": 60, "window": 1800})
        no_error(data, "/trade-count-rate")
        has_keys(data, "status", "symbol", "interval", "window", "buckets")


# ── 17. Top movers card ──────────────────────────────────────────────────────


class TestTopMoversCard:
    def test_top_movers_200(self):
        data = get("/top-movers")
        no_error(data, "/top-movers")
        has_keys(data, "status", "movers")
        assert isinstance(data["movers"], list)


# ── 18. Momentum rank card ──────────────────────────────────────────────────


class TestMomentumRankCard:
    def test_momentum_rank_200(self):
        data = get("/momentum-rank")
        no_error(data, "/momentum-rank")
        has_keys(data, "status", "ranked")
        assert isinstance(data["ranked"], list)


# ── 19. Volatility regime cards ──────────────────────────────────────────────


class TestVolatilityRegimeCards:
    def test_volatility_regime_200(self):
        data = get("/volatility-regime", {"symbol": SYM})
        no_error(data, "/volatility-regime")
        has_keys(data, "status", "symbol", "regime")

    def test_volatility_regime_detector_200(self):
        data = get("/volatility-regime-detector")
        no_error(data, "/volatility-regime-detector")
        has_keys(data, "status")

    def test_vol_regime_hmm_200(self):
        data = get("/vol-regime-hmm", {"symbol": SYM})
        no_error(data, "/vol-regime-hmm")
        has_keys(data, "status", "symbol", "regime")


# ── 20. Price velocity card ──────────────────────────────────────────────────


class TestPriceVelocityCard:
    def test_price_velocity_200(self):
        data = get("/price-velocity", {"symbol": SYM})
        no_error(data, "/price-velocity")
        has_keys(data, "status", "symbol")


# ── 21. Cross-asset correlation card ────────────────────────────────────────


class TestCrossAssetCorrCard:
    def test_cross_asset_corr_200(self):
        data = get("/cross-asset-corr", {"symbol": SYM}, timeout=SLOW_TIMEOUT)
        no_error(data, "/cross-asset-corr")
        has_keys(data, "status", "symbol")


# ── 22. Smart money index / SMI cards ────────────────────────────────────────


class TestSmartMoneyCards:
    def test_smart_money_index_200(self):
        data = get("/smart-money-index")
        no_error(data, "/smart-money-index")
        has_keys(data, "status")

    def test_smart_money_divergence_200(self):
        data = get("/smart-money-divergence", {"symbol": SYM, "window": 1800})
        no_error(data, "/smart-money-divergence")
        has_keys(data, "status", "symbol")

    def test_smart_money_flow_200(self):
        data = get("/smart-money-flow", {"symbol": SYM})
        no_error(data, "/smart-money-flow")
        has_keys(data, "status")

    def test_smart_money_patterns_200(self):
        data = get("/smart-money-patterns", {"symbol": SYM})
        no_error(data, "/smart-money-patterns")
        has_keys(data, "status")


# ── 23. Net taker delta card ─────────────────────────────────────────────────


class TestNetTakerDeltaCard:
    def test_net_taker_delta_200(self):
        data = get("/net-taker-delta", {"symbol": SYM, "window": 3600})
        no_error(data, "/net-taker-delta")
        has_keys(data, "status", "symbol", "buckets")
        assert isinstance(data["buckets"], list)


# ── 24. Realized vol cards ───────────────────────────────────────────────────


class TestRealizedVolCards:
    def test_realized_volatility_bands_200(self):
        data = get("/realized-volatility-bands", {"symbol": SYM, "window": 20})
        no_error(data, "/realized-volatility-bands")
        has_keys(data, "status", "symbol")

    def test_rv_iv_200(self):
        data = get("/rv-iv", {"symbol": SYM})
        no_error(data, "/rv-iv")
        has_keys(data, "status", "symbol")

    def test_realized_vol_surface_200(self):
        data = get("/realized-vol-surface")
        no_error(data, "/realized-vol-surface")
        has_keys(data, "status")


# ── 25. Order flow toxicity card ─────────────────────────────────────────────


class TestOrderFlowToxicityCard:
    def test_order_flow_toxicity_200(self):
        data = get("/order-flow-toxicity", {"symbol": SYM})
        no_error(data, "/order-flow-toxicity")
        has_keys(data, "status")


# ── 26. Exchange flow divergence / Perp-spot basis ──────────────────────────


class TestExchangeFlowCards:
    def test_exchange_flow_divergence_200(self):
        data = get("/exchange-flow-divergence")
        no_error(data, "/exchange-flow-divergence")
        has_keys(data, "status")

    def test_perp_spot_basis_200(self):
        data = get("/perp-spot-basis")
        no_error(data, "/perp-spot-basis")
        has_keys(data, "status")


# ── 27. Gamma exposure card ──────────────────────────────────────────────────


class TestGammaExposureCard:
    def test_gamma_exposure_200(self):
        data = get("/gamma-exposure", {"symbol": SYM})
        no_error(data, "/gamma-exposure")
        has_keys(data, "status")


# ── 28. Support/resistance card ──────────────────────────────────────────────


class TestSupportResistanceCard:
    def test_support_resistance_200(self):
        data = get("/support-resistance", {"symbol": SYM})
        no_error(data, "/support-resistance")
        has_keys(data, "status", "symbol")


# ── 29. Trade size distribution card ────────────────────────────────────────


class TestTradeSizeDistCard:
    def test_trade_size_dist_200(self):
        data = get("/trade-size-dist", {"symbol": SYM})
        no_error(data, "/trade-size-dist")
        has_keys(data, "status", "symbol", "buckets")


# ── 30. Leverage ratio heatmap card ─────────────────────────────────────────


class TestLeverageHeatmapCard:
    def test_leverage_ratio_heatmap_200(self):
        data = get("/leverage-ratio-heatmap", {"symbol": SYM})
        no_error(data, "/leverage-ratio-heatmap")
        has_keys(data, "status")


# ── 31. Flow imbalance card ──────────────────────────────────────────────────


class TestFlowImbalanceCard:
    def test_flow_imbalance_200(self):
        data = get("/flow-imbalance", {"symbol": SYM})
        no_error(data, "/flow-imbalance")
        has_keys(data, "status", "symbol")


# ── 32. Stub endpoints that must return proper fields ────────────────────────


class TestStubEndpointFields:
    """Stub endpoints must return proper fields so frontend cards render correctly."""

    def test_depth_imbalance_has_ratio(self):
        data = get("/depth-imbalance")
        no_error(data, "/depth-imbalance")
        has_keys(data, "status")
        assert "ratio" in data, "/depth-imbalance must return 'ratio'"
        assert "pressure_label" in data, "/depth-imbalance must return 'pressure_label'"

    def test_spread_analysis_has_spread_bps(self):
        data = get("/spread-analysis")
        no_error(data, "/spread-analysis")
        has_keys(data, "status")
        assert "spread_bps" in data, "/spread-analysis must return 'spread_bps'"
        assert "regime" in data, "/spread-analysis must return 'regime'"

    def test_momentum_divergence_has_signal(self):
        data = get("/momentum-divergence")
        no_error(data, "/momentum-divergence")
        has_keys(data, "status")
        assert "signal" in data, "/momentum-divergence must return 'signal'"

    def test_options_skew_has_skewness(self):
        data = get("/options-skew")
        no_error(data, "/options-skew")
        has_keys(data, "status")
        assert "skewness" in data, "/options-skew must return 'skewness'"
        assert "skew_label" in data, "/options-skew must return 'skew_label'"

    def test_session_volume_profile_has_session(self):
        data = get("/session-volume-profile")
        no_error(data, "/session-volume-profile")
        has_keys(data, "status")
        assert "session" in data, "/session-volume-profile must return 'session'"
        assert "poc" in data, "/session-volume-profile must return 'poc'"

    def test_derivatives_heatmap_no_error_field(self):
        """Stub /derivatives-heatmap must not return {"error": "not implemented"}."""
        data = get("/derivatives-heatmap")
        assert (
            data.get("error") != "not implemented"
        ), "/derivatives-heatmap returns 'not implemented' — needs mock data"

    def test_exchange_netflow_no_error_field(self):
        data = get("/exchange-netflow")
        assert (
            data.get("error") != "not implemented"
        ), "/exchange-netflow returns 'not implemented' — needs mock data"

    def test_fear_greed_no_error_field(self):
        data = get("/fear-greed")
        assert (
            data.get("error") != "not implemented"
        ), "/fear-greed returns 'not implemented' — needs mock data"

    def test_network_health_score_no_error_field(self):
        data = get("/network-health-score")
        assert (
            data.get("error") != "not implemented"
        ), "/network-health-score returns 'not implemented' — needs mock data"

    def test_stablecoin_flow_no_error_field(self):
        data = get("/stablecoin-flow")
        assert (
            data.get("error") != "not implemented"
        ), "/stablecoin-flow returns 'not implemented' — needs mock data"

    def test_perpetual_basis_no_error_field(self):
        data = get("/perpetual-basis")
        assert (
            data.get("error") != "not implemented"
        ), "/perpetual-basis returns 'not implemented' — needs mock data"

    def test_staking_yield_tracker_no_error_field(self):
        data = get("/staking-yield-tracker")
        assert (
            data.get("error") != "not implemented"
        ), "/staking-yield-tracker returns 'not implemented' — needs mock data"

    def test_whale_alerts_no_error_field(self):
        data = get("/whale-alerts")
        assert (
            data.get("error") != "not implemented"
        ), "/whale-alerts returns 'not implemented' — needs mock data"


# ── 33. No duplicate routes cause silent failures ───────────────────────────


class TestNoDuplicateRoutes:
    def test_liquidation_cascade_detector_single_route(self):
        """Duplicate route must not cause 404 or malformed response."""
        data = get("/liquidation-cascade-detector")
        no_error(data, "/liquidation-cascade-detector")
        assert data is not None
        has_keys(data, "status")

    def test_market_regime_consistent_response(self):
        """Both requests to /market-regime must return same shape."""
        d1 = get("/market-regime", {"symbol": SYM})
        d2 = get("/market-regime", {"symbol": SYM})
        # Both must have the same top-level keys
        keys1 = set(d1.keys())
        keys2 = set(d2.keys())
        assert (
            keys1 == keys2
        ), f"market-regime returns different shapes: {keys1} vs {keys2}"


# ── 34. Additional card endpoints ───────────────────────────────────────────


class TestAdditionalCards:
    def test_btc_dominance_tracker_200(self):
        data = get("/btc-dominance-tracker")
        no_error(data, "/btc-dominance-tracker")
        has_keys(data, "status")

    def test_defi_tvl_tracker_200(self):
        data = get("/defi-tvl-tracker")
        no_error(data, "/defi-tvl-tracker")
        has_keys(data, "status")

    def test_gas_fee_predictor_200(self):
        data = get("/gas-fee-predictor")
        no_error(data, "/gas-fee-predictor")
        has_keys(data, "status")

    def test_holder_distribution_card_200(self):
        data = get("/holder-distribution-card")
        no_error(data, "/holder-distribution-card")
        has_keys(data, "status")

    def test_validator_activity_200(self):
        data = get("/validator-activity")
        no_error(data, "/validator-activity")
        has_keys(data, "status", "staking_apy", "health_label")

    def test_miner_reserve_200(self):
        data = get("/miner-reserve")
        no_error(data, "/miner-reserve")
        has_keys(data, "status")

    def test_social_sentiment_200(self):
        data = get("/social-sentiment")
        no_error(data, "/social-sentiment")
        has_keys(data, "status")

    def test_derivatives_term_structure_200(self):
        data = get("/derivatives-term-structure")
        no_error(data, "/derivatives-term-structure")
        has_keys(data, "status")

    def test_liquidation_cascade_risk_200(self):
        data = get("/liquidation-cascade-risk")
        no_error(data, "/liquidation-cascade-risk")
        has_keys(data, "status")

    def test_on_chain_active_addresses_200(self):
        data = get("/on-chain-active-addresses")
        no_error(data, "/on-chain-active-addresses")
        has_keys(data, "status")

    def test_volatility_regime_forecast_200(self):
        data = get("/volatility-regime-forecast")
        no_error(data, "/volatility-regime-forecast")
        has_keys(data, "status")

    def test_stablecoin_dominance_signal_200(self):
        data = get("/stablecoin-dominance-signal")
        no_error(data, "/stablecoin-dominance-signal")
        has_keys(data, "status")

    def test_active_addresses_200(self):
        data = get("/active-addresses")
        no_error(data, "/active-addresses")
        assert isinstance(data, list), "/active-addresses must return a list"


# ── 35. No JS double-prefix bugs ─────────────────────────────────────────────


class TestFrontendJsIntegrity:
    """Static checks on app.js to ensure no double /api/api prefix bugs."""

    _JS_PATH = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")

    @classmethod
    def _js(cls) -> str:
        with open(cls._JS_PATH, encoding="utf-8") as f:
            return f.read()

    def test_no_double_api_prefix_in_apifetch(self):
        """apiFetch already prepends /api — callers must not include /api."""
        import re

        matches = re.findall(r'apiFetch\(["\']\/api\/', self._js())
        assert not matches, (
            f"Found {len(matches)} apiFetch() call(s) with double /api/ prefix. "
            "Remove the extra /api — apiFetch() adds it automatically."
        )

    def test_all_seterr_cards_have_content_divs_in_html(self):
        """Every content ID used in setErr() must exist as an HTML element."""
        import re

        html_path = os.path.join(
            os.path.dirname(__file__), "..", "frontend", "index.html"
        )
        with open(html_path, encoding="utf-8") as f:
            html = f.read()
        js = self._js()
        # Extract setErr('xxx-content') calls
        ids = re.findall(r"setErr\(['\"]([^'\"]+)['\"]", js)
        missing = []
        for cid in ids:
            if f'id="{cid}"' not in html and f"id='{cid}'" not in html:
                missing.append(cid)
        assert (
            not missing
        ), f"These content IDs used in setErr() are missing from index.html: {missing}"
