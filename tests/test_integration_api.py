"""
Integration tests for svc-dash live /api endpoints.

Run against a live server:
    pytest -m integration tests/test_integration_api.py -v

Server must be running at http://localhost:8765 (Docker) or set BASE_URL env var.
Tests skip gracefully if server is not reachable or endpoint is slow.
"""
import os
import pytest
import requests
from requests.exceptions import ConnectionError as ConnErr, ReadTimeout

pytestmark = pytest.mark.integration

SYM = "BANANAS31USDT"
DEFAULT_TIMEOUT = 15   # seconds
SLOW_TIMEOUT    = 45   # for known slow computation endpoints


# ── Helpers ───────────────────────────────────────────────────────────────────

def get(base_url, path, params=None, timeout=DEFAULT_TIMEOUT, expected_status=200):
    """GET with skip-on-connection-error, skip-on-timeout, assert status."""
    try:
        r = requests.get(f"{base_url}{path}", params=params, timeout=timeout)
    except ConnErr:
        pytest.skip("server not reachable")
    except ReadTimeout:
        pytest.skip(f"{path} timed out after {timeout}s (endpoint may be slow)")
    assert r.status_code == expected_status, \
        f"GET {path} → {r.status_code}: {r.text[:300]}"
    return r.json()


def check_keys(data, *keys):
    for k in keys:
        assert k in data, f"missing key '{k}' in {list(data.keys())}"


def get_or_skip_404(base_url, path, params=None, timeout=DEFAULT_TIMEOUT):
    """Like get(), but also skip on 404 (endpoint not in this server version)."""
    try:
        r = requests.get(f"{base_url}{path}", params=params, timeout=timeout)
    except ConnErr:
        pytest.skip("server not reachable")
    except ReadTimeout:
        pytest.skip(f"{path} timed out after {timeout}s")
    if r.status_code == 404:
        pytest.skip(f"{path} not available on this server version")
    assert r.status_code == 200, f"GET {path} → {r.status_code}: {r.text[:300]}"
    return r.json()


# ── Server health ─────────────────────────────────────────────────────────────

class TestServerHealth:
    def test_health(self, base_url):
        data = get(base_url, "/health")
        check_keys(data, "status", "db_size_mb", "record_counts", "symbols", "symbol_count")
        assert data["status"] == "ok"
        assert isinstance(data["symbol_count"], int)

    def test_symbols(self, base_url):
        data = get(base_url, "/symbols")
        check_keys(data, "status", "symbols")
        assert isinstance(data["symbols"], list)
        assert len(data["symbols"]) > 0

    def test_freshness(self, base_url, symbol):
        data = get(base_url, "/freshness", {"symbol": symbol})
        check_keys(data, "status", "freshness")
        assert symbol in data["freshness"]

    def test_freshness_global(self, base_url):
        data = get(base_url, "/freshness")
        check_keys(data, "status", "freshness")
        assert isinstance(data["freshness"], dict)

    def test_stats_summary(self, base_url, symbol):
        data = get(base_url, "/stats/summary", {"symbol": symbol})
        check_keys(data, "status", "summary")

    def test_multi_summary(self, base_url):
        data = get(base_url, "/multi-summary", timeout=SLOW_TIMEOUT)
        check_keys(data, "status", "symbols")
        assert isinstance(data["symbols"], dict)


# ── Raw data feeds ────────────────────────────────────────────────────────────

class TestRawDataEndpoints:
    def test_trades_recent(self, base_url, symbol):
        data = get(base_url, "/trades/recent", {"symbol": symbol, "limit": 10})
        check_keys(data, "status", "data", "count")
        assert isinstance(data["data"], list)
        if data["data"]:
            t = data["data"][0]
            assert "price" in t and "qty" in t and "side" in t and "ts" in t

    def test_oi_history(self, base_url, symbol):
        data = get(base_url, "/oi/history", {"symbol": symbol, "limit": 10})
        check_keys(data, "status", "data", "count")
        assert isinstance(data["data"], list)
        if data["data"]:
            assert "oi_value" in data["data"][0]

    def test_funding_history(self, base_url, symbol):
        data = get(base_url, "/funding/history", {"symbol": symbol, "limit": 10})
        check_keys(data, "status", "data", "count")
        assert isinstance(data["data"], list)
        if data["data"]:
            assert "rate" in data["data"][0]

    def test_liquidations_recent(self, base_url, symbol):
        data = get(base_url, "/liquidations/recent", {"symbol": symbol, "limit": 10})
        check_keys(data, "status", "data", "count")
        assert isinstance(data["data"], list)

    def test_cvd_history(self, base_url, symbol):
        data = get(base_url, "/cvd/history", {"symbol": symbol, "window": 3600})
        check_keys(data, "status", "data", "count")
        assert isinstance(data["data"], list)
        if data["data"]:
            assert "cvd" in data["data"][0]

    def test_orderbook_latest(self, base_url, symbol):
        data = get(base_url, "/orderbook/latest", {"symbol": symbol})
        check_keys(data, "status", "data", "count")

    def test_alerts(self, base_url, symbol):
        data = get(base_url, "/alerts", {"symbol": symbol, "limit": 10})
        check_keys(data, "status", "data", "count")
        assert isinstance(data["data"], list)

    def test_ohlcv(self, base_url, symbol):
        data = get(base_url, "/ohlcv", {"symbol": symbol, "interval": 60, "window": 300})
        check_keys(data, "status", "symbol", "interval", "data", "count")
        assert data["symbol"] == symbol
        assert data["interval"] == 60

    def test_large_trades(self, base_url, symbol):
        data = get(base_url, "/large-trades", {"symbol": symbol, "window": 3600, "min_usd": 1000})
        check_keys(data, "status", "symbol", "count", "trades")
        assert isinstance(data["trades"], list)

    def test_whale_history(self, base_url, symbol):
        data = get(base_url, "/whale-history", {"symbol": symbol, "window": 3600, "min_usd": 10000})
        check_keys(data, "status", "count", "trades")
        assert isinstance(data["trades"], list)

    def test_volume_spike(self, base_url, symbol):
        data = get(base_url, "/volume-spike", {"symbol": symbol}, timeout=SLOW_TIMEOUT)
        check_keys(data, "status", "symbol")


# ── OHLCV and depth analytics ─────────────────────────────────────────────────

class TestDepthAndVolume:
    def test_volume_profile(self, base_url, symbol):
        data = get(base_url, "/volume-profile", {"symbol": symbol})
        check_keys(data, "status", "symbol", "poc", "poc_volume", "vah", "val")

    def test_market_depth(self, base_url, symbol):
        data = get(base_url, "/market-depth", {"symbol": symbol})
        check_keys(data, "status", "symbol", "mid_price", "bids", "asks")
        assert isinstance(data["bids"], list)
        assert isinstance(data["asks"], list)

    def test_oi_delta(self, base_url, symbol):
        data = get(base_url, "/oi-delta", {"symbol": symbol, "interval": 300, "window": 3600})
        check_keys(data, "status", "symbol", "interval", "candles")

    def test_trade_count_rate(self, base_url, symbol):
        data = get(base_url, "/trade-count-rate", {"symbol": symbol, "interval": 60, "window": 1800})
        check_keys(data, "status", "symbol", "interval", "window", "buckets")

    def test_trade_size_dist(self, base_url, symbol):
        data = get(base_url, "/trade-size-dist", {"symbol": symbol})
        check_keys(data, "status", "symbol", "window", "buckets")

    def test_spread_tracker(self, base_url, symbol):
        data = get(base_url, "/spread-tracker", {"symbol": symbol})
        check_keys(data, "status", "symbol", "current_pct", "current_bps")

    def test_flow_imbalance(self, base_url, symbol):
        data = get(base_url, "/flow-imbalance", {"symbol": symbol, "window": 3600, "bucket_size": 60})
        check_keys(data, "status", "symbol", "window_s", "bucket_size_s", "summary", "series")
        assert isinstance(data["series"], list)
        s = data["summary"]
        assert "avg_ratio" in s and "bias" in s

    def test_orderbook_heatmap(self, base_url, symbol):
        data = get(base_url, "/orderbook-heatmap", {"symbol": symbol})
        check_keys(data, "status", "symbol")

    def test_trade_flow_heatmap(self, base_url, symbol):
        data = get(base_url, "/trade-flow-heatmap", {"symbol": symbol})
        check_keys(data, "status", "symbol")

    def test_ob_wall_decay(self, base_url, symbol):
        data = get(base_url, "/ob-wall-decay", {"symbol": symbol})
        check_keys(data, "status", "symbol")


# ── Alert / Signal endpoints ──────────────────────────────────────────────────

class TestSignalEndpoints:
    def test_liq_cascade(self, base_url, symbol):
        data = get(base_url, "/liq-cascade", {"symbol": symbol})
        check_keys(data, "status", "symbol", "cascade", "total_usd", "buy_usd", "sell_usd")
        assert isinstance(data["cascade"], bool)

    def test_oi_spike(self, base_url, symbol):
        data = get(base_url, "/oi-spike", {"symbol": symbol})
        check_keys(data, "status", "symbol", "spike", "exchanges", "description")
        assert isinstance(data["spike"], bool)

    def test_delta_divergence(self, base_url, symbol):
        data = get(base_url, "/delta-divergence", {"symbol": symbol})
        check_keys(data, "status", "symbol", "divergence", "severity", "price_change_pct")
        assert data["divergence"] in ("none", "bullish", "bearish", True, False)

    def test_funding_extreme(self, base_url, symbol):
        data = get(base_url, "/funding-extreme", {"symbol": symbol})
        check_keys(data, "status", "symbol", "extreme", "avg_rate", "avg_rate_pct")
        assert isinstance(data["extreme"], bool)

    def test_cascade_predictor(self, base_url, symbol):
        data = get(base_url, "/cascade-predictor", {"symbol": symbol})
        check_keys(data, "status", "symbol", "level", "high_risk", "oi_building")
        assert isinstance(data["high_risk"], bool)


# ── Analytical / derived metrics ──────────────────────────────────────────────

class TestAnalyticalEndpoints:
    def test_oi_mcap(self, base_url, symbol):
        data = get(base_url, "/oi-mcap", {"symbol": symbol})
        check_keys(data, "status", "symbol", "oi_usd", "price")

    def test_vwap_deviation(self, base_url, symbol):
        data = get(base_url, "/vwap-deviation", {"symbol": symbol})
        check_keys(data, "status", "symbol", "vwap", "price", "deviation_pct")

    def test_funding_arb(self, base_url, symbol):
        data = get(base_url, "/funding-arb", {"symbol": symbol})
        check_keys(data, "status", "symbol", "arb", "binance", "bybit")

    def test_ob_pressure_gradient(self, base_url, symbol):
        data = get(base_url, "/ob-pressure-gradient", {"symbol": symbol})
        check_keys(data, "status", "symbol", "gradient", "avg_gradient")

    def test_kalman_price(self, base_url, symbol):
        data = get(base_url, "/kalman-price", {"symbol": symbol})
        check_keys(data, "status", "symbol", "smoothed_price", "raw_price", "deviation_pct")

    def test_aggressor_ratio(self, base_url, symbol):
        data = get(base_url, "/aggressor-ratio", {"symbol": symbol}, timeout=SLOW_TIMEOUT)
        check_keys(data, "status", "symbol", "series", "current_ratio")

    def test_mtf_rsi_divergence(self, base_url, symbol):
        data = get(base_url, "/mtf-rsi-divergence", {"symbol": symbol})
        check_keys(data, "status", "symbol", "rsi_5m", "rsi_1h", "zone_5m")

    def test_realized_implied_vol(self, base_url, symbol):
        data = get(base_url, "/realized-implied-vol", {"symbol": symbol})
        check_keys(data, "status", "symbol", "realized_vol_pct", "implied_vol_pct", "vol_ratio")

    def test_vpin(self, base_url, symbol):
        data = get(base_url, "/vpin", {"symbol": symbol})
        check_keys(data, "status", "symbol", "vpin", "toxicity", "trend")

    def test_oi_concentration(self, base_url, symbol):
        data = get(base_url, "/oi-concentration", {"symbol": symbol})
        check_keys(data, "status", "symbol", "concentration_pct")

    def test_funding_divergence(self, base_url, symbol):
        data = get(base_url, "/funding-divergence", {"symbol": symbol})
        check_keys(data, "status", "divergence")

    def test_cvd_momentum(self, base_url, symbol):
        data = get(base_url, "/cvd-momentum", {"symbol": symbol})
        check_keys(data, "status", "symbol", "cvd_rate", "direction")

    def test_volatility_regime(self, base_url, symbol):
        data = get(base_url, "/volatility-regime", {"symbol": symbol})
        check_keys(data, "status", "symbol", "regime", "regime_label", "regime_color")

    def test_volatility_regime_all(self, base_url):
        data = get(base_url, "/volatility-regime/all")
        check_keys(data, "status")

    def test_cdv_oscillator(self, base_url, symbol):
        data = get(base_url, "/cdv-oscillator", {"symbol": symbol}, timeout=SLOW_TIMEOUT)
        check_keys(data, "status", "symbol")

    def test_vwap_band(self, base_url, symbol):
        data = get(base_url, "/vwap-band", {"symbol": symbol}, timeout=SLOW_TIMEOUT)
        check_keys(data, "status", "symbol")

    def test_liq_pressure(self, base_url, symbol):
        data = get(base_url, "/liq-pressure", {"symbol": symbol})
        check_keys(data, "status")

    def test_price_velocity(self, base_url, symbol):
        data = get(base_url, "/price-velocity", {"symbol": symbol})
        check_keys(data, "status")

    def test_cvd_divergence(self, base_url, symbol):
        data = get(base_url, "/cvd-divergence", {"symbol": symbol}, timeout=SLOW_TIMEOUT)
        check_keys(data, "status", "symbol")

    def test_trade_bursts(self, base_url, symbol):
        data = get(base_url, "/trade-bursts", {"symbol": symbol})
        check_keys(data, "status")

    def test_funding_cost(self, base_url, symbol):
        data = get(base_url, "/funding-cost", {"symbol": symbol}, timeout=SLOW_TIMEOUT)
        check_keys(data, "status", "symbol")

    def test_funding_heatmap(self, base_url, symbol):
        data = get(base_url, "/funding-heatmap", {"symbol": symbol}, timeout=SLOW_TIMEOUT)
        check_keys(data, "status", "symbol")

    def test_max_drawdown(self, base_url, symbol):
        data = get(base_url, "/max-drawdown", {"symbol": symbol}, timeout=SLOW_TIMEOUT)
        check_keys(data, "status", "symbol")

    def test_spread_history(self, base_url, symbol):
        data = get(base_url, "/spread-history", {"symbol": symbol}, timeout=SLOW_TIMEOUT)
        check_keys(data, "status", "symbol")

    def test_momentum(self, base_url):
        data = get(base_url, "/momentum", timeout=SLOW_TIMEOUT)
        check_keys(data, "status", "symbols")

    def test_correlations(self, base_url):
        data = get(base_url, "/correlations")
        check_keys(data, "status", "matrix", "symbols")


# ── Market regime / phase ─────────────────────────────────────────────────────

class TestMarketRegime:
    def test_market_regime(self, base_url, symbol):
        data = get(base_url, "/market-regime", {"symbol": symbol})
        check_keys(data, "status", "score", "regime", "action", "phase")
        assert data["status"] == "ok"

    def test_market_regime_all(self, base_url):
        data = get(base_url, "/market-regime/all")
        check_keys(data, "status")

    def test_pattern(self, base_url, symbol):
        data = get(base_url, "/pattern", {"symbol": symbol})
        check_keys(data, "status", "pattern", "confidence")

    def test_pattern_all(self, base_url):
        data = get(base_url, "/pattern/all")
        check_keys(data, "status")

    def test_pattern_history(self, base_url, symbol):
        data = get(base_url, "/pattern-history", {"symbol": symbol})
        check_keys(data, "status", "data", "count")

    def test_phase_history(self, base_url, symbol):
        data = get(base_url, "/phase-history", {"symbol": symbol})
        check_keys(data, "status", "symbol")


# ── Price analysis ────────────────────────────────────────────────────────────

class TestPriceAnalysis:
    def test_support_resistance(self, base_url, symbol):
        data = get(base_url, "/support-resistance", {"symbol": symbol})
        check_keys(data, "status", "symbol", "current_price", "levels")

    def test_pivots(self, base_url, symbol):
        data = get(base_url, "/pivots", {"symbol": symbol})
        check_keys(data, "status", "symbol", "pivots")

    def test_atr(self, base_url, symbol):
        data = get(base_url, "/atr", {"symbol": symbol})
        check_keys(data, "status", "symbol", "atr", "atr_pct", "last_close")
        assert data["atr"] >= 0

    def test_microstructure(self, base_url, symbol):
        data = get(base_url, "/microstructure", {"symbol": symbol})
        check_keys(data, "status", "symbol", "window", "data")

    def test_session(self, base_url, symbol):
        data = get(base_url, "/session", {"symbol": symbol})
        check_keys(data, "status", "symbol", "session")

    def test_stats(self, base_url, symbol):
        data = get(base_url, "/stats", {"symbol": symbol})
        check_keys(data, "status", "symbol", "stats")

    def test_metrics_summary(self, base_url, symbol):
        data = get(base_url, "/metrics/summary", {"symbol": symbol})
        check_keys(data, "status", "symbol", "price")


# ── Cross-symbol endpoints ────────────────────────────────────────────────────

class TestCrossSymbol:
    def test_cross_symbol_oi(self, base_url):
        data = get(base_url, "/cross-symbol-oi")
        check_keys(data, "status", "data")
        # data may be a dict (all_symbols keyed) or list depending on version
        assert isinstance(data["data"], (list, dict))


# ── Newer endpoints (present in latest api.py, may 404 on older container) ────

class TestNewerEndpoints:
    """Added in later PRs — skip gracefully on 404 (older server)."""

    def test_smart_money_divergence(self, base_url, symbol):
        data = get_or_skip_404(base_url, "/smart-money-divergence", {
            "symbol": symbol, "window": 1800, "threshold_usd": 10000, "bucket_seconds": 300
        })
        check_keys(data, "status", "symbol")

    def test_smart_money_divergence_all(self, base_url):
        data = get_or_skip_404(base_url, "/smart-money-divergence/all")
        check_keys(data, "status")

    def test_ob_recovery_speed(self, base_url, symbol):
        data = get_or_skip_404(base_url, "/ob-recovery-speed", {
            "symbol": symbol, "window": 900, "threshold_usd": 50000, "alert_seconds": 10
        })
        check_keys(data, "status", "symbol")

    def test_tod_volatility(self, base_url, symbol):
        data = get_or_skip_404(base_url, "/tod-volatility", {"symbol": symbol})
        check_keys(data, "status", "symbol")

    def test_tod_volatility_all(self, base_url):
        data = get_or_skip_404(base_url, "/tod-volatility/all")
        check_keys(data, "status")

    def test_net_taker_delta(self, base_url, symbol):
        data = get_or_skip_404(base_url, "/net-taker-delta", {
            "symbol": symbol, "window": 3600, "bucket_seconds": 60
        })
        check_keys(data, "status", "symbol")

    def test_squeeze_setup(self, base_url, symbol):
        data = get_or_skip_404(base_url, "/squeeze-setup", {"symbol": symbol})
        check_keys(data, "status", "symbol")

    def test_squeeze_setup_all(self, base_url):
        data = get_or_skip_404(base_url, "/squeeze-setup/all")
        check_keys(data, "status")

    def test_tick_imbalance(self, base_url, symbol):
        data = get_or_skip_404(base_url, "/tick-imbalance", {
            "symbol": symbol, "threshold": 20, "limit": 50
        })
        check_keys(data, "status", "symbol")

    def test_session_stats(self, base_url, symbol):
        data = get_or_skip_404(base_url, "/session-stats", {"symbol": symbol})
        check_keys(data, "status", "symbol")

    def test_volume_clock(self, base_url, symbol):
        data = get_or_skip_404(base_url, "/volume-clock", {
            "symbol": symbol, "window": 3600, "volume_threshold": 10
        })
        check_keys(data, "status", "symbol")

    def test_price_ladder(self, base_url, symbol):
        data = get_or_skip_404(base_url, "/price-ladder", {
            "symbol": symbol, "window": 300, "num_levels": 20
        })
        check_keys(data, "status", "symbol")

    def test_market_microstructure(self, base_url, symbol):
        data = get_or_skip_404(base_url, "/market-microstructure", {"symbol": symbol})
        check_keys(data, "status", "symbol")


# ── No-500 smoke test ─────────────────────────────────────────────────────────

class TestNoServerErrors:
    """Ensures no registered endpoint returns HTTP 500."""

    ENDPOINTS = [
        ("/symbols", {}),
        ("/health", {}),
        ("/freshness", {"symbol": SYM}),
        ("/stats/summary", {"symbol": SYM}),
        ("/trades/recent", {"symbol": SYM, "limit": 5}),
        ("/oi/history", {"symbol": SYM, "limit": 5}),
        ("/funding/history", {"symbol": SYM, "limit": 5}),
        ("/liquidations/recent", {"symbol": SYM, "limit": 5}),
        ("/cvd/history", {"symbol": SYM, "window": 600}),
        ("/volume-profile", {"symbol": SYM}),
        ("/market-depth", {"symbol": SYM}),
        ("/liq-cascade", {"symbol": SYM}),
        ("/oi-spike", {"symbol": SYM}),
        ("/delta-divergence", {"symbol": SYM}),
        ("/funding-extreme", {"symbol": SYM}),
        ("/cascade-predictor", {"symbol": SYM}),
        ("/oi-mcap", {"symbol": SYM}),
        ("/vwap-deviation", {"symbol": SYM}),
        ("/funding-arb", {"symbol": SYM}),
        ("/ob-pressure-gradient", {"symbol": SYM}),
        ("/kalman-price", {"symbol": SYM}),
        ("/mtf-rsi-divergence", {"symbol": SYM}),
        ("/realized-implied-vol", {"symbol": SYM}),
        ("/vpin", {"symbol": SYM}),
        ("/oi-concentration", {"symbol": SYM}),
        ("/funding-divergence", {"symbol": SYM}),
        ("/cvd-momentum", {"symbol": SYM}),
        ("/market-regime", {"symbol": SYM}),
        ("/market-regime/all", {}),
        ("/volatility-regime", {"symbol": SYM}),
        ("/ohlcv", {"symbol": SYM, "interval": 60, "window": 300}),
        ("/flow-imbalance", {"symbol": SYM, "window": 3600, "bucket_size": 60}),
        ("/large-trades", {"symbol": SYM, "window": 3600, "min_usd": 1000}),
        ("/whale-history", {"symbol": SYM, "window": 3600, "min_usd": 10000}),
        ("/pattern", {"symbol": SYM}),
        ("/support-resistance", {"symbol": SYM}),
        ("/atr", {"symbol": SYM}),
        ("/microstructure", {"symbol": SYM}),
        ("/correlations", {}),
        ("/cross-symbol-oi", {}),
        ("/spread-tracker", {"symbol": SYM}),
        ("/trade-count-rate", {"symbol": SYM, "interval": 60, "window": 600}),
        ("/oi-delta", {"symbol": SYM, "interval": 300, "window": 3600}),
        ("/metrics/summary", {"symbol": SYM}),
        ("/ob-wall-decay", {"symbol": SYM}),
        ("/liq-pressure", {"symbol": SYM}),
        ("/price-velocity", {"symbol": SYM}),
        ("/trade-bursts", {"symbol": SYM}),
    ]

    @pytest.mark.parametrize("path,params", ENDPOINTS)
    def test_no_500(self, base_url, path, params):
        try:
            r = requests.get(f"{base_url}{path}", params=params, timeout=DEFAULT_TIMEOUT)
        except ConnErr:
            pytest.skip("server not reachable")
        except ReadTimeout:
            pytest.skip(f"{path} timed out (slow endpoint)")
        assert r.status_code != 500, f"GET {path} returned 500: {r.text[:300]}"
        assert r.headers.get("content-type", "").startswith("application/json"), \
            f"GET {path} returned non-JSON content-type: {r.headers.get('content-type')}"
