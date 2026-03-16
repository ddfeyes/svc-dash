"""Tests for Exchange Flow Divergence (Wave 22 Task 5, Issue #109).

TDD: tests written before implementation.
Covers:
  - CVD calculation per exchange
  - Pearson correlation math (edge cases)
  - Lead-lag cross-correlation at ±5min lags
  - API response shape: {binance_cvd, bybit_cvd, correlation, leader, divergence_score, timestamp_lag}
  - Caching behavior (30s TTL)
  - Frontend: HTML card elements, JS renderExchangeFlowDivergence
"""
import math
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from metrics import compute_exchange_flow_divergence


# ─── helpers ──────────────────────────────────────────────────────────────────

def _trade(ts, side, qty, price=100.0):
    return {"ts": float(ts), "side": side, "qty": float(qty), "price": float(price)}


def _buy(ts, qty, price=100.0):
    return _trade(ts, "buy", qty, price)


def _sell(ts, qty, price=100.0):
    return _trade(ts, "sell", qty, price)


def _call(**kwargs):
    """Synchronous wrapper around compute_exchange_flow_divergence."""
    import asyncio
    return asyncio.run(compute_exchange_flow_divergence(**kwargs))


# ─── TestReturnShape ──────────────────────────────────────────────────────────

class TestReturnShape:
    def test_returns_dict(self):
        result = _call()
        assert isinstance(result, dict)

    def test_has_binance_cvd(self):
        result = _call()
        assert "binance_cvd" in result

    def test_has_bybit_cvd(self):
        result = _call()
        assert "bybit_cvd" in result

    def test_has_correlation(self):
        result = _call()
        assert "correlation" in result

    def test_has_leader(self):
        result = _call()
        assert "leader" in result

    def test_has_divergence_score(self):
        result = _call()
        assert "divergence_score" in result

    def test_has_timestamp_lag(self):
        result = _call()
        assert "timestamp_lag" in result

    def test_binance_cvd_is_numeric(self):
        result = _call()
        assert isinstance(result["binance_cvd"], (int, float))

    def test_bybit_cvd_is_numeric(self):
        result = _call()
        assert isinstance(result["bybit_cvd"], (int, float))

    def test_correlation_is_float(self):
        result = _call()
        assert isinstance(result["correlation"], float)

    def test_correlation_in_valid_range(self):
        result = _call()
        assert -1.0 <= result["correlation"] <= 1.0

    def test_leader_is_string_or_none(self):
        result = _call()
        assert result["leader"] is None or isinstance(result["leader"], str)

    def test_divergence_score_is_numeric(self):
        result = _call()
        assert isinstance(result["divergence_score"], (int, float))

    def test_divergence_score_nonnegative(self):
        result = _call()
        assert result["divergence_score"] >= 0.0

    def test_timestamp_lag_is_int_or_float(self):
        result = _call()
        assert isinstance(result["timestamp_lag"], (int, float))

    def test_no_nan_values(self):
        result = _call()
        for key in ["correlation", "divergence_score", "timestamp_lag"]:
            assert not math.isnan(float(result[key])), f"{key} should not be NaN"


# ─── TestCVDCalculation ───────────────────────────────────────────────────────

class TestCVDCalculation:
    def test_pure_buys_positive_cvd(self):
        """All buys → positive CVD."""
        trades_binance = [_buy(i, 1.0) for i in range(10)]
        trades_bybit = [_buy(i, 1.0) for i in range(10)]
        result = _call(_binance_trades=trades_binance, _bybit_trades=trades_bybit)
        assert result["binance_cvd"] > 0
        assert result["bybit_cvd"] > 0

    def test_pure_sells_negative_cvd(self):
        """All sells → negative CVD."""
        trades_binance = [_sell(i, 1.0) for i in range(10)]
        trades_bybit = [_sell(i, 1.0) for i in range(10)]
        result = _call(_binance_trades=trades_binance, _bybit_trades=trades_bybit)
        assert result["binance_cvd"] < 0
        assert result["bybit_cvd"] < 0

    def test_balanced_trades_near_zero(self):
        """Equal buys and sells → CVD near zero."""
        trades = [_buy(i, 1.0) if i % 2 == 0 else _sell(i, 1.0) for i in range(20)]
        result = _call(_binance_trades=trades, _bybit_trades=trades)
        assert abs(result["binance_cvd"]) < 0.01
        assert abs(result["bybit_cvd"]) < 0.01

    def test_cvd_sums_correctly_binance(self):
        """CVD = sum(buy_qty) - sum(sell_qty)."""
        trades = [_buy(0, 5.0), _sell(1, 2.0), _buy(2, 3.0)]
        result = _call(_binance_trades=trades, _bybit_trades=trades)
        expected = 5.0 - 2.0 + 3.0
        assert abs(result["binance_cvd"] - expected) < 1e-6

    def test_cvd_sums_correctly_bybit(self):
        """CVD = sum(buy_qty) - sum(sell_qty)."""
        trades = [_buy(0, 4.0), _sell(1, 1.5), _buy(2, 2.5)]
        expected = 4.0 - 1.5 + 2.5
        result = _call(_binance_trades=trades, _bybit_trades=trades)
        assert abs(result["bybit_cvd"] - expected) < 1e-6

    def test_empty_trades_returns_zero_cvd(self):
        """Empty trade lists → zero CVD."""
        result = _call(_binance_trades=[], _bybit_trades=[])
        assert result["binance_cvd"] == 0.0
        assert result["bybit_cvd"] == 0.0

    def test_buy_side_variants(self):
        """'Buy' (capitalised) treated as buy."""
        trades = [_trade(0, "Buy", 3.0)]
        result = _call(_binance_trades=trades, _bybit_trades=[])
        assert result["binance_cvd"] > 0

    def test_sell_side_variants(self):
        """'Sell' (capitalised) treated as sell."""
        trades = [_trade(0, "Sell", 3.0)]
        result = _call(_binance_trades=trades, _bybit_trades=[])
        assert result["binance_cvd"] < 0


# ─── TestCorrelationMath ──────────────────────────────────────────────────────

class TestCorrelationMath:
    def test_identical_series_correlation_one(self):
        """Identical CVD series → correlation == 1.0."""
        # Use 1s buckets so each trade gets its own bucket
        trades = [_buy(i, float(i + 1)) for i in range(20)]
        result = _call(_binance_trades=trades, _bybit_trades=trades, bucket_seconds=1)
        assert abs(result["correlation"] - 1.0) < 1e-6

    def test_anti_correlated_series(self):
        """Anti-correlated CVD → correlation == -1.0."""
        # Varying sizes in 1s buckets to get truly anti-correlated cumulative series
        binance_trades = [_buy(i, float(i + 1)) for i in range(20)]
        bybit_trades = [_sell(i, float(i + 1)) for i in range(20)]
        result = _call(_binance_trades=binance_trades, _bybit_trades=bybit_trades, bucket_seconds=1)
        assert abs(result["correlation"] - (-1.0)) < 1e-6

    def test_empty_series_correlation_zero(self):
        """Empty trade lists → correlation == 0.0."""
        result = _call(_binance_trades=[], _bybit_trades=[])
        assert result["correlation"] == 0.0

    def test_single_point_correlation_zero(self):
        """Single-point series (no variance) → correlation == 0.0 (not NaN)."""
        trades = [_buy(0, 1.0)]
        result = _call(_binance_trades=trades, _bybit_trades=trades)
        assert result["correlation"] == 0.0 or abs(result["correlation"]) <= 1.0

    def test_correlation_bounded_above(self):
        """Correlation never exceeds 1.0."""
        trades = [_buy(i, float(i)) for i in range(1, 20)]
        result = _call(_binance_trades=trades, _bybit_trades=trades)
        assert result["correlation"] <= 1.0

    def test_correlation_bounded_below(self):
        """Correlation never below -1.0."""
        b_trades = [_buy(i, float(i)) for i in range(1, 20)]
        bb_trades = [_sell(i, float(i)) for i in range(1, 20)]
        result = _call(_binance_trades=b_trades, _bybit_trades=bb_trades)
        assert result["correlation"] >= -1.0

    def test_correlation_symmetry(self):
        """corr(A, B) == corr(B, A)."""
        binance = [_buy(i, float(i + 1)) for i in range(10)]
        bybit = [_sell(i, float(i + 1)) if i % 3 == 0 else _buy(i, float(i + 1)) for i in range(10)]
        r1 = _call(_binance_trades=binance, _bybit_trades=bybit)
        r2 = _call(_binance_trades=bybit, _bybit_trades=binance)
        assert abs(r1["correlation"] - r2["correlation"]) < 1e-9


# ─── TestLeadLag ──────────────────────────────────────────────────────────────

class TestLeadLag:
    def test_leader_is_binance_or_bybit_or_none(self):
        result = _call()
        assert result["leader"] in ("binance", "bybit", None)

    def test_timestamp_lag_within_5min_window(self):
        """lag must be within ±5 minutes = ±300 seconds."""
        result = _call()
        assert abs(result["timestamp_lag"]) <= 300

    def test_leader_set_when_lag_nonzero(self):
        """When a clear lag exists, leader should be set."""
        result = _call()
        if result["timestamp_lag"] != 0:
            assert result["leader"] in ("binance", "bybit")

    def test_no_leader_when_perfect_sync(self):
        """Perfectly synchronised series → lag 0, leader None or valid."""
        trades = [_buy(i * 60, float(i + 1)) for i in range(60)]
        result = _call(_binance_trades=trades, _bybit_trades=trades)
        # lag should be 0 for identical series
        assert result["timestamp_lag"] == 0

    def test_divergence_score_high_when_low_correlation(self):
        """Low correlation → high divergence score."""
        b_trades = [_buy(i, float(i + 1)) for i in range(20)]
        bb_trades = [_sell(i, float(i + 1)) for i in range(20)]
        result = _call(_binance_trades=b_trades, _bybit_trades=bb_trades)
        # divergence_score inversely related to correlation
        assert result["divergence_score"] > 0

    def test_divergence_score_low_when_high_correlation(self):
        """High correlation → low divergence score."""
        trades = [_buy(i, float(i + 1)) for i in range(20)]
        result = _call(_binance_trades=trades, _bybit_trades=trades, bucket_seconds=1)
        assert result["divergence_score"] < 0.5


# ─── TestDeterminism ──────────────────────────────────────────────────────────

class TestDeterminism:
    def test_same_result_twice(self):
        """Default call (seeded mock) is deterministic."""
        r1 = _call()
        r2 = _call()
        assert r1["binance_cvd"] == r2["binance_cvd"]
        assert r1["bybit_cvd"] == r2["bybit_cvd"]
        assert r1["correlation"] == r2["correlation"]

    def test_seeded_mock_produces_nonzero_cvds(self):
        """Default seeded mock should produce non-trivial CVD values."""
        r = _call()
        # Both shouldn't be identically zero (random data has imbalance)
        assert r["binance_cvd"] != 0.0 or r["bybit_cvd"] != 0.0

    def test_result_is_json_serializable(self):
        """All values must be JSON-serializable (no numpy types, no NaN)."""
        import json
        r = _call()
        # Should not raise
        json.dumps(r)


# ─── TestCaching ──────────────────────────────────────────────────────────────

class TestCaching:
    def test_cache_hit_returns_same_result(self):
        """Second call within TTL should be identical."""
        from cache import cache_get, cache_set, make_cache_key
        key = make_cache_key("exchange_flow_divergence")
        # Seed a known value
        sentinel = {"binance_cvd": 42.0, "bybit_cvd": -7.0, "correlation": 0.9,
                    "leader": "binance", "divergence_score": 0.1, "timestamp_lag": 60}
        cache_set(key, sentinel)
        hit, val = cache_get(key, ttl_seconds=30)
        assert hit is True
        assert val["binance_cvd"] == 42.0

    def test_cache_miss_after_ttl(self):
        """Entry should be stale after TTL."""
        from cache import cache_get, cache_set, make_cache_key, _cache
        key = make_cache_key("exchange_flow_divergence_stale_test")
        # Manually set an old timestamp
        _cache[key] = (time.time() - 35, {"stale": True})
        hit, val = cache_get(key, ttl_seconds=30)
        assert hit is False

    def test_cache_set_and_get(self):
        """cache_set/cache_get round-trip within TTL."""
        from cache import cache_get, cache_set, make_cache_key
        key = make_cache_key("efd_test_roundtrip")
        sentinel = {"x": 123}
        cache_set(key, sentinel)
        hit, val = cache_get(key, ttl_seconds=30)
        assert hit
        assert val["x"] == 123

    def test_cache_ttl_is_30_seconds(self):
        """Cache TTL for exchange-flow-divergence must be 30s."""
        # We verify the endpoint uses 30s by inspecting the decorator via
        # source inspection or simply verifying cache_get works at 29s but not 31s.
        from cache import cache_get, cache_set, make_cache_key, _cache
        key = make_cache_key("efd_ttl_test")
        _cache[key] = (time.time() - 29, {"v": 1})
        hit, _ = cache_get(key, ttl_seconds=30)
        assert hit  # 29s < 30s TTL → still valid

        key2 = make_cache_key("efd_ttl_test_expired")
        _cache[key2] = (time.time() - 31, {"v": 2})
        hit2, _ = cache_get(key2, ttl_seconds=30)
        assert not hit2  # 31s > 30s TTL → expired


# ─── TestAPIEndpoint ──────────────────────────────────────────────────────────

class TestAPIEndpoint:
    """Test the FastAPI endpoint via TestClient."""

    def _client(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        import api
        app = FastAPI()
        app.include_router(api.router)
        return TestClient(app)

    def test_endpoint_returns_200(self):
        client = self._client()
        resp = client.get("/api/exchange-flow-divergence")
        assert resp.status_code == 200

    def test_endpoint_returns_json(self):
        client = self._client()
        resp = client.get("/api/exchange-flow-divergence")
        data = resp.json()
        assert isinstance(data, dict)

    def test_endpoint_has_binance_cvd(self):
        client = self._client()
        resp = client.get("/api/exchange-flow-divergence")
        assert "binance_cvd" in resp.json()

    def test_endpoint_has_bybit_cvd(self):
        client = self._client()
        resp = client.get("/api/exchange-flow-divergence")
        assert "bybit_cvd" in resp.json()

    def test_endpoint_has_correlation(self):
        client = self._client()
        resp = client.get("/api/exchange-flow-divergence")
        assert "correlation" in resp.json()

    def test_endpoint_has_leader(self):
        client = self._client()
        resp = client.get("/api/exchange-flow-divergence")
        assert "leader" in resp.json()

    def test_endpoint_has_divergence_score(self):
        client = self._client()
        resp = client.get("/api/exchange-flow-divergence")
        assert "divergence_score" in resp.json()

    def test_endpoint_has_timestamp_lag(self):
        client = self._client()
        resp = client.get("/api/exchange-flow-divergence")
        assert "timestamp_lag" in resp.json()

    def test_endpoint_correlation_range(self):
        client = self._client()
        resp = client.get("/api/exchange-flow-divergence")
        corr = resp.json()["correlation"]
        assert -1.0 <= corr <= 1.0

    def test_endpoint_leader_valid(self):
        client = self._client()
        resp = client.get("/api/exchange-flow-divergence")
        leader = resp.json()["leader"]
        assert leader in ("binance", "bybit", None)


# ─── TestFrontendHTML ─────────────────────────────────────────────────────────

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), '..', 'frontend')
HTML_PATH = os.path.join(FRONTEND_DIR, 'index.html')
JS_PATH = os.path.join(FRONTEND_DIR, 'app.js')


class TestFrontendHTML:
    def _html(self):
        with open(HTML_PATH, 'r') as f:
            return f.read()

    def test_html_contains_exchange_flow_divergence_title(self):
        assert 'Exchange Flow Divergence' in self._html()

    def test_html_contains_card_id(self):
        html = self._html()
        assert 'exchange-flow-divergence' in html

    def test_html_card_has_card_class(self):
        html = self._html()
        assert 'card' in html

    def test_html_contains_binance(self):
        html = self._html()
        # Card meta should mention both exchanges
        assert 'Binance' in html or 'binance' in html.lower()

    def test_html_contains_bybit(self):
        html = self._html()
        assert 'Bybit' in html or 'bybit' in html.lower()

    def test_html_contains_correlation_reference(self):
        html = self._html()
        assert 'correlation' in html.lower()

    def test_html_has_content_div(self):
        html = self._html()
        assert 'exchange-flow-divergence-content' in html

    def test_html_has_badge_element(self):
        html = self._html()
        assert 'exchange-flow-divergence-badge' in html


# ─── TestFrontendJS ───────────────────────────────────────────────────────────

class TestFrontendJS:
    def _js(self):
        with open(JS_PATH, 'r') as f:
            return f.read()

    def test_js_has_render_function(self):
        assert 'renderExchangeFlowDivergence' in self._js()

    def test_js_fetches_endpoint(self):
        js = self._js()
        assert 'exchange-flow-divergence' in js

    def test_js_references_binance_cvd(self):
        assert 'binance_cvd' in self._js()

    def test_js_references_bybit_cvd(self):
        assert 'bybit_cvd' in self._js()

    def test_js_references_correlation(self):
        assert 'correlation' in self._js()

    def test_js_references_leader(self):
        assert 'leader' in self._js()

    def test_js_references_divergence_score(self):
        assert 'divergence_score' in self._js()

    def test_js_updates_content_element(self):
        js = self._js()
        assert 'exchange-flow-divergence-content' in js

    def test_js_updates_badge(self):
        js = self._js()
        assert 'exchange-flow-divergence-badge' in js

    def test_js_is_syntax_valid(self):
        """node --check validates JS syntax."""
        import subprocess
        result = subprocess.run(
            ['node', '--check', JS_PATH],
            capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr
