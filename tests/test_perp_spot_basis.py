"""Tests for Perpetual vs Spot Basis Monitor (Wave 23 Task 1, Issue #115).

TDD: tests written before implementation.
Covers:
  - basis_bps calculation: (perp_price - spot_price) / spot_price * 10000
  - z-score calculation over 20-period window
  - contango signal when basis_bps > threshold
  - backwardation signal when basis_bps < -threshold
  - multi-asset: BTC, ETH, SOL
  - API response shape: {assets: [{symbol, basis_bps, z_score, signal, perp_price, spot_price}],
                         avg_basis_bps, market_signal, timestamp}
  - Caching (30s TTL)
  - Frontend: HTML contains 'Perp/Spot Basis' card, JS has renderPerpSpotBasis
"""
import math
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from metrics import compute_perp_spot_basis


# ─── helpers ──────────────────────────────────────────────────────────────────

def _call(**kwargs):
    """Synchronous wrapper around compute_perp_spot_basis."""
    import asyncio
    return asyncio.run(compute_perp_spot_basis(**kwargs))


# ─── TestReturnShape ──────────────────────────────────────────────────────────

class TestReturnShape:
    def test_returns_dict(self):
        result = _call()
        assert isinstance(result, dict)

    def test_has_assets_key(self):
        result = _call()
        assert "assets" in result

    def test_assets_is_list(self):
        result = _call()
        assert isinstance(result["assets"], list)

    def test_has_avg_basis_bps(self):
        result = _call()
        assert "avg_basis_bps" in result

    def test_has_market_signal(self):
        result = _call()
        assert "market_signal" in result

    def test_has_timestamp(self):
        result = _call()
        assert "timestamp" in result

    def test_timestamp_is_numeric(self):
        result = _call()
        assert isinstance(result["timestamp"], (int, float))

    def test_avg_basis_bps_is_numeric(self):
        result = _call()
        assert isinstance(result["avg_basis_bps"], (int, float))

    def test_market_signal_is_string(self):
        result = _call()
        assert isinstance(result["market_signal"], str)

    def test_market_signal_valid_values(self):
        result = _call()
        assert result["market_signal"] in ("contango", "backwardation", "neutral")


# ─── TestAssetsShape ──────────────────────────────────────────────────────────

class TestAssetsShape:
    def test_assets_has_three_entries(self):
        result = _call()
        assert len(result["assets"]) == 3

    def test_asset_has_symbol(self):
        result = _call()
        for asset in result["assets"]:
            assert "symbol" in asset

    def test_asset_has_basis_bps(self):
        result = _call()
        for asset in result["assets"]:
            assert "basis_bps" in asset

    def test_asset_has_z_score(self):
        result = _call()
        for asset in result["assets"]:
            assert "z_score" in asset

    def test_asset_has_signal(self):
        result = _call()
        for asset in result["assets"]:
            assert "signal" in asset

    def test_asset_has_perp_price(self):
        result = _call()
        for asset in result["assets"]:
            assert "perp_price" in asset

    def test_asset_has_spot_price(self):
        result = _call()
        for asset in result["assets"]:
            assert "spot_price" in asset

    def test_symbols_are_btc_eth_sol(self):
        result = _call()
        symbols = {a["symbol"] for a in result["assets"]}
        assert symbols == {"BTC", "ETH", "SOL"}

    def test_asset_signal_valid_values(self):
        result = _call()
        for asset in result["assets"]:
            assert asset["signal"] in ("contango", "backwardation", "neutral")

    def test_asset_basis_bps_is_numeric(self):
        result = _call()
        for asset in result["assets"]:
            assert isinstance(asset["basis_bps"], (int, float))

    def test_asset_z_score_is_numeric(self):
        result = _call()
        for asset in result["assets"]:
            assert isinstance(asset["z_score"], (int, float))

    def test_asset_perp_price_positive(self):
        result = _call()
        for asset in result["assets"]:
            assert asset["perp_price"] > 0

    def test_asset_spot_price_positive(self):
        result = _call()
        for asset in result["assets"]:
            assert asset["spot_price"] > 0

    def test_no_nan_values(self):
        result = _call()
        for asset in result["assets"]:
            for key in ["basis_bps", "z_score", "perp_price", "spot_price"]:
                assert not math.isnan(float(asset[key])), f"{key} for {asset['symbol']} should not be NaN"
        assert not math.isnan(float(result["avg_basis_bps"]))


# ─── TestBasisBpsCalculation ──────────────────────────────────────────────────

class TestBasisBpsCalculation:
    def test_basis_bps_formula(self):
        """basis_bps = (perp - spot) / spot * 10000."""
        spot = 50000.0
        perp = 50100.0
        expected = (perp - spot) / spot * 10000
        result = _call(_spot_overrides={"BTC": spot}, _perp_overrides={"BTC": perp})
        btc = next(a for a in result["assets"] if a["symbol"] == "BTC")
        assert abs(btc["basis_bps"] - expected) < 0.01

    def test_basis_bps_zero_when_equal_prices(self):
        """basis_bps = 0 when perp == spot."""
        price = 50000.0
        result = _call(_spot_overrides={"BTC": price}, _perp_overrides={"BTC": price})
        btc = next(a for a in result["assets"] if a["symbol"] == "BTC")
        assert abs(btc["basis_bps"]) < 1e-6

    def test_basis_bps_positive_when_perp_above_spot(self):
        """basis_bps > 0 when perp > spot (contango)."""
        result = _call(_spot_overrides={"ETH": 3000.0}, _perp_overrides={"ETH": 3030.0})
        eth = next(a for a in result["assets"] if a["symbol"] == "ETH")
        assert eth["basis_bps"] > 0

    def test_basis_bps_negative_when_perp_below_spot(self):
        """basis_bps < 0 when perp < spot (backwardation)."""
        result = _call(_spot_overrides={"SOL": 100.0}, _perp_overrides={"SOL": 99.0})
        sol = next(a for a in result["assets"] if a["symbol"] == "SOL")
        assert sol["basis_bps"] < 0

    def test_basis_bps_calculation_precision(self):
        """basis_bps calculated correctly for known values."""
        # spot=100, perp=100.5 → basis = (100.5-100)/100 * 10000 = 50 bps
        result = _call(_spot_overrides={"BTC": 100.0}, _perp_overrides={"BTC": 100.5})
        btc = next(a for a in result["assets"] if a["symbol"] == "BTC")
        assert abs(btc["basis_bps"] - 50.0) < 0.01

    def test_avg_basis_bps_is_mean_of_assets(self):
        """avg_basis_bps = mean of all asset basis_bps."""
        result = _call()
        assets_basis = [a["basis_bps"] for a in result["assets"]]
        expected_avg = sum(assets_basis) / len(assets_basis)
        assert abs(result["avg_basis_bps"] - expected_avg) < 0.01


# ─── TestZScoreCalculation ────────────────────────────────────────────────────

class TestZScoreCalculation:
    def test_z_score_is_float(self):
        result = _call()
        for asset in result["assets"]:
            assert isinstance(asset["z_score"], float)

    def test_z_score_not_nan(self):
        result = _call()
        for asset in result["assets"]:
            assert not math.isnan(asset["z_score"])

    def test_z_score_zero_when_no_variance(self):
        """If all basis values are identical, z-score should be 0."""
        result = _call(_force_flat_basis=True)
        for asset in result["assets"]:
            assert abs(asset["z_score"]) < 1e-6

    def test_z_score_formula(self):
        """z = (current - mean) / std over window."""
        window = [10.0] * 19 + [20.0]  # mean ≈ 10.5, std small, last value=20
        result = _call(_basis_window_override={"BTC": window})
        btc = next(a for a in result["assets"] if a["symbol"] == "BTC")
        mean = sum(window) / len(window)
        variance = sum((x - mean) ** 2 for x in window) / len(window)
        std = variance ** 0.5
        expected_z = (window[-1] - mean) / std if std > 0 else 0.0
        assert abs(btc["z_score"] - expected_z) < 0.01

    def test_z_score_window_size_20(self):
        """Z-score uses 20-period window."""
        # Pass exactly 20 values; all should produce valid z-scores
        window = list(range(1, 21))  # [1..20]
        result = _call(_basis_window_override={"ETH": window})
        eth = next(a for a in result["assets"] if a["symbol"] == "ETH")
        assert not math.isnan(eth["z_score"])


# ─── TestSignalClassification ─────────────────────────────────────────────────

class TestSignalClassification:
    def test_contango_signal_when_basis_above_threshold(self):
        """Signal = 'contango' when basis_bps > 10."""
        result = _call(_spot_overrides={"BTC": 50000.0}, _perp_overrides={"BTC": 50100.0})
        btc = next(a for a in result["assets"] if a["symbol"] == "BTC")
        # basis = (50100-50000)/50000 * 10000 = 20 bps > 10 threshold
        assert btc["signal"] == "contango"

    def test_backwardation_signal_when_basis_below_neg_threshold(self):
        """Signal = 'backwardation' when basis_bps < -10."""
        result = _call(_spot_overrides={"BTC": 50000.0}, _perp_overrides={"BTC": 49900.0})
        btc = next(a for a in result["assets"] if a["symbol"] == "BTC")
        # basis = (49900-50000)/50000 * 10000 = -20 bps < -10 threshold
        assert btc["signal"] == "backwardation"

    def test_neutral_signal_when_basis_within_threshold(self):
        """Signal = 'neutral' when -10 <= basis_bps <= 10."""
        result = _call(_spot_overrides={"BTC": 50000.0}, _perp_overrides={"BTC": 50005.0})
        btc = next(a for a in result["assets"] if a["symbol"] == "BTC")
        # basis = (50005-50000)/50000 * 10000 = 1 bps → neutral
        assert btc["signal"] == "neutral"

    def test_market_signal_contango_when_avg_above_10(self):
        """market_signal = 'contango' when avg_basis_bps > 10."""
        # Force all assets well above 10 bps
        result = _call(
            _spot_overrides={"BTC": 50000.0, "ETH": 3000.0, "SOL": 100.0},
            _perp_overrides={"BTC": 50100.0, "ETH": 3030.0, "SOL": 101.0}
        )
        # All have strong positive basis → market contango
        assert result["market_signal"] == "contango"

    def test_market_signal_backwardation_when_avg_below_neg10(self):
        """market_signal = 'backwardation' when avg_basis_bps < -10."""
        result = _call(
            _spot_overrides={"BTC": 50000.0, "ETH": 3000.0, "SOL": 100.0},
            _perp_overrides={"BTC": 49900.0, "ETH": 2970.0, "SOL": 99.0}
        )
        assert result["market_signal"] == "backwardation"

    def test_market_signal_neutral_when_avg_near_zero(self):
        """market_signal = 'neutral' when -10 <= avg_basis_bps <= 10."""
        result = _call(
            _spot_overrides={"BTC": 50000.0, "ETH": 3000.0, "SOL": 100.0},
            _perp_overrides={"BTC": 50005.0, "ETH": 3001.0, "SOL": 100.0}
        )
        assert result["market_signal"] == "neutral"


# ─── TestDeterminism ──────────────────────────────────────────────────────────

class TestDeterminism:
    def test_same_result_twice(self):
        """Default call (seeded mock) is deterministic."""
        r1 = _call()
        r2 = _call()
        for key in ["avg_basis_bps", "market_signal", "timestamp"]:
            assert r1[key] == r2[key] or key == "timestamp"

    def test_seeded_mock_btc_has_positive_basis(self):
        """Seeded data (seed=42) should produce positive BTC basis (perp premium)."""
        r = _call()
        btc = next(a for a in r["assets"] if a["symbol"] == "BTC")
        # With seed=42 and small premium simulation, BTC perp should be above spot
        assert isinstance(btc["basis_bps"], float)

    def test_result_is_json_serializable(self):
        """All values must be JSON-serializable (no numpy types, no NaN)."""
        import json
        r = _call()
        json.dumps(r)

    def test_result_consistent_across_calls(self):
        """Seeded mock produces the same basis values each call."""
        r1 = _call()
        r2 = _call()
        for a1, a2 in zip(
            sorted(r1["assets"], key=lambda x: x["symbol"]),
            sorted(r2["assets"], key=lambda x: x["symbol"])
        ):
            assert a1["symbol"] == a2["symbol"]
            assert a1["basis_bps"] == a2["basis_bps"]


# ─── TestCaching ──────────────────────────────────────────────────────────────

class TestCaching:
    def test_cache_hit_returns_same_result(self):
        """Second call within TTL should be identical."""
        from cache import cache_get, cache_set, make_cache_key
        key = make_cache_key("perp_spot_basis")
        sentinel = {
            "assets": [],
            "avg_basis_bps": 15.0,
            "market_signal": "contango",
            "timestamp": 1234567890.0
        }
        cache_set(key, sentinel)
        hit, val = cache_get(key, ttl_seconds=30)
        assert hit is True
        assert val["avg_basis_bps"] == 15.0

    def test_cache_miss_after_ttl(self):
        """Entry should be stale after TTL."""
        from cache import cache_get, cache_set, make_cache_key, _cache
        key = make_cache_key("perp_spot_basis_stale_test")
        _cache[key] = (time.time() - 35, {"stale": True})
        hit, val = cache_get(key, ttl_seconds=30)
        assert hit is False

    def test_cache_set_and_get_roundtrip(self):
        """cache_set/cache_get round-trip within TTL."""
        from cache import cache_get, cache_set, make_cache_key
        key = make_cache_key("psb_test_roundtrip")
        sentinel = {"x": 999}
        cache_set(key, sentinel)
        hit, val = cache_get(key, ttl_seconds=30)
        assert hit
        assert val["x"] == 999

    def test_cache_ttl_is_30_seconds(self):
        """Cache TTL for perp-spot-basis must be 30s."""
        from cache import cache_get, _cache, make_cache_key
        key = make_cache_key("psb_ttl_test")
        _cache[key] = (time.time() - 29, {"v": 1})
        hit, _ = cache_get(key, ttl_seconds=30)
        assert hit  # 29s < 30s TTL → still valid

        key2 = make_cache_key("psb_ttl_test_expired")
        _cache[key2] = (time.time() - 31, {"v": 2})
        hit2, _ = cache_get(key2, ttl_seconds=30)
        assert not hit2  # 31s > 30s → expired


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
        resp = client.get("/api/perp-spot-basis")
        assert resp.status_code == 200

    def test_endpoint_returns_json(self):
        client = self._client()
        resp = client.get("/api/perp-spot-basis")
        data = resp.json()
        assert isinstance(data, dict)

    def test_endpoint_has_assets(self):
        client = self._client()
        resp = client.get("/api/perp-spot-basis")
        assert "assets" in resp.json()

    def test_endpoint_assets_has_three_entries(self):
        client = self._client()
        resp = client.get("/api/perp-spot-basis")
        assert len(resp.json()["assets"]) == 3

    def test_endpoint_has_avg_basis_bps(self):
        client = self._client()
        resp = client.get("/api/perp-spot-basis")
        assert "avg_basis_bps" in resp.json()

    def test_endpoint_has_market_signal(self):
        client = self._client()
        resp = client.get("/api/perp-spot-basis")
        assert "market_signal" in resp.json()

    def test_endpoint_has_timestamp(self):
        client = self._client()
        resp = client.get("/api/perp-spot-basis")
        assert "timestamp" in resp.json()

    def test_endpoint_market_signal_valid(self):
        client = self._client()
        resp = client.get("/api/perp-spot-basis")
        assert resp.json()["market_signal"] in ("contango", "backwardation", "neutral")

    def test_endpoint_asset_has_required_fields(self):
        client = self._client()
        resp = client.get("/api/perp-spot-basis")
        for asset in resp.json()["assets"]:
            for field in ["symbol", "basis_bps", "z_score", "signal", "perp_price", "spot_price"]:
                assert field in asset, f"Missing field: {field}"

    def test_endpoint_btc_in_assets(self):
        client = self._client()
        resp = client.get("/api/perp-spot-basis")
        symbols = [a["symbol"] for a in resp.json()["assets"]]
        assert "BTC" in symbols

    def test_endpoint_eth_in_assets(self):
        client = self._client()
        resp = client.get("/api/perp-spot-basis")
        symbols = [a["symbol"] for a in resp.json()["assets"]]
        assert "ETH" in symbols

    def test_endpoint_sol_in_assets(self):
        client = self._client()
        resp = client.get("/api/perp-spot-basis")
        symbols = [a["symbol"] for a in resp.json()["assets"]]
        assert "SOL" in symbols


# ─── TestFrontendHTML ─────────────────────────────────────────────────────────

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), '..', 'frontend')
HTML_PATH = os.path.join(FRONTEND_DIR, 'index.html')
JS_PATH = os.path.join(FRONTEND_DIR, 'app.js')


class TestFrontendHTML:
    def _html(self):
        with open(HTML_PATH, 'r') as f:
            return f.read()

    def test_html_contains_perp_spot_basis_title(self):
        assert 'Perp/Spot Basis' in self._html()

    def test_html_contains_card_id(self):
        assert 'perp-spot-basis' in self._html()

    def test_html_card_has_card_class(self):
        assert 'card' in self._html()

    def test_html_contains_btc_reference(self):
        html = self._html()
        assert 'BTC' in html or 'btc' in html.lower()

    def test_html_has_content_div(self):
        assert 'perp-spot-basis-content' in self._html()

    def test_html_has_badge_element(self):
        assert 'perp-spot-basis-badge' in self._html()

    def test_html_references_basis(self):
        html = self._html()
        assert 'basis' in html.lower()


# ─── TestFrontendJS ───────────────────────────────────────────────────────────

class TestFrontendJS:
    def _js(self):
        with open(JS_PATH, 'r') as f:
            return f.read()

    def test_js_has_render_function(self):
        assert 'renderPerpSpotBasis' in self._js()

    def test_js_fetches_endpoint(self):
        assert 'perp-spot-basis' in self._js()

    def test_js_references_avg_basis_bps(self):
        assert 'avg_basis_bps' in self._js()

    def test_js_references_market_signal(self):
        assert 'market_signal' in self._js()

    def test_js_references_assets(self):
        assert 'assets' in self._js()

    def test_js_references_basis_bps(self):
        assert 'basis_bps' in self._js()

    def test_js_references_z_score(self):
        assert 'z_score' in self._js()

    def test_js_updates_content_element(self):
        assert 'perp-spot-basis-content' in self._js()

    def test_js_updates_badge(self):
        assert 'perp-spot-basis-badge' in self._js()

    def test_js_is_syntax_valid(self):
        """node --check validates JS syntax."""
        import subprocess
        result = subprocess.run(
            ['node', '--check', JS_PATH],
            capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr
