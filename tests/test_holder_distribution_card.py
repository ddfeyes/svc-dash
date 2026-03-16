"""
Unit / smoke tests for /api/holder-distribution-card.

Holder distribution card — tracks BTC/ETH address distribution across
wallet size bands (shrimp → whale), Gini coefficient, and HHI supply
concentration.

Approach:
  - Wallet band classification by USD balance threshold
  - Gini coefficient: inequality measure [0, 1]
  - Herfindahl-Hirschman Index (HHI) for supply concentration
  - Whale 7d accumulation / distribution delta
  - Normalized HHI [0, 100] and concentration risk label

Wallet size bands (by BTC equivalent or USD value):
  shrimp  — < $1k
  crab    — $1k – $10k
  fish    — $10k – $100k
  shark   — $100k – $1M
  whale   — >= $1M

Covers:
  - _hd_wallet_band
  - _hd_gini
  - _hd_herfindahl
  - _hd_normalize_hhi
  - _hd_whale_delta
  - _hd_whale_signal
  - _hd_concentration_risk
  - _hd_band_pct
  - Response shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _hd_wallet_band,
    _hd_gini,
    _hd_herfindahl,
    _hd_normalize_hhi,
    _hd_whale_delta,
    _hd_whale_signal,
    _hd_concentration_risk,
    _hd_band_pct,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "bands": {
        "shrimp": {"count": 450_000, "pct_supply": 2.1},
        "crab":   {"count": 180_000, "pct_supply": 5.4},
        "fish":   {"count":  90_000, "pct_supply": 8.7},
        "shark":  {"count":  25_000, "pct_supply": 18.3},
        "whale":  {"count":   2_500, "pct_supply": 65.5},
    },
    "whale_delta": {
        "7d_change_pct": 1.8,
        "signal": "accumulating",
    },
    "gini": {
        "current": 0.87,
        "30d_ago": 0.85,
        "trend": "rising",
    },
    "hhi": {
        "raw": 0.43,
        "normalized": 67.4,
        "risk": "high",
    },
    "top_whales": [
        {"rank": 1, "pct_supply": 4.2, "band": "whale"},
        {"rank": 2, "pct_supply": 3.1, "band": "whale"},
        {"rank": 3, "pct_supply": 2.8, "band": "whale"},
    ],
    "zscore": 1.2,
    "description": "High concentration: top whales hold 65% of supply, Gini 0.87",
}


# ===========================================================================
# 1. _hd_wallet_band
# ===========================================================================

class TestHdWalletBand:
    def test_zero_balance_is_shrimp(self):
        assert _hd_wallet_band(0.0) == "shrimp"

    def test_tiny_balance_is_shrimp(self):
        assert _hd_wallet_band(500.0) == "shrimp"

    def test_boundary_1k_is_crab(self):
        assert _hd_wallet_band(1_000.0) == "crab"

    def test_mid_crab(self):
        assert _hd_wallet_band(5_000.0) == "crab"

    def test_boundary_10k_is_fish(self):
        assert _hd_wallet_band(10_000.0) == "fish"

    def test_mid_fish(self):
        assert _hd_wallet_band(50_000.0) == "fish"

    def test_boundary_100k_is_shark(self):
        assert _hd_wallet_band(100_000.0) == "shark"

    def test_mid_shark(self):
        assert _hd_wallet_band(500_000.0) == "shark"

    def test_boundary_1m_is_whale(self):
        assert _hd_wallet_band(1_000_000.0) == "whale"

    def test_large_balance_is_whale(self):
        assert _hd_wallet_band(1_000_000_000.0) == "whale"

    def test_returns_valid_band(self):
        for val in [0, 999, 1000, 9999, 10000, 99999, 100000, 999999, 1000000]:
            assert _hd_wallet_band(float(val)) in (
                "shrimp", "crab", "fish", "shark", "whale"
            )


# ===========================================================================
# 2. _hd_gini
# ===========================================================================

class TestHdGini:
    def test_empty_returns_zero(self):
        assert _hd_gini([]) == pytest.approx(0.0, abs=1e-6)

    def test_single_returns_zero(self):
        assert _hd_gini([100.0]) == pytest.approx(0.0, abs=1e-6)

    def test_equal_distribution_returns_zero(self):
        balances = [100.0] * 10
        assert _hd_gini(balances) == pytest.approx(0.0, abs=1e-4)

    def test_perfect_inequality_returns_near_one(self):
        # One entity holds everything; Gini = (n-1)/n = 0.9 for n=10
        balances = [0.0] * 9 + [1000.0]
        assert _hd_gini(balances) == pytest.approx(0.9, abs=1e-6)

    def test_result_in_0_1_range(self):
        for balances in [
            [1, 2, 3, 4, 5],
            [10, 100, 1000, 10000],
            [50] * 5,
            [0, 0, 0, 1000],
        ]:
            g = _hd_gini([float(b) for b in balances])
            assert 0.0 <= g <= 1.0

    def test_higher_inequality_higher_gini(self):
        equal = _hd_gini([100.0] * 10)
        unequal = _hd_gini([1.0] * 9 + [1000.0])
        assert unequal > equal

    def test_returns_float(self):
        assert isinstance(_hd_gini([10.0, 20.0, 30.0]), float)


# ===========================================================================
# 3. _hd_herfindahl
# ===========================================================================

class TestHdHerfindahl:
    def test_empty_returns_zero(self):
        assert _hd_herfindahl([]) == pytest.approx(0.0, abs=1e-6)

    def test_single_entity_returns_one(self):
        assert _hd_herfindahl([1.0]) == pytest.approx(1.0, abs=1e-6)

    def test_equal_shares_returns_1_over_n(self):
        n = 5
        shares = [1.0 / n] * n
        assert _hd_herfindahl(shares) == pytest.approx(1.0 / n, abs=1e-4)

    def test_result_in_0_1_range(self):
        for shares in [
            [0.5, 0.3, 0.2],
            [0.25] * 4,
            [0.9, 0.1],
            [1.0 / 10] * 10,
        ]:
            h = _hd_herfindahl(shares)
            assert 0.0 <= h <= 1.0

    def test_monopoly_returns_one(self):
        assert _hd_herfindahl([1.0, 0.0, 0.0]) == pytest.approx(1.0, abs=1e-6)

    def test_higher_concentration_higher_hhi(self):
        even = _hd_herfindahl([0.25] * 4)
        concentrated = _hd_herfindahl([0.7, 0.1, 0.1, 0.1])
        assert concentrated > even

    def test_returns_float(self):
        assert isinstance(_hd_herfindahl([0.5, 0.3, 0.2]), float)


# ===========================================================================
# 4. _hd_normalize_hhi
# ===========================================================================

class TestHdNormalizeHhi:
    def test_min_hhi_returns_near_zero(self):
        # With n=10, min HHI = 1/10 = 0.1
        result = _hd_normalize_hhi(0.1, 10)
        assert result == pytest.approx(0.0, abs=1.0)

    def test_max_hhi_returns_100(self):
        result = _hd_normalize_hhi(1.0, 10)
        assert result == pytest.approx(100.0, abs=1.0)

    def test_mid_hhi_between_0_and_100(self):
        result = _hd_normalize_hhi(0.55, 10)
        assert 0.0 < result < 100.0

    def test_single_entity_returns_100(self):
        result = _hd_normalize_hhi(1.0, 1)
        assert result == pytest.approx(100.0, abs=1.0)

    def test_result_in_0_100_range(self):
        for hhi, n in [(0.1, 10), (0.5, 4), (1.0, 1), (0.25, 4)]:
            r = _hd_normalize_hhi(hhi, n)
            assert 0.0 <= r <= 100.0

    def test_returns_float(self):
        assert isinstance(_hd_normalize_hhi(0.3, 5), float)


# ===========================================================================
# 5. _hd_whale_delta
# ===========================================================================

class TestHdWhaleDelta:
    def test_accumulation_positive(self):
        assert _hd_whale_delta(110.0, 100.0) == pytest.approx(10.0, abs=1e-4)

    def test_distribution_negative(self):
        assert _hd_whale_delta(90.0, 100.0) == pytest.approx(-10.0, abs=1e-4)

    def test_no_change_zero(self):
        assert _hd_whale_delta(100.0, 100.0) == pytest.approx(0.0, abs=1e-6)

    def test_zero_previous_returns_zero(self):
        assert _hd_whale_delta(100.0, 0.0) == pytest.approx(0.0, abs=1e-6)

    def test_large_accumulation(self):
        delta = _hd_whale_delta(200.0, 100.0)
        assert delta == pytest.approx(100.0, abs=1e-4)

    def test_returns_float(self):
        assert isinstance(_hd_whale_delta(105.0, 100.0), float)


# ===========================================================================
# 6. _hd_whale_signal
# ===========================================================================

class TestHdWhaleSignal:
    def test_positive_delta_is_accumulating(self):
        assert _hd_whale_signal(5.0) == "accumulating"

    def test_negative_delta_is_distributing(self):
        assert _hd_whale_signal(-5.0) == "distributing"

    def test_near_zero_is_neutral(self):
        assert _hd_whale_signal(0.0) == "neutral"

    def test_small_positive_below_threshold_is_neutral(self):
        # threshold should be around 1%
        assert _hd_whale_signal(0.5) == "neutral"

    def test_small_negative_above_threshold_is_neutral(self):
        assert _hd_whale_signal(-0.5) == "neutral"

    def test_returns_valid_string(self):
        for delta in [-10.0, -1.5, 0.0, 1.5, 10.0]:
            assert _hd_whale_signal(delta) in ("accumulating", "distributing", "neutral")


# ===========================================================================
# 7. _hd_concentration_risk
# ===========================================================================

class TestHdConcentrationRisk:
    def test_low_gini_is_low(self):
        assert _hd_concentration_risk(0.2) == "low"

    def test_moderate_gini(self):
        assert _hd_concentration_risk(0.5) == "moderate"

    def test_high_gini(self):
        assert _hd_concentration_risk(0.75) == "high"

    def test_extreme_gini(self):
        assert _hd_concentration_risk(0.95) == "extreme"

    def test_boundary_0_is_low(self):
        assert _hd_concentration_risk(0.0) == "low"

    def test_boundary_1_is_extreme(self):
        assert _hd_concentration_risk(1.0) == "extreme"

    def test_returns_valid_string(self):
        for g in [0.0, 0.3, 0.6, 0.8, 1.0]:
            assert _hd_concentration_risk(g) in ("low", "moderate", "high", "extreme")


# ===========================================================================
# 8. _hd_band_pct
# ===========================================================================

class TestHdBandPct:
    def test_empty_band_map_returns_zero(self):
        assert _hd_band_pct({}, "whale") == pytest.approx(0.0, abs=1e-6)

    def test_missing_band_returns_zero(self):
        band_map = {"whale": 500.0, "shark": 300.0}
        assert _hd_band_pct(band_map, "shrimp") == pytest.approx(0.0, abs=1e-6)

    def test_correct_percentage(self):
        band_map = {"whale": 60.0, "shark": 20.0, "fish": 10.0, "crab": 6.0, "shrimp": 4.0}
        pct = _hd_band_pct(band_map, "whale")
        assert pct == pytest.approx(60.0, abs=1e-4)

    def test_all_in_one_band_is_100(self):
        band_map = {"whale": 100.0}
        pct = _hd_band_pct(band_map, "whale")
        assert pct == pytest.approx(100.0, abs=1e-4)

    def test_result_in_0_100_range(self):
        band_map = {"whale": 65.5, "shark": 18.3, "fish": 8.7, "crab": 5.4, "shrimp": 2.1}
        for band in ("whale", "shark", "fish", "crab", "shrimp"):
            assert 0.0 <= _hd_band_pct(band_map, band) <= 100.0

    def test_returns_float(self):
        assert isinstance(_hd_band_pct({"whale": 50.0, "shrimp": 50.0}, "whale"), float)


# ===========================================================================
# 9. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_bands_dict(self):
        assert isinstance(SAMPLE_RESPONSE["bands"], dict)

    def test_bands_has_all_five_bands(self):
        for band in ("shrimp", "crab", "fish", "shark", "whale"):
            assert band in SAMPLE_RESPONSE["bands"], f"bands missing '{band}'"

    def test_each_band_has_count_and_pct(self):
        for band, data in SAMPLE_RESPONSE["bands"].items():
            assert "count" in data, f"{band} missing 'count'"
            assert "pct_supply" in data, f"{band} missing 'pct_supply'"

    def test_band_pct_supply_sums_to_100(self):
        total = sum(v["pct_supply"] for v in SAMPLE_RESPONSE["bands"].values())
        assert total == pytest.approx(100.0, abs=0.5)

    def test_has_whale_delta_dict(self):
        assert isinstance(SAMPLE_RESPONSE["whale_delta"], dict)

    def test_whale_delta_has_required_keys(self):
        for key in ("7d_change_pct", "signal"):
            assert key in SAMPLE_RESPONSE["whale_delta"], f"whale_delta missing '{key}'"

    def test_whale_delta_signal_is_valid(self):
        assert SAMPLE_RESPONSE["whale_delta"]["signal"] in (
            "accumulating", "distributing", "neutral"
        )

    def test_has_gini_dict(self):
        assert isinstance(SAMPLE_RESPONSE["gini"], dict)

    def test_gini_has_required_keys(self):
        for key in ("current", "30d_ago", "trend"):
            assert key in SAMPLE_RESPONSE["gini"], f"gini missing '{key}'"

    def test_gini_current_in_range(self):
        g = SAMPLE_RESPONSE["gini"]["current"]
        assert 0.0 <= g <= 1.0

    def test_gini_trend_is_valid(self):
        assert SAMPLE_RESPONSE["gini"]["trend"] in ("rising", "falling", "stable")

    def test_has_hhi_dict(self):
        assert isinstance(SAMPLE_RESPONSE["hhi"], dict)

    def test_hhi_has_required_keys(self):
        for key in ("raw", "normalized", "risk"):
            assert key in SAMPLE_RESPONSE["hhi"], f"hhi missing '{key}'"

    def test_hhi_normalized_in_range(self):
        assert 0.0 <= SAMPLE_RESPONSE["hhi"]["normalized"] <= 100.0

    def test_hhi_risk_is_valid(self):
        assert SAMPLE_RESPONSE["hhi"]["risk"] in ("low", "moderate", "high", "extreme")

    def test_has_top_whales_list(self):
        assert isinstance(SAMPLE_RESPONSE["top_whales"], list)

    def test_top_whales_items_have_required_keys(self):
        for item in SAMPLE_RESPONSE["top_whales"]:
            for key in ("rank", "pct_supply", "band"):
                assert key in item, f"top_whales item missing '{key}'"

    def test_has_zscore(self):
        assert "zscore" in SAMPLE_RESPONSE
        assert isinstance(SAMPLE_RESPONSE["zscore"], float)

    def test_has_description(self):
        assert isinstance(SAMPLE_RESPONSE["description"], str)


# ===========================================================================
# 10. Structural tests
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api_path = os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")
        content = open(api_path).read()
        assert "/holder-distribution-card" in content, "/holder-distribution-card route missing"

    def test_html_card_exists(self):
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        content = open(html_path).read()
        assert "card-holder-distribution" in content, "card-holder-distribution missing"

    def test_js_render_function_exists(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "renderHolderDistribution" in content, "renderHolderDistribution missing"

    def test_js_api_call_to_endpoint(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "/holder-distribution-card" in content, "/holder-distribution-card call missing"
