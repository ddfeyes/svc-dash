"""
Unit / smoke tests for /api/derivatives-heatmap.

Open interest heatmap by strike and expiry for BTC/ETH options.

Key metrics:
  OI heatmap     — open interest by (strike, expiry) for calls and puts
  Max pain       — strike price minimising total payout to option buyers
  GEX            — Gamma Exposure = gamma × OI × contract_size × spot² / 100
  PCR            — Put/Call Ratio = total_put_oi / total_call_oi
  OI concentration — top-3 strikes share of total OI

Max pain algorithm:
  For each candidate strike S, compute:
    payout = Σ calls: max(0, S - K) × OI_call(K)
            + Σ puts:  max(0, K - S) × OI_put(K)
  Max pain = argmin(payout)

GEX interpretation:
  positive GEX → dealers long gamma → suppress volatility
  negative GEX → dealers short gamma → amplify volatility
  GEX flip point → strike where GEX crosses zero (key level)

Covers:
  - _dh_parse_instrument
  - _dh_total_payout
  - _dh_max_pain
  - _dh_gex_at_strike
  - _dh_oi_concentration
  - _dh_put_call_ratio
  - _dh_nearest_expiries
  - Response shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _dh_parse_instrument,
    _dh_total_payout,
    _dh_max_pain,
    _dh_gex_at_strike,
    _dh_oi_concentration,
    _dh_put_call_ratio,
    _dh_nearest_expiries,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "asset":       "BTC",
    "spot_price":  98_500.0,
    "max_pain": {
        "strike":        95_000.0,
        "total_payout":  1_250_000_000.0,
        "distance_pct": -3.55,
    },
    "gex": {
        "total":            125_000_000.0,
        "by_strike":        {"90000": 5_000_000.0, "95000": 45_000_000.0, "100000": 75_000_000.0},
        "dominant_strike":  100_000.0,
        "flip_point":        93_000.0,
    },
    "oi_heatmap": {
        "strikes":  [85_000, 90_000, 95_000, 100_000, 105_000],
        "expiries": ["2024-12-27", "2025-01-31"],
        "calls":    {"95000":  {"2024-12-27": 1_200.5, "2025-01-31": 850.0}},
        "puts":     {"95000":  {"2024-12-27":   820.3, "2025-01-31": 430.0}},
    },
    "summary": {
        "total_call_oi":    45_000.0,
        "total_put_oi":     38_000.0,
        "put_call_ratio":    0.844,
        "oi_concentration":  0.65,
    },
    "description": "Max pain $95k — OI concentrated at 95k/100k strikes",
}


# ===========================================================================
# 1. _dh_parse_instrument
# ===========================================================================

class TestDhParseInstrument:
    def test_btc_call_parsed(self):
        r = _dh_parse_instrument("BTC-27DEC24-95000-C")
        assert r["asset"]  == "BTC"
        assert r["strike"] == 95_000.0
        assert r["option_type"] == "call"

    def test_eth_put_parsed(self):
        r = _dh_parse_instrument("ETH-31JAN25-3500-P")
        assert r["asset"]  == "ETH"
        assert r["strike"] == 3_500.0
        assert r["option_type"] == "put"

    def test_expiry_field_present(self):
        r = _dh_parse_instrument("BTC-27DEC24-100000-C")
        assert "expiry" in r

    def test_invalid_returns_none(self):
        assert _dh_parse_instrument("NOT-VALID") is None

    def test_empty_string_returns_none(self):
        assert _dh_parse_instrument("") is None

    def test_strike_is_float(self):
        r = _dh_parse_instrument("BTC-27DEC24-50000-C")
        assert isinstance(r["strike"], float)

    def test_c_suffix_is_call(self):
        r = _dh_parse_instrument("BTC-27DEC24-80000-C")
        assert r["option_type"] == "call"

    def test_p_suffix_is_put(self):
        r = _dh_parse_instrument("BTC-27DEC24-80000-P")
        assert r["option_type"] == "put"


# ===========================================================================
# 2. _dh_total_payout
# ===========================================================================

class TestDhTotalPayout:
    # Simple 2-strike scenario:
    # calls: {100: 10, 110: 5}   puts: {90: 8, 100: 3}
    CALLS = {100.0: 10.0, 110.0: 5.0}
    PUTS  = {90.0:   8.0, 100.0: 3.0}

    def test_payout_at_call_atm(self):
        # At S=100: calls owe 0 (S=K) + 0 (S<K); puts owe 0 (K=90 < S) + 0 (K=100=S)
        p = _dh_total_payout(100.0, self.CALLS, self.PUTS)
        assert p == pytest.approx(0.0, abs=1e-6)

    def test_payout_above_all_strikes(self):
        # At S=120: calls owe (120-100)*10 + (120-110)*5 = 200+50 = 250
        #           puts owe 0 (all K < S)
        p = _dh_total_payout(120.0, self.CALLS, self.PUTS)
        assert p == pytest.approx(250.0, rel=1e-4)

    def test_payout_below_all_strikes(self):
        # At S=80: calls owe 0 (all K > S)
        #          puts owe (90-80)*8 + (100-80)*3 = 80+60 = 140
        p = _dh_total_payout(80.0, self.CALLS, self.PUTS)
        assert p == pytest.approx(140.0, rel=1e-4)

    def test_empty_calls_only_puts(self):
        p = _dh_total_payout(80.0, {}, {90.0: 10.0})
        assert p == pytest.approx(100.0, rel=1e-4)  # (90-80)*10

    def test_empty_puts_only_calls(self):
        p = _dh_total_payout(120.0, {100.0: 5.0}, {})
        assert p == pytest.approx(100.0, rel=1e-4)  # (120-100)*5

    def test_returns_float(self):
        assert isinstance(_dh_total_payout(100.0, self.CALLS, self.PUTS), float)


# ===========================================================================
# 3. _dh_max_pain
# ===========================================================================

class TestDhMaxPain:
    def test_single_strike_returns_that_strike(self):
        assert _dh_max_pain([100.0], {100.0: 10.0}, {100.0: 5.0}) == 100.0

    def test_max_pain_minimises_payout(self):
        # Calls heavy at 110, puts heavy at 90 → max pain near 100
        calls = {100.0: 5.0, 110.0: 20.0}
        puts  = {90.0: 20.0, 100.0: 5.0}
        mp = _dh_max_pain([90.0, 100.0, 110.0], calls, puts)
        assert mp == 100.0

    def test_empty_strikes_returns_zero(self):
        assert _dh_max_pain([], {}, {}) == pytest.approx(0.0, abs=1e-6)

    def test_returns_float(self):
        mp = _dh_max_pain([90.0, 100.0], {100.0: 5.0}, {90.0: 5.0})
        assert isinstance(mp, float)

    def test_returns_one_of_input_strikes(self):
        strikes = [80.0, 90.0, 100.0, 110.0, 120.0]
        calls   = {k: 10.0 for k in strikes}
        puts    = {k: 10.0 for k in strikes}
        mp = _dh_max_pain(strikes, calls, puts)
        assert mp in strikes

    def test_symmetric_oi_returns_middle_strike(self):
        # Symmetric OI → max pain at middle strike
        strikes = [90.0, 100.0, 110.0]
        calls   = {90.0: 10.0, 100.0: 10.0, 110.0: 10.0}
        puts    = {90.0: 10.0, 100.0: 10.0, 110.0: 10.0}
        mp = _dh_max_pain(strikes, calls, puts)
        assert mp == 100.0

    def test_all_calls_max_pain_at_lowest_strike(self):
        # If only calls, max pain is lowest strike (no call payouts there)
        strikes = [80.0, 90.0, 100.0]
        calls   = {90.0: 10.0, 100.0: 10.0}
        mp = _dh_max_pain(strikes, calls, {})
        assert mp == 80.0


# ===========================================================================
# 4. _dh_gex_at_strike
# ===========================================================================

class TestDhGexAtStrike:
    def test_typical_gex_positive(self):
        # gamma=0.001, oi=100, spot=50000, contract=1
        gex = _dh_gex_at_strike(0.001, 100.0, 50_000.0, 1.0)
        # = 0.001 × 100 × 1 × 50000² / 100 = 0.1 × 2.5e9 / 100 = 250000
        assert gex == pytest.approx(250_000.0, rel=1e-4)

    def test_zero_gamma_returns_zero(self):
        assert _dh_gex_at_strike(0.0, 100.0, 50_000.0, 1.0) == pytest.approx(0.0, abs=1e-9)

    def test_zero_oi_returns_zero(self):
        assert _dh_gex_at_strike(0.001, 0.0, 50_000.0, 1.0) == pytest.approx(0.0, abs=1e-9)

    def test_zero_spot_returns_zero(self):
        assert _dh_gex_at_strike(0.001, 100.0, 0.0, 1.0) == pytest.approx(0.0, abs=1e-9)

    def test_returns_float(self):
        assert isinstance(_dh_gex_at_strike(0.001, 100.0, 50_000.0, 1.0), float)

    def test_scales_with_spot_squared(self):
        gex1 = _dh_gex_at_strike(0.001, 10.0, 50_000.0, 1.0)
        gex2 = _dh_gex_at_strike(0.001, 10.0, 100_000.0, 1.0)
        # 100k² / 50k² = 4
        assert gex2 == pytest.approx(gex1 * 4.0, rel=1e-4)


# ===========================================================================
# 5. _dh_oi_concentration
# ===========================================================================

class TestDhOiConcentration:
    def test_empty_returns_zero(self):
        assert _dh_oi_concentration({}) == pytest.approx(0.0, abs=1e-6)

    def test_single_strike_returns_one(self):
        assert _dh_oi_concentration({"100": 500.0}) == pytest.approx(1.0, rel=1e-4)

    def test_uniform_distribution_lower_concentration(self):
        oi = {str(k): 100.0 for k in range(10)}
        c = _dh_oi_concentration(oi)
        # top 3 / total = 300/1000 = 0.3
        assert c == pytest.approx(0.3, rel=1e-4)

    def test_concentrated_returns_high_value(self):
        oi = {"95000": 900.0, "100000": 50.0, "90000": 50.0}
        c = _dh_oi_concentration(oi)
        assert c > 0.9

    def test_result_in_0_1_range(self):
        oi = {str(i): float(i * 10) for i in range(1, 8)}
        c = _dh_oi_concentration(oi)
        assert 0.0 <= c <= 1.0

    def test_two_strikes_concentration(self):
        oi = {"90000": 600.0, "100000": 400.0}
        # top 3 (only 2 exist) = 1000 / 1000 = 1.0
        assert _dh_oi_concentration(oi) == pytest.approx(1.0, rel=1e-4)


# ===========================================================================
# 6. _dh_put_call_ratio
# ===========================================================================

class TestDhPutCallRatio:
    def test_equal_oi_returns_one(self):
        assert _dh_put_call_ratio(1000.0, 1000.0) == pytest.approx(1.0, rel=1e-4)

    def test_zero_call_oi_returns_zero(self):
        assert _dh_put_call_ratio(0.0, 1000.0) == pytest.approx(0.0, abs=1e-9)

    def test_zero_put_oi_returns_zero(self):
        assert _dh_put_call_ratio(1000.0, 0.0) == pytest.approx(0.0, abs=1e-9)

    def test_more_puts_than_calls_above_one(self):
        pcr = _dh_put_call_ratio(1000.0, 2000.0)
        assert pcr > 1.0

    def test_correct_ratio(self):
        assert _dh_put_call_ratio(2000.0, 1000.0) == pytest.approx(0.5, rel=1e-4)


# ===========================================================================
# 7. _dh_nearest_expiries
# ===========================================================================

class TestDhNearestExpiries:
    EXPIRIES = ["2025-03-28", "2025-01-31", "2024-12-27", "2025-06-27", "2025-12-26"]

    def test_returns_sorted_ascending(self):
        result = _dh_nearest_expiries(self.EXPIRIES, 3)
        assert result == sorted(result)

    def test_returns_n_items(self):
        result = _dh_nearest_expiries(self.EXPIRIES, 3)
        assert len(result) == 3

    def test_returns_nearest_first(self):
        result = _dh_nearest_expiries(self.EXPIRIES, 1)
        assert result[0] == "2024-12-27"

    def test_n_greater_than_len_returns_all(self):
        result = _dh_nearest_expiries(self.EXPIRIES, 100)
        assert len(result) == len(self.EXPIRIES)

    def test_empty_returns_empty(self):
        assert _dh_nearest_expiries([], 3) == []


# ===========================================================================
# 8. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_asset(self):
        assert SAMPLE_RESPONSE["asset"] in ("BTC", "ETH")

    def test_has_spot_price(self):
        assert SAMPLE_RESPONSE["spot_price"] > 0

    def test_has_max_pain_dict(self):
        assert isinstance(SAMPLE_RESPONSE["max_pain"], dict)

    def test_max_pain_has_required_keys(self):
        for key in ("strike", "total_payout", "distance_pct"):
            assert key in SAMPLE_RESPONSE["max_pain"], f"max_pain missing '{key}'"

    def test_has_gex_dict(self):
        assert isinstance(SAMPLE_RESPONSE["gex"], dict)

    def test_gex_has_required_keys(self):
        for key in ("total", "by_strike", "dominant_strike", "flip_point"):
            assert key in SAMPLE_RESPONSE["gex"], f"gex missing '{key}'"

    def test_has_oi_heatmap_dict(self):
        assert isinstance(SAMPLE_RESPONSE["oi_heatmap"], dict)

    def test_oi_heatmap_has_required_keys(self):
        for key in ("strikes", "expiries", "calls", "puts"):
            assert key in SAMPLE_RESPONSE["oi_heatmap"], f"oi_heatmap missing '{key}'"

    def test_has_summary_dict(self):
        assert isinstance(SAMPLE_RESPONSE["summary"], dict)

    def test_summary_has_required_keys(self):
        for key in ("total_call_oi", "total_put_oi", "put_call_ratio", "oi_concentration"):
            assert key in SAMPLE_RESPONSE["summary"], f"summary missing '{key}'"

    def test_put_call_ratio_is_positive(self):
        assert SAMPLE_RESPONSE["summary"]["put_call_ratio"] > 0

    def test_has_description(self):
        assert isinstance(SAMPLE_RESPONSE["description"], str)


# ===========================================================================
# 9. Structural tests
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api_path = os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")
        content = open(api_path).read()
        assert "/derivatives-heatmap" in content, "/derivatives-heatmap route missing"

    def test_html_card_exists(self):
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        content = open(html_path).read()
        assert "card-derivatives-heatmap" in content, "card-derivatives-heatmap missing"

    def test_js_render_function_exists(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "renderDerivativesHeatmap" in content, "renderDerivativesHeatmap missing"

    def test_js_api_call_to_endpoint(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "/derivatives-heatmap" in content, "/derivatives-heatmap call missing"
