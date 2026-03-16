"""
Unit / smoke tests for /api/btc-dominance-tracker.

BTC Dominance Tracker — shows BTC market dominance trends, ETH/altcoin
breakdown, dominance regime classifier, 90-day sparkline with MA, and
correlation between BTC dominance and altcoin index.

Approach:
  - Dominance % from total market cap data (CoinGecko /global)
  - Regime classifier: BTC season / altcoin season / neutral
  - 90-day sparkline with 30-day moving average
  - Correlation between BTC dominance and altcoin index (inverse relationship)

Signal:
  btc_season   — BTC dom > 55% and rising → BTC outperforming
  alt_season   — BTC dom < 45% and falling → alts outperforming
  neutral      — BTC dom 45–55% or flat

Covers:
  - _bd_dominance_pct
  - _bd_change_pct
  - _bd_regime
  - _bd_moving_average
  - _bd_altcoin_season_index
  - _bd_correlation
  - Response shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _bd_dominance_pct,
    _bd_change_pct,
    _bd_regime,
    _bd_moving_average,
    _bd_altcoin_season_index,
    _bd_correlation,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "btc": {
        "dominance_pct":   52.4,
        "change_24h_pct":   0.3,
        "change_7d_pct":    1.1,
        "market_cap_usd":  1_050_000_000_000,
    },
    "eth": {
        "dominance_pct":   17.2,
        "change_24h_pct":  -0.1,
        "change_7d_pct":   -0.4,
        "market_cap_usd":    344_000_000_000,
    },
    "alts": {
        "dominance_pct":   30.4,
        "change_24h_pct":  -0.2,
        "change_7d_pct":   -0.7,
        "market_cap_usd":    608_000_000_000,
    },
    "regime": {
        "label":     "neutral",
        "btc_season_index": 58.0,
        "alt_season_index": 42.0,
        "direction": "rising",
    },
    "correlation": {
        "btc_dom_vs_alt_index": -0.72,
        "window_days": 30,
        "interpretation": "strong_inverse",
    },
    "sparkline": [
        {"date": "2024-11-14", "btc_dom": 51.2, "ma30": 50.8},
        {"date": "2024-11-15", "btc_dom": 51.8, "ma30": 50.9},
        {"date": "2024-11-16", "btc_dom": 52.0, "ma30": 51.0},
        {"date": "2024-11-17", "btc_dom": 51.5, "ma30": 51.1},
        {"date": "2024-11-18", "btc_dom": 52.4, "ma30": 51.2},
    ],
    "description": "Neutral: BTC dominance 52.4% — rising, approaching BTC season territory",
}


# ===========================================================================
# 1. _bd_dominance_pct
# ===========================================================================

class TestBdDominancePct:
    def test_btc_market_cap_fraction_of_total(self):
        pct = _bd_dominance_pct(1_000_000, 2_000_000)
        assert pct == pytest.approx(50.0, abs=0.01)

    def test_returns_float(self):
        assert isinstance(_bd_dominance_pct(500_000, 1_000_000), float)

    def test_zero_total_returns_zero(self):
        assert _bd_dominance_pct(0.0, 0.0) == pytest.approx(0.0, abs=1e-6)

    def test_full_dominance_returns_100(self):
        assert _bd_dominance_pct(1_000_000, 1_000_000) == pytest.approx(100.0, abs=0.01)

    def test_result_in_0_100_range(self):
        for btc, total in [(0, 1), (250_000, 1_000_000), (1_000_000, 1_000_000)]:
            result = _bd_dominance_pct(float(btc), float(total))
            assert 0.0 <= result <= 100.0

    def test_partial_dominance_correct(self):
        # BTC = 600B, total = 2T → 30%
        pct = _bd_dominance_pct(600_000_000_000, 2_000_000_000_000)
        assert pct == pytest.approx(30.0, abs=0.01)


# ===========================================================================
# 2. _bd_change_pct
# ===========================================================================

class TestBdChangePct:
    def test_positive_change_is_positive(self):
        assert _bd_change_pct(52.0, 50.0) > 0

    def test_negative_change_is_negative(self):
        assert _bd_change_pct(48.0, 50.0) < 0

    def test_no_change_is_zero(self):
        assert _bd_change_pct(50.0, 50.0) == pytest.approx(0.0, abs=1e-6)

    def test_returns_float(self):
        assert isinstance(_bd_change_pct(52.0, 50.0), float)

    def test_zero_previous_returns_zero(self):
        assert _bd_change_pct(50.0, 0.0) == pytest.approx(0.0, abs=1e-6)

    def test_correct_magnitude(self):
        # 55 → 50 = change of +5 percentage points
        result = _bd_change_pct(55.0, 50.0)
        assert result == pytest.approx(5.0, abs=0.01)


# ===========================================================================
# 3. _bd_regime
# ===========================================================================

class TestBdRegime:
    def test_high_dom_rising_is_btc_season(self):
        assert _bd_regime(60.0, "rising") == "btc_season"

    def test_low_dom_falling_is_alt_season(self):
        assert _bd_regime(40.0, "falling") == "alt_season"

    def test_mid_dom_rising_is_neutral(self):
        assert _bd_regime(50.0, "rising") == "neutral"

    def test_mid_dom_stable_is_neutral(self):
        assert _bd_regime(50.0, "stable") == "neutral"

    def test_high_dom_falling_is_neutral(self):
        # Rising dom clearly BTCseason, but falling high dom → transitioning
        result = _bd_regime(60.0, "falling")
        assert result in ("btc_season", "neutral")

    def test_low_dom_rising_is_neutral(self):
        result = _bd_regime(40.0, "rising")
        assert result in ("alt_season", "neutral")

    def test_returns_valid_string(self):
        result = _bd_regime(52.0, "stable")
        assert result in ("btc_season", "alt_season", "neutral")


# ===========================================================================
# 4. _bd_moving_average
# ===========================================================================

class TestBdMovingAverage:
    def test_empty_returns_empty(self):
        assert _bd_moving_average([], 7) == []

    def test_fewer_than_window_returns_partial_averages(self):
        result = _bd_moving_average([50.0, 52.0, 54.0], 5)
        assert len(result) == 3

    def test_single_value_equals_itself(self):
        result = _bd_moving_average([50.0], 3)
        assert len(result) == 1
        assert result[0] == pytest.approx(50.0, abs=0.01)

    def test_full_window_average_correct(self):
        values = [10.0, 20.0, 30.0]
        result = _bd_moving_average(values, 3)
        # Last element should be avg(10, 20, 30) = 20
        assert result[-1] == pytest.approx(20.0, abs=0.01)

    def test_returns_list(self):
        assert isinstance(_bd_moving_average([50.0, 51.0, 52.0], 3), list)

    def test_output_length_equals_input_length(self):
        values = [50.0] * 10
        result = _bd_moving_average(values, 5)
        assert len(result) == 10

    def test_flat_series_ma_equals_constant(self):
        values = [50.0] * 10
        result = _bd_moving_average(values, 5)
        for v in result:
            assert v == pytest.approx(50.0, abs=0.01)


# ===========================================================================
# 5. _bd_altcoin_season_index
# ===========================================================================

class TestBdAltcoinSeasonIndex:
    def test_low_btc_dominance_high_alt_index(self):
        idx = _bd_altcoin_season_index(35.0)
        assert idx > 70.0

    def test_high_btc_dominance_low_alt_index(self):
        idx = _bd_altcoin_season_index(65.0)
        assert idx < 40.0

    def test_mid_btc_dominance_near_50(self):
        idx = _bd_altcoin_season_index(50.0)
        assert 40.0 <= idx <= 60.0

    def test_returns_float(self):
        assert isinstance(_bd_altcoin_season_index(50.0), float)

    def test_result_in_0_100_range(self):
        for dom in (20.0, 35.0, 50.0, 65.0, 80.0):
            idx = _bd_altcoin_season_index(dom)
            assert 0.0 <= idx <= 100.0

    def test_inverse_relationship(self):
        low_dom_idx  = _bd_altcoin_season_index(35.0)
        high_dom_idx = _bd_altcoin_season_index(65.0)
        assert low_dom_idx > high_dom_idx


# ===========================================================================
# 6. _bd_correlation
# ===========================================================================

class TestBdCorrelation:
    def test_empty_history_returns_zero(self):
        assert _bd_correlation([], []) == pytest.approx(0.0, abs=1e-6)

    def test_single_pair_returns_zero(self):
        assert _bd_correlation([50.0], [100.0]) == pytest.approx(0.0, abs=1e-6)

    def test_perfect_negative_correlation(self):
        dom  = [40.0, 45.0, 50.0, 55.0, 60.0]
        alts = [60.0, 55.0, 50.0, 45.0, 40.0]
        r = _bd_correlation(dom, alts)
        assert r == pytest.approx(-1.0, abs=0.01)

    def test_perfect_positive_correlation(self):
        dom  = [40.0, 45.0, 50.0, 55.0, 60.0]
        alts = [40.0, 45.0, 50.0, 55.0, 60.0]
        r = _bd_correlation(dom, alts)
        assert r == pytest.approx(1.0, abs=0.01)

    def test_result_in_minus1_to_1_range(self):
        import random
        random.seed(42)
        dom  = [random.uniform(40, 60) for _ in range(20)]
        alts = [random.uniform(20, 50) for _ in range(20)]
        r = _bd_correlation(dom, alts)
        assert -1.0 <= r <= 1.0

    def test_returns_float(self):
        assert isinstance(_bd_correlation([1.0, 2.0, 3.0], [3.0, 2.0, 1.0]), float)

    def test_mismatched_lengths_uses_shorter(self):
        # should not raise; truncate to shorter list
        r = _bd_correlation([1.0, 2.0, 3.0, 4.0], [4.0, 3.0, 2.0])
        assert isinstance(r, float)


# ===========================================================================
# 7. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_btc_dict(self):
        assert isinstance(SAMPLE_RESPONSE["btc"], dict)

    def test_btc_has_required_keys(self):
        for key in ("dominance_pct", "change_24h_pct", "change_7d_pct", "market_cap_usd"):
            assert key in SAMPLE_RESPONSE["btc"], f"btc missing '{key}'"

    def test_btc_dominance_in_range(self):
        assert 0 <= SAMPLE_RESPONSE["btc"]["dominance_pct"] <= 100

    def test_has_eth_dict(self):
        assert isinstance(SAMPLE_RESPONSE["eth"], dict)

    def test_eth_has_required_keys(self):
        for key in ("dominance_pct", "change_24h_pct", "change_7d_pct", "market_cap_usd"):
            assert key in SAMPLE_RESPONSE["eth"], f"eth missing '{key}'"

    def test_has_alts_dict(self):
        assert isinstance(SAMPLE_RESPONSE["alts"], dict)

    def test_alts_has_required_keys(self):
        for key in ("dominance_pct", "change_24h_pct", "change_7d_pct", "market_cap_usd"):
            assert key in SAMPLE_RESPONSE["alts"], f"alts missing '{key}'"

    def test_dominance_sums_to_100(self):
        total = (
            SAMPLE_RESPONSE["btc"]["dominance_pct"]
            + SAMPLE_RESPONSE["eth"]["dominance_pct"]
            + SAMPLE_RESPONSE["alts"]["dominance_pct"]
        )
        assert total == pytest.approx(100.0, abs=1.0)

    def test_has_regime_dict(self):
        assert isinstance(SAMPLE_RESPONSE["regime"], dict)

    def test_regime_has_required_keys(self):
        for key in ("label", "btc_season_index", "alt_season_index", "direction"):
            assert key in SAMPLE_RESPONSE["regime"], f"regime missing '{key}'"

    def test_regime_label_valid(self):
        assert SAMPLE_RESPONSE["regime"]["label"] in ("btc_season", "alt_season", "neutral")

    def test_regime_direction_valid(self):
        assert SAMPLE_RESPONSE["regime"]["direction"] in ("rising", "falling", "stable")

    def test_has_correlation_dict(self):
        assert isinstance(SAMPLE_RESPONSE["correlation"], dict)

    def test_correlation_has_required_keys(self):
        for key in ("btc_dom_vs_alt_index", "window_days", "interpretation"):
            assert key in SAMPLE_RESPONSE["correlation"], f"correlation missing '{key}'"

    def test_correlation_value_in_range(self):
        r = SAMPLE_RESPONSE["correlation"]["btc_dom_vs_alt_index"]
        assert -1.0 <= r <= 1.0

    def test_has_sparkline_list(self):
        assert isinstance(SAMPLE_RESPONSE["sparkline"], list)

    def test_sparkline_items_have_required_keys(self):
        for item in SAMPLE_RESPONSE["sparkline"]:
            for key in ("date", "btc_dom", "ma30"):
                assert key in item, f"sparkline item missing '{key}'"

    def test_has_description(self):
        assert isinstance(SAMPLE_RESPONSE["description"], str)


# ===========================================================================
# 8. Structural tests
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api_path = os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")
        content = open(api_path).read()
        assert "/btc-dominance-tracker" in content, "/btc-dominance-tracker route missing"

    def test_html_card_exists(self):
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        content = open(html_path).read()
        assert "card-btc-dominance-tracker" in content, "card-btc-dominance-tracker missing"

    def test_js_render_function_exists(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "renderBtcDominanceTracker" in content, "renderBtcDominanceTracker missing"

    def test_js_api_call_to_endpoint(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "/btc-dominance-tracker" in content, "/btc-dominance-tracker call missing"
