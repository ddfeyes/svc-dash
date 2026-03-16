"""
Unit / smoke tests for /api/fear-greed.

Composite Fear & Greed Index combining 6 market signals, each normalized to 0-100:
  - Funding rate sentiment  (weight 20%) — positive funding → greed
  - OI momentum             (weight 15%) — rising OI → greed / falling → fear
  - Price vs SMA deviation  (weight 20%) — above SMA → greed / below → fear
  - Volatility regime       (weight 15%) — low vol → greed / extreme vol → fear
  - Net taker buy pressure  (weight 20%) — strong buys → greed / sells → fear
  - Liquidation pressure    (weight 10%) — recent liquidations → fear

Composite score 0-100:
  0-20   Extreme Fear
  21-40  Fear
  41-59  Neutral
  60-79  Greed
  80-100 Extreme Greed

Covers:
  - _fg_clamp
  - _fg_normalize
  - _fg_label
  - _fg_label_color
  - _fg_funding_score
  - _fg_oi_momentum_score
  - _fg_price_deviation_score
  - _fg_volatility_score
  - _fg_taker_score
  - _fg_liquidation_score
  - _fg_composite
  - Response shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _fg_clamp,
    _fg_normalize,
    _fg_label,
    _fg_label_color,
    _fg_funding_score,
    _fg_oi_momentum_score,
    _fg_price_deviation_score,
    _fg_volatility_score,
    _fg_taker_score,
    _fg_liquidation_score,
    _fg_composite,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "symbol": "BANANAS31USDT",
    "score": 62.5,
    "label": "Greed",
    "label_prev": "Neutral",
    "delta": 12.5,
    "trend": "rising",
    "signals": {
        "funding": {
            "score": 70.0,
            "weight": 0.20,
            "raw": 0.01,
            "label": "Greed",
        },
        "oi_momentum": {
            "score": 65.0,
            "weight": 0.15,
            "raw": 2.5,
            "label": "Greed",
        },
        "price_deviation": {
            "score": 60.0,
            "weight": 0.20,
            "raw": 1.5,
            "label": "Greed",
        },
        "volatility": {
            "score": 55.0,
            "weight": 0.15,
            "raw": "mid",
            "label": "Neutral",
        },
        "taker_pressure": {
            "score": 68.0,
            "weight": 0.20,
            "raw": 0.56,
            "label": "Greed",
        },
        "liquidation": {
            "score": 55.0,
            "weight": 0.10,
            "raw": 1,
            "label": "Neutral",
        },
    },
    "history": [
        {"ts": 1700000000.0, "score": 50.0, "label": "Neutral"},
        {"ts": 1700003600.0, "score": 62.5, "label": "Greed"},
    ],
    "description": "Greed: market showing moderate buying pressure",
}


# ===========================================================================
# 1. _fg_clamp
# ===========================================================================

class TestFgClamp:
    def test_value_within_range_unchanged(self):
        assert _fg_clamp(50.0, 0, 100) == 50.0

    def test_value_below_lo_clamped_to_lo(self):
        assert _fg_clamp(-10.0, 0, 100) == 0.0

    def test_value_above_hi_clamped_to_hi(self):
        assert _fg_clamp(110.0, 0, 100) == 100.0

    def test_exact_boundary_unchanged(self):
        assert _fg_clamp(0.0, 0, 100) == 0.0
        assert _fg_clamp(100.0, 0, 100) == 100.0


# ===========================================================================
# 2. _fg_normalize
# ===========================================================================

class TestFgNormalize:
    def test_midpoint_returns_50(self):
        assert _fg_normalize(0.0, -10, 10) == pytest.approx(50.0, abs=0.1)

    def test_at_min_returns_0(self):
        assert _fg_normalize(-10, -10, 10) == pytest.approx(0.0, abs=0.1)

    def test_at_max_returns_100(self):
        assert _fg_normalize(10, -10, 10) == pytest.approx(100.0, abs=0.1)

    def test_above_max_clamped_to_100(self):
        assert _fg_normalize(20, -10, 10) == pytest.approx(100.0, abs=0.1)

    def test_below_min_clamped_to_0(self):
        assert _fg_normalize(-20, -10, 10) == pytest.approx(0.0, abs=0.1)

    def test_equal_lo_hi_returns_50(self):
        # degenerate: lo == hi
        assert _fg_normalize(5, 5, 5) == pytest.approx(50.0, abs=0.1)


# ===========================================================================
# 3. _fg_label
# ===========================================================================

class TestFgLabel:
    def test_score_10_is_extreme_fear(self):
        assert _fg_label(10) == "Extreme Fear"

    def test_score_20_is_extreme_fear(self):
        assert _fg_label(20) == "Extreme Fear"

    def test_score_21_is_fear(self):
        assert _fg_label(21) == "Fear"

    def test_score_40_is_fear(self):
        assert _fg_label(40) == "Fear"

    def test_score_41_is_neutral(self):
        assert _fg_label(41) == "Neutral"

    def test_score_59_is_neutral(self):
        assert _fg_label(59) == "Neutral"

    def test_score_60_is_greed(self):
        assert _fg_label(60) == "Greed"

    def test_score_79_is_greed(self):
        assert _fg_label(79) == "Greed"

    def test_score_80_is_extreme_greed(self):
        assert _fg_label(80) == "Extreme Greed"

    def test_score_100_is_extreme_greed(self):
        assert _fg_label(100) == "Extreme Greed"


# ===========================================================================
# 4. _fg_label_color
# ===========================================================================

class TestFgLabelColor:
    def test_extreme_fear_returns_string(self):
        assert isinstance(_fg_label_color("Extreme Fear"), str)

    def test_fear_returns_string(self):
        assert isinstance(_fg_label_color("Fear"), str)

    def test_neutral_returns_string(self):
        assert isinstance(_fg_label_color("Neutral"), str)

    def test_greed_returns_string(self):
        assert isinstance(_fg_label_color("Greed"), str)

    def test_extreme_greed_returns_string(self):
        assert isinstance(_fg_label_color("Extreme Greed"), str)

    def test_fear_and_greed_different_colors(self):
        assert _fg_label_color("Fear") != _fg_label_color("Greed")

    def test_extreme_fear_and_extreme_greed_different(self):
        assert _fg_label_color("Extreme Fear") != _fg_label_color("Extreme Greed")


# ===========================================================================
# 5. _fg_funding_score
# ===========================================================================

class TestFgFundingScore:
    def test_zero_rate_returns_50(self):
        assert _fg_funding_score(0.0) == pytest.approx(50.0, abs=0.1)

    def test_positive_rate_above_50(self):
        assert _fg_funding_score(0.01) > 50

    def test_negative_rate_below_50(self):
        assert _fg_funding_score(-0.01) < 50

    def test_large_positive_clamped_to_100(self):
        assert _fg_funding_score(1.0) == 100.0

    def test_large_negative_clamped_to_0(self):
        assert _fg_funding_score(-1.0) == 0.0

    def test_returns_float(self):
        assert isinstance(_fg_funding_score(0.005), float)

    def test_symmetric_around_50(self):
        pos = _fg_funding_score(0.01)
        neg = _fg_funding_score(-0.01)
        assert abs((pos - 50) + (neg - 50)) < 0.1


# ===========================================================================
# 6. _fg_oi_momentum_score
# ===========================================================================

class TestFgOiMomentumScore:
    def test_zero_change_returns_50(self):
        assert _fg_oi_momentum_score(0.0) == pytest.approx(50.0, abs=0.1)

    def test_positive_oi_change_above_50(self):
        assert _fg_oi_momentum_score(5.0) > 50

    def test_negative_oi_change_below_50(self):
        assert _fg_oi_momentum_score(-5.0) < 50

    def test_large_positive_clamped_to_100(self):
        assert _fg_oi_momentum_score(1000.0) == 100.0

    def test_large_negative_clamped_to_0(self):
        assert _fg_oi_momentum_score(-1000.0) == 0.0

    def test_returns_float(self):
        assert isinstance(_fg_oi_momentum_score(2.5), float)


# ===========================================================================
# 7. _fg_price_deviation_score
# ===========================================================================

class TestFgPriceDeviationScore:
    def test_zero_deviation_returns_50(self):
        assert _fg_price_deviation_score(0.0) == pytest.approx(50.0, abs=0.1)

    def test_positive_deviation_above_50(self):
        assert _fg_price_deviation_score(3.0) > 50

    def test_negative_deviation_below_50(self):
        assert _fg_price_deviation_score(-3.0) < 50

    def test_large_positive_clamped_to_100(self):
        assert _fg_price_deviation_score(100.0) == 100.0

    def test_large_negative_clamped_to_0(self):
        assert _fg_price_deviation_score(-100.0) == 0.0

    def test_returns_float(self):
        assert isinstance(_fg_price_deviation_score(1.5), float)


# ===========================================================================
# 8. _fg_volatility_score
# ===========================================================================

class TestFgVolatilityScore:
    def test_low_regime_returns_above_50(self):
        # low vol = complacency = mild greed
        score = _fg_volatility_score("low")
        assert score > 50

    def test_mid_regime_returns_near_50(self):
        score = _fg_volatility_score("mid")
        assert 35 <= score <= 65

    def test_high_regime_returns_below_50(self):
        score = _fg_volatility_score("high")
        assert score < 50

    def test_extreme_regime_returns_low(self):
        score = _fg_volatility_score("extreme")
        assert score < 35

    def test_unknown_regime_returns_50(self):
        score = _fg_volatility_score("unknown")
        assert score == pytest.approx(50.0, abs=1.0)

    def test_returns_float(self):
        assert isinstance(_fg_volatility_score("low"), float)


# ===========================================================================
# 9. _fg_taker_score
# ===========================================================================

class TestFgTakerScore:
    def test_buy_ratio_50pct_returns_50(self):
        assert _fg_taker_score(0.5) == pytest.approx(50.0, abs=0.1)

    def test_buy_ratio_100pct_returns_100(self):
        assert _fg_taker_score(1.0) == pytest.approx(100.0, abs=0.1)

    def test_buy_ratio_0pct_returns_0(self):
        assert _fg_taker_score(0.0) == pytest.approx(0.0, abs=0.1)

    def test_buy_ratio_above_1_clamped(self):
        assert _fg_taker_score(2.0) == 100.0

    def test_buy_ratio_below_0_clamped(self):
        assert _fg_taker_score(-0.5) == 0.0

    def test_returns_float(self):
        assert isinstance(_fg_taker_score(0.6), float)


# ===========================================================================
# 10. _fg_liquidation_score
# ===========================================================================

class TestFgLiquidationScore:
    def test_zero_liquidations_returns_high(self):
        # no liquidations = no fear from this signal
        score = _fg_liquidation_score(0)
        assert score >= 50

    def test_many_liquidations_returns_low(self):
        score = _fg_liquidation_score(50)
        assert score < 50

    def test_score_decreases_with_more_liquidations(self):
        s1 = _fg_liquidation_score(1)
        s5 = _fg_liquidation_score(5)
        s20 = _fg_liquidation_score(20)
        assert s1 >= s5 >= s20

    def test_returns_float(self):
        assert isinstance(_fg_liquidation_score(3), float)

    def test_result_in_0_100_range(self):
        for n in (0, 1, 5, 10, 50, 100):
            s = _fg_liquidation_score(n)
            assert 0 <= s <= 100


# ===========================================================================
# 11. _fg_composite
# ===========================================================================

class TestFgComposite:
    def test_equal_weights_returns_average(self):
        scores = [60.0, 40.0]
        weights = [0.5, 0.5]
        assert _fg_composite(scores, weights) == pytest.approx(50.0, abs=0.1)

    def test_dominant_weight_biases_result(self):
        scores = [80.0, 20.0]
        weights = [0.9, 0.1]
        result = _fg_composite(scores, weights)
        assert result > 70

    def test_all_100_returns_100(self):
        scores = [100.0, 100.0, 100.0]
        weights = [0.5, 0.3, 0.2]
        assert _fg_composite(scores, weights) == pytest.approx(100.0, abs=0.1)

    def test_all_0_returns_0(self):
        scores = [0.0, 0.0, 0.0]
        weights = [0.5, 0.3, 0.2]
        assert _fg_composite(scores, weights) == pytest.approx(0.0, abs=0.1)

    def test_result_in_0_100_range(self):
        scores = [45.0, 55.0, 62.0, 38.0]
        weights = [0.25, 0.25, 0.25, 0.25]
        result = _fg_composite(scores, weights)
        assert 0 <= result <= 100

    def test_returns_float(self):
        result = _fg_composite([50.0], [1.0])
        assert isinstance(result, float)


# ===========================================================================
# 12. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_symbol(self):
        assert "symbol" in SAMPLE_RESPONSE

    def test_has_score(self):
        assert "score" in SAMPLE_RESPONSE
        assert 0 <= SAMPLE_RESPONSE["score"] <= 100

    def test_has_label(self):
        assert "label" in SAMPLE_RESPONSE
        assert SAMPLE_RESPONSE["label"] in (
            "Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"
        )

    def test_has_delta(self):
        assert "delta" in SAMPLE_RESPONSE

    def test_has_trend(self):
        assert SAMPLE_RESPONSE["trend"] in ("rising", "falling", "stable")

    def test_has_signals_dict(self):
        assert isinstance(SAMPLE_RESPONSE["signals"], dict)

    def test_signals_has_all_six_components(self):
        signals = SAMPLE_RESPONSE["signals"]
        for key in ("funding", "oi_momentum", "price_deviation",
                    "volatility", "taker_pressure", "liquidation"):
            assert key in signals, f"Missing signal: {key}"

    def test_each_signal_has_score_weight_raw_label(self):
        for name, sig in SAMPLE_RESPONSE["signals"].items():
            for key in ("score", "weight", "raw", "label"):
                assert key in sig, f"Signal '{name}' missing key '{key}'"

    def test_signal_scores_in_0_100(self):
        for name, sig in SAMPLE_RESPONSE["signals"].items():
            assert 0 <= sig["score"] <= 100, f"Signal '{name}' score out of range"

    def test_signal_weights_sum_to_one(self):
        total_weight = sum(
            sig["weight"] for sig in SAMPLE_RESPONSE["signals"].values()
        )
        assert total_weight == pytest.approx(1.0, abs=0.01)

    def test_has_history_list(self):
        assert isinstance(SAMPLE_RESPONSE["history"], list)

    def test_history_items_have_ts_score_label(self):
        for item in SAMPLE_RESPONSE["history"]:
            assert "ts" in item
            assert "score" in item
            assert "label" in item

    def test_has_description(self):
        assert "description" in SAMPLE_RESPONSE
        assert isinstance(SAMPLE_RESPONSE["description"], str)

    def test_label_prev_present(self):
        assert "label_prev" in SAMPLE_RESPONSE


# ===========================================================================
# 13. Structural tests
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api_path = os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")
        content = open(api_path).read()
        assert "/fear-greed" in content, "Route /fear-greed not found in api.py"

    def test_html_card_exists(self):
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        content = open(html_path).read()
        assert "card-fear-greed" in content, "card-fear-greed not found in index.html"

    def test_js_render_function_exists(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "renderFearGreed" in content, "renderFearGreed not found in app.js"

    def test_js_api_call_to_endpoint(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "/fear-greed" in content, "/fear-greed API call not found in app.js"
