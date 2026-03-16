"""
Unit / smoke tests for /api/macro-liquidity-indicator.

Macro Liquidity Indicator — global M2 growth proxy, Fed balance sheet delta,
USD index trend vs BTC price divergence, risk-on/risk-off regime score
with 90d MA comparison.

Helpers covered:
  - _ml_m2_growth_rate
  - _ml_fed_balance_delta
  - _ml_usd_btc_divergence
  - _ml_regime_score
  - _ml_regime_label
  - _ml_moving_average
  - _ml_liquidity_trend
  - _ml_zscore
  - SAMPLE_RESPONSE shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys, os, pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _ml_m2_growth_rate,
    _ml_fed_balance_delta,
    _ml_usd_btc_divergence,
    _ml_regime_score,
    _ml_regime_label,
    _ml_moving_average,
    _ml_liquidity_trend,
    _ml_zscore,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "m2": {
        "current_proxy_usd":    21_500_000_000_000,
        "growth_rate_yoy_pct":  3.2,
        "growth_rate_mom_pct":  0.4,
        "trend":                "expanding",
        "history_90d": [
            {"date": "2024-08-22", "proxy_usd": 20_800_000_000_000, "growth_yoy_pct": 2.8},
            {"date": "2024-09-20", "proxy_usd": 21_100_000_000_000, "growth_yoy_pct": 3.0},
            {"date": "2024-10-18", "proxy_usd": 21_300_000_000_000, "growth_yoy_pct": 3.1},
            {"date": "2024-11-20", "proxy_usd": 21_500_000_000_000, "growth_yoy_pct": 3.2},
        ],
    },
    "fed_balance_sheet": {
        "current_usd":    7_800_000_000_000,
        "delta_30d_usd":   -50_000_000_000,
        "delta_pct":              -0.64,
        "trend":              "contracting",
    },
    "usd_index": {
        "current":           104.2,
        "change_30d_pct":     -1.2,
        "change_90d_pct":     -2.5,
        "trend":          "weakening",
        "btc_divergence":     15.3,
    },
    "regime": {
        "score":     62.5,
        "label":     "risk_on",
        "ma_90d":    58.2,
        "trend":     "expanding",
        "zscore":     0.8,
    },
    "history_90d": [
        {"date": "2024-08-22", "score": 52.0, "label": "neutral"},
        {"date": "2024-09-20", "score": 55.0, "label": "neutral"},
        {"date": "2024-10-18", "score": 59.0, "label": "neutral"},
        {"date": "2024-11-20", "score": 62.5, "label": "risk_on"},
    ],
    "description": "Risk-on: macro liquidity expanding — M2 +3.2% YoY, USD weakening",
}


# ===========================================================================
# 1. _ml_m2_growth_rate
# ===========================================================================

class TestMlM2GrowthRate:
    def test_positive_growth(self):
        assert _ml_m2_growth_rate(21_000_000, 20_000_000) > 0

    def test_negative_growth(self):
        assert _ml_m2_growth_rate(19_000_000, 20_000_000) < 0

    def test_no_change_is_zero(self):
        assert _ml_m2_growth_rate(20_000_000, 20_000_000) == pytest.approx(0.0)

    def test_zero_previous_returns_zero(self):
        assert _ml_m2_growth_rate(21_000_000, 0) == pytest.approx(0.0)

    def test_correct_magnitude(self):
        assert _ml_m2_growth_rate(21_000_000, 20_000_000) == pytest.approx(5.0, abs=0.01)

    def test_returns_float(self):
        assert isinstance(_ml_m2_growth_rate(21_000_000, 20_000_000), float)


# ===========================================================================
# 2. _ml_fed_balance_delta
# ===========================================================================

class TestMlFedBalanceDelta:
    def test_expansion_positive(self):
        assert _ml_fed_balance_delta(8_000_000_000_000, 7_800_000_000_000) > 0

    def test_contraction_negative(self):
        assert _ml_fed_balance_delta(7_800_000_000_000, 8_000_000_000_000) < 0

    def test_no_change_is_zero(self):
        assert _ml_fed_balance_delta(8_000_000_000_000, 8_000_000_000_000) == pytest.approx(0.0)

    def test_correct_magnitude(self):
        assert _ml_fed_balance_delta(8_100_000, 8_000_000) == pytest.approx(100_000.0)

    def test_returns_float(self):
        assert isinstance(_ml_fed_balance_delta(8_000_000, 7_900_000), float)


# ===========================================================================
# 3. _ml_usd_btc_divergence
# ===========================================================================

class TestMlUsdBtcDivergence:
    def test_btc_rising_usd_falling_large_positive(self):
        # BTC +10%, USD -5% → divergence = 10 - (-5) = 15
        result = _ml_usd_btc_divergence(usd_change_pct=-5.0, btc_change_pct=10.0)
        assert result > 0

    def test_btc_falling_usd_rising_negative(self):
        result = _ml_usd_btc_divergence(usd_change_pct=5.0, btc_change_pct=-10.0)
        assert result < 0

    def test_equal_moves_near_zero(self):
        assert _ml_usd_btc_divergence(5.0, 5.0) == pytest.approx(0.0)

    def test_correct_magnitude(self):
        # BTC +12%, USD -3% → divergence = 12 - (-3) = 15
        assert _ml_usd_btc_divergence(usd_change_pct=-3.0, btc_change_pct=12.0) == pytest.approx(15.0, abs=0.01)

    def test_returns_float(self):
        assert isinstance(_ml_usd_btc_divergence(-2.0, 8.0), float)


# ===========================================================================
# 4. _ml_regime_score
# ===========================================================================

class TestMlRegimeScore:
    def test_all_risk_on_above_50(self):
        # M2 growing, Fed expanding, USD weak, BTC rising
        assert _ml_regime_score(m2_growth=5.0, fed_delta_pct=2.0,
                                usd_change_pct=-3.0, btc_change_pct=15.0) > 50.0

    def test_all_risk_off_below_50(self):
        # M2 shrinking, Fed contracting, USD strong, BTC falling
        assert _ml_regime_score(m2_growth=-3.0, fed_delta_pct=-2.0,
                                usd_change_pct=3.0, btc_change_pct=-15.0) < 50.0

    def test_neutral_near_50(self):
        score = _ml_regime_score(m2_growth=0.0, fed_delta_pct=0.0,
                                 usd_change_pct=0.0, btc_change_pct=0.0)
        assert score == pytest.approx(50.0, abs=1.0)

    def test_result_in_0_100_range(self):
        for args in [
            {"m2_growth": 20.0,  "fed_delta_pct": 10.0, "usd_change_pct": -10.0, "btc_change_pct": 50.0},
            {"m2_growth": -20.0, "fed_delta_pct": -10.0, "usd_change_pct": 10.0, "btc_change_pct": -50.0},
        ]:
            r = _ml_regime_score(**args)
            assert 0.0 <= r <= 100.0

    def test_stronger_risk_on_higher_score(self):
        mild = _ml_regime_score(m2_growth=1.0, fed_delta_pct=0.5,
                                usd_change_pct=-1.0, btc_change_pct=5.0)
        strong = _ml_regime_score(m2_growth=5.0, fed_delta_pct=3.0,
                                  usd_change_pct=-5.0, btc_change_pct=20.0)
        assert strong > mild

    def test_returns_float(self):
        assert isinstance(_ml_regime_score(2.0, 1.0, -1.0, 8.0), float)


# ===========================================================================
# 5. _ml_regime_label
# ===========================================================================

class TestMlRegimeLabel:
    def test_high_score_is_risk_on(self):
        assert _ml_regime_label(70.0) == "risk_on"

    def test_mid_score_is_neutral(self):
        assert _ml_regime_label(50.0) == "neutral"

    def test_low_score_is_risk_off(self):
        assert _ml_regime_label(25.0) == "risk_off"

    def test_boundary_60_is_risk_on(self):
        assert _ml_regime_label(60.0) == "risk_on"

    def test_returns_valid_string(self):
        assert _ml_regime_label(45.0) in ("risk_on", "neutral", "risk_off")


# ===========================================================================
# 6. _ml_moving_average
# ===========================================================================

class TestMlMovingAverage:
    def test_empty_returns_zero(self):
        assert _ml_moving_average([], 10) == pytest.approx(0.0)

    def test_single_value_returns_value(self):
        assert _ml_moving_average([42.0], 10) == pytest.approx(42.0)

    def test_correct_average(self):
        assert _ml_moving_average([10.0, 20.0, 30.0], 3) == pytest.approx(20.0)

    def test_uses_last_window_values(self):
        # Window=2, last 2 values are 40 and 50 → avg 45
        assert _ml_moving_average([10.0, 20.0, 30.0, 40.0, 50.0], 2) == pytest.approx(45.0)

    def test_window_larger_than_series(self):
        # Falls back to average of all
        assert _ml_moving_average([10.0, 20.0], 10) == pytest.approx(15.0)

    def test_returns_float(self):
        assert isinstance(_ml_moving_average([50.0, 60.0, 55.0], 3), float)


# ===========================================================================
# 7. _ml_liquidity_trend
# ===========================================================================

class TestMlLiquidityTrend:
    def test_current_well_above_ma_is_expanding(self):
        assert _ml_liquidity_trend(70.0, 55.0) == "expanding"

    def test_current_well_below_ma_is_contracting(self):
        assert _ml_liquidity_trend(40.0, 55.0) == "contracting"

    def test_current_near_ma_is_stable(self):
        assert _ml_liquidity_trend(55.5, 55.0) == "stable"

    def test_returns_valid_string(self):
        assert _ml_liquidity_trend(60.0, 58.0) in ("expanding", "contracting", "stable")

    def test_equal_is_stable(self):
        assert _ml_liquidity_trend(55.0, 55.0) == "stable"


# ===========================================================================
# 8. _ml_zscore
# ===========================================================================

class TestMlZscore:
    def test_empty_history_returns_zero(self):
        assert _ml_zscore(60.0, []) == pytest.approx(0.0)

    def test_single_history_returns_zero(self):
        assert _ml_zscore(60.0, [60.0]) == pytest.approx(0.0)

    def test_current_at_mean_returns_near_zero(self):
        history = [50.0, 55.0, 60.0, 55.0, 50.0]
        mean = sum(history) / len(history)
        assert abs(_ml_zscore(mean, history)) < 0.01

    def test_above_mean_returns_positive(self):
        history = [50.0, 52.0, 54.0, 56.0]
        assert _ml_zscore(100.0, history) > 0

    def test_below_mean_returns_negative(self):
        history = [60.0, 65.0, 62.0, 68.0]
        assert _ml_zscore(0.0, history) < 0

    def test_uniform_history_returns_zero(self):
        assert _ml_zscore(55.0, [55.0] * 10) == pytest.approx(0.0, abs=0.01)


# ===========================================================================
# 9. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_m2_dict(self):
        assert isinstance(SAMPLE_RESPONSE["m2"], dict)

    def test_m2_has_required_keys(self):
        for k in ("current_proxy_usd", "growth_rate_yoy_pct",
                  "growth_rate_mom_pct", "trend", "history_90d"):
            assert k in SAMPLE_RESPONSE["m2"], f"m2 missing '{k}'"

    def test_m2_trend_valid(self):
        assert SAMPLE_RESPONSE["m2"]["trend"] in ("expanding", "contracting", "stable")

    def test_m2_history_has_required_keys(self):
        for item in SAMPLE_RESPONSE["m2"]["history_90d"]:
            for k in ("date", "proxy_usd", "growth_yoy_pct"):
                assert k in item, f"m2 history item missing '{k}'"

    def test_has_fed_balance_sheet_dict(self):
        assert isinstance(SAMPLE_RESPONSE["fed_balance_sheet"], dict)

    def test_fed_has_required_keys(self):
        for k in ("current_usd", "delta_30d_usd", "delta_pct", "trend"):
            assert k in SAMPLE_RESPONSE["fed_balance_sheet"], f"fed missing '{k}'"

    def test_fed_trend_valid(self):
        assert SAMPLE_RESPONSE["fed_balance_sheet"]["trend"] in (
            "expanding", "contracting", "stable"
        )

    def test_has_usd_index_dict(self):
        assert isinstance(SAMPLE_RESPONSE["usd_index"], dict)

    def test_usd_index_has_required_keys(self):
        for k in ("current", "change_30d_pct", "change_90d_pct", "trend", "btc_divergence"):
            assert k in SAMPLE_RESPONSE["usd_index"], f"usd_index missing '{k}'"

    def test_usd_trend_valid(self):
        assert SAMPLE_RESPONSE["usd_index"]["trend"] in (
            "strengthening", "weakening", "stable"
        )

    def test_has_regime_dict(self):
        assert isinstance(SAMPLE_RESPONSE["regime"], dict)

    def test_regime_has_required_keys(self):
        for k in ("score", "label", "ma_90d", "trend", "zscore"):
            assert k in SAMPLE_RESPONSE["regime"], f"regime missing '{k}'"

    def test_regime_label_valid(self):
        assert SAMPLE_RESPONSE["regime"]["label"] in ("risk_on", "neutral", "risk_off")

    def test_regime_score_in_range(self):
        assert 0.0 <= SAMPLE_RESPONSE["regime"]["score"] <= 100.0

    def test_regime_trend_valid(self):
        assert SAMPLE_RESPONSE["regime"]["trend"] in ("expanding", "contracting", "stable")

    def test_has_history_90d_list(self):
        assert isinstance(SAMPLE_RESPONSE["history_90d"], list)

    def test_history_items_have_required_keys(self):
        for item in SAMPLE_RESPONSE["history_90d"]:
            for k in ("date", "score", "label"):
                assert k in item, f"history_90d item missing '{k}'"

    def test_history_labels_valid(self):
        for item in SAMPLE_RESPONSE["history_90d"]:
            assert item["label"] in ("risk_on", "neutral", "risk_off"), \
                f"invalid label '{item['label']}'"

    def test_has_description_string(self):
        assert isinstance(SAMPLE_RESPONSE["description"], str)


# ===========================================================================
# 10. Structural
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api = open(os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")).read()
        assert "/macro-liquidity-indicator" in api, "/macro-liquidity-indicator route missing"

    def test_html_card_exists(self):
        html = open(os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")).read()
        assert "card-macro-liquidity" in html, "card-macro-liquidity missing"

    def test_js_render_function_exists(self):
        js = open(os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")).read()
        assert "renderMacroLiquidity" in js, "renderMacroLiquidity missing"

    def test_js_api_call_to_endpoint(self):
        js = open(os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")).read()
        assert "/macro-liquidity-indicator" in js, "/macro-liquidity-indicator call missing"
