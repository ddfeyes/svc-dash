"""
Unit / smoke tests for /api/leverage-ratio-heatmap.

Leverage Ratio Heatmap — estimated leverage ratio across BTC/ETH/SOL/BNB
perps (OI / market cap), historical percentile rank, deleveraging risk
signal when leverage > 80th percentile.

Helpers covered:
  - _lv_leverage_ratio
  - _lv_percentile_rank
  - _lv_deleverage_risk
  - _lv_risk_score
  - _lv_zscore
  - _lv_trend
  - _lv_heatmap_color
  - _lv_sector_avg
  - SAMPLE_RESPONSE shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys, os, pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _lv_leverage_ratio,
    _lv_percentile_rank,
    _lv_deleverage_risk,
    _lv_risk_score,
    _lv_zscore,
    _lv_trend,
    _lv_heatmap_color,
    _lv_sector_avg,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "assets": {
        "BTC": {
            "oi_usd":           18_500_000_000,
            "market_cap_usd":  1_200_000_000_000,
            "leverage_ratio":             1.54,
            "percentile_rank":           72.0,
            "risk_signal":           "elevated",
            "risk_score":                68.0,
            "zscore":                     0.8,
            "trend":                  "rising",
            "heatmap_color":          "orange",
            "history_30d": [
                {"date": "2024-10-21", "leverage_ratio": 1.35, "percentile": 62.0},
                {"date": "2024-10-28", "leverage_ratio": 1.40, "percentile": 65.0},
                {"date": "2024-11-04", "leverage_ratio": 1.44, "percentile": 67.0},
                {"date": "2024-11-11", "leverage_ratio": 1.49, "percentile": 70.0},
                {"date": "2024-11-20", "leverage_ratio": 1.54, "percentile": 72.0},
            ],
        },
        "ETH": {
            "oi_usd":            9_800_000_000,
            "market_cap_usd":    380_000_000_000,
            "leverage_ratio":              2.58,
            "percentile_rank":            65.0,
            "risk_signal":           "elevated",
            "risk_score":                 60.5,
            "zscore":                      0.5,
            "trend":                   "stable",
            "heatmap_color":           "orange",
            "history_30d": [
                {"date": "2024-10-21", "leverage_ratio": 2.45, "percentile": 58.0},
                {"date": "2024-11-20", "leverage_ratio": 2.58, "percentile": 65.0},
            ],
        },
        "SOL": {
            "oi_usd":            4_200_000_000,
            "market_cap_usd":     75_000_000_000,
            "leverage_ratio":              5.60,
            "percentile_rank":            88.0,
            "risk_signal":              "high",
            "risk_score":                 86.0,
            "zscore":                      1.6,
            "trend":                  "rising",
            "heatmap_color":             "red",
            "history_30d": [
                {"date": "2024-10-21", "leverage_ratio": 4.80, "percentile": 78.0},
                {"date": "2024-11-20", "leverage_ratio": 5.60, "percentile": 88.0},
            ],
        },
        "BNB": {
            "oi_usd":            1_800_000_000,
            "market_cap_usd":     85_000_000_000,
            "leverage_ratio":              2.12,
            "percentile_rank":            45.0,
            "risk_signal":            "normal",
            "risk_score":                 38.0,
            "zscore":                     -0.2,
            "trend":                  "stable",
            "heatmap_color":          "yellow",
            "history_30d": [
                {"date": "2024-10-21", "leverage_ratio": 2.18, "percentile": 48.0},
                {"date": "2024-11-20", "leverage_ratio": 2.12, "percentile": 45.0},
            ],
        },
    },
    "sector": {
        "avg_leverage_ratio":     2.96,
        "avg_percentile":        67.5,
        "max_risk_asset":        "SOL",
        "deleverage_risk_count":    1,
        "sector_risk_score":     63.1,
    },
    "history_30d": [
        {"date": "2024-10-21", "avg_leverage_ratio": 2.69, "avg_percentile": 61.5},
        {"date": "2024-11-20", "avg_leverage_ratio": 2.96, "avg_percentile": 67.5},
    ],
    "description": "Leverage elevated: SOL at 88th pct — 1 asset in deleveraging risk zone",
}


# ===========================================================================
# 1. _lv_leverage_ratio
# ===========================================================================

class TestLvLeverageRatio:
    def test_zero_market_cap_returns_zero(self):
        assert _lv_leverage_ratio(1_000_000_000, 0) == pytest.approx(0.0)

    def test_correct_magnitude_btc(self):
        # $18.5B OI / $1.2T mcap = 1.5417%
        result = _lv_leverage_ratio(18_500_000_000, 1_200_000_000_000)
        assert result == pytest.approx(1.5417, abs=0.01)

    def test_higher_oi_higher_ratio(self):
        low  = _lv_leverage_ratio(10_000_000_000, 1_000_000_000_000)
        high = _lv_leverage_ratio(50_000_000_000, 1_000_000_000_000)
        assert high > low

    def test_higher_mcap_lower_ratio(self):
        small_mcap = _lv_leverage_ratio(10_000_000_000, 100_000_000_000)
        large_mcap = _lv_leverage_ratio(10_000_000_000, 500_000_000_000)
        assert large_mcap < small_mcap

    def test_zero_oi_returns_zero(self):
        assert _lv_leverage_ratio(0, 1_000_000_000_000) == pytest.approx(0.0)

    def test_returns_float(self):
        assert isinstance(_lv_leverage_ratio(5_000_000_000, 100_000_000_000), float)


# ===========================================================================
# 2. _lv_percentile_rank
# ===========================================================================

class TestLvPercentileRank:
    def test_empty_history_returns_50(self):
        assert _lv_percentile_rank(2.5, []) == pytest.approx(50.0)

    def test_value_above_all_returns_100(self):
        assert _lv_percentile_rank(10.0, [1.0, 2.0, 3.0, 4.0, 5.0]) == pytest.approx(100.0)

    def test_value_below_all_returns_near_zero(self):
        assert _lv_percentile_rank(0.0, [1.0, 2.0, 3.0, 4.0, 5.0]) == pytest.approx(0.0)

    def test_middle_value_near_50(self):
        # 2 of 4 values are <= 2.0 → 50th pct
        assert _lv_percentile_rank(2.0, [1.0, 2.0, 3.0, 4.0]) == pytest.approx(50.0)

    def test_result_in_0_100_range(self):
        history = [1.0, 2.0, 3.0, 4.0, 5.0]
        for v in [0.5, 2.5, 5.5]:
            r = _lv_percentile_rank(v, history)
            assert 0.0 <= r <= 100.0

    def test_returns_float(self):
        assert isinstance(_lv_percentile_rank(2.5, [1.0, 2.0, 3.0]), float)


# ===========================================================================
# 3. _lv_deleverage_risk
# ===========================================================================

class TestLvDeleverageRisk:
    def test_very_high_percentile_is_high(self):
        assert _lv_deleverage_risk(85.0) == "high"

    def test_elevated_percentile(self):
        assert _lv_deleverage_risk(70.0) == "elevated"

    def test_normal_percentile(self):
        assert _lv_deleverage_risk(50.0) == "normal"

    def test_low_percentile(self):
        assert _lv_deleverage_risk(25.0) == "low"

    def test_boundary_80_is_high(self):
        assert _lv_deleverage_risk(80.0) == "high"


# ===========================================================================
# 4. _lv_risk_score
# ===========================================================================

class TestLvRiskScore:
    def test_high_leverage_and_percentile_high_score(self):
        assert _lv_risk_score(leverage_ratio=5.0, percentile=85.0) >= 70.0

    def test_low_leverage_and_percentile_low_score(self):
        assert _lv_risk_score(leverage_ratio=0.5, percentile=20.0) <= 30.0

    def test_result_in_0_100_range(self):
        for lev, pct in [(0.1, 5.0), (3.0, 50.0), (10.0, 99.0)]:
            r = _lv_risk_score(lev, pct)
            assert 0.0 <= r <= 100.0

    def test_higher_leverage_higher_score(self):
        low  = _lv_risk_score(leverage_ratio=1.0, percentile=50.0)
        high = _lv_risk_score(leverage_ratio=5.0, percentile=50.0)
        assert high > low

    def test_higher_percentile_higher_score(self):
        low  = _lv_risk_score(leverage_ratio=2.0, percentile=30.0)
        high = _lv_risk_score(leverage_ratio=2.0, percentile=80.0)
        assert high > low

    def test_returns_float(self):
        assert isinstance(_lv_risk_score(2.0, 60.0), float)


# ===========================================================================
# 5. _lv_zscore
# ===========================================================================

class TestLvZscore:
    def test_empty_history_returns_zero(self):
        assert _lv_zscore(2.5, []) == pytest.approx(0.0)

    def test_single_history_returns_zero(self):
        assert _lv_zscore(2.5, [2.5]) == pytest.approx(0.0)

    def test_current_at_mean_near_zero(self):
        history = [1.5, 2.0, 2.5, 2.0, 1.5]
        mean = sum(history) / len(history)
        assert abs(_lv_zscore(mean, history)) < 0.01

    def test_above_mean_positive(self):
        history = [1.0, 1.5, 2.0, 1.5]
        assert _lv_zscore(10.0, history) > 0

    def test_below_mean_negative(self):
        history = [4.0, 5.0, 4.5, 5.5]
        assert _lv_zscore(0.0, history) < 0

    def test_uniform_history_returns_zero(self):
        assert _lv_zscore(2.0, [2.0] * 10) == pytest.approx(0.0, abs=0.01)


# ===========================================================================
# 6. _lv_trend
# ===========================================================================

class TestLvTrend:
    def test_empty_returns_stable(self):
        assert _lv_trend([]) == "stable"

    def test_single_returns_stable(self):
        assert _lv_trend([2.0]) == "stable"

    def test_rising_values(self):
        assert _lv_trend([1.0, 1.5, 2.0, 2.5, 3.0]) == "rising"

    def test_falling_values(self):
        assert _lv_trend([3.0, 2.5, 2.0, 1.5, 1.0]) == "falling"

    def test_flat_values_is_stable(self):
        assert _lv_trend([2.0] * 7) == "stable"

    def test_returns_valid_string(self):
        assert _lv_trend([1.0, 2.0, 3.0]) in ("rising", "falling", "stable")


# ===========================================================================
# 7. _lv_heatmap_color
# ===========================================================================

class TestLvHeatmapColor:
    def test_very_high_percentile_is_red(self):
        assert _lv_heatmap_color(85.0) == "red"

    def test_high_percentile_is_orange(self):
        assert _lv_heatmap_color(70.0) == "orange"

    def test_mid_percentile_is_yellow(self):
        assert _lv_heatmap_color(50.0) == "yellow"

    def test_low_percentile_is_green(self):
        assert _lv_heatmap_color(25.0) == "green"

    def test_returns_valid_string(self):
        assert _lv_heatmap_color(60.0) in ("red", "orange", "yellow", "green")


# ===========================================================================
# 8. _lv_sector_avg
# ===========================================================================

class TestLvSectorAvg:
    def test_empty_returns_zero(self):
        assert _lv_sector_avg({}) == pytest.approx(0.0)

    def test_single_value(self):
        assert _lv_sector_avg({"BTC": 1.54}) == pytest.approx(1.54)

    def test_correct_average(self):
        assert _lv_sector_avg({"A": 2.0, "B": 4.0}) == pytest.approx(3.0)

    def test_all_equal(self):
        d = {"A": 2.5, "B": 2.5, "C": 2.5}
        assert _lv_sector_avg(d) == pytest.approx(2.5)

    def test_returns_float(self):
        assert isinstance(_lv_sector_avg({"BTC": 1.54, "ETH": 2.58}), float)

    def test_four_assets_correct_mean(self):
        d = {"BTC": 1.54, "ETH": 2.58, "SOL": 5.60, "BNB": 2.12}
        expected = (1.54 + 2.58 + 5.60 + 2.12) / 4
        assert _lv_sector_avg(d) == pytest.approx(expected, abs=0.01)


# ===========================================================================
# 9. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    ASSETS = ("BTC", "ETH", "SOL", "BNB")
    ASSET_KEYS = (
        "oi_usd", "market_cap_usd", "leverage_ratio", "percentile_rank",
        "risk_signal", "risk_score", "zscore", "trend", "heatmap_color",
        "history_30d",
    )

    def test_has_assets_dict(self):
        assert isinstance(SAMPLE_RESPONSE["assets"], dict)

    def test_has_all_four_assets(self):
        for a in self.ASSETS:
            assert a in SAMPLE_RESPONSE["assets"], f"Missing asset: {a}"

    def test_each_asset_has_required_keys(self):
        for name, asset in SAMPLE_RESPONSE["assets"].items():
            for k in self.ASSET_KEYS:
                assert k in asset, f"{name} missing key '{k}'"

    def test_risk_signals_valid(self):
        for name, asset in SAMPLE_RESPONSE["assets"].items():
            assert asset["risk_signal"] in ("high", "elevated", "normal", "low"), \
                f"{name} invalid risk_signal '{asset['risk_signal']}'"

    def test_heatmap_colors_valid(self):
        for name, asset in SAMPLE_RESPONSE["assets"].items():
            assert asset["heatmap_color"] in ("red", "orange", "yellow", "green"), \
                f"{name} invalid heatmap_color '{asset['heatmap_color']}'"

    def test_trends_valid(self):
        for name, asset in SAMPLE_RESPONSE["assets"].items():
            assert asset["trend"] in ("rising", "falling", "stable"), \
                f"{name} invalid trend '{asset['trend']}'"

    def test_percentile_ranks_in_range(self):
        for name, asset in SAMPLE_RESPONSE["assets"].items():
            assert 0.0 <= asset["percentile_rank"] <= 100.0, \
                f"{name} percentile_rank out of range"

    def test_history_30d_is_list(self):
        for name, asset in SAMPLE_RESPONSE["assets"].items():
            assert isinstance(asset["history_30d"], list), \
                f"{name} history_30d not a list"

    def test_history_items_have_required_keys(self):
        for name, asset in SAMPLE_RESPONSE["assets"].items():
            for item in asset["history_30d"]:
                for k in ("date", "leverage_ratio", "percentile"):
                    assert k in item, f"{name} history item missing '{k}'"

    def test_has_sector_dict(self):
        assert isinstance(SAMPLE_RESPONSE["sector"], dict)

    def test_sector_has_required_keys(self):
        for k in ("avg_leverage_ratio", "avg_percentile", "max_risk_asset",
                  "deleverage_risk_count", "sector_risk_score"):
            assert k in SAMPLE_RESPONSE["sector"], f"sector missing '{k}'"

    def test_max_risk_asset_is_valid(self):
        assert SAMPLE_RESPONSE["sector"]["max_risk_asset"] in self.ASSETS

    def test_has_history_30d_list(self):
        assert isinstance(SAMPLE_RESPONSE["history_30d"], list)

    def test_history_30d_items_have_keys(self):
        for item in SAMPLE_RESPONSE["history_30d"]:
            for k in ("date", "avg_leverage_ratio", "avg_percentile"):
                assert k in item, f"history_30d item missing '{k}'"

    def test_has_description_string(self):
        assert isinstance(SAMPLE_RESPONSE["description"], str)


# ===========================================================================
# 10. Structural
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api = open(os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")).read()
        assert "/leverage-ratio-heatmap" in api, "/leverage-ratio-heatmap route missing"

    def test_html_card_exists(self):
        html = open(os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")).read()
        assert "card-leverage-ratio-heatmap" in html, "card-leverage-ratio-heatmap missing"

    def test_js_render_function_exists(self):
        js = open(os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")).read()
        assert "renderLeverageRatioHeatmap" in js, "renderLeverageRatioHeatmap missing"

    def test_js_api_call_to_endpoint(self):
        js = open(os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")).read()
        assert "/leverage-ratio-heatmap" in js, "/leverage-ratio-heatmap call missing"
