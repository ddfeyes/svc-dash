"""
Unit / smoke tests for /api/staking-yield-tracker.

Staking Yield Tracker — ETH/SOL/ADA/DOT/AVAX staking APY trends (30d),
validator count growth, yield vs inflation comparison, stake ratio (%
of supply staked), protocol-level stake concentration risk score.

Helpers covered:
  - _sy_real_yield
  - _sy_stake_ratio
  - _sy_concentration_risk
  - _sy_apy_trend
  - _sy_yield_label
  - _sy_validator_growth
  - _sy_risk_label
  - _sy_apy_zscore
  - SAMPLE_RESPONSE shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys, os, pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _sy_real_yield,
    _sy_stake_ratio,
    _sy_concentration_risk,
    _sy_apy_trend,
    _sy_yield_label,
    _sy_validator_growth,
    _sy_risk_label,
    _sy_apy_zscore,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "protocols": {
        "ETH": {
            "apy": 3.85,
            "apy_change_30d": -0.15,
            "inflation_rate": 0.6,
            "real_yield": 3.25,
            "yield_label": "attractive",
            "stake_ratio": 27.4,
            "validators": 980_000,
            "validator_growth_30d_pct": 2.1,
            "concentration_risk": 42.0,
            "risk_label": "medium",
            "history_30d": [
                {"date": "2024-10-21", "apy": 3.95, "real_yield": 3.35},
                {"date": "2024-11-20", "apy": 3.85, "real_yield": 3.25},
            ],
        },
        "SOL": {
            "apy": 7.20,
            "apy_change_30d": 0.30,
            "inflation_rate": 5.0,
            "real_yield": 2.20,
            "yield_label": "neutral",
            "stake_ratio": 65.0,
            "validators": 1_700,
            "validator_growth_30d_pct": 1.2,
            "concentration_risk": 68.0,
            "risk_label": "high",
            "history_30d": [
                {"date": "2024-10-21", "apy": 6.90, "real_yield": 1.90},
                {"date": "2024-11-20", "apy": 7.20, "real_yield": 2.20},
            ],
        },
        "ADA": {
            "apy": 3.30,
            "apy_change_30d": -0.10,
            "inflation_rate": 0.0,
            "real_yield": 3.30,
            "yield_label": "attractive",
            "stake_ratio": 62.0,
            "validators": 3_200,
            "validator_growth_30d_pct": 0.5,
            "concentration_risk": 22.0,
            "risk_label": "low",
            "history_30d": [
                {"date": "2024-10-21", "apy": 3.35, "real_yield": 3.35},
                {"date": "2024-11-20", "apy": 3.30, "real_yield": 3.30},
            ],
        },
        "DOT": {
            "apy": 12.0,
            "apy_change_30d": -1.0,
            "inflation_rate": 8.0,
            "real_yield": 4.0,
            "yield_label": "attractive",
            "stake_ratio": 52.0,
            "validators": 297,
            "validator_growth_30d_pct": 0.0,
            "concentration_risk": 55.0,
            "risk_label": "medium",
            "history_30d": [
                {"date": "2024-10-21", "apy": 13.0, "real_yield": 5.0},
                {"date": "2024-11-20", "apy": 12.0, "real_yield": 4.0},
            ],
        },
        "AVAX": {
            "apy": 8.50,
            "apy_change_30d": 0.20,
            "inflation_rate": 3.5,
            "real_yield": 5.0,
            "yield_label": "attractive",
            "stake_ratio": 58.0,
            "validators": 1_400,
            "validator_growth_30d_pct": 1.8,
            "concentration_risk": 35.0,
            "risk_label": "medium",
            "history_30d": [
                {"date": "2024-10-21", "apy": 8.30, "real_yield": 4.80},
                {"date": "2024-11-20", "apy": 8.50, "real_yield": 5.00},
            ],
        },
    },
    "aggregate": {
        "avg_apy": 6.97,
        "avg_real_yield": 3.55,
        "best_yield_protocol": "DOT",
        "lowest_risk_protocol": "ADA",
        "total_value_staked_usd": 95_000_000_000,
    },
    "history_30d": [
        {"date": "2024-10-21", "avg_apy": 7.10, "avg_real_yield": 3.48},
        {"date": "2024-11-20", "avg_apy": 6.97, "avg_real_yield": 3.55},
    ],
    "description": "Staking yields: DOT leads at 4.0% real yield — ETH stake ratio 27.4%",
}


# ===========================================================================
# 1. _sy_real_yield
# ===========================================================================

class TestSyRealYield:
    def test_positive_when_apy_exceeds_inflation(self):
        assert _sy_real_yield(5.0, 2.0) > 0

    def test_negative_when_inflation_exceeds_apy(self):
        assert _sy_real_yield(2.0, 5.0) < 0

    def test_zero_when_equal(self):
        assert _sy_real_yield(3.0, 3.0) == pytest.approx(0.0)

    def test_correct_magnitude(self):
        assert _sy_real_yield(8.0, 3.0) == pytest.approx(5.0, abs=1e-6)

    def test_both_zero_returns_zero(self):
        assert _sy_real_yield(0.0, 0.0) == pytest.approx(0.0)

    def test_returns_float(self):
        assert isinstance(_sy_real_yield(4.0, 1.5), float)


# ===========================================================================
# 2. _sy_stake_ratio
# ===========================================================================

class TestSyStakeRatio:
    def test_half_staked_returns_50(self):
        assert _sy_stake_ratio(500_000, 1_000_000) == pytest.approx(50.0, abs=0.01)

    def test_all_staked_returns_100(self):
        assert _sy_stake_ratio(1_000_000, 1_000_000) == pytest.approx(100.0, abs=0.01)

    def test_zero_total_returns_zero(self):
        assert _sy_stake_ratio(0, 0) == pytest.approx(0.0)

    def test_result_in_0_100_range(self):
        for staked, total in [(0, 1_000_000), (500_000, 1_000_000), (1_000_000, 1_000_000)]:
            assert 0.0 <= _sy_stake_ratio(staked, total) <= 100.0

    def test_correct_magnitude_eth(self):
        # ~27% of ETH supply staked
        result = _sy_stake_ratio(32_000_000, 120_000_000)
        assert result == pytest.approx(26.67, abs=0.1)

    def test_returns_float(self):
        assert isinstance(_sy_stake_ratio(100, 1000), float)


# ===========================================================================
# 3. _sy_concentration_risk
# ===========================================================================

class TestSyConcentrationRisk:
    def test_empty_returns_zero(self):
        assert _sy_concentration_risk([]) == pytest.approx(0.0)

    def test_single_validator_returns_100(self):
        assert _sy_concentration_risk([1_000_000]) == pytest.approx(100.0, abs=0.01)

    def test_equal_distribution_near_zero(self):
        equal = [100] * 10
        assert _sy_concentration_risk(equal) == pytest.approx(0.0, abs=0.5)

    def test_unequal_higher_than_equal(self):
        equal   = [100, 100, 100, 100]
        unequal = [700, 100, 100, 100]
        assert _sy_concentration_risk(unequal) > _sy_concentration_risk(equal)

    def test_result_in_0_100_range(self):
        for vlist in [[], [100], [100]*5, [500, 100, 100]]:
            r = _sy_concentration_risk(vlist)
            assert 0.0 <= r <= 100.0

    def test_returns_float(self):
        assert isinstance(_sy_concentration_risk([100, 200, 300]), float)


# ===========================================================================
# 4. _sy_apy_trend
# ===========================================================================

class TestSyApyTrend:
    def test_empty_returns_stable(self):
        assert _sy_apy_trend([]) == "stable"

    def test_single_returns_stable(self):
        assert _sy_apy_trend([5.0]) == "stable"

    def test_rising_apys(self):
        assert _sy_apy_trend([3.0, 3.5, 4.0, 4.5, 5.0]) == "rising"

    def test_falling_apys(self):
        assert _sy_apy_trend([5.0, 4.5, 4.0, 3.5, 3.0]) == "falling"

    def test_flat_apys_is_stable(self):
        assert _sy_apy_trend([4.5] * 10) == "stable"

    def test_returns_valid_string(self):
        assert _sy_apy_trend([3.0, 3.2, 3.4]) in ("rising", "falling", "stable")


# ===========================================================================
# 5. _sy_yield_label
# ===========================================================================

class TestSyYieldLabel:
    def test_high_real_yield_is_attractive(self):
        assert _sy_yield_label(5.0) == "attractive"

    def test_small_positive_is_neutral(self):
        assert _sy_yield_label(1.0) == "neutral"

    def test_zero_is_negative(self):
        assert _sy_yield_label(0.0) == "negative"

    def test_negative_real_yield_is_negative(self):
        assert _sy_yield_label(-2.0) == "negative"

    def test_boundary_2_is_attractive(self):
        assert _sy_yield_label(2.0) == "attractive"


# ===========================================================================
# 6. _sy_validator_growth
# ===========================================================================

class TestSyValidatorGrowth:
    def test_positive_growth(self):
        assert _sy_validator_growth(1_100, 1_000) > 0

    def test_negative_growth(self):
        assert _sy_validator_growth(900, 1_000) < 0

    def test_no_growth_is_zero(self):
        assert _sy_validator_growth(1_000, 1_000) == pytest.approx(0.0)

    def test_zero_previous_returns_zero(self):
        assert _sy_validator_growth(500, 0) == pytest.approx(0.0)

    def test_correct_magnitude(self):
        assert _sy_validator_growth(1_100, 1_000) == pytest.approx(10.0, abs=0.01)

    def test_returns_float(self):
        assert isinstance(_sy_validator_growth(1_100, 1_000), float)


# ===========================================================================
# 7. _sy_risk_label
# ===========================================================================

class TestSyRiskLabel:
    def test_high_concentration_is_high_risk(self):
        assert _sy_risk_label(70.0) == "high"

    def test_medium_concentration_is_medium(self):
        assert _sy_risk_label(45.0) == "medium"

    def test_low_concentration_is_low(self):
        assert _sy_risk_label(20.0) == "low"

    def test_boundary_60_is_high(self):
        assert _sy_risk_label(60.0) == "high"

    def test_returns_valid_string(self):
        assert _sy_risk_label(50.0) in ("high", "medium", "low")


# ===========================================================================
# 8. _sy_apy_zscore
# ===========================================================================

class TestSyApyZscore:
    def test_empty_history_returns_zero(self):
        assert _sy_apy_zscore(5.0, []) == pytest.approx(0.0)

    def test_single_history_returns_zero(self):
        assert _sy_apy_zscore(5.0, [5.0]) == pytest.approx(0.0)

    def test_current_at_mean_returns_near_zero(self):
        history = [3.0, 4.0, 5.0, 4.0, 3.0]
        mean = sum(history) / len(history)
        assert abs(_sy_apy_zscore(mean, history)) < 0.01

    def test_above_mean_returns_positive(self):
        history = [3.0, 3.5, 4.0, 3.5]
        assert _sy_apy_zscore(20.0, history) > 0

    def test_below_mean_returns_negative(self):
        history = [8.0, 9.0, 8.5, 9.5]
        assert _sy_apy_zscore(0.0, history) < 0

    def test_uniform_history_returns_zero(self):
        history = [4.5] * 10
        assert _sy_apy_zscore(4.5, history) == pytest.approx(0.0, abs=0.01)


# ===========================================================================
# 9. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    PROTOCOLS = ("ETH", "SOL", "ADA", "DOT", "AVAX")
    PROTOCOL_KEYS = (
        "apy", "apy_change_30d", "inflation_rate", "real_yield",
        "yield_label", "stake_ratio", "validators",
        "validator_growth_30d_pct", "concentration_risk", "risk_label",
        "history_30d",
    )

    def test_has_protocols_dict(self):
        assert isinstance(SAMPLE_RESPONSE["protocols"], dict)

    def test_has_all_five_protocols(self):
        for p in self.PROTOCOLS:
            assert p in SAMPLE_RESPONSE["protocols"], f"Missing protocol: {p}"

    def test_each_protocol_has_required_keys(self):
        for name, proto in SAMPLE_RESPONSE["protocols"].items():
            for k in self.PROTOCOL_KEYS:
                assert k in proto, f"{name} missing key '{k}'"

    def test_yield_labels_valid(self):
        for name, proto in SAMPLE_RESPONSE["protocols"].items():
            assert proto["yield_label"] in ("attractive", "neutral", "negative"), \
                f"{name} invalid yield_label '{proto['yield_label']}'"

    def test_risk_labels_valid(self):
        for name, proto in SAMPLE_RESPONSE["protocols"].items():
            assert proto["risk_label"] in ("high", "medium", "low"), \
                f"{name} invalid risk_label '{proto['risk_label']}'"

    def test_stake_ratio_in_range(self):
        for name, proto in SAMPLE_RESPONSE["protocols"].items():
            assert 0.0 <= proto["stake_ratio"] <= 100.0, \
                f"{name} stake_ratio out of range"

    def test_concentration_risk_in_range(self):
        for name, proto in SAMPLE_RESPONSE["protocols"].items():
            assert 0.0 <= proto["concentration_risk"] <= 100.0, \
                f"{name} concentration_risk out of range"

    def test_history_30d_is_list(self):
        for name, proto in SAMPLE_RESPONSE["protocols"].items():
            assert isinstance(proto["history_30d"], list), \
                f"{name} history_30d not a list"

    def test_history_items_have_keys(self):
        for name, proto in SAMPLE_RESPONSE["protocols"].items():
            for item in proto["history_30d"]:
                for k in ("date", "apy", "real_yield"):
                    assert k in item, f"{name} history item missing '{k}'"

    def test_has_aggregate_dict(self):
        assert isinstance(SAMPLE_RESPONSE["aggregate"], dict)

    def test_aggregate_has_required_keys(self):
        for k in ("avg_apy", "avg_real_yield", "best_yield_protocol",
                  "lowest_risk_protocol", "total_value_staked_usd"):
            assert k in SAMPLE_RESPONSE["aggregate"], f"aggregate missing '{k}'"

    def test_best_yield_protocol_is_valid(self):
        assert SAMPLE_RESPONSE["aggregate"]["best_yield_protocol"] in self.PROTOCOLS

    def test_has_history_30d_list(self):
        assert isinstance(SAMPLE_RESPONSE["history_30d"], list)

    def test_history_30d_items_have_keys(self):
        for item in SAMPLE_RESPONSE["history_30d"]:
            for k in ("date", "avg_apy", "avg_real_yield"):
                assert k in item, f"history_30d item missing '{k}'"

    def test_has_description_string(self):
        assert isinstance(SAMPLE_RESPONSE["description"], str)


# ===========================================================================
# 10. Structural
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api = open(os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")).read()
        assert "/staking-yield-tracker" in api, "/staking-yield-tracker route missing"

    def test_html_card_exists(self):
        html = open(os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")).read()
        assert "card-staking-yield-tracker" in html, "card-staking-yield-tracker missing"

    def test_js_render_function_exists(self):
        js = open(os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")).read()
        assert "renderStakingYieldTracker" in js, "renderStakingYieldTracker missing"

    def test_js_api_call_to_endpoint(self):
        js = open(os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")).read()
        assert "/staking-yield-tracker" in js, "/staking-yield-tracker call missing"
