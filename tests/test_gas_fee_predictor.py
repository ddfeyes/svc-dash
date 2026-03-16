"""
Unit / smoke tests for /api/gas-fee-predictor.

Gas Fee Predictor — EIP-1559 base fee trend analysis, priority fee
percentiles, next-block fee estimate, and spike detection for Ethereum.

Approach:
  - Base fee 7d history with trend analysis (linear regression)
  - Priority fee percentiles: 10th (slow), 50th (standard), 90th (fast)
  - Next-block base fee estimate (EIP-1559: base adjusts ±12.5% per block)
  - Spike detector: z-score vs 24h rolling window; alerts on z > 2.0

Signal:
  spike      — z-score > 2.0 → fees unusually high
  elevated   — z-score 1.0–2.0 → fees above average
  normal     — z-score -1.0 to 1.0
  low        — z-score < -1.0 → fees below average

Covers:
  - _gf_base_fee_trend
  - _gf_priority_percentile
  - _gf_next_block_estimate
  - _gf_zscore
  - _gf_spike_label
  - _gf_fee_usd
  - _gf_moving_average
  - Response shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _gf_base_fee_trend,
    _gf_priority_percentile,
    _gf_next_block_estimate,
    _gf_zscore,
    _gf_spike_label,
    _gf_fee_usd,
    _gf_moving_average,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "current": {
        "base_fee_gwei":      42.5,
        "priority_slow_gwei":  1.0,
        "priority_std_gwei":   2.0,
        "priority_fast_gwei":  5.0,
        "next_block_gwei":    43.8,
        "total_slow_gwei":    43.5,
        "total_std_gwei":     44.5,
        "total_fast_gwei":    47.5,
        "total_slow_usd":     0.42,
        "total_std_usd":      0.43,
        "total_fast_usd":     0.46,
    },
    "spike": {
        "zscore":       0.4,
        "label":        "normal",
        "threshold":    2.0,
        "percentile":   65.2,
    },
    "trend": {
        "direction":    "rising",
        "slope_gwei_per_hour": 1.2,
        "ma_24h_gwei":  38.5,
        "ma_7d_gwei":   35.0,
    },
    "history_7d": [
        {"timestamp": "2024-11-14T00:00:00", "base_fee_gwei": 35.0, "ma_gwei": 34.5},
        {"timestamp": "2024-11-15T00:00:00", "base_fee_gwei": 37.2, "ma_gwei": 35.1},
        {"timestamp": "2024-11-16T00:00:00", "base_fee_gwei": 36.8, "ma_gwei": 35.8},
        {"timestamp": "2024-11-17T00:00:00", "base_fee_gwei": 39.5, "ma_gwei": 36.6},
        {"timestamp": "2024-11-18T00:00:00", "base_fee_gwei": 41.0, "ma_gwei": 37.4},
        {"timestamp": "2024-11-19T00:00:00", "base_fee_gwei": 40.5, "ma_gwei": 38.0},
        {"timestamp": "2024-11-20T00:00:00", "base_fee_gwei": 42.5, "ma_gwei": 38.5},
    ],
    "description": "Normal: base fee 42.5 Gwei — rising trend, z-score 0.4",
}


# ===========================================================================
# 1. _gf_base_fee_trend
# ===========================================================================

class TestGfBaseFeerTrend:
    def test_rising_series_is_rising(self):
        fees = [30.0, 32.0, 34.0, 36.0, 38.0, 40.0]
        assert _gf_base_fee_trend(fees) == "rising"

    def test_falling_series_is_falling(self):
        fees = [50.0, 46.0, 42.0, 38.0, 34.0, 30.0]
        assert _gf_base_fee_trend(fees) == "falling"

    def test_flat_series_is_stable(self):
        fees = [40.0] * 8
        assert _gf_base_fee_trend(fees) == "stable"

    def test_empty_returns_stable(self):
        assert _gf_base_fee_trend([]) == "stable"

    def test_single_returns_stable(self):
        assert _gf_base_fee_trend([42.0]) == "stable"

    def test_returns_valid_string(self):
        result = _gf_base_fee_trend([10.0, 20.0, 30.0])
        assert result in ("rising", "falling", "stable")


# ===========================================================================
# 2. _gf_priority_percentile
# ===========================================================================

class TestGfPriorityPercentile:
    SAMPLES = [1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0]

    def test_p10_returns_low_value(self):
        p10 = _gf_priority_percentile(self.SAMPLES, 10)
        assert p10 <= 2.0

    def test_p50_returns_median(self):
        p50 = _gf_priority_percentile(self.SAMPLES, 50)
        assert 2.5 <= p50 <= 4.0

    def test_p90_returns_high_value(self):
        p90 = _gf_priority_percentile(self.SAMPLES, 90)
        assert p90 >= 7.0

    def test_p10_less_than_p50(self):
        p10 = _gf_priority_percentile(self.SAMPLES, 10)
        p50 = _gf_priority_percentile(self.SAMPLES, 50)
        assert p10 < p50

    def test_p50_less_than_p90(self):
        p50 = _gf_priority_percentile(self.SAMPLES, 50)
        p90 = _gf_priority_percentile(self.SAMPLES, 90)
        assert p50 < p90

    def test_empty_returns_zero(self):
        assert _gf_priority_percentile([], 50) == pytest.approx(0.0, abs=1e-6)

    def test_returns_float(self):
        assert isinstance(_gf_priority_percentile(self.SAMPLES, 50), float)


# ===========================================================================
# 3. _gf_next_block_estimate
# ===========================================================================

class TestGfNextBlockEstimate:
    def test_full_block_increases_fee(self):
        # util > 50% → next base fee goes up
        est = _gf_next_block_estimate(40.0, utilization=0.80)
        assert est > 40.0

    def test_empty_block_decreases_fee(self):
        # util < 50% → next base fee goes down
        est = _gf_next_block_estimate(40.0, utilization=0.20)
        assert est < 40.0

    def test_half_full_keeps_fee_same(self):
        est = _gf_next_block_estimate(40.0, utilization=0.50)
        assert est == pytest.approx(40.0, abs=0.01)

    def test_max_increase_is_12_5_pct(self):
        # At 100% utilization, max increase = +12.5%
        est = _gf_next_block_estimate(40.0, utilization=1.0)
        assert est <= 40.0 * 1.125 + 0.1

    def test_max_decrease_is_12_5_pct(self):
        # At 0% utilization, max decrease = -12.5%
        est = _gf_next_block_estimate(40.0, utilization=0.0)
        assert est >= 40.0 * 0.875 - 0.1

    def test_returns_float(self):
        assert isinstance(_gf_next_block_estimate(40.0, utilization=0.5), float)

    def test_zero_base_returns_zero(self):
        assert _gf_next_block_estimate(0.0, utilization=1.0) == pytest.approx(0.0, abs=1e-6)


# ===========================================================================
# 4. _gf_zscore
# ===========================================================================

class TestGfZscore:
    def test_empty_history_returns_zero(self):
        assert _gf_zscore(50.0, []) == 0.0

    def test_single_history_returns_zero(self):
        assert _gf_zscore(50.0, [50.0]) == 0.0

    def test_current_at_mean_near_zero(self):
        history = [30.0, 35.0, 40.0, 45.0, 50.0]
        mean = sum(history) / len(history)
        assert abs(_gf_zscore(mean, history)) < 0.01

    def test_above_mean_is_positive(self):
        history = [30.0, 32.0, 34.0, 36.0]
        assert _gf_zscore(100.0, history) > 0

    def test_below_mean_is_negative(self):
        history = [80.0, 85.0, 90.0, 95.0]
        assert _gf_zscore(0.0, history) < 0

    def test_uniform_history_returns_zero(self):
        history = [40.0] * 10
        assert _gf_zscore(40.0, history) == pytest.approx(0.0, abs=0.01)

    def test_returns_float(self):
        assert isinstance(_gf_zscore(50.0, [30.0, 70.0]), float)


# ===========================================================================
# 5. _gf_spike_label
# ===========================================================================

class TestGfSpikeLabel:
    def test_high_zscore_is_spike(self):
        assert _gf_spike_label(2.5) == "spike"

    def test_elevated_zscore(self):
        assert _gf_spike_label(1.5) == "elevated"

    def test_normal_zscore(self):
        assert _gf_spike_label(0.5) == "normal"

    def test_low_zscore(self):
        assert _gf_spike_label(-1.5) == "low"

    def test_boundary_2_is_spike(self):
        assert _gf_spike_label(2.0) == "spike"

    def test_boundary_1_is_elevated(self):
        assert _gf_spike_label(1.0) == "elevated"

    def test_returns_valid_string(self):
        result = _gf_spike_label(0.0)
        assert result in ("spike", "elevated", "normal", "low")


# ===========================================================================
# 6. _gf_fee_usd
# ===========================================================================

class TestGfFeeUsd:
    def test_basic_conversion(self):
        # 21000 gas * 50 Gwei * $3000/ETH / 1e9 = $3.15
        result = _gf_fee_usd(21_000, 50.0, 3000.0)
        assert result == pytest.approx(3.15, rel=1e-3)

    def test_zero_gas_returns_zero(self):
        assert _gf_fee_usd(0, 50.0, 3000.0) == pytest.approx(0.0, abs=1e-6)

    def test_zero_price_returns_zero(self):
        assert _gf_fee_usd(21_000, 50.0, 0.0) == pytest.approx(0.0, abs=1e-6)

    def test_returns_float(self):
        assert isinstance(_gf_fee_usd(21_000, 40.0, 3000.0), float)

    def test_higher_gwei_higher_cost(self):
        cheap = _gf_fee_usd(21_000, 20.0, 3000.0)
        expensive = _gf_fee_usd(21_000, 80.0, 3000.0)
        assert expensive > cheap

    def test_result_is_positive(self):
        assert _gf_fee_usd(21_000, 40.0, 2500.0) > 0


# ===========================================================================
# 7. _gf_moving_average
# ===========================================================================

class TestGfMovingAverage:
    def test_empty_returns_empty(self):
        assert _gf_moving_average([], 5) == []

    def test_output_length_equals_input(self):
        vals = [40.0] * 10
        assert len(_gf_moving_average(vals, 5)) == 10

    def test_flat_series_ma_equals_constant(self):
        vals = [42.0] * 8
        result = _gf_moving_average(vals, 4)
        for v in result:
            assert v == pytest.approx(42.0, abs=0.01)

    def test_full_window_correct_average(self):
        vals = [10.0, 20.0, 30.0, 40.0, 50.0]
        result = _gf_moving_average(vals, 3)
        # index 2 (first full window): avg(10, 20, 30) = 20
        assert result[2] == pytest.approx(20.0, abs=0.01)

    def test_partial_window_uses_available(self):
        vals = [10.0, 20.0, 30.0]
        result = _gf_moving_average(vals, 5)
        # index 0: avg([10]) = 10
        assert result[0] == pytest.approx(10.0, abs=0.01)

    def test_returns_list_of_floats(self):
        result = _gf_moving_average([1.0, 2.0, 3.0], 2)
        assert isinstance(result, list)
        assert all(isinstance(v, float) for v in result)


# ===========================================================================
# 8. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_current_dict(self):
        assert isinstance(SAMPLE_RESPONSE["current"], dict)

    def test_current_has_required_keys(self):
        for key in (
            "base_fee_gwei", "priority_slow_gwei", "priority_std_gwei",
            "priority_fast_gwei", "next_block_gwei",
            "total_slow_gwei", "total_std_gwei", "total_fast_gwei",
            "total_slow_usd", "total_std_usd", "total_fast_usd",
        ):
            assert key in SAMPLE_RESPONSE["current"], f"current missing '{key}'"

    def test_base_fee_is_positive(self):
        assert SAMPLE_RESPONSE["current"]["base_fee_gwei"] > 0

    def test_priority_fees_ordered(self):
        c = SAMPLE_RESPONSE["current"]
        assert c["priority_slow_gwei"] <= c["priority_std_gwei"] <= c["priority_fast_gwei"]

    def test_total_fees_ordered(self):
        c = SAMPLE_RESPONSE["current"]
        assert c["total_slow_gwei"] <= c["total_std_gwei"] <= c["total_fast_gwei"]

    def test_has_spike_dict(self):
        assert isinstance(SAMPLE_RESPONSE["spike"], dict)

    def test_spike_has_required_keys(self):
        for key in ("zscore", "label", "threshold", "percentile"):
            assert key in SAMPLE_RESPONSE["spike"], f"spike missing '{key}'"

    def test_spike_label_valid(self):
        assert SAMPLE_RESPONSE["spike"]["label"] in ("spike", "elevated", "normal", "low")

    def test_has_trend_dict(self):
        assert isinstance(SAMPLE_RESPONSE["trend"], dict)

    def test_trend_has_required_keys(self):
        for key in ("direction", "slope_gwei_per_hour", "ma_24h_gwei", "ma_7d_gwei"):
            assert key in SAMPLE_RESPONSE["trend"], f"trend missing '{key}'"

    def test_trend_direction_valid(self):
        assert SAMPLE_RESPONSE["trend"]["direction"] in ("rising", "falling", "stable")

    def test_has_history_7d_list(self):
        assert isinstance(SAMPLE_RESPONSE["history_7d"], list)

    def test_history_items_have_required_keys(self):
        for item in SAMPLE_RESPONSE["history_7d"]:
            for key in ("timestamp", "base_fee_gwei", "ma_gwei"):
                assert key in item, f"history_7d item missing '{key}'"

    def test_has_description(self):
        assert isinstance(SAMPLE_RESPONSE["description"], str)


# ===========================================================================
# 9. Structural tests
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api_path = os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")
        content = open(api_path).read()
        assert "/gas-fee-predictor" in content, "/gas-fee-predictor route missing"

    def test_html_card_exists(self):
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        content = open(html_path).read()
        assert "card-gas-fee-predictor" in content, "card-gas-fee-predictor missing"

    def test_js_render_function_exists(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "renderGasFeePredictor" in content, "renderGasFeePredictor missing"

    def test_js_api_call_to_endpoint(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "/gas-fee-predictor" in content, "/gas-fee-predictor call missing"
