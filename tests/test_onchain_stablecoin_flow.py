"""
Unit / smoke tests for /api/stablecoin-flow.

On-chain stablecoin flow tracker (USDT / USDC / DAI):
  - Net flow = change in circulating supply over a period
  - Positive flow = stablecoins minted / entering ecosystem  → buying power ↑ (bullish)
  - Negative flow = stablecoins burned / leaving ecosystem   → buying power ↓ (bearish)
  - 7-day rolling trend + flow momentum (acceleration)
  - Bullish / bearish / neutral signal based on 7d aggregate
  - Per-coin breakdown + aggregate summary

Data source: DeFi Llama stablecoin API (public, free).

Covers:
  - _sf_net_flow
  - _sf_flow_direction
  - _sf_flow_signal
  - _sf_momentum
  - _sf_rolling_average
  - _sf_flow_zscore
  - _sf_combine_stables
  - Response shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _sf_net_flow,
    _sf_flow_direction,
    _sf_flow_signal,
    _sf_momentum,
    _sf_rolling_average,
    _sf_flow_zscore,
    _sf_combine_stables,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "stablecoins": {
        "USDT": {
            "symbol": "USDT",
            "current_supply": 85_000_000_000.0,
            "inflow_24h":  500_000_000.0,
            "inflow_7d":  2_500_000_000.0,
            "flow_direction_24h": "inflow",
        },
        "USDC": {
            "symbol": "USDC",
            "current_supply": 25_000_000_000.0,
            "inflow_24h":  200_000_000.0,
            "inflow_7d":  800_000_000.0,
            "flow_direction_24h": "inflow",
        },
        "DAI": {
            "symbol": "DAI",
            "current_supply": 5_000_000_000.0,
            "inflow_24h": -50_000_000.0,
            "inflow_7d":  100_000_000.0,
            "flow_direction_24h": "outflow",
        },
    },
    "aggregate": {
        "net_flow_24h":  650_000_000.0,
        "net_flow_7d":  3_400_000_000.0,
        "flow_direction": "inflow",
        "flow_signal":   "bullish",
        "flow_momentum": 150_000_000.0,
        "flow_zscore":   1.4,
    },
    "history": [
        {"date": "2024-11-14", "net_flow":  300_000_000.0, "flow_direction": "inflow"},
        {"date": "2024-11-15", "net_flow":  450_000_000.0, "flow_direction": "inflow"},
        {"date": "2024-11-16", "net_flow": -100_000_000.0, "flow_direction": "outflow"},
        {"date": "2024-11-17", "net_flow":  500_000_000.0, "flow_direction": "inflow"},
        {"date": "2024-11-18", "net_flow":  600_000_000.0, "flow_direction": "inflow"},
        {"date": "2024-11-19", "net_flow":  700_000_000.0, "flow_direction": "inflow"},
        {"date": "2024-11-20", "net_flow":  950_000_000.0, "flow_direction": "inflow"},
    ],
    "description": "Bullish: $650M stablecoin inflow in 24h — buying power increasing",
}


# ===========================================================================
# 1. _sf_net_flow
# ===========================================================================

class TestSfNetFlow:
    def test_positive_when_supply_grows(self):
        assert _sf_net_flow(110.0, 100.0) == pytest.approx(10.0, rel=1e-4)

    def test_negative_when_supply_shrinks(self):
        assert _sf_net_flow(90.0, 100.0) == pytest.approx(-10.0, rel=1e-4)

    def test_zero_when_unchanged(self):
        assert _sf_net_flow(100.0, 100.0) == pytest.approx(0.0, abs=1e-6)

    def test_large_values_precision(self):
        result = _sf_net_flow(85_500_000_000.0, 85_000_000_000.0)
        assert result == pytest.approx(500_000_000.0, rel=1e-4)

    def test_returns_float(self):
        assert isinstance(_sf_net_flow(100.0, 90.0), float)


# ===========================================================================
# 2. _sf_flow_direction
# ===========================================================================

class TestSfFlowDirection:
    def test_large_positive_is_inflow(self):
        assert _sf_flow_direction(500_000_000) == "inflow"

    def test_large_negative_is_outflow(self):
        assert _sf_flow_direction(-500_000_000) == "outflow"

    def test_zero_is_neutral(self):
        assert _sf_flow_direction(0) == "neutral"

    def test_small_positive_below_threshold_is_neutral(self):
        assert _sf_flow_direction(500_000, threshold=1_000_000) == "neutral"

    def test_custom_threshold_respected(self):
        assert _sf_flow_direction(200_000_000, threshold=100_000_000) == "inflow"
        assert _sf_flow_direction(50_000_000, threshold=100_000_000) == "neutral"

    def test_negative_just_below_threshold_is_neutral(self):
        assert _sf_flow_direction(-999_999, threshold=1_000_000) == "neutral"


# ===========================================================================
# 3. _sf_flow_signal
# ===========================================================================

class TestSfFlowSignal:
    def test_large_positive_7d_is_bullish(self):
        assert _sf_flow_signal(3_000_000_000) == "bullish"

    def test_large_negative_7d_is_bearish(self):
        assert _sf_flow_signal(-3_000_000_000) == "bearish"

    def test_small_flow_is_neutral(self):
        assert _sf_flow_signal(0) == "neutral"

    def test_just_above_threshold_is_bullish(self):
        threshold = 100_000_000
        assert _sf_flow_signal(threshold + 1, threshold=threshold) == "bullish"

    def test_just_below_neg_threshold_is_bearish(self):
        threshold = 100_000_000
        assert _sf_flow_signal(-(threshold + 1), threshold=threshold) == "bearish"

    def test_returns_string(self):
        assert isinstance(_sf_flow_signal(1_000_000_000), str)


# ===========================================================================
# 4. _sf_momentum
# ===========================================================================

class TestSfMomentum:
    def test_empty_returns_zero(self):
        assert _sf_momentum([]) == 0.0

    def test_single_value_returns_zero(self):
        assert _sf_momentum([100.0]) == 0.0

    def test_two_values_returns_zero(self):
        assert _sf_momentum([100.0, 200.0]) == 0.0

    def test_accelerating_flow_positive_momentum(self):
        # Later flows are bigger → positive momentum
        flows = [100.0, 150.0, 200.0, 300.0, 400.0, 500.0]
        result = _sf_momentum(flows)
        assert result > 0

    def test_decelerating_flow_negative_momentum(self):
        # Later flows are smaller → negative momentum
        flows = [500.0, 400.0, 300.0, 200.0, 150.0, 100.0]
        result = _sf_momentum(flows)
        assert result < 0

    def test_uniform_flow_near_zero_momentum(self):
        flows = [200.0] * 6
        result = _sf_momentum(flows)
        assert abs(result) < 1.0

    def test_returns_float(self):
        assert isinstance(_sf_momentum([100.0, 200.0, 300.0]), float)


# ===========================================================================
# 5. _sf_rolling_average
# ===========================================================================

class TestSfRollingAverage:
    def test_empty_returns_zero(self):
        assert _sf_rolling_average([], 7) == 0.0

    def test_fewer_values_than_n_uses_all(self):
        result = _sf_rolling_average([100.0, 200.0, 300.0], 7)
        assert result == pytest.approx(200.0, rel=1e-4)

    def test_exact_n_values(self):
        result = _sf_rolling_average([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0], 7)
        assert result == pytest.approx(4.0, rel=1e-4)

    def test_takes_last_n_values(self):
        # Only last 3 used: [8, 9, 10] → avg = 9
        result = _sf_rolling_average([1.0, 2.0, 3.0, 8.0, 9.0, 10.0], 3)
        assert result == pytest.approx(9.0, rel=1e-4)

    def test_returns_float(self):
        assert isinstance(_sf_rolling_average([100.0], 7), float)


# ===========================================================================
# 6. _sf_flow_zscore
# ===========================================================================

class TestSfFlowZscore:
    def test_empty_history_returns_zero(self):
        assert _sf_flow_zscore(500.0, []) == 0.0

    def test_single_item_returns_zero(self):
        assert _sf_flow_zscore(500.0, [500.0]) == 0.0

    def test_current_at_mean_returns_near_zero(self):
        history = [100.0, 200.0, 300.0, 400.0, 500.0]
        mean = sum(history) / len(history)  # 300
        result = _sf_flow_zscore(mean, history)
        assert abs(result) < 0.01

    def test_above_mean_returns_positive(self):
        history = [100.0, 200.0, 300.0]
        result = _sf_flow_zscore(1000.0, history)
        assert result > 0

    def test_below_mean_returns_negative(self):
        history = [400.0, 500.0, 600.0]
        result = _sf_flow_zscore(0.0, history)
        assert result < 0

    def test_uniform_history_returns_zero(self):
        history = [300.0] * 10
        result = _sf_flow_zscore(300.0, history)
        assert result == pytest.approx(0.0, abs=0.01)

    def test_returns_float(self):
        history = [100.0, 200.0, 300.0]
        assert isinstance(_sf_flow_zscore(250.0, history), float)


# ===========================================================================
# 7. _sf_combine_stables
# ===========================================================================

class TestSfCombineStables:
    def test_empty_dict_returns_zero_flows(self):
        result = _sf_combine_stables({})
        assert result["net_flow_24h"] == 0.0
        assert result["net_flow_7d"] == 0.0

    def test_all_inflows_aggregated(self):
        flows = {
            "USDT": {"inflow_24h": 500e6, "inflow_7d": 2_000e6},
            "USDC": {"inflow_24h": 200e6, "inflow_7d":   800e6},
        }
        result = _sf_combine_stables(flows)
        assert result["net_flow_24h"] == pytest.approx(700e6, rel=1e-4)
        assert result["net_flow_7d"]  == pytest.approx(2_800e6, rel=1e-4)

    def test_mixed_flows_net_correctly(self):
        flows = {
            "USDT": {"inflow_24h":  500e6, "inflow_7d": 1_000e6},
            "USDC": {"inflow_24h": -200e6, "inflow_7d":  -500e6},
        }
        result = _sf_combine_stables(flows)
        assert result["net_flow_24h"] == pytest.approx(300e6, rel=1e-4)

    def test_flow_direction_present(self):
        flows = {"USDT": {"inflow_24h": 1e9, "inflow_7d": 5e9}}
        result = _sf_combine_stables(flows)
        assert "flow_direction" in result
        assert result["flow_direction"] in ("inflow", "outflow", "neutral")

    def test_flow_signal_present(self):
        flows = {"USDT": {"inflow_24h": 1e9, "inflow_7d": 5e9}}
        result = _sf_combine_stables(flows)
        assert "flow_signal" in result
        assert result["flow_signal"] in ("bullish", "bearish", "neutral")

    def test_large_net_outflow_is_bearish(self):
        flows = {"USDT": {"inflow_24h": -2e9, "inflow_7d": -10e9}}
        result = _sf_combine_stables(flows)
        assert result["flow_signal"] == "bearish"


# ===========================================================================
# 8. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_stablecoins_dict(self):
        assert isinstance(SAMPLE_RESPONSE["stablecoins"], dict)

    def test_stablecoins_has_usdt(self):
        assert "USDT" in SAMPLE_RESPONSE["stablecoins"]

    def test_stablecoins_has_usdc(self):
        assert "USDC" in SAMPLE_RESPONSE["stablecoins"]

    def test_stablecoins_has_dai(self):
        assert "DAI" in SAMPLE_RESPONSE["stablecoins"]

    def test_each_coin_has_required_keys(self):
        for coin, data in SAMPLE_RESPONSE["stablecoins"].items():
            for key in ("current_supply", "inflow_24h", "inflow_7d", "flow_direction_24h"):
                assert key in data, f"Coin {coin} missing key {key}"

    def test_has_aggregate(self):
        assert "aggregate" in SAMPLE_RESPONSE

    def test_aggregate_has_required_keys(self):
        agg = SAMPLE_RESPONSE["aggregate"]
        for key in ("net_flow_24h", "net_flow_7d", "flow_direction",
                    "flow_signal", "flow_momentum", "flow_zscore"):
            assert key in agg, f"aggregate missing key {key}"

    def test_flow_signal_valid(self):
        assert SAMPLE_RESPONSE["aggregate"]["flow_signal"] in ("bullish", "bearish", "neutral")

    def test_flow_direction_valid(self):
        assert SAMPLE_RESPONSE["aggregate"]["flow_direction"] in ("inflow", "outflow", "neutral")

    def test_has_history_list(self):
        assert isinstance(SAMPLE_RESPONSE["history"], list)

    def test_history_items_have_required_keys(self):
        for item in SAMPLE_RESPONSE["history"]:
            for key in ("date", "net_flow", "flow_direction"):
                assert key in item, f"history item missing key {key}"

    def test_history_length_seven_days(self):
        assert len(SAMPLE_RESPONSE["history"]) == 7

    def test_has_description(self):
        assert isinstance(SAMPLE_RESPONSE["description"], str)
        assert len(SAMPLE_RESPONSE["description"]) > 0


# ===========================================================================
# 9. Structural tests
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api_path = os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")
        content = open(api_path).read()
        assert "/stablecoin-flow" in content, "/stablecoin-flow route missing from api.py"

    def test_html_card_exists(self):
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        content = open(html_path).read()
        assert "card-stablecoin-flow" in content, "card-stablecoin-flow missing from index.html"

    def test_js_render_function_exists(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "renderStablecoinFlow" in content, "renderStablecoinFlow missing from app.js"

    def test_js_api_call_to_endpoint(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "/stablecoin-flow" in content, "/stablecoin-flow call missing from app.js"
