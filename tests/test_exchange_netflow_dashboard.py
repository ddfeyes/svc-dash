"""
Unit / smoke tests for /api/exchange-netflow.

Exchange net flow dashboard — aggregates BTC inflows vs outflows across
top 5 exchanges (Binance, Coinbase, OKX, Bybit, Kraken).

Flow proxy model (from CryptoCompare per-exchange OHLCV):
  inflow_proxy  = volume_btc × max(0, close − open)  — buying pressure
  outflow_proxy = volume_btc × max(0, open − close)  — selling pressure
  net_flow      = inflow_proxy − outflow_proxy

Signal:
  accumulation — aggregate net flow positive and trending up  → bullish
  distribution — aggregate net flow negative and trending down → bearish
  neutral      — otherwise

Covers:
  - _enf_net_flow_proxy
  - _enf_flow_direction
  - _enf_accumulation_signal
  - _enf_flow_strength
  - _enf_exchange_rank
  - _enf_trend
  - _enf_zscore
  - Response shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _enf_net_flow_proxy,
    _enf_flow_direction,
    _enf_accumulation_signal,
    _enf_flow_strength,
    _enf_exchange_rank,
    _enf_trend,
    _enf_zscore,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "exchanges": {
        "Binance": {
            "inflow_proxy":  120_000.0,
            "outflow_proxy":  80_000.0,
            "net_flow":       40_000.0,
            "direction":      "inflow",
            "volume_btc":    5_000.0,
        },
        "Coinbase": {
            "inflow_proxy":  90_000.0,
            "outflow_proxy": 110_000.0,
            "net_flow":      -20_000.0,
            "direction":     "outflow",
            "volume_btc":    3_200.0,
        },
        "OKX": {
            "inflow_proxy":  60_000.0,
            "outflow_proxy":  55_000.0,
            "net_flow":        5_000.0,
            "direction":      "inflow",
            "volume_btc":    2_100.0,
        },
        "Bybit": {
            "inflow_proxy":  45_000.0,
            "outflow_proxy":  40_000.0,
            "net_flow":        5_000.0,
            "direction":      "inflow",
            "volume_btc":    1_800.0,
        },
        "Kraken": {
            "inflow_proxy":  30_000.0,
            "outflow_proxy":  35_000.0,
            "net_flow":       -5_000.0,
            "direction":     "outflow",
            "volume_btc":      900.0,
        },
    },
    "aggregate": {
        "net_flow_24h":  25_000.0,
        "net_flow_7d":  140_000.0,
        "signal":        "accumulation",
        "strength":       62.5,
        "trend":         "increasing",
        "zscore":          1.1,
    },
    "history": [
        {"date": "2024-11-14", "net_flow":  10_000.0, "direction": "inflow"},
        {"date": "2024-11-15", "net_flow":  18_000.0, "direction": "inflow"},
        {"date": "2024-11-16", "net_flow":  -5_000.0, "direction": "outflow"},
        {"date": "2024-11-17", "net_flow":  22_000.0, "direction": "inflow"},
        {"date": "2024-11-18", "net_flow":  30_000.0, "direction": "inflow"},
        {"date": "2024-11-19", "net_flow":  40_000.0, "direction": "inflow"},
        {"date": "2024-11-20", "net_flow":  25_000.0, "direction": "inflow"},
    ],
    "description": "Accumulation: $25k net BTC inflow across 5 exchanges — buying pressure dominant",
}


# ===========================================================================
# 1. _enf_net_flow_proxy
# ===========================================================================

class TestEnfNetFlowProxy:
    def test_up_candle_returns_positive(self):
        # open=100, close=105 → inflow proxy dominates
        net = _enf_net_flow_proxy(10.0, 100.0, 105.0)
        assert net > 0

    def test_down_candle_returns_negative(self):
        # open=105, close=100 → outflow proxy dominates
        net = _enf_net_flow_proxy(10.0, 105.0, 100.0)
        assert net < 0

    def test_flat_candle_returns_zero(self):
        net = _enf_net_flow_proxy(10.0, 100.0, 100.0)
        assert net == pytest.approx(0.0, abs=1e-6)

    def test_zero_volume_returns_zero(self):
        net = _enf_net_flow_proxy(0.0, 100.0, 110.0)
        assert net == pytest.approx(0.0, abs=1e-6)

    def test_zero_open_price_returns_zero(self):
        net = _enf_net_flow_proxy(10.0, 0.0, 100.0)
        assert net == pytest.approx(0.0, abs=1e-6)

    def test_correct_magnitude(self):
        # volume=10, open=100, close=110 → net = 10 × 10 = 100
        net = _enf_net_flow_proxy(10.0, 100.0, 110.0)
        assert net == pytest.approx(100.0, rel=1e-4)

    def test_returns_float(self):
        assert isinstance(_enf_net_flow_proxy(5.0, 50_000.0, 51_000.0), float)


# ===========================================================================
# 2. _enf_flow_direction
# ===========================================================================

class TestEnfFlowDirection:
    def test_positive_net_is_inflow(self):
        assert _enf_flow_direction(50_000.0) == "inflow"

    def test_negative_net_is_outflow(self):
        assert _enf_flow_direction(-50_000.0) == "outflow"

    def test_zero_is_neutral(self):
        assert _enf_flow_direction(0.0) == "neutral"

    def test_small_positive_below_threshold_is_neutral(self):
        assert _enf_flow_direction(500.0, threshold=1_000.0) == "neutral"

    def test_custom_threshold_respected(self):
        assert _enf_flow_direction(10_001.0, threshold=10_000.0) == "inflow"
        assert _enf_flow_direction(9_999.0, threshold=10_000.0) == "neutral"


# ===========================================================================
# 3. _enf_accumulation_signal
# ===========================================================================

class TestEnfAccumulationSignal:
    def test_positive_7d_increasing_is_accumulation(self):
        assert _enf_accumulation_signal(200_000.0, "increasing") == "accumulation"

    def test_negative_7d_decreasing_is_distribution(self):
        assert _enf_accumulation_signal(-200_000.0, "decreasing") == "distribution"

    def test_small_flow_stable_is_neutral(self):
        assert _enf_accumulation_signal(0.0, "stable") == "neutral"

    def test_positive_flow_decreasing_is_neutral(self):
        # positive but trend decreasing → not strongly bullish
        result = _enf_accumulation_signal(50_000.0, "decreasing")
        assert result in ("neutral", "distribution")

    def test_negative_flow_increasing_is_neutral(self):
        result = _enf_accumulation_signal(-50_000.0, "increasing")
        assert result in ("neutral", "accumulation")

    def test_returns_valid_string(self):
        result = _enf_accumulation_signal(100_000.0, "stable")
        assert result in ("accumulation", "distribution", "neutral")


# ===========================================================================
# 4. _enf_flow_strength
# ===========================================================================

class TestEnfFlowStrength:
    def test_zero_net_returns_zero(self):
        assert _enf_flow_strength(0.0, 100_000.0) == pytest.approx(0.0, abs=0.1)

    def test_net_equals_max_returns_100(self):
        assert _enf_flow_strength(100_000.0, 100_000.0) == pytest.approx(100.0, abs=0.1)

    def test_negative_net_uses_abs(self):
        # strength is magnitude-based
        s_pos = _enf_flow_strength(50_000.0, 100_000.0)
        s_neg = _enf_flow_strength(-50_000.0, 100_000.0)
        assert s_pos == pytest.approx(s_neg, rel=1e-4)

    def test_result_in_0_100_range(self):
        for net in (0, 10_000, 50_000, 100_000, 200_000, -50_000):
            s = _enf_flow_strength(float(net), 100_000.0)
            assert 0 <= s <= 100

    def test_zero_max_returns_0(self):
        assert _enf_flow_strength(50_000.0, 0.0) == pytest.approx(0.0, abs=0.1)

    def test_returns_float(self):
        assert isinstance(_enf_flow_strength(25_000.0, 100_000.0), float)


# ===========================================================================
# 5. _enf_exchange_rank
# ===========================================================================

class TestEnfExchangeRank:
    def test_empty_returns_empty(self):
        assert _enf_exchange_rank({}) == []

    def test_sorted_by_net_flow_descending(self):
        exchanges = {
            "A": {"net_flow": 10.0},
            "B": {"net_flow": 30.0},
            "C": {"net_flow": 20.0},
        }
        ranked = _enf_exchange_rank(exchanges)
        assert ranked[0][0] == "B"
        assert ranked[1][0] == "C"
        assert ranked[2][0] == "A"

    def test_negative_flows_at_bottom(self):
        exchanges = {
            "A": {"net_flow": 100.0},
            "B": {"net_flow": -50.0},
            "C": {"net_flow": 200.0},
        }
        ranked = _enf_exchange_rank(exchanges)
        assert ranked[-1][0] == "B"

    def test_returns_list_of_tuples(self):
        exchanges = {"A": {"net_flow": 5.0}, "B": {"net_flow": 3.0}}
        ranked = _enf_exchange_rank(exchanges)
        assert isinstance(ranked, list)
        assert all(isinstance(item, tuple) and len(item) == 2 for item in ranked)

    def test_preserves_all_exchanges(self):
        exchanges = {k: {"net_flow": float(i)} for i, k in enumerate("ABCDE")}
        ranked = _enf_exchange_rank(exchanges)
        assert len(ranked) == 5


# ===========================================================================
# 6. _enf_trend
# ===========================================================================

class TestEnfTrend:
    def test_empty_is_stable(self):
        assert _enf_trend([]) == "stable"

    def test_single_is_stable(self):
        assert _enf_trend([100.0]) == "stable"

    def test_rising_values_is_increasing(self):
        assert _enf_trend([10.0, 20.0, 30.0, 40.0, 50.0]) == "increasing"

    def test_falling_values_is_decreasing(self):
        assert _enf_trend([50.0, 40.0, 30.0, 20.0, 10.0]) == "decreasing"

    def test_flat_values_is_stable(self):
        assert _enf_trend([100.0] * 7) == "stable"

    def test_returns_valid_string(self):
        result = _enf_trend([1.0, 2.0, 3.0])
        assert result in ("increasing", "decreasing", "stable")


# ===========================================================================
# 7. _enf_zscore
# ===========================================================================

class TestEnfZscore:
    def test_empty_returns_zero(self):
        assert _enf_zscore(100.0, []) == 0.0

    def test_single_returns_zero(self):
        assert _enf_zscore(100.0, [100.0]) == 0.0

    def test_current_at_mean_returns_near_zero(self):
        history = [10.0, 20.0, 30.0, 40.0, 50.0]
        mean = 30.0
        assert abs(_enf_zscore(mean, history)) < 0.01

    def test_above_mean_returns_positive(self):
        history = [10.0, 15.0, 20.0, 25.0]
        assert _enf_zscore(100.0, history) > 0

    def test_below_mean_returns_negative(self):
        history = [100.0, 110.0, 120.0, 130.0]
        assert _enf_zscore(0.0, history) < 0

    def test_uniform_history_returns_zero(self):
        history = [50.0] * 10
        assert _enf_zscore(50.0, history) == pytest.approx(0.0, abs=0.01)

    def test_returns_float(self):
        assert isinstance(_enf_zscore(50.0, [30.0, 70.0]), float)


# ===========================================================================
# 8. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_exchanges_dict(self):
        assert isinstance(SAMPLE_RESPONSE["exchanges"], dict)

    def test_has_all_five_exchanges(self):
        for ex in ("Binance", "Coinbase", "OKX", "Bybit", "Kraken"):
            assert ex in SAMPLE_RESPONSE["exchanges"], f"{ex} missing"

    def test_each_exchange_has_required_keys(self):
        for name, ex in SAMPLE_RESPONSE["exchanges"].items():
            for key in ("inflow_proxy", "outflow_proxy", "net_flow", "direction"):
                assert key in ex, f"{name} missing '{key}'"

    def test_exchange_direction_valid(self):
        for name, ex in SAMPLE_RESPONSE["exchanges"].items():
            assert ex["direction"] in ("inflow", "outflow", "neutral"), \
                f"{name} has invalid direction"

    def test_has_aggregate(self):
        assert "aggregate" in SAMPLE_RESPONSE

    def test_aggregate_has_required_keys(self):
        agg = SAMPLE_RESPONSE["aggregate"]
        for key in ("net_flow_24h", "net_flow_7d", "signal", "strength", "trend", "zscore"):
            assert key in agg, f"aggregate missing '{key}'"

    def test_signal_valid(self):
        assert SAMPLE_RESPONSE["aggregate"]["signal"] in (
            "accumulation", "distribution", "neutral"
        )

    def test_strength_in_range(self):
        s = SAMPLE_RESPONSE["aggregate"]["strength"]
        assert 0 <= s <= 100

    def test_trend_valid(self):
        assert SAMPLE_RESPONSE["aggregate"]["trend"] in (
            "increasing", "decreasing", "stable"
        )

    def test_has_history_list(self):
        assert isinstance(SAMPLE_RESPONSE["history"], list)

    def test_history_items_have_required_keys(self):
        for item in SAMPLE_RESPONSE["history"]:
            for key in ("date", "net_flow", "direction"):
                assert key in item, f"history item missing '{key}'"

    def test_has_description(self):
        assert isinstance(SAMPLE_RESPONSE["description"], str)


# ===========================================================================
# 9. Structural tests
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api_path = os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")
        content = open(api_path).read()
        assert "/exchange-netflow" in content, "/exchange-netflow route missing"

    def test_html_card_exists(self):
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        content = open(html_path).read()
        assert "card-exchange-netflow" in content, "card-exchange-netflow missing"

    def test_js_render_function_exists(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "renderExchangeNetflow" in content, "renderExchangeNetflow missing"

    def test_js_api_call_to_endpoint(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "/exchange-netflow" in content, "/exchange-netflow call missing"
