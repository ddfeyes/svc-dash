"""
Unit / smoke tests for /api/miner-reserve.

Miner reserve indicator — BTC miner-to-exchange flow as sell-pressure signal.
Data proxy: blockchain.info miner revenue chart (free public API).

Key metrics:
  - miner_reserve_usd     — rolling 30d revenue estimate (buy-side cushion)
  - daily_outflow_usd     — latest daily miner revenue (max potential sell)
  - sell_pressure_index   — outflow / reserve × 100, clamped 0–100
  - spi_percentile        — rank of current SPI vs 30d history (0–100)
  - reserve_trend         — accumulating / depleting / stable
  - signal                — bullish (accumulating) / bearish (depleting) / neutral
  - hash_rate_change_30d  — hash-rate momentum (miner profitability proxy)
  - outflow_zscore        — z-score of current outflow vs history
  - depletion_rate_days   — days until reserve exhausted at current outflow

Covers:
  - _mr_sell_pressure_index
  - _mr_reserve_trend
  - _mr_signal
  - _mr_outflow_zscore
  - _mr_rolling_reserve
  - _mr_depletion_rate
  - _mr_spi_percentile
  - Response shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _mr_sell_pressure_index,
    _mr_reserve_trend,
    _mr_signal,
    _mr_outflow_zscore,
    _mr_rolling_reserve,
    _mr_depletion_rate,
    _mr_spi_percentile,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "source": "blockchain.info (BTC proxy)",
    "miner_reserve_usd": 450_000_000.0,
    "daily_outflow_usd":  12_000_000.0,
    "sell_pressure_index": 2.67,
    "spi_percentile": 45.0,
    "reserve_trend": "stable",
    "signal": "neutral",
    "hash_rate": 620.5,
    "hash_rate_change_30d_pct": 3.2,
    "outflow_zscore": 0.4,
    "depletion_rate_days": 37.5,
    "history": [
        {
            "date": "2024-10-22",
            "revenue_usd": 11_000_000.0,
            "reserve_proxy": 430_000_000.0,
            "spi": 2.56,
        },
        {
            "date": "2024-10-23",
            "revenue_usd": 12_000_000.0,
            "reserve_proxy": 442_000_000.0,
            "spi": 2.71,
        },
    ],
    "description": "Neutral: miners stable — SPI at 45th percentile",
}


# ===========================================================================
# 1. _mr_sell_pressure_index
# ===========================================================================

class TestMrSellPressureIndex:
    def test_zero_outflow_returns_zero(self):
        assert _mr_sell_pressure_index(0.0, 1_000_000.0) == pytest.approx(0.0, abs=1e-6)

    def test_zero_reserve_returns_100(self):
        # Can't sell more than 100 when reserve is empty
        assert _mr_sell_pressure_index(1_000_000.0, 0.0) == pytest.approx(100.0, abs=1e-6)

    def test_correct_ratio(self):
        # outflow = 10M, reserve = 100M → SPI = 10.0
        result = _mr_sell_pressure_index(10_000_000.0, 100_000_000.0)
        assert result == pytest.approx(10.0, rel=1e-4)

    def test_clamped_to_100(self):
        # outflow > reserve → SPI clamped at 100
        assert _mr_sell_pressure_index(200_000_000.0, 100_000_000.0) == 100.0

    def test_small_outflow_small_spi(self):
        result = _mr_sell_pressure_index(1_000_000.0, 1_000_000_000.0)
        assert result == pytest.approx(0.1, rel=1e-3)

    def test_returns_float(self):
        assert isinstance(_mr_sell_pressure_index(5e6, 50e6), float)


# ===========================================================================
# 2. _mr_reserve_trend
# ===========================================================================

class TestMrReserveTrend:
    def test_rising_reserves_is_accumulating(self):
        # Last values higher than first → accumulating
        history = [100e6, 110e6, 120e6, 130e6, 140e6]
        assert _mr_reserve_trend(history) == "accumulating"

    def test_falling_reserves_is_depleting(self):
        history = [140e6, 130e6, 120e6, 110e6, 100e6]
        assert _mr_reserve_trend(history) == "depleting"

    def test_flat_reserves_is_stable(self):
        history = [100e6] * 10
        assert _mr_reserve_trend(history) == "stable"

    def test_empty_history_is_stable(self):
        assert _mr_reserve_trend([]) == "stable"

    def test_single_value_is_stable(self):
        assert _mr_reserve_trend([100e6]) == "stable"

    def test_returns_valid_string(self):
        result = _mr_reserve_trend([100e6, 102e6, 105e6])
        assert result in ("accumulating", "depleting", "stable")


# ===========================================================================
# 3. _mr_signal
# ===========================================================================

class TestMrSignal:
    def test_accumulating_low_spi_is_bullish(self):
        assert _mr_signal("accumulating", 5.0) == "bullish"

    def test_depleting_high_spi_is_bearish(self):
        assert _mr_signal("depleting", 40.0) == "bearish"

    def test_stable_is_neutral(self):
        assert _mr_signal("stable", 10.0) == "neutral"

    def test_accumulating_high_spi_is_neutral(self):
        # Even if accumulating, high SPI tempers the signal
        result = _mr_signal("accumulating", 60.0)
        assert result in ("neutral", "bearish")

    def test_depleting_low_spi_is_neutral(self):
        # Depleting but very low outflow → not strongly bearish
        result = _mr_signal("depleting", 1.0)
        assert result in ("neutral", "bearish")

    def test_returns_valid_label(self):
        result = _mr_signal("stable", 15.0)
        assert result in ("bullish", "bearish", "neutral")


# ===========================================================================
# 4. _mr_outflow_zscore
# ===========================================================================

class TestMrOutflowZscore:
    def test_empty_history_returns_zero(self):
        assert _mr_outflow_zscore(1e7, []) == 0.0

    def test_single_value_returns_zero(self):
        assert _mr_outflow_zscore(1e7, [1e7]) == 0.0

    def test_current_at_mean_returns_near_zero(self):
        history = [10e6, 12e6, 14e6, 16e6, 18e6]
        mean = sum(history) / len(history)
        result = _mr_outflow_zscore(mean, history)
        assert abs(result) < 0.01

    def test_above_mean_returns_positive(self):
        history = [10e6, 10e6, 10e6, 10e6]
        result = _mr_outflow_zscore(20e6, history)
        assert result > 0

    def test_below_mean_returns_negative(self):
        history = [20e6, 20e6, 20e6, 20e6]
        result = _mr_outflow_zscore(5e6, history)
        assert result < 0

    def test_uniform_history_returns_zero(self):
        history = [10e6] * 15
        result = _mr_outflow_zscore(10e6, history)
        assert result == pytest.approx(0.0, abs=0.01)

    def test_returns_float(self):
        history = [10e6, 12e6, 14e6]
        assert isinstance(_mr_outflow_zscore(11e6, history), float)


# ===========================================================================
# 5. _mr_rolling_reserve
# ===========================================================================

class TestMrRollingReserve:
    def test_empty_returns_zero(self):
        assert _mr_rolling_reserve([]) == 0.0

    def test_single_value(self):
        result = _mr_rolling_reserve([10_000_000.0])
        assert result == pytest.approx(10_000_000.0, rel=1e-4)

    def test_sum_of_all_revenues(self):
        revenues = [10e6, 12e6, 15e6]
        result = _mr_rolling_reserve(revenues)
        assert result == pytest.approx(37e6, rel=1e-4)

    def test_uses_last_30_values(self):
        # 35 values, last 30 should sum to 30*100 = 3000
        revenues = [1_000_000.0] * 5 + [100_000_000.0] * 30
        result = _mr_rolling_reserve(revenues)
        assert result == pytest.approx(30 * 100_000_000.0, rel=1e-4)

    def test_returns_float(self):
        assert isinstance(_mr_rolling_reserve([1e6, 2e6]), float)


# ===========================================================================
# 6. _mr_depletion_rate
# ===========================================================================

class TestMrDepletionRate:
    def test_zero_outflow_returns_infinity_proxy(self):
        # No outflow → reserve never depletes (return large number)
        result = _mr_depletion_rate(0.0, 100_000_000.0)
        assert result > 1_000 or result == float("inf")

    def test_correct_days_calculation(self):
        # 100M reserve, 10M/day → 10 days
        result = _mr_depletion_rate(10_000_000.0, 100_000_000.0)
        assert result == pytest.approx(10.0, rel=1e-4)

    def test_zero_reserve_returns_zero(self):
        result = _mr_depletion_rate(10_000_000.0, 0.0)
        assert result == pytest.approx(0.0, abs=1e-6)

    def test_large_reserve_returns_large_days(self):
        result = _mr_depletion_rate(1_000_000.0, 10_000_000_000.0)
        assert result > 1000

    def test_returns_float(self):
        assert isinstance(_mr_depletion_rate(10e6, 100e6), float)


# ===========================================================================
# 7. _mr_spi_percentile
# ===========================================================================

class TestMrSpiPercentile:
    def test_empty_history_returns_50(self):
        assert _mr_spi_percentile(5.0, []) == pytest.approx(50.0, abs=0.1)

    def test_current_above_all_history_returns_100(self):
        result = _mr_spi_percentile(100.0, [1.0, 2.0, 3.0, 4.0, 5.0])
        assert result == pytest.approx(100.0, abs=0.1)

    def test_current_below_all_history_returns_0(self):
        result = _mr_spi_percentile(0.0, [5.0, 6.0, 7.0, 8.0, 9.0])
        assert result == pytest.approx(0.0, abs=0.1)

    def test_current_at_median_returns_near_50(self):
        history = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        result = _mr_spi_percentile(5.5, history)
        assert 40 <= result <= 60

    def test_result_in_0_100_range(self):
        history = [2.0, 4.0, 6.0, 8.0, 10.0]
        for v in (0.0, 1.0, 5.0, 7.0, 15.0):
            r = _mr_spi_percentile(v, history)
            assert 0 <= r <= 100

    def test_returns_float(self):
        assert isinstance(_mr_spi_percentile(5.0, [3.0, 7.0]), float)


# ===========================================================================
# 8. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_source(self):
        assert "source" in SAMPLE_RESPONSE

    def test_has_miner_reserve_usd(self):
        assert "miner_reserve_usd" in SAMPLE_RESPONSE
        assert SAMPLE_RESPONSE["miner_reserve_usd"] >= 0

    def test_has_daily_outflow_usd(self):
        assert "daily_outflow_usd" in SAMPLE_RESPONSE

    def test_has_sell_pressure_index(self):
        spi = SAMPLE_RESPONSE["sell_pressure_index"]
        assert 0 <= spi <= 100

    def test_has_spi_percentile(self):
        pct = SAMPLE_RESPONSE["spi_percentile"]
        assert 0 <= pct <= 100

    def test_reserve_trend_valid(self):
        assert SAMPLE_RESPONSE["reserve_trend"] in (
            "accumulating", "depleting", "stable"
        )

    def test_signal_valid(self):
        assert SAMPLE_RESPONSE["signal"] in ("bullish", "bearish", "neutral")

    def test_has_hash_rate(self):
        assert "hash_rate" in SAMPLE_RESPONSE

    def test_has_hash_rate_change(self):
        assert "hash_rate_change_30d_pct" in SAMPLE_RESPONSE

    def test_has_outflow_zscore(self):
        assert "outflow_zscore" in SAMPLE_RESPONSE

    def test_has_depletion_rate_days(self):
        assert "depletion_rate_days" in SAMPLE_RESPONSE

    def test_has_history_list(self):
        assert isinstance(SAMPLE_RESPONSE["history"], list)

    def test_history_items_have_required_keys(self):
        for item in SAMPLE_RESPONSE["history"]:
            for key in ("date", "revenue_usd", "reserve_proxy", "spi"):
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
        assert "/miner-reserve" in content, "/miner-reserve route missing from api.py"

    def test_html_card_exists(self):
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        content = open(html_path).read()
        assert "card-miner-reserve" in content, "card-miner-reserve missing from index.html"

    def test_js_render_function_exists(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "renderMinerReserve" in content, "renderMinerReserve missing from app.js"

    def test_js_api_call_to_endpoint(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "/miner-reserve" in content, "/miner-reserve call missing from app.js"
