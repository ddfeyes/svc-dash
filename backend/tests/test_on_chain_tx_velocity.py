"""
Unit tests for compute_on_chain_tx_velocity and its helper functions.

On-chain transaction velocity tracker — ETH/SOL/BNB TPS, fee revenue,
congestion index, throughput percentile, 24h history.

Covers:
  - _octv_seed_for_symbol
  - _octv_tps
  - _octv_fee_revenue_per_block
  - _octv_avg_tx_value
  - _octv_congestion_index
  - _octv_throughput_percentile
  - _octv_trend
  - compute_on_chain_tx_velocity (async, full response shape)
"""

import sys
import os
import math
import random
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from metrics import (
    _octv_seed_for_symbol,
    _octv_tps,
    _octv_fee_revenue_per_block,
    _octv_avg_tx_value,
    _octv_congestion_index,
    _octv_throughput_percentile,
    _octv_trend,
    compute_on_chain_tx_velocity,
)


# ===========================================================================
# 1. _octv_seed_for_symbol
# ===========================================================================

class TestOctvSeedForSymbol:
    def test_returns_int(self):
        assert isinstance(_octv_seed_for_symbol("BANANAS31USDT"), int)

    def test_known_symbols_return_different_seeds(self):
        seeds = {_octv_seed_for_symbol(s) for s in
                 ("BANANAS31USDT", "COSUSDT", "DEXEUSDT", "LYNUSDT")}
        assert len(seeds) == 4

    def test_same_symbol_same_seed(self):
        assert _octv_seed_for_symbol("COSUSDT") == _octv_seed_for_symbol("COSUSDT")

    def test_unknown_symbol_returns_int(self):
        assert isinstance(_octv_seed_for_symbol("XYZUSDT"), int)

    def test_unknown_symbol_non_zero(self):
        # sum of ord chars for a non-empty string is > 0
        assert _octv_seed_for_symbol("XYZUSDT") > 0


# ===========================================================================
# 2. _octv_tps
# ===========================================================================

class TestOctvTps:
    def _rng(self, seed: int = 42) -> random.Random:
        return random.Random(seed)

    def test_eth_tps_positive(self):
        assert _octv_tps("ETH", self._rng()) > 0.0

    def test_sol_tps_positive(self):
        assert _octv_tps("SOL", self._rng()) > 0.0

    def test_bnb_tps_positive(self):
        assert _octv_tps("BNB", self._rng()) > 0.0

    def test_sol_tps_much_greater_than_eth(self):
        rng1 = self._rng(1)
        rng2 = self._rng(1)
        eth = _octv_tps("ETH", rng1)
        sol = _octv_tps("SOL", rng2)
        assert sol > eth * 10

    def test_bnb_tps_greater_than_eth(self):
        rng1 = self._rng(2)
        rng2 = self._rng(2)
        eth = _octv_tps("ETH", rng1)
        bnb = _octv_tps("BNB", rng2)
        assert bnb > eth

    def test_returns_float(self):
        assert isinstance(_octv_tps("ETH", self._rng()), float)

    def test_eth_tps_within_expected_range(self):
        # ETH base ~15, ±20% → [12, 18]
        for seed in range(10):
            val = _octv_tps("ETH", random.Random(seed))
            assert 5.0 <= val <= 50.0

    def test_different_rng_seeds_different_values(self):
        vals = {_octv_tps("ETH", random.Random(s)) for s in range(5)}
        assert len(vals) > 1


# ===========================================================================
# 3. _octv_fee_revenue_per_block
# ===========================================================================

class TestOctvFeeRevenuePerBlock:
    def test_eth_fee_revenue_positive(self):
        assert _octv_fee_revenue_per_block("ETH", 15.0) > 0.0

    def test_sol_fee_revenue_positive(self):
        assert _octv_fee_revenue_per_block("SOL", 3000.0) > 0.0

    def test_bnb_fee_revenue_positive(self):
        assert _octv_fee_revenue_per_block("BNB", 100.0) > 0.0

    def test_eth_fee_revenue_greater_than_sol(self):
        eth_rev = _octv_fee_revenue_per_block("ETH", 15.0)
        sol_rev = _octv_fee_revenue_per_block("SOL", 3000.0)
        # ETH has much higher fee per tx ($5 vs $0.00025), so per-block dominates
        assert eth_rev > sol_rev

    def test_zero_tps_returns_zero(self):
        assert _octv_fee_revenue_per_block("ETH", 0.0) == pytest.approx(0.0, abs=1e-9)

    def test_higher_tps_higher_fee_revenue(self):
        low  = _octv_fee_revenue_per_block("ETH", 10.0)
        high = _octv_fee_revenue_per_block("ETH", 30.0)
        assert high > low

    def test_returns_float(self):
        assert isinstance(_octv_fee_revenue_per_block("ETH", 15.0), float)

    def test_proportional_to_tps(self):
        rev_10 = _octv_fee_revenue_per_block("ETH", 10.0)
        rev_20 = _octv_fee_revenue_per_block("ETH", 20.0)
        assert rev_20 == pytest.approx(rev_10 * 2.0, rel=1e-4)


# ===========================================================================
# 4. _octv_avg_tx_value
# ===========================================================================

class TestOctvAvgTxValue:
    def _rng(self, seed: int = 42) -> random.Random:
        return random.Random(seed)

    def test_eth_avg_tx_value_positive(self):
        assert _octv_avg_tx_value("ETH", self._rng()) > 0.0

    def test_sol_avg_tx_value_positive(self):
        assert _octv_avg_tx_value("SOL", self._rng()) > 0.0

    def test_bnb_avg_tx_value_positive(self):
        assert _octv_avg_tx_value("BNB", self._rng()) > 0.0

    def test_eth_avg_greater_than_sol(self):
        eth = _octv_avg_tx_value("ETH", self._rng(7))
        sol = _octv_avg_tx_value("SOL", self._rng(7))
        # ETH base ~$850, SOL base ~$120
        assert eth > sol

    def test_returns_float(self):
        assert isinstance(_octv_avg_tx_value("ETH", self._rng()), float)

    def test_within_reasonable_range(self):
        # ETH base ~$850 ±15%
        for seed in range(5):
            val = _octv_avg_tx_value("ETH", random.Random(seed))
            assert 500.0 <= val <= 1500.0


# ===========================================================================
# 5. _octv_congestion_index
# ===========================================================================

class TestOctvCongestionIndex:
    def test_zero_tps_returns_zero(self):
        assert _octv_congestion_index(0.0, "ETH") == pytest.approx(0.0, abs=1e-9)

    def test_returns_float(self):
        assert isinstance(_octv_congestion_index(15.0, "ETH"), float)

    def test_result_0_to_100(self):
        for tps in (0.0, 10.0, 30.0, 50.0, 100.0):
            val = _octv_congestion_index(tps, "ETH")
            assert 0.0 <= val <= 100.0

    def test_higher_tps_higher_congestion(self):
        low  = _octv_congestion_index(5.0,  "ETH")
        high = _octv_congestion_index(40.0, "ETH")
        assert high > low

    def test_max_tps_clamps_to_100(self):
        # passing TPS >> max
        val = _octv_congestion_index(1_000_000.0, "ETH")
        assert val == pytest.approx(100.0, abs=1e-6)

    def test_sol_congestion_positive(self):
        assert _octv_congestion_index(3000.0, "SOL") > 0.0

    def test_bnb_congestion_within_range(self):
        val = _octv_congestion_index(100.0, "BNB")
        assert 0.0 <= val <= 100.0

    def test_never_negative(self):
        assert _octv_congestion_index(-5.0, "ETH") >= 0.0


# ===========================================================================
# 6. _octv_throughput_percentile
# ===========================================================================

class TestOctvThroughputPercentile:
    def test_empty_history_returns_50(self):
        assert _octv_throughput_percentile([], 15.0) == pytest.approx(50.0, rel=1e-4)

    def test_current_above_all_returns_100(self):
        hist = [5.0, 10.0, 12.0]
        assert _octv_throughput_percentile(hist, 20.0) == pytest.approx(100.0, rel=1e-4)

    def test_current_below_all_returns_low(self):
        hist = [10.0, 20.0, 30.0]
        result = _octv_throughput_percentile(hist, 1.0)
        assert result < 50.0

    def test_returns_float(self):
        assert isinstance(_octv_throughput_percentile([5.0, 10.0], 7.0), float)

    def test_result_0_to_100(self):
        for curr in (0.0, 5.0, 15.0, 50.0):
            val = _octv_throughput_percentile([5.0, 10.0, 20.0], curr)
            assert 0.0 <= val <= 100.0

    def test_monotone_with_current_tps(self):
        hist = [5.0, 10.0, 15.0, 20.0]
        p1 = _octv_throughput_percentile(hist, 7.0)
        p2 = _octv_throughput_percentile(hist, 12.0)
        p3 = _octv_throughput_percentile(hist, 18.0)
        assert p1 <= p2 <= p3

    def test_single_element_history(self):
        val = _octv_throughput_percentile([10.0], 10.0)
        assert 0.0 <= val <= 100.0


# ===========================================================================
# 7. _octv_trend
# ===========================================================================

class TestOctvTrend:
    def test_rising_values_returns_rising(self):
        history = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0]
        assert _octv_trend(history) == "rising"

    def test_falling_values_returns_falling(self):
        history = [20.0, 19.0, 18.0, 17.0, 16.0, 15.0, 14.0, 13.0, 12.0, 11.0]
        assert _octv_trend(history) == "falling"

    def test_stable_values_returns_stable(self):
        history = [15.0] * 10
        assert _octv_trend(history) == "stable"

    def test_empty_list_returns_stable(self):
        assert _octv_trend([]) == "stable"

    def test_single_value_returns_stable(self):
        assert _octv_trend([15.0]) == "stable"

    def test_two_equal_values_returns_stable(self):
        assert _octv_trend([15.0, 15.0]) == "stable"

    def test_returns_valid_string(self):
        for h in ([10.0, 20.0], [20.0, 10.0], [15.0, 15.0]):
            assert _octv_trend(h) in ("rising", "stable", "falling")

    def test_strongly_rising_detects_rising(self):
        # second half avg >> first half avg
        history = [5.0, 5.0, 5.0, 5.0, 5.0, 20.0, 20.0, 20.0, 20.0, 20.0]
        assert _octv_trend(history) == "rising"

    def test_strongly_falling_detects_falling(self):
        history = [20.0, 20.0, 20.0, 20.0, 20.0, 5.0, 5.0, 5.0, 5.0, 5.0]
        assert _octv_trend(history) == "falling"


# ===========================================================================
# 8. compute_on_chain_tx_velocity (async, full response)
# ===========================================================================

@pytest.mark.asyncio
async def test_returns_dict():
    result = await compute_on_chain_tx_velocity("BANANAS31USDT")
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_has_required_keys():
    result = await compute_on_chain_tx_velocity("BANANAS31USDT")
    for key in ("eth_tps", "sol_tps", "bnb_tps", "fee_revenue_block",
                "avg_tx_value", "congestion_index", "throughput_percentile",
                "trend", "tps_history_24h"):
        assert key in result, f"Missing key: {key}"


@pytest.mark.asyncio
async def test_eth_tps_positive():
    result = await compute_on_chain_tx_velocity("COSUSDT")
    assert result["eth_tps"] > 0.0


@pytest.mark.asyncio
async def test_sol_tps_positive():
    result = await compute_on_chain_tx_velocity("COSUSDT")
    assert result["sol_tps"] > 0.0


@pytest.mark.asyncio
async def test_bnb_tps_positive():
    result = await compute_on_chain_tx_velocity("DEXEUSDT")
    assert result["bnb_tps"] > 0.0


@pytest.mark.asyncio
async def test_sol_tps_greater_than_eth():
    result = await compute_on_chain_tx_velocity("BANANAS31USDT")
    assert result["sol_tps"] > result["eth_tps"]


@pytest.mark.asyncio
async def test_bnb_tps_greater_than_eth():
    result = await compute_on_chain_tx_velocity("BANANAS31USDT")
    assert result["bnb_tps"] > result["eth_tps"]


@pytest.mark.asyncio
async def test_fee_revenue_block_positive():
    result = await compute_on_chain_tx_velocity("LYNUSDT")
    assert result["fee_revenue_block"] > 0.0


@pytest.mark.asyncio
async def test_avg_tx_value_positive():
    result = await compute_on_chain_tx_velocity("LYNUSDT")
    assert result["avg_tx_value"] > 0.0


@pytest.mark.asyncio
async def test_congestion_index_range():
    result = await compute_on_chain_tx_velocity("BANANAS31USDT")
    ci = result["congestion_index"]
    assert 0.0 <= ci <= 100.0


@pytest.mark.asyncio
async def test_throughput_percentile_range():
    result = await compute_on_chain_tx_velocity("BANANAS31USDT")
    tp = result["throughput_percentile"]
    assert 0.0 <= tp <= 100.0


@pytest.mark.asyncio
async def test_trend_valid_values():
    result = await compute_on_chain_tx_velocity("COSUSDT")
    assert result["trend"] in ("rising", "stable", "falling")


@pytest.mark.asyncio
async def test_tps_history_24h_is_list():
    result = await compute_on_chain_tx_velocity("BANANAS31USDT")
    assert isinstance(result["tps_history_24h"], list)


@pytest.mark.asyncio
async def test_tps_history_24h_has_24_entries():
    result = await compute_on_chain_tx_velocity("BANANAS31USDT")
    assert len(result["tps_history_24h"]) == 24


@pytest.mark.asyncio
async def test_tps_history_entries_have_chain_keys():
    result = await compute_on_chain_tx_velocity("BANANAS31USDT")
    for entry in result["tps_history_24h"]:
        assert "eth" in entry
        assert "sol" in entry
        assert "bnb" in entry


@pytest.mark.asyncio
async def test_tps_history_values_positive():
    result = await compute_on_chain_tx_velocity("DEXEUSDT")
    for entry in result["tps_history_24h"]:
        assert entry["eth"] > 0.0
        assert entry["sol"] > 0.0
        assert entry["bnb"] > 0.0


@pytest.mark.asyncio
async def test_deterministic_same_symbol():
    r1 = await compute_on_chain_tx_velocity("BANANAS31USDT")
    r2 = await compute_on_chain_tx_velocity("BANANAS31USDT")
    assert r1["eth_tps"] == r2["eth_tps"]
    assert r1["sol_tps"] == r2["sol_tps"]
    assert r1["bnb_tps"] == r2["bnb_tps"]


@pytest.mark.asyncio
async def test_different_symbols_different_eth_tps():
    r1 = await compute_on_chain_tx_velocity("BANANAS31USDT")
    r2 = await compute_on_chain_tx_velocity("COSUSDT")
    # Different seeds → different values
    assert r1["eth_tps"] != r2["eth_tps"]


@pytest.mark.asyncio
async def test_fee_revenue_is_float():
    result = await compute_on_chain_tx_velocity("COSUSDT")
    assert isinstance(result["fee_revenue_block"], float)


@pytest.mark.asyncio
async def test_avg_tx_value_is_float():
    result = await compute_on_chain_tx_velocity("LYNUSDT")
    assert isinstance(result["avg_tx_value"], float)


@pytest.mark.asyncio
async def test_congestion_index_is_float():
    result = await compute_on_chain_tx_velocity("DEXEUSDT")
    assert isinstance(result["congestion_index"], float)


@pytest.mark.asyncio
async def test_throughput_percentile_is_float():
    result = await compute_on_chain_tx_velocity("DEXEUSDT")
    assert isinstance(result["throughput_percentile"], float)


@pytest.mark.asyncio
async def test_all_symbols_work():
    for sym in ("BANANAS31USDT", "COSUSDT", "DEXEUSDT", "LYNUSDT"):
        result = await compute_on_chain_tx_velocity(sym)
        assert result["eth_tps"] > 0.0


@pytest.mark.asyncio
async def test_tps_history_sol_greater_than_eth_per_entry():
    result = await compute_on_chain_tx_velocity("BANANAS31USDT")
    for entry in result["tps_history_24h"]:
        assert entry["sol"] > entry["eth"]
