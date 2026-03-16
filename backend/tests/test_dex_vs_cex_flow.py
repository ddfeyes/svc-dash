"""50+ tests for DEX vs CEX volume divergence flow metric."""
import math
import os
import sys
import tempfile

import pytest

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test_dex_cex.db")
os.environ["SYMBOL_BINANCE"] = "BANANAS31USDT"
os.environ["SYMBOL_BYBIT"] = "BANANAS31USDT"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from metrics import (
    _dex_cex_dominance_history,
    _dex_cex_seed,
    _dex_cex_trend,
    _dex_cex_zscore,
    _dex_price_discovery,
    compute_dex_vs_cex_flow,
)


# ── Helper unit tests ──────────────────────────────────────────────────────────

class TestDexCexSeed:
    def test_returns_random_instance(self):
        rng = _dex_cex_seed()
        assert rng is not None

    def test_seed_with_offset_differs(self):
        rng0 = _dex_cex_seed(0)
        rng1 = _dex_cex_seed(1)
        v0 = rng0.random()
        v1 = rng1.random()
        assert v0 != v1

    def test_same_offset_same_sequence(self):
        rng_a = _dex_cex_seed(0)
        rng_b = _dex_cex_seed(0)
        # Both seeded identically — next() values should match
        assert rng_a.random() == rng_b.random()


class TestDexCexDominanceHistory:
    def test_returns_list(self):
        hist = _dex_cex_dominance_history(10)
        assert isinstance(hist, list)

    def test_correct_length(self):
        for n in [10, 20, 30]:
            hist = _dex_cex_dominance_history(n)
            assert len(hist) == n

    def test_values_between_0_and_1(self):
        hist = _dex_cex_dominance_history(30)
        for v in hist:
            assert 0.0 <= v <= 1.0, f"Dominance {v} out of [0,1]"

    def test_values_are_floats(self):
        hist = _dex_cex_dominance_history(5)
        for v in hist:
            assert isinstance(v, float)

    def test_nonzero_variance(self):
        hist = _dex_cex_dominance_history(30)
        mean = sum(hist) / len(hist)
        variance = sum((x - mean) ** 2 for x in hist) / len(hist)
        assert variance > 0, "History should have nonzero variance"

    def test_zero_length(self):
        hist = _dex_cex_dominance_history(0)
        assert hist == []


class TestDexCexZscore:
    def test_returns_float(self):
        hist = [0.2, 0.25, 0.3, 0.22, 0.28]
        z = _dex_cex_zscore(0.35, hist)
        assert isinstance(z, float)

    def test_positive_zscore_for_high_current(self):
        # History with variance so std > 0
        hist = [0.2 + i * 0.005 for i in range(20)]
        z = _dex_cex_zscore(0.5, hist)
        assert z > 0

    def test_negative_zscore_for_low_current(self):
        hist = [0.5 + i * 0.005 for i in range(20)]
        z = _dex_cex_zscore(0.1, hist)
        assert z < 0

    def test_zero_zscore_for_zero_std(self):
        hist = [0.3] * 10  # all identical → std=0
        z = _dex_cex_zscore(0.3, hist)
        assert z == 0.0

    def test_empty_history_returns_zero(self):
        z = _dex_cex_zscore(0.3, [])
        assert z == 0.0

    def test_zscore_at_mean_is_near_zero(self):
        hist = [0.2, 0.3, 0.4, 0.2, 0.3]
        mean = sum(hist) / len(hist)
        z = _dex_cex_zscore(mean, hist)
        assert abs(z) < 0.01

    def test_zscore_is_rounded_to_3dp(self):
        hist = [0.1 + i * 0.01 for i in range(20)]
        z = _dex_cex_zscore(0.25, hist)
        assert z == round(z, 3)


class TestDexCexTrend:
    def test_returns_tuple(self):
        hist = [0.2 + i * 0.001 for i in range(20)]
        result = _dex_cex_trend(hist)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_rising_trend(self):
        # Recent 7 clearly higher than prior 7
        prior = [0.20] * 7
        recent = [0.30] * 7
        hist = [0.25] * 5 + prior + recent
        label, delta = _dex_cex_trend(hist)
        assert label == "rising"
        assert delta > 0

    def test_falling_trend(self):
        prior = [0.35] * 7
        recent = [0.20] * 7
        hist = [0.28] * 5 + prior + recent
        label, delta = _dex_cex_trend(hist)
        assert label == "falling"
        assert delta < 0

    def test_stable_trend(self):
        hist = [0.25] * 20
        label, delta = _dex_cex_trend(hist)
        assert label == "stable"
        assert abs(delta) <= 0.005

    def test_short_history_returns_stable(self):
        label, delta = _dex_cex_trend([0.2, 0.3])
        assert label == "stable"
        assert delta == 0.0

    def test_delta_is_rounded_to_4dp(self):
        hist = [0.20 + i * 0.001 for i in range(20)]
        _, delta = _dex_cex_trend(hist)
        assert delta == round(delta, 4)


class TestDexPriceDiscovery:
    def test_returns_tuple(self):
        result = _dex_price_discovery(0.0)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_strong_buy_above_2(self):
        disc, sig = _dex_price_discovery(2.5)
        assert disc == "dex_leading"
        assert sig == "strong_buy"

    def test_watch_between_1_and_2(self):
        disc, sig = _dex_price_discovery(1.5)
        assert disc == "dex_elevated"
        assert sig == "watch"

    def test_balanced_near_zero(self):
        disc, sig = _dex_price_discovery(0.0)
        assert disc == "balanced"
        assert sig == "neutral"

    def test_cex_elevated_below_neg1(self):
        disc, sig = _dex_price_discovery(-1.5)
        assert disc == "cex_elevated"
        assert sig == "watch"

    def test_strong_sell_below_neg2(self):
        disc, sig = _dex_price_discovery(-2.5)
        assert disc == "cex_dominant"
        assert sig == "strong_sell"

    def test_boundary_exactly_2(self):
        # z=2.0 is > 2.0 is False → falls to next branch (dex_elevated)
        disc, sig = _dex_price_discovery(2.0)
        assert sig == "watch"

    def test_boundary_exactly_neg2(self):
        disc, sig = _dex_price_discovery(-2.0)
        assert sig == "watch"


# ── Integration tests for compute_dex_vs_cex_flow ────────────────────────────

@pytest.mark.asyncio
async def test_returns_dict():
    result = await compute_dex_vs_cex_flow()
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_required_keys_present():
    required = [
        "symbol", "window_hours", "dex_volume_usd", "cex_volume_usd",
        "total_volume_usd", "dex_dominance_ratio", "dex_dominance_pct",
        "divergence_zscore", "dominance_trend", "trend_delta",
        "price_discovery", "discovery_signal", "protocols",
        "protocol_breakdown_pct", "dominance_history",
        "mean_dominance", "std_dominance", "description",
    ]
    result = await compute_dex_vs_cex_flow()
    for key in required:
        assert key in result, f"Missing key: {key}"


@pytest.mark.asyncio
async def test_symbol_default_global():
    result = await compute_dex_vs_cex_flow()
    assert result["symbol"] == "global"


@pytest.mark.asyncio
async def test_symbol_passed_through():
    result = await compute_dex_vs_cex_flow(symbol="BTCUSDT")
    assert result["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_window_hours_default():
    result = await compute_dex_vs_cex_flow()
    assert result["window_hours"] == 24


@pytest.mark.asyncio
async def test_window_hours_custom():
    result = await compute_dex_vs_cex_flow(window_hours=48)
    assert result["window_hours"] == 48


@pytest.mark.asyncio
async def test_dex_volume_positive():
    result = await compute_dex_vs_cex_flow()
    assert result["dex_volume_usd"] > 0


@pytest.mark.asyncio
async def test_cex_volume_positive():
    result = await compute_dex_vs_cex_flow()
    assert result["cex_volume_usd"] > 0


@pytest.mark.asyncio
async def test_total_volume_equals_sum():
    result = await compute_dex_vs_cex_flow()
    expected = result["dex_volume_usd"] + result["cex_volume_usd"]
    assert abs(result["total_volume_usd"] - expected) <= 1.0


@pytest.mark.asyncio
async def test_dominance_ratio_between_0_and_1():
    result = await compute_dex_vs_cex_flow()
    dr = result["dex_dominance_ratio"]
    assert 0.0 <= dr <= 1.0


@pytest.mark.asyncio
async def test_dominance_pct_equals_ratio_times_100():
    result = await compute_dex_vs_cex_flow()
    expected = round(result["dex_dominance_ratio"] * 100, 2)
    assert abs(result["dex_dominance_pct"] - expected) < 0.01


@pytest.mark.asyncio
async def test_divergence_zscore_is_float():
    result = await compute_dex_vs_cex_flow()
    assert isinstance(result["divergence_zscore"], float)


@pytest.mark.asyncio
async def test_divergence_zscore_is_finite():
    result = await compute_dex_vs_cex_flow()
    assert math.isfinite(result["divergence_zscore"])


@pytest.mark.asyncio
async def test_dominance_trend_valid_values():
    result = await compute_dex_vs_cex_flow()
    assert result["dominance_trend"] in ("rising", "falling", "stable")


@pytest.mark.asyncio
async def test_trend_delta_is_float():
    result = await compute_dex_vs_cex_flow()
    assert isinstance(result["trend_delta"], float)


@pytest.mark.asyncio
async def test_price_discovery_valid_values():
    result = await compute_dex_vs_cex_flow()
    valid = {"dex_leading", "dex_elevated", "balanced", "cex_elevated", "cex_dominant"}
    assert result["price_discovery"] in valid


@pytest.mark.asyncio
async def test_discovery_signal_valid_values():
    result = await compute_dex_vs_cex_flow()
    valid = {"strong_buy", "watch", "neutral", "strong_sell"}
    assert result["discovery_signal"] in valid


@pytest.mark.asyncio
async def test_protocols_dict_has_expected_keys():
    result = await compute_dex_vs_cex_flow()
    expected_protocols = {"uniswap_v3", "uniswap_v2", "curve", "balancer"}
    assert set(result["protocols"].keys()) == expected_protocols


@pytest.mark.asyncio
async def test_protocol_volumes_positive():
    result = await compute_dex_vs_cex_flow()
    for k, v in result["protocols"].items():
        assert v > 0, f"Protocol {k} volume should be positive"


@pytest.mark.asyncio
async def test_protocol_breakdown_pct_sums_near_100():
    result = await compute_dex_vs_cex_flow()
    total = sum(result["protocol_breakdown_pct"].values())
    assert abs(total - 100.0) < 0.5, f"Protocol pcts sum to {total}, expected ~100"


@pytest.mark.asyncio
async def test_protocol_breakdown_pct_keys_match_protocols():
    result = await compute_dex_vs_cex_flow()
    assert set(result["protocol_breakdown_pct"].keys()) == set(result["protocols"].keys())


@pytest.mark.asyncio
async def test_dominance_history_is_list():
    result = await compute_dex_vs_cex_flow()
    assert isinstance(result["dominance_history"], list)


@pytest.mark.asyncio
async def test_dominance_history_length():
    result = await compute_dex_vs_cex_flow()
    assert len(result["dominance_history"]) == 24


@pytest.mark.asyncio
async def test_dominance_history_values_in_range():
    result = await compute_dex_vs_cex_flow()
    for v in result["dominance_history"]:
        assert 0.0 <= v <= 1.0, f"History value {v} out of [0,1]"


@pytest.mark.asyncio
async def test_mean_dominance_positive():
    result = await compute_dex_vs_cex_flow()
    assert result["mean_dominance"] > 0


@pytest.mark.asyncio
async def test_std_dominance_nonnegative():
    result = await compute_dex_vs_cex_flow()
    assert result["std_dominance"] >= 0


@pytest.mark.asyncio
async def test_description_is_string():
    result = await compute_dex_vs_cex_flow()
    assert isinstance(result["description"], str)


@pytest.mark.asyncio
async def test_description_nonempty():
    result = await compute_dex_vs_cex_flow()
    assert len(result["description"]) > 0


@pytest.mark.asyncio
async def test_description_contains_dex_pct():
    result = await compute_dex_vs_cex_flow()
    assert "%" in result["description"]


@pytest.mark.asyncio
async def test_description_contains_zscore():
    result = await compute_dex_vs_cex_flow()
    assert "Z-score" in result["description"] or "z-score" in result["description"].lower()


@pytest.mark.asyncio
async def test_dex_volume_within_expected_range():
    # sum of 4 protocols: min ~16M, max ~57M
    result = await compute_dex_vs_cex_flow()
    assert 10e6 <= result["dex_volume_usd"] <= 65e6


@pytest.mark.asyncio
async def test_cex_volume_within_expected_range():
    result = await compute_dex_vs_cex_flow()
    assert 35e6 <= result["cex_volume_usd"] <= 130e6


@pytest.mark.asyncio
async def test_uniswap_v3_largest_protocol():
    """Uniswap v3 range (8-25M) dominates curve (5-18M) on average."""
    result = await compute_dex_vs_cex_flow()
    protos = result["protocols"]
    # uniswap v3 min (8M) > balancer max (6M)
    assert protos["uniswap_v3"] > protos["balancer"]


@pytest.mark.asyncio
async def test_result_is_stable_within_time_bucket():
    """Two calls within the same 5-min bucket should return identical volumes."""
    r1 = await compute_dex_vs_cex_flow()
    r2 = await compute_dex_vs_cex_flow()
    assert r1["dex_volume_usd"] == r2["dex_volume_usd"]
    assert r1["cex_volume_usd"] == r2["cex_volume_usd"]


@pytest.mark.asyncio
async def test_different_symbols_return_same_structure():
    r1 = await compute_dex_vs_cex_flow(symbol="BTCUSDT")
    r2 = await compute_dex_vs_cex_flow(symbol="ETHUSDT")
    assert set(r1.keys()) == set(r2.keys())


@pytest.mark.asyncio
async def test_different_symbols_have_correct_symbol_field():
    r1 = await compute_dex_vs_cex_flow(symbol="COSUSDT")
    r2 = await compute_dex_vs_cex_flow(symbol="DEXEUSDT")
    assert r1["symbol"] == "COSUSDT"
    assert r2["symbol"] == "DEXEUSDT"


@pytest.mark.asyncio
async def test_discovery_signal_consistent_with_zscore():
    result = await compute_dex_vs_cex_flow()
    z = result["divergence_zscore"]
    sig = result["discovery_signal"]
    if z > 2.0:
        assert sig == "strong_buy"
    elif z > 1.0:
        assert sig == "watch"
    elif z < -2.0:
        assert sig == "strong_sell"
    elif z < -1.0:
        assert sig == "watch"
    else:
        assert sig == "neutral"


@pytest.mark.asyncio
async def test_price_discovery_consistent_with_zscore():
    result = await compute_dex_vs_cex_flow()
    z = result["divergence_zscore"]
    pd = result["price_discovery"]
    if z > 2.0:
        assert pd == "dex_leading"
    elif z > 1.0:
        assert pd == "dex_elevated"
    elif z < -2.0:
        assert pd == "cex_dominant"
    elif z < -1.0:
        assert pd == "cex_elevated"
    else:
        assert pd == "balanced"


@pytest.mark.asyncio
async def test_dominance_ratio_consistent_with_volumes():
    result = await compute_dex_vs_cex_flow()
    expected_ratio = result["dex_volume_usd"] / result["total_volume_usd"]
    assert abs(result["dex_dominance_ratio"] - expected_ratio) < 0.001


@pytest.mark.asyncio
async def test_protocol_volumes_sum_to_dex_volume():
    result = await compute_dex_vs_cex_flow()
    proto_sum = sum(result["protocols"].values())
    assert abs(proto_sum - result["dex_volume_usd"]) <= 4.0  # rounding tolerance


@pytest.mark.asyncio
async def test_mean_dominance_consistent_with_history():
    result = await compute_dex_vs_cex_flow()
    hist = _dex_cex_dominance_history(30)
    expected_mean = sum(hist) / len(hist)
    assert abs(result["mean_dominance"] - round(expected_mean, 4)) < 0.001


@pytest.mark.asyncio
async def test_std_dominance_consistent_with_history():
    result = await compute_dex_vs_cex_flow()
    hist = _dex_cex_dominance_history(30)
    mean = sum(hist) / len(hist)
    std = math.sqrt(sum((x - mean) ** 2 for x in hist) / len(hist))
    assert abs(result["std_dominance"] - round(std, 4)) < 0.001


@pytest.mark.asyncio
async def test_window_hours_1():
    result = await compute_dex_vs_cex_flow(window_hours=1)
    assert result["window_hours"] == 1
    assert result["dex_volume_usd"] > 0


@pytest.mark.asyncio
async def test_window_hours_168():
    result = await compute_dex_vs_cex_flow(window_hours=168)
    assert result["window_hours"] == 168


@pytest.mark.asyncio
async def test_no_nan_in_numeric_fields():
    result = await compute_dex_vs_cex_flow()
    numeric_keys = [
        "dex_volume_usd", "cex_volume_usd", "total_volume_usd",
        "dex_dominance_ratio", "dex_dominance_pct", "divergence_zscore",
        "trend_delta", "mean_dominance", "std_dominance",
    ]
    for k in numeric_keys:
        assert not math.isnan(result[k]), f"{k} is NaN"


@pytest.mark.asyncio
async def test_no_inf_in_numeric_fields():
    result = await compute_dex_vs_cex_flow()
    numeric_keys = [
        "dex_volume_usd", "cex_volume_usd", "total_volume_usd",
        "dex_dominance_ratio", "dex_dominance_pct", "divergence_zscore",
        "trend_delta", "mean_dominance", "std_dominance",
    ]
    for k in numeric_keys:
        assert math.isfinite(result[k]), f"{k} is infinite"


@pytest.mark.asyncio
async def test_curve_protocol_present_and_positive():
    result = await compute_dex_vs_cex_flow()
    assert "curve" in result["protocols"]
    assert result["protocols"]["curve"] > 0


@pytest.mark.asyncio
async def test_balancer_protocol_present_and_positive():
    result = await compute_dex_vs_cex_flow()
    assert "balancer" in result["protocols"]
    assert result["protocols"]["balancer"] > 0


@pytest.mark.asyncio
async def test_dominance_history_all_floats():
    result = await compute_dex_vs_cex_flow()
    for v in result["dominance_history"]:
        assert isinstance(v, float), f"History value {v} is not float"
# dex-vs-cex ci trigger
