"""Tests for compute_order_flow_toxicity() — enhanced VPIN implementation.

72 tests covering all required keys, value ranges, structural invariants,
determinism, and alert logic.
"""
import asyncio
import math
import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from metrics import compute_order_flow_toxicity

REQUIRED_KEYS = {
    "vpin_score",
    "buy_volume_frac",
    "sell_volume_frac",
    "toxicity_level",
    "volume_buckets",
    "informed_trading_signal",
    "rolling_vpin_50",
    "timestamp",
    "toxicity_percentile",
    "toxicity_alert",
    "bucket_classifications",
    "symbol_comparison",
    "alert_threshold",
    "vpin_history",
}

TOXICITY_LEVELS = {"low", "medium", "high", "extreme"}
INFORMED_SIGNALS = {"high_toxicity", "low_toxicity", "neutral"}
BUCKET_KEYS = {"bucket_id", "buy_vol", "sell_vol", "imbalance"}
CLASSIFICATION_KEYS = {"bucket_id", "vpin_window", "toxicity_class"}
SYMBOL_KEYS = {"symbol", "vpin_score", "toxicity_level", "toxicity_percentile", "rank"}


def run(coro):
    return asyncio.run(coro)


# ── Fixture ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def result():
    return run(compute_order_flow_toxicity())


@pytest.fixture(scope="module")
def result2():
    return run(compute_order_flow_toxicity())


# ── Return type ──────────────────────────────────────────────────────────────


def test_returns_dict(result):
    assert isinstance(result, dict)


# ── All required keys present ────────────────────────────────────────────────


def test_has_vpin_score(result):
    assert "vpin_score" in result


def test_has_buy_volume_frac(result):
    assert "buy_volume_frac" in result


def test_has_sell_volume_frac(result):
    assert "sell_volume_frac" in result


def test_has_toxicity_level(result):
    assert "toxicity_level" in result


def test_has_volume_buckets(result):
    assert "volume_buckets" in result


def test_has_informed_trading_signal(result):
    assert "informed_trading_signal" in result


def test_has_rolling_vpin_50(result):
    assert "rolling_vpin_50" in result


def test_has_timestamp(result):
    assert "timestamp" in result


def test_has_toxicity_percentile(result):
    assert "toxicity_percentile" in result


def test_has_toxicity_alert(result):
    assert "toxicity_alert" in result


def test_has_bucket_classifications(result):
    assert "bucket_classifications" in result


def test_has_symbol_comparison(result):
    assert "symbol_comparison" in result


def test_has_alert_threshold(result):
    assert "alert_threshold" in result


def test_has_vpin_history(result):
    assert "vpin_history" in result


def test_all_required_keys(result):
    assert REQUIRED_KEYS.issubset(result.keys())


# ── vpin_score ────────────────────────────────────────────────────────────────


def test_vpin_score_in_range(result):
    assert 0.0 <= result["vpin_score"] <= 1.0


def test_vpin_score_is_float(result):
    assert isinstance(result["vpin_score"], float)


def test_vpin_score_matches_mean_imbalance(result):
    buckets = result["volume_buckets"]
    expected = sum(b["imbalance"] for b in buckets) / len(buckets)
    assert math.isclose(result["vpin_score"], expected, rel_tol=1e-5)


# ── buy/sell fractions ────────────────────────────────────────────────────────


def test_buy_volume_frac_in_range(result):
    assert 0.0 < result["buy_volume_frac"] < 1.0


def test_sell_volume_frac_in_range(result):
    assert 0.0 < result["sell_volume_frac"] < 1.0


def test_fracs_sum_to_one(result):
    total = result["buy_volume_frac"] + result["sell_volume_frac"]
    assert math.isclose(total, 1.0, abs_tol=1e-4)


# ── toxicity_level ────────────────────────────────────────────────────────────


def test_toxicity_level_valid(result):
    assert result["toxicity_level"] in TOXICITY_LEVELS


def test_toxicity_level_is_string(result):
    assert isinstance(result["toxicity_level"], str)


def test_toxicity_level_consistent_with_vpin(result):
    score = result["vpin_score"]
    level = result["toxicity_level"]
    if score < 0.25:
        assert level == "low"
    elif score < 0.50:
        assert level == "medium"
    elif score < 0.75:
        assert level == "high"
    else:
        assert level == "extreme"


# ── informed_trading_signal ───────────────────────────────────────────────────


def test_informed_trading_signal_valid(result):
    assert result["informed_trading_signal"] in INFORMED_SIGNALS


def test_informed_trading_signal_consistent_with_vpin(result):
    score = result["vpin_score"]
    sig = result["informed_trading_signal"]
    if score >= 0.50:
        assert sig == "high_toxicity"
    elif score <= 0.20:
        assert sig == "low_toxicity"
    else:
        assert sig == "neutral"


# ── volume_buckets ────────────────────────────────────────────────────────────


def test_volume_buckets_is_list(result):
    assert isinstance(result["volume_buckets"], list)


def test_volume_buckets_length_50(result):
    assert len(result["volume_buckets"]) == 50


def test_volume_buckets_have_required_keys(result):
    for b in result["volume_buckets"]:
        assert BUCKET_KEYS.issubset(b.keys())


def test_volume_buckets_imbalance_in_range(result):
    for b in result["volume_buckets"]:
        assert 0.0 <= b["imbalance"] <= 1.0


def test_volume_buckets_buy_sell_sum_approx_1000(result):
    for b in result["volume_buckets"]:
        total = b["buy_vol"] + b["sell_vol"]
        assert math.isclose(total, 1000.0, abs_tol=0.01)


def test_volume_buckets_bucket_ids_sequential(result):
    for i, b in enumerate(result["volume_buckets"]):
        assert b["bucket_id"] == i


# ── rolling_vpin_50 ───────────────────────────────────────────────────────────


def test_rolling_vpin_50_is_list(result):
    assert isinstance(result["rolling_vpin_50"], list)


def test_rolling_vpin_50_length_50(result):
    assert len(result["rolling_vpin_50"]) == 50


def test_rolling_vpin_50_values_in_range(result):
    for v in result["rolling_vpin_50"]:
        assert 0.0 <= v <= 1.0


# ── toxicity_percentile ───────────────────────────────────────────────────────


def test_toxicity_percentile_in_range(result):
    assert 0.0 <= result["toxicity_percentile"] <= 100.0


def test_toxicity_percentile_is_float(result):
    assert isinstance(result["toxicity_percentile"], float)


# ── toxicity_alert ────────────────────────────────────────────────────────────


def test_toxicity_alert_is_bool(result):
    assert isinstance(result["toxicity_alert"], bool)


def test_toxicity_alert_consistent_with_percentile(result):
    assert result["toxicity_alert"] == (result["toxicity_percentile"] > 80.0)


# ── alert_threshold ───────────────────────────────────────────────────────────


def test_alert_threshold_is_80(result):
    assert result["alert_threshold"] == 80.0


def test_alert_threshold_is_float(result):
    assert isinstance(result["alert_threshold"], float)


# ── vpin_history ──────────────────────────────────────────────────────────────


def test_vpin_history_is_list(result):
    assert isinstance(result["vpin_history"], list)


def test_vpin_history_length_100(result):
    assert len(result["vpin_history"]) == 100


def test_vpin_history_values_in_range(result):
    for v in result["vpin_history"]:
        assert 0.0 <= v <= 1.0


# ── bucket_classifications ────────────────────────────────────────────────────


def test_bucket_classifications_is_list(result):
    assert isinstance(result["bucket_classifications"], list)


def test_bucket_classifications_length_50(result):
    assert len(result["bucket_classifications"]) == 50


def test_bucket_classifications_have_required_keys(result):
    for c in result["bucket_classifications"]:
        assert CLASSIFICATION_KEYS.issubset(c.keys())


def test_bucket_classifications_toxicity_class_valid(result):
    for c in result["bucket_classifications"]:
        assert c["toxicity_class"] in TOXICITY_LEVELS


def test_bucket_classifications_bucket_ids_sequential(result):
    for i, c in enumerate(result["bucket_classifications"]):
        assert c["bucket_id"] == i


def test_bucket_classifications_vpin_window_matches_rolling(result):
    for c, rv in zip(result["bucket_classifications"], result["rolling_vpin_50"]):
        assert c["vpin_window"] == rv


# ── symbol_comparison ─────────────────────────────────────────────────────────


def test_symbol_comparison_is_list(result):
    assert isinstance(result["symbol_comparison"], list)


def test_symbol_comparison_length_10(result):
    assert len(result["symbol_comparison"]) == 10


def test_symbol_comparison_have_required_keys(result):
    for s in result["symbol_comparison"]:
        assert SYMBOL_KEYS.issubset(s.keys())


def test_symbol_comparison_ranks_1_to_10(result):
    ranks = sorted(s["rank"] for s in result["symbol_comparison"])
    assert ranks == list(range(1, 11))


def test_symbol_comparison_ranks_unique(result):
    ranks = [s["rank"] for s in result["symbol_comparison"]]
    assert len(ranks) == len(set(ranks))


def test_symbol_comparison_sorted_by_vpin_descending(result):
    scores = [s["vpin_score"] for s in result["symbol_comparison"]]
    assert scores == sorted(scores, reverse=True)


def test_symbol_comparison_vpin_scores_in_range(result):
    for s in result["symbol_comparison"]:
        assert 0.0 <= s["vpin_score"] <= 1.0


def test_symbol_comparison_toxicity_levels_valid(result):
    for s in result["symbol_comparison"]:
        assert s["toxicity_level"] in TOXICITY_LEVELS


def test_symbol_comparison_percentiles_in_range(result):
    for s in result["symbol_comparison"]:
        assert 0.0 <= s["toxicity_percentile"] <= 100.0


def test_symbol_comparison_symbols_are_strings(result):
    for s in result["symbol_comparison"]:
        assert isinstance(s["symbol"], str) and len(s["symbol"]) > 0


# ── timestamp ─────────────────────────────────────────────────────────────────


def test_timestamp_is_string(result):
    assert isinstance(result["timestamp"], str)


def test_timestamp_nonempty(result):
    assert len(result["timestamp"]) > 0


# ── async / determinism ───────────────────────────────────────────────────────


def test_function_is_async():
    import inspect
    assert inspect.iscoroutinefunction(compute_order_flow_toxicity)


def test_determinism_vpin_score(result, result2):
    assert result["vpin_score"] == result2["vpin_score"]


def test_determinism_volume_buckets(result, result2):
    assert result["volume_buckets"] == result2["volume_buckets"]


def test_determinism_rolling_vpin_50(result, result2):
    assert result["rolling_vpin_50"] == result2["rolling_vpin_50"]


def test_determinism_vpin_history(result, result2):
    assert result["vpin_history"] == result2["vpin_history"]


def test_determinism_toxicity_percentile(result, result2):
    assert result["toxicity_percentile"] == result2["toxicity_percentile"]


def test_determinism_toxicity_alert(result, result2):
    assert result["toxicity_alert"] == result2["toxicity_alert"]


def test_determinism_symbol_comparison(result, result2):
    for s1, s2 in zip(result["symbol_comparison"], result2["symbol_comparison"]):
        assert s1["symbol"] == s2["symbol"]
        assert s1["vpin_score"] == s2["vpin_score"]
        assert s1["rank"] == s2["rank"]


def test_determinism_bucket_classifications(result, result2):
    assert result["bucket_classifications"] == result2["bucket_classifications"]
