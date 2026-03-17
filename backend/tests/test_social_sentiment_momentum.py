"""Tests for compute_social_sentiment_momentum() — Wave 25.

55 tests covering all required keys, value ranges, structural invariants,
label consistency, determinism, and trending-tokens logic.
"""
import asyncio
import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from metrics import compute_social_sentiment_momentum

REQUIRED_KEYS = {
    "tweet_volume",
    "reddit_volume",
    "bull_bear_ratio",
    "sentiment_score",
    "sentiment_label",
    "sentiment_velocity",
    "velocity_direction",
    "fear_greed_index",
    "fear_greed_label",
    "trending_tokens",
    "timestamp",
}

SENTIMENT_LABELS = {"very_bearish", "bearish", "neutral", "bullish", "very_bullish"}
VELOCITY_DIRECTIONS = {"accelerating", "decelerating", "stable"}
FEAR_GREED_LABELS = {"extreme_fear", "fear", "neutral", "greed", "extreme_greed"}
TOKEN_DIRECTIONS = {"up", "down", "flat"}
TOKEN_KEYS = {"symbol", "sentiment_shift", "direction", "rank"}


def run(coro):
    return asyncio.run(coro)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def result():
    return run(compute_social_sentiment_momentum())


@pytest.fixture(scope="module")
def result2():
    return run(compute_social_sentiment_momentum())


# ── Return type ──────────────────────────────────────────────────────────────


def test_returns_dict(result):
    assert isinstance(result, dict)


# ── Required keys present ────────────────────────────────────────────────────


def test_has_tweet_volume(result):
    assert "tweet_volume" in result


def test_has_reddit_volume(result):
    assert "reddit_volume" in result


def test_has_bull_bear_ratio(result):
    assert "bull_bear_ratio" in result


def test_has_sentiment_score(result):
    assert "sentiment_score" in result


def test_has_sentiment_label(result):
    assert "sentiment_label" in result


def test_has_sentiment_velocity(result):
    assert "sentiment_velocity" in result


def test_has_velocity_direction(result):
    assert "velocity_direction" in result


def test_has_fear_greed_index(result):
    assert "fear_greed_index" in result


def test_has_fear_greed_label(result):
    assert "fear_greed_label" in result


def test_has_trending_tokens(result):
    assert "trending_tokens" in result


def test_has_timestamp(result):
    assert "timestamp" in result


def test_all_required_keys_present(result):
    assert REQUIRED_KEYS.issubset(result.keys())


# ── Type checks ───────────────────────────────────────────────────────────────


def test_tweet_volume_is_int(result):
    assert isinstance(result["tweet_volume"], int)


def test_reddit_volume_is_int(result):
    assert isinstance(result["reddit_volume"], int)


def test_bull_bear_ratio_is_float(result):
    assert isinstance(result["bull_bear_ratio"], float)


def test_sentiment_score_is_float(result):
    assert isinstance(result["sentiment_score"], float)


def test_sentiment_label_is_str(result):
    assert isinstance(result["sentiment_label"], str)


def test_sentiment_velocity_is_float(result):
    assert isinstance(result["sentiment_velocity"], float)


def test_velocity_direction_is_str(result):
    assert isinstance(result["velocity_direction"], str)


def test_fear_greed_index_is_float(result):
    assert isinstance(result["fear_greed_index"], float)


def test_fear_greed_label_is_str(result):
    assert isinstance(result["fear_greed_label"], str)


def test_trending_tokens_is_list(result):
    assert isinstance(result["trending_tokens"], list)


def test_timestamp_is_str(result):
    assert isinstance(result["timestamp"], str)


# ── Value ranges ──────────────────────────────────────────────────────────────


def test_tweet_volume_range(result):
    assert 50_000 <= result["tweet_volume"] <= 500_000


def test_reddit_volume_range(result):
    assert 500 <= result["reddit_volume"] <= 5_000


def test_bull_bear_ratio_range(result):
    assert 0.5 <= result["bull_bear_ratio"] <= 3.0


def test_sentiment_score_range(result):
    assert 0.0 <= result["sentiment_score"] <= 100.0


def test_sentiment_velocity_range(result):
    assert -100.0 <= result["sentiment_velocity"] <= 100.0


def test_fear_greed_index_range(result):
    assert 0.0 <= result["fear_greed_index"] <= 100.0


# ── Valid enum values ─────────────────────────────────────────────────────────


def test_sentiment_label_valid(result):
    assert result["sentiment_label"] in SENTIMENT_LABELS


def test_velocity_direction_valid(result):
    assert result["velocity_direction"] in VELOCITY_DIRECTIONS


def test_fear_greed_label_valid(result):
    assert result["fear_greed_label"] in FEAR_GREED_LABELS


# ── Label consistency with scores ─────────────────────────────────────────────


def test_sentiment_label_consistent_with_score_very_bullish(result):
    score = result["sentiment_score"]
    label = result["sentiment_label"]
    if score >= 75:
        assert label == "very_bullish"


def test_sentiment_label_consistent_with_score_bullish(result):
    score = result["sentiment_score"]
    label = result["sentiment_label"]
    if 60 <= score < 75:
        assert label == "bullish"


def test_sentiment_label_consistent_with_score_neutral(result):
    score = result["sentiment_score"]
    label = result["sentiment_label"]
    if 40 <= score < 60:
        assert label == "neutral"


def test_sentiment_label_consistent_with_score_bearish(result):
    score = result["sentiment_score"]
    label = result["sentiment_label"]
    if 25 <= score < 40:
        assert label == "bearish"


def test_sentiment_label_consistent_with_score_very_bearish(result):
    score = result["sentiment_score"]
    label = result["sentiment_label"]
    if score < 25:
        assert label == "very_bearish"


def test_fear_greed_label_consistent_with_index_extreme_greed(result):
    idx = result["fear_greed_index"]
    label = result["fear_greed_label"]
    if idx >= 75:
        assert label == "extreme_greed"


def test_fear_greed_label_consistent_with_index_greed(result):
    idx = result["fear_greed_index"]
    label = result["fear_greed_label"]
    if 60 <= idx < 75:
        assert label == "greed"


def test_fear_greed_label_consistent_with_index_neutral(result):
    idx = result["fear_greed_index"]
    label = result["fear_greed_label"]
    if 40 <= idx < 60:
        assert label == "neutral"


def test_fear_greed_label_consistent_with_index_fear(result):
    idx = result["fear_greed_index"]
    label = result["fear_greed_label"]
    if 25 <= idx < 40:
        assert label == "fear"


def test_fear_greed_label_consistent_with_index_extreme_fear(result):
    idx = result["fear_greed_index"]
    label = result["fear_greed_label"]
    if idx < 25:
        assert label == "extreme_fear"


def test_velocity_direction_accelerating_consistent(result):
    vel = result["sentiment_velocity"]
    dirn = result["velocity_direction"]
    if vel > 5.0:
        assert dirn == "accelerating"


def test_velocity_direction_decelerating_consistent(result):
    vel = result["sentiment_velocity"]
    dirn = result["velocity_direction"]
    if vel < -5.0:
        assert dirn == "decelerating"


def test_velocity_direction_stable_consistent(result):
    vel = result["sentiment_velocity"]
    dirn = result["velocity_direction"]
    if -5.0 <= vel <= 5.0:
        assert dirn == "stable"


# ── Trending tokens structural invariants ─────────────────────────────────────


def test_trending_tokens_length(result):
    assert len(result["trending_tokens"]) == 5


def test_trending_tokens_required_keys(result):
    for token in result["trending_tokens"]:
        assert TOKEN_KEYS.issubset(token.keys())


def test_trending_tokens_ranks_are_1_to_5(result):
    ranks = [t["rank"] for t in result["trending_tokens"]]
    assert sorted(ranks) == [1, 2, 3, 4, 5]


def test_trending_tokens_ranks_unique(result):
    ranks = [t["rank"] for t in result["trending_tokens"]]
    assert len(set(ranks)) == 5


def test_trending_tokens_symbols_are_strings(result):
    for token in result["trending_tokens"]:
        assert isinstance(token["symbol"], str)
        assert len(token["symbol"]) >= 2


def test_trending_tokens_symbols_unique(result):
    symbols = [t["symbol"] for t in result["trending_tokens"]]
    assert len(set(symbols)) == 5


def test_trending_tokens_sentiment_shift_range(result):
    for token in result["trending_tokens"]:
        assert -50.0 <= token["sentiment_shift"] <= 50.0


def test_trending_tokens_direction_valid(result):
    for token in result["trending_tokens"]:
        assert token["direction"] in TOKEN_DIRECTIONS


def test_trending_tokens_direction_up_consistent(result):
    for token in result["trending_tokens"]:
        if token["sentiment_shift"] > 1.0:
            assert token["direction"] == "up"


def test_trending_tokens_direction_down_consistent(result):
    for token in result["trending_tokens"]:
        if token["sentiment_shift"] < -1.0:
            assert token["direction"] == "down"


def test_trending_tokens_direction_flat_consistent(result):
    for token in result["trending_tokens"]:
        if -1.0 <= token["sentiment_shift"] <= 1.0:
            assert token["direction"] == "flat"


def test_trending_tokens_rank_matches_order(result):
    ranks = [t["rank"] for t in result["trending_tokens"]]
    assert ranks == list(range(1, 6))


def test_trending_tokens_shift_is_float_or_int(result):
    for token in result["trending_tokens"]:
        assert isinstance(token["sentiment_shift"], (float, int))


# ── Timestamp format ──────────────────────────────────────────────────────────


def test_timestamp_is_iso_format(result):
    ts = result["timestamp"]
    assert "T" in ts
    assert ts.endswith("Z") or "+" in ts or len(ts) >= 19


def test_timestamp_has_date_part(result):
    ts = result["timestamp"]
    date_part = ts.split("T")[0]
    parts = date_part.split("-")
    assert len(parts) == 3
    assert len(parts[0]) == 4  # year


# ── Determinism ───────────────────────────────────────────────────────────────


def test_determinism_tweet_volume(result, result2):
    assert result["tweet_volume"] == result2["tweet_volume"]


def test_determinism_reddit_volume(result, result2):
    assert result["reddit_volume"] == result2["reddit_volume"]


def test_determinism_bull_bear_ratio(result, result2):
    assert result["bull_bear_ratio"] == result2["bull_bear_ratio"]


def test_determinism_sentiment_score(result, result2):
    assert result["sentiment_score"] == result2["sentiment_score"]


def test_determinism_sentiment_label(result, result2):
    assert result["sentiment_label"] == result2["sentiment_label"]


def test_determinism_sentiment_velocity(result, result2):
    assert result["sentiment_velocity"] == result2["sentiment_velocity"]


def test_determinism_velocity_direction(result, result2):
    assert result["velocity_direction"] == result2["velocity_direction"]


def test_determinism_fear_greed_index(result, result2):
    assert result["fear_greed_index"] == result2["fear_greed_index"]


def test_determinism_fear_greed_label(result, result2):
    assert result["fear_greed_label"] == result2["fear_greed_label"]


def test_determinism_trending_tokens_symbols(result, result2):
    syms1 = [t["symbol"] for t in result["trending_tokens"]]
    syms2 = [t["symbol"] for t in result2["trending_tokens"]]
    assert syms1 == syms2


def test_determinism_trending_tokens_shifts(result, result2):
    shifts1 = [t["sentiment_shift"] for t in result["trending_tokens"]]
    shifts2 = [t["sentiment_shift"] for t in result2["trending_tokens"]]
    assert shifts1 == shifts2
