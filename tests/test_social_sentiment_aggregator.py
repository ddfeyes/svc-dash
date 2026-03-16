"""
Unit / smoke tests for /api/social-sentiment.

Social sentiment aggregator — combines Twitter/Reddit volume proxy with
keyword-based sentiment scoring for crypto assets.

Approach:
  - Social volume proxy: Reddit posts/comments per hour + Twitter engagement
  - Keyword scoring: bullish/bearish keyword detection against news headlines
  - Normalized 0–100 sentiment score with trend direction and momentum

Signal:
  very_bullish  — score >= 70  → strong buying pressure
  bullish       — score >= 55  → mild buying pressure
  neutral       — 40 < score < 55
  bearish       — score <= 40  → mild selling pressure
  very_bearish  — score <= 25  → strong selling pressure

Covers:
  - _ss_keyword_score
  - _ss_normalize_score
  - _ss_sentiment_label
  - _ss_momentum
  - _ss_trend
  - _ss_volume_proxy
  - _ss_buzz_level
  - _ss_zscore
  - Response shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _ss_keyword_score,
    _ss_normalize_score,
    _ss_sentiment_label,
    _ss_momentum,
    _ss_trend,
    _ss_volume_proxy,
    _ss_buzz_level,
    _ss_zscore,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "sentiment": {
        "score":     65.2,
        "label":     "bullish",
        "direction": "rising",
        "momentum":  12.5,
    },
    "social_volume": {
        "reddit_posts_per_hour":    42,
        "reddit_comments_per_hour": 318,
        "twitter_points":           950_000,
        "volume_proxy":             73.4,
        "buzz":                     "high",
    },
    "keywords": {
        "bullish_count": 14,
        "bearish_count":  6,
        "neutral_count": 10,
        "dominant":      "bullish",
        "top_bullish":   ["breakout", "rally", "accumulation"],
        "top_bearish":   ["dump", "crash", "sell"],
    },
    "history": [
        {"date": "2024-11-14", "score": 48.0, "label": "neutral"},
        {"date": "2024-11-15", "score": 52.1, "label": "neutral"},
        {"date": "2024-11-16", "score": 45.3, "label": "neutral"},
        {"date": "2024-11-17", "score": 58.0, "label": "bullish"},
        {"date": "2024-11-18", "score": 61.5, "label": "bullish"},
        {"date": "2024-11-19", "score": 63.0, "label": "bullish"},
        {"date": "2024-11-20", "score": 65.2, "label": "bullish"},
    ],
    "zscore":      0.8,
    "description": "Bullish: social sentiment score 65/100 — buying signals dominant",
}


# ===========================================================================
# 1. _ss_keyword_score
# ===========================================================================

class TestSsKeywordScore:
    BULL = ["moon", "rally", "breakout", "accumulation", "buy"]
    BEAR = ["crash", "dump", "sell", "panic", "collapse"]

    def test_pure_bullish_text_returns_positive(self):
        score = _ss_keyword_score("moon rally breakout", self.BULL, self.BEAR)
        assert score > 0

    def test_pure_bearish_text_returns_negative(self):
        score = _ss_keyword_score("crash dump panic", self.BULL, self.BEAR)
        assert score < 0

    def test_neutral_text_returns_zero(self):
        score = _ss_keyword_score("the price is stable today", self.BULL, self.BEAR)
        assert score == pytest.approx(0.0, abs=1e-6)

    def test_empty_text_returns_zero(self):
        score = _ss_keyword_score("", self.BULL, self.BEAR)
        assert score == pytest.approx(0.0, abs=1e-6)

    def test_case_insensitive(self):
        lower = _ss_keyword_score("rally", self.BULL, self.BEAR)
        upper = _ss_keyword_score("RALLY", self.BULL, self.BEAR)
        assert lower == pytest.approx(upper, rel=1e-4)

    def test_result_in_minus1_to_1_range(self):
        text = "moon moon rally breakout buy crash"
        score = _ss_keyword_score(text, self.BULL, self.BEAR)
        assert -1.0 <= score <= 1.0

    def test_returns_float(self):
        assert isinstance(_ss_keyword_score("moon", self.BULL, self.BEAR), float)


# ===========================================================================
# 2. _ss_normalize_score
# ===========================================================================

class TestSsNormalizeScore:
    def test_min_returns_0(self):
        assert _ss_normalize_score(-1.0, -1.0, 1.0) == pytest.approx(0.0, abs=0.1)

    def test_max_returns_100(self):
        assert _ss_normalize_score(1.0, -1.0, 1.0) == pytest.approx(100.0, abs=0.1)

    def test_mid_returns_50(self):
        assert _ss_normalize_score(0.0, -1.0, 1.0) == pytest.approx(50.0, abs=0.1)

    def test_clamps_below_min(self):
        assert _ss_normalize_score(-5.0, -1.0, 1.0) == pytest.approx(0.0, abs=0.1)

    def test_clamps_above_max(self):
        assert _ss_normalize_score(5.0, -1.0, 1.0) == pytest.approx(100.0, abs=0.1)

    def test_zero_range_returns_50(self):
        assert _ss_normalize_score(0.5, 0.5, 0.5) == pytest.approx(50.0, abs=0.1)


# ===========================================================================
# 3. _ss_sentiment_label
# ===========================================================================

class TestSsSentimentLabel:
    def test_very_bullish_high_score(self):
        assert _ss_sentiment_label(80.0) == "very_bullish"

    def test_bullish_mid_high_score(self):
        assert _ss_sentiment_label(62.0) == "bullish"

    def test_neutral_middle_score(self):
        assert _ss_sentiment_label(50.0) == "neutral"

    def test_bearish_mid_low_score(self):
        assert _ss_sentiment_label(35.0) == "bearish"

    def test_very_bearish_low_score(self):
        assert _ss_sentiment_label(15.0) == "very_bearish"

    def test_returns_valid_string(self):
        result = _ss_sentiment_label(50.0)
        assert result in ("very_bullish", "bullish", "neutral", "bearish", "very_bearish")

    def test_boundary_70_is_very_bullish(self):
        assert _ss_sentiment_label(70.0) == "very_bullish"


# ===========================================================================
# 4. _ss_momentum
# ===========================================================================

class TestSsMomentum:
    def test_empty_returns_zero(self):
        assert _ss_momentum([]) == pytest.approx(0.0, abs=1e-6)

    def test_single_returns_zero(self):
        assert _ss_momentum([50.0]) == pytest.approx(0.0, abs=1e-6)

    def test_rising_scores_positive_momentum(self):
        scores = [40.0, 45.0, 50.0, 55.0, 60.0, 65.0]
        assert _ss_momentum(scores) > 0

    def test_falling_scores_negative_momentum(self):
        scores = [65.0, 60.0, 55.0, 50.0, 45.0, 40.0]
        assert _ss_momentum(scores) < 0

    def test_flat_scores_near_zero(self):
        scores = [50.0] * 6
        assert _ss_momentum(scores) == pytest.approx(0.0, abs=1e-6)

    def test_returns_float(self):
        assert isinstance(_ss_momentum([40.0, 50.0, 60.0, 70.0]), float)


# ===========================================================================
# 5. _ss_trend
# ===========================================================================

class TestSsTrend:
    def test_empty_returns_stable(self):
        assert _ss_trend([]) == "stable"

    def test_single_returns_stable(self):
        assert _ss_trend([50.0]) == "stable"

    def test_rising_scores_is_rising(self):
        assert _ss_trend([40.0, 45.0, 50.0, 55.0, 60.0]) == "rising"

    def test_falling_scores_is_falling(self):
        assert _ss_trend([60.0, 55.0, 50.0, 45.0, 40.0]) == "falling"

    def test_flat_scores_is_stable(self):
        assert _ss_trend([50.0] * 7) == "stable"

    def test_returns_valid_string(self):
        result = _ss_trend([1.0, 2.0, 3.0])
        assert result in ("rising", "falling", "stable")


# ===========================================================================
# 6. _ss_volume_proxy
# ===========================================================================

class TestSsVolumeProxy:
    def test_all_zero_returns_zero(self):
        assert _ss_volume_proxy(0, 0, 0) == pytest.approx(0.0, abs=0.1)

    def test_result_in_0_100_range(self):
        for posts, comments, twt in [
            (0, 0, 0), (10, 100, 500_000), (100, 1000, 5_000_000),
            (200, 5000, 50_000_000),
        ]:
            result = _ss_volume_proxy(posts, comments, twt)
            assert 0 <= result <= 100

    def test_higher_activity_higher_proxy(self):
        low  = _ss_volume_proxy(5,  50,  100_000)
        high = _ss_volume_proxy(50, 500, 1_000_000)
        assert high > low

    def test_returns_float(self):
        assert isinstance(_ss_volume_proxy(10, 100, 500_000), float)

    def test_zero_twitter_still_works(self):
        result = _ss_volume_proxy(20, 200, 0)
        assert 0 <= result <= 100

    def test_zero_reddit_still_works(self):
        result = _ss_volume_proxy(0, 0, 2_000_000)
        assert 0 <= result <= 100


# ===========================================================================
# 7. _ss_buzz_level
# ===========================================================================

class TestSsBuzzLevel:
    def test_very_high_proxy(self):
        assert _ss_buzz_level(90.0) == "very_high"

    def test_high_proxy(self):
        assert _ss_buzz_level(70.0) == "high"

    def test_moderate_proxy(self):
        assert _ss_buzz_level(50.0) == "moderate"

    def test_low_proxy(self):
        assert _ss_buzz_level(30.0) == "low"

    def test_very_low_proxy(self):
        assert _ss_buzz_level(10.0) == "very_low"


# ===========================================================================
# 8. _ss_zscore
# ===========================================================================

class TestSsZscore:
    def test_empty_history_returns_zero(self):
        assert _ss_zscore(50.0, []) == 0.0

    def test_single_history_returns_zero(self):
        assert _ss_zscore(50.0, [50.0]) == 0.0

    def test_current_at_mean_returns_near_zero(self):
        history = [40.0, 50.0, 60.0, 50.0, 40.0]
        mean = sum(history) / len(history)
        assert abs(_ss_zscore(mean, history)) < 0.01

    def test_above_mean_returns_positive(self):
        history = [40.0, 45.0, 50.0, 55.0]
        assert _ss_zscore(100.0, history) > 0

    def test_below_mean_returns_negative(self):
        history = [60.0, 65.0, 70.0, 75.0]
        assert _ss_zscore(0.0, history) < 0

    def test_uniform_history_returns_zero(self):
        history = [50.0] * 10
        assert _ss_zscore(50.0, history) == pytest.approx(0.0, abs=0.01)


# ===========================================================================
# 9. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_sentiment_dict(self):
        assert isinstance(SAMPLE_RESPONSE["sentiment"], dict)

    def test_sentiment_has_required_keys(self):
        for key in ("score", "label", "direction", "momentum"):
            assert key in SAMPLE_RESPONSE["sentiment"], f"sentiment missing '{key}'"

    def test_score_in_0_100_range(self):
        s = SAMPLE_RESPONSE["sentiment"]["score"]
        assert 0 <= s <= 100

    def test_label_is_valid(self):
        assert SAMPLE_RESPONSE["sentiment"]["label"] in (
            "very_bullish", "bullish", "neutral", "bearish", "very_bearish"
        )

    def test_direction_is_valid(self):
        assert SAMPLE_RESPONSE["sentiment"]["direction"] in ("rising", "falling", "stable")

    def test_has_social_volume_dict(self):
        assert isinstance(SAMPLE_RESPONSE["social_volume"], dict)

    def test_social_volume_has_required_keys(self):
        for key in ("reddit_posts_per_hour", "reddit_comments_per_hour",
                    "twitter_points", "volume_proxy", "buzz"):
            assert key in SAMPLE_RESPONSE["social_volume"], f"social_volume missing '{key}'"

    def test_volume_proxy_in_range(self):
        vp = SAMPLE_RESPONSE["social_volume"]["volume_proxy"]
        assert 0 <= vp <= 100

    def test_has_keywords_dict(self):
        assert isinstance(SAMPLE_RESPONSE["keywords"], dict)

    def test_keywords_has_required_keys(self):
        for key in ("bullish_count", "bearish_count", "neutral_count",
                    "dominant", "top_bullish", "top_bearish"):
            assert key in SAMPLE_RESPONSE["keywords"], f"keywords missing '{key}'"

    def test_dominant_is_valid(self):
        assert SAMPLE_RESPONSE["keywords"]["dominant"] in ("bullish", "bearish", "neutral")

    def test_has_history_list(self):
        assert isinstance(SAMPLE_RESPONSE["history"], list)

    def test_history_items_have_required_keys(self):
        for item in SAMPLE_RESPONSE["history"]:
            for key in ("date", "score", "label"):
                assert key in item, f"history item missing '{key}'"

    def test_has_zscore(self):
        assert "zscore" in SAMPLE_RESPONSE
        assert isinstance(SAMPLE_RESPONSE["zscore"], float)

    def test_has_description(self):
        assert isinstance(SAMPLE_RESPONSE["description"], str)


# ===========================================================================
# 10. Structural tests
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api_path = os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")
        content = open(api_path).read()
        assert "/social-sentiment" in content, "/social-sentiment route missing"

    def test_html_card_exists(self):
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        content = open(html_path).read()
        assert "card-social-sentiment" in content, "card-social-sentiment missing"

    def test_js_render_function_exists(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "renderSocialSentiment" in content, "renderSocialSentiment missing"

    def test_js_api_call_to_endpoint(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "/social-sentiment" in content, "/social-sentiment call missing"
