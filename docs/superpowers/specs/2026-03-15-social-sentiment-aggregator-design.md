# Social Sentiment Aggregator — Design Spec

**Date:** 2026-03-15
**Status:** Self-approved (autonomous mode)
**Branch:** feat/social-sentiment-aggregator

---

## Overview

A dashboard card that aggregates social sentiment for major crypto assets by combining:
1. **Social volume proxy** — Reddit posts/comments per hour + Twitter engagement points from CryptoCompare's free social stats endpoint (no API key required)
2. **Keyword sentiment scoring** — Scoring recent crypto news headlines (CryptoCompare news API, free) against a curated bullish/bearish keyword dictionary

Signal: `bullish` / `neutral` / `bearish` based on composite score 0–100.

---

## Data Sources (free, no auth)

- `https://min-api.cryptocompare.com/data/v2/news/?lang=EN&categories=BTC` — latest crypto news headlines
- `https://min-api.cryptocompare.com/data/social/coin/latest?coinId=1182` — BTC social stats (Reddit + Twitter)

---

## Helper Functions (prefix `_ss_`)

| Function | Purpose |
|----------|---------|
| `_ss_keyword_score(text, bullish_words, bearish_words) -> float` | Count keyword hits, return normalized [-1, 1] |
| `_ss_normalize_score(raw, min_val, max_val) -> float` | Map to [0, 100], clamped |
| `_ss_sentiment_label(score) -> str` | 5-level label: very_bullish / bullish / neutral / bearish / very_bearish |
| `_ss_momentum(scores) -> float` | Recent half avg minus prior half avg |
| `_ss_trend(scores) -> str` | rising / falling / stable (linear regression slope) |
| `_ss_volume_proxy(posts_ph, comments_ph, twitter_pts) -> float` | Weighted composite [0-100] |
| `_ss_buzz_level(proxy) -> str` | very_high / high / moderate / low / very_low |
| `_ss_zscore(current, history) -> float` | Standard z-score; ±3.0 on zero-std with non-zero diff |

---

## Response Shape

```json
{
  "sentiment": {
    "score": 65.2,
    "label": "bullish",
    "direction": "rising",
    "momentum": 12.5
  },
  "social_volume": {
    "reddit_posts_per_hour": 42,
    "reddit_comments_per_hour": 318,
    "twitter_points": 950000,
    "volume_proxy": 73.4,
    "buzz": "high"
  },
  "keywords": {
    "bullish_count": 14,
    "bearish_count": 6,
    "neutral_count": 10,
    "dominant": "bullish",
    "top_bullish": ["breakout", "rally", "accumulation"],
    "top_bearish": ["dump", "crash", "sell"]
  },
  "history": [
    {"date": "2024-11-14", "score": 58.1, "label": "neutral"},
    ...
  ],
  "zscore": 0.8,
  "description": "Bullish: social sentiment score 65/100 — buying signals dominant"
}
```

---

## API Endpoint

`GET /api/social-sentiment` — no symbol parameter (global macro signal)

---

## Frontend

- Card ID: `card-social-sentiment`
- Badge: `BULLISH` / `NEUTRAL` / `BEARISH` (color-coded)
- Render function: `renderSocialSentiment()`
- Shows: score gauge, keyword counts, volume proxy, trend direction

---

## Tests (target 65)

- `_ss_keyword_score`: 7 tests
- `_ss_normalize_score`: 6 tests
- `_ss_sentiment_label`: 7 tests
- `_ss_momentum`: 6 tests
- `_ss_trend`: 6 tests
- `_ss_volume_proxy`: 6 tests
- `_ss_buzz_level`: 5 tests
- `_ss_zscore`: 6 tests
- SAMPLE_RESPONSE shape: 12 tests
- Structural: 4 tests

**Total: 65 tests**

---

## Self-Approval

Design reviewed. Scope is appropriate, all data is from free APIs, helpers are pure functions suitable for TDD.
**Approved for implementation.**
