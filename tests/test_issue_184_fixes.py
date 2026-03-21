"""
Tests for issue #184 fixes — dashboard audit & UX improvements:
  1. Duplicate HTML IDs removed (liq-cascade-detector, leverage-heatmap, social-sentiment)
  2. Social Sentiment Momentum card has own unique IDs
  3. refreshSocialSentimentMomentum JS uses new momentum-specific IDs
  4. Error timeout increased from 10s → 35s and no longer fires on "No data" states
  5. renderLeverageHeatmap alias removed; no redundant double-render
"""
import os
import re
from collections import Counter

ROOT = os.path.join(os.path.dirname(__file__), "..")

_html_cache: str | None = None
_js_cache: str | None = None


def _html() -> str:
    global _html_cache
    if _html_cache is None:
        with open(os.path.join(ROOT, "frontend", "index.html"), encoding="utf-8") as f:
            _html_cache = f.read()
    return _html_cache


def _js() -> str:
    global _js_cache
    if _js_cache is None:
        with open(os.path.join(ROOT, "frontend", "app.js"), encoding="utf-8") as f:
            _js_cache = f.read()
    return _js_cache


def _all_ids(html: str) -> Counter:
    """Return Counter of all id="..." occurrences in the HTML."""
    return Counter(re.findall(r'id="([^"]+)"', html))


# ── Group 1: No duplicate HTML IDs ───────────────────────────────────────────


class TestNoDuplicateHtmlIds:
    def test_liq_cascade_detector_content_unique(self):
        counts = _all_ids(_html())
        assert counts["liq-cascade-detector-content"] == 1, (
            f"'liq-cascade-detector-content' appears {counts['liq-cascade-detector-content']}x — must be unique"
        )

    def test_liq_cascade_detector_badge_unique(self):
        counts = _all_ids(_html())
        assert counts["liq-cascade-detector-badge"] == 1, (
            f"'liq-cascade-detector-badge' appears {counts['liq-cascade-detector-badge']}x — must be unique"
        )

    def test_card_liq_cascade_detector_unique(self):
        counts = _all_ids(_html())
        assert counts["card-liq-cascade-detector"] == 1, (
            f"'card-liq-cascade-detector' section appears {counts['card-liq-cascade-detector']}x — must be unique"
        )

    def test_leverage_heatmap_content_unique(self):
        counts = _all_ids(_html())
        assert counts["leverage-heatmap-content"] == 1, (
            f"'leverage-heatmap-content' appears {counts['leverage-heatmap-content']}x — must be unique"
        )

    def test_leverage_heatmap_badge_unique(self):
        counts = _all_ids(_html())
        assert counts["leverage-heatmap-badge"] == 1, (
            f"'leverage-heatmap-badge' appears {counts['leverage-heatmap-badge']}x — must be unique"
        )

    def test_social_sentiment_content_unique(self):
        counts = _all_ids(_html())
        assert counts["social-sentiment-content"] == 1, (
            f"'social-sentiment-content' appears {counts['social-sentiment-content']}x — must be unique"
        )

    def test_social_sentiment_badge_unique(self):
        counts = _all_ids(_html())
        assert counts["social-sentiment-badge"] == 1, (
            f"'social-sentiment-badge' appears {counts['social-sentiment-badge']}x — must be unique"
        )

    def test_card_social_sentiment_unique(self):
        counts = _all_ids(_html())
        assert counts["card-social-sentiment"] == 1, (
            f"'card-social-sentiment' section appears {counts['card-social-sentiment']}x — must be unique"
        )

    def test_no_globally_duplicated_ids(self):
        """All id= attributes in HTML must be unique."""
        counts = _all_ids(_html())
        dupes = {k: v for k, v in counts.items() if v > 1}
        assert not dupes, f"Duplicate HTML IDs found: {dupes}"


# ── Group 2: Social Sentiment Momentum has its own unique IDs ─────────────────


class TestSocialSentimentMomentumCard:
    def test_momentum_content_id_exists(self):
        assert 'id="social-sentiment-momentum-content"' in _html(), (
            "social-sentiment-momentum-content ID not found — "
            "Social Sentiment Momentum card needs its own content element"
        )

    def test_momentum_badge_id_exists(self):
        assert 'id="social-sentiment-momentum-badge"' in _html(), (
            "social-sentiment-momentum-badge ID not found — "
            "Social Sentiment Momentum card needs its own badge element"
        )

    def test_momentum_section_id_exists(self):
        assert 'id="card-social-sentiment-momentum"' in _html(), (
            "card-social-sentiment-momentum section ID not found"
        )

    def test_momentum_card_has_title(self):
        html = _html()
        idx = html.find('id="card-social-sentiment-momentum"')
        assert idx != -1, "card-social-sentiment-momentum not found"
        snippet = html[idx : idx + 400]
        assert "Social Sentiment Momentum" in snippet, (
            "Social Sentiment Momentum card missing its title"
        )

    def test_momentum_card_has_card_header(self):
        """Momentum card must have a card-header div for consistent layout."""
        html = _html()
        idx = html.find('id="card-social-sentiment-momentum"')
        assert idx != -1
        snippet = html[idx : idx + 500]
        assert "card-header" in snippet, (
            "Social Sentiment Momentum card missing card-header structure"
        )


# ── Group 3: JS uses momentum-specific IDs ───────────────────────────────────


def _fn_body(js: str, fn_name: str) -> str:
    """Extract function body (roughly) by finding its start and the next function."""
    start = js.find(f"async function {fn_name}(")
    if start == -1:
        start = js.find(f"function {fn_name}(")
    assert start != -1, f"Function {fn_name} not found in app.js"
    # Find next top-level async function or const = function
    nxt = js.find("\nasync function ", start + 1)
    nxt2 = js.find("\nfunction ", start + 1)
    end = min(x for x in [nxt, nxt2, len(js)] if x > start)
    return js[start:end]


class TestRefreshSocialSentimentMomentumIds:
    def test_uses_momentum_content_id(self):
        body = _fn_body(_js(), "refreshSocialSentimentMomentum")
        assert "social-sentiment-momentum-content" in body, (
            "refreshSocialSentimentMomentum must target 'social-sentiment-momentum-content', "
            "not the shared 'social-sentiment-content'"
        )

    def test_does_not_use_plain_social_sentiment_content(self):
        body = _fn_body(_js(), "refreshSocialSentimentMomentum")
        # Must NOT use bare 'social-sentiment-content' (without -momentum-)
        assert "'social-sentiment-content'" not in body, (
            "refreshSocialSentimentMomentum must not reference 'social-sentiment-content' "
            "(that belongs to the Social Sentiment card)"
        )

    def test_uses_momentum_badge_id(self):
        body = _fn_body(_js(), "refreshSocialSentimentMomentum")
        assert "social-sentiment-momentum-badge" in body, (
            "refreshSocialSentimentMomentum must target 'social-sentiment-momentum-badge'"
        )

    def test_does_not_use_plain_social_sentiment_badge(self):
        body = _fn_body(_js(), "refreshSocialSentimentMomentum")
        assert "'social-sentiment-badge'" not in body, (
            "refreshSocialSentimentMomentum must not reference 'social-sentiment-badge' "
            "(that belongs to the Social Sentiment card)"
        )


# ── Group 4: Error timeout fixed ─────────────────────────────────────────────


class TestErrorTimeout:
    def _get_timeout_ms(self) -> int:
        """Extract the ms value from the 'still-Loading' cleanup setTimeout in init()."""
        js = _js()
        # Find init() function
        init_start = js.index("async function init()")
        # Find the setTimeout that replaces Loading cards
        region = js[init_start : init_start + 2000]
        m = re.search(r'setTimeout\([^,]+,\s*(\d+)\)', region)
        assert m, "setTimeout not found in init()"
        return int(m.group(1))

    def test_timeout_is_at_least_20_seconds(self):
        ms = self._get_timeout_ms()
        assert ms >= 20000, (
            f"Error timeout is {ms}ms — must be ≥ 20000ms so loading cards "
            "aren't incorrectly flagged before API responses arrive (15s timeout)"
        )

    def test_timeout_is_at_least_30_seconds(self):
        ms = self._get_timeout_ms()
        assert ms >= 30000, (
            f"Error timeout is {ms}ms — should be ≥ 30000ms to allow all "
            "40+ sequential render batches to complete"
        )

    def test_timeout_does_not_replace_no_data_available(self):
        """'No data available' is a valid successful state — must not be overwritten with Error."""
        js = _js()
        init_start = js.index("async function init()")
        region = js[init_start : init_start + 2000]
        # Extract the setTimeout callback content
        to_start = region.find("setTimeout(")
        assert to_start != -1
        # The condition should NOT check 'No data available'
        assert "'No data available'" not in region[to_start : to_start + 600], (
            "Error timeout must not replace 'No data available' with Error — "
            "that's a valid state from a successful empty API response"
        )

    def test_timeout_still_replaces_loading(self):
        """Cards genuinely stuck on Loading should still get an Error badge."""
        js = _js()
        init_start = js.index("async function init()")
        region = js[init_start : init_start + 2000]
        to_start = region.find("setTimeout(")
        callback = region[to_start : to_start + 600]
        assert "Loading" in callback, (
            "Error timeout must still replace 'Loading…' cards with Error badge"
        )


# ── Group 5: Leverage heatmap alias removed ───────────────────────────────────


class TestLeverageHeatmapAlias:
    def test_no_renderLeverageHeatmap_alias(self):
        """renderLeverageHeatmap = renderLeverageRatioHeatmap alias must be removed."""
        js = _js()
        assert "renderLeverageHeatmap = renderLeverageRatioHeatmap" not in js, (
            "renderLeverageHeatmap alias still present — caused duplicate renders "
            "and double-writes to the same DOM element. Remove it."
        )

    def test_renderLeverageRatioHeatmap_called_once_in_refresh(self):
        """renderLeverageRatioHeatmap should be called exactly once per refresh cycle."""
        js = _js()
        refresh_start = js.index("async function refresh()")
        refresh_end = js.index("\nasync function ", refresh_start + 1)
        refresh_body = js[refresh_start:refresh_end]
        count = refresh_body.count("renderLeverageRatioHeatmap")
        assert count == 1, (
            f"renderLeverageRatioHeatmap called {count}x in refresh() — must be exactly once"
        )


# ── Group 6: Render functions target correct unique elements ──────────────────


class TestRenderFunctionTargets:
    def test_renderLiqCascadeDetector_targets_content(self):
        body = _fn_body(_js(), "renderLiqCascadeDetector")
        assert "liq-cascade-detector-content" in body

    def test_renderLiqCascadeDetector_targets_badge(self):
        body = _fn_body(_js(), "renderLiqCascadeDetector")
        assert "liq-cascade-detector-badge" in body

    def test_renderLeverageRatioHeatmap_targets_content(self):
        body = _fn_body(_js(), "renderLeverageRatioHeatmap")
        assert "leverage-heatmap-content" in body

    def test_renderLeverageRatioHeatmap_targets_badge(self):
        body = _fn_body(_js(), "renderLeverageRatioHeatmap")
        assert "leverage-heatmap-badge" in body

    def test_renderSocialSentiment_targets_correct_content(self):
        """renderSocialSentiment targets social-sentiment-content (the original card)."""
        body = _fn_body(_js(), "renderSocialSentiment")
        assert "social-sentiment-content" in body

    def test_renderSocialSentiment_does_not_target_momentum(self):
        """renderSocialSentiment must not target the momentum card's elements."""
        body = _fn_body(_js(), "renderSocialSentiment")
        assert "social-sentiment-momentum-content" not in body
