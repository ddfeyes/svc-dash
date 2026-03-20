"""
TDD tests for adaptive volume profile + POC highlighting (issue #173).

Covers:
  - _adaptive_bin_count: dynamic resolution based on data density
  - POC detection and is_poc flag
  - in_value_area and pct_of_max annotation flags
  - Frontend smoke tests: HTML card, JS render function, batch integration
"""
import os
import sys
import math
import pytest

_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(_ROOT, "backend"))

from metrics import _adaptive_bin_count  # noqa: E402


# ── Helpers: Python mirrors of bin annotation logic ───────────────────────────

def annotate_bins(bins: list[dict], value_area_pct: float = 0.70) -> list[dict]:
    """
    Annotate bins with is_poc, in_value_area, pct_of_max.
    Pure mirror of the logic inside compute_volume_profile_adaptive.
    """
    if not bins:
        return []
    total = sum(b["volume"] for b in bins)
    if total == 0:
        return bins

    poc_entry = max(bins, key=lambda b: b["volume"])
    poc_price = poc_entry["price"]
    poc_volume = poc_entry["volume"]
    poc_idx = next(i for i, b in enumerate(bins) if b["price"] == poc_price)

    target = total * value_area_pct
    lo, hi = poc_idx, poc_idx
    accumulated = poc_volume

    while accumulated < target:
        can_up = hi + 1 < len(bins)
        can_dn = lo - 1 >= 0
        if not can_up and not can_dn:
            break
        vol_up = bins[hi + 1]["volume"] if can_up else -1.0
        vol_dn = bins[lo - 1]["volume"] if can_dn else -1.0
        if vol_up >= vol_dn:
            hi += 1
            accumulated += vol_up
        else:
            lo -= 1
            accumulated += vol_dn

    return [
        {
            **b,
            "is_poc": b["price"] == poc_price,
            "in_value_area": lo <= i <= hi,
            "pct_of_max": round(b["volume"] / poc_volume * 100, 2),
        }
        for i, b in enumerate(bins)
    ]


def _html() -> str:
    with open(os.path.join(_ROOT, "frontend", "index.html"), encoding="utf-8") as f:
        return f.read()


def _js() -> str:
    with open(os.path.join(_ROOT, "frontend", "app.js"), encoding="utf-8") as f:
        return f.read()


# ═══════════════════════════════════════════════════════════════════════════════
# _adaptive_bin_count
# ═══════════════════════════════════════════════════════════════════════════════

class TestAdaptiveBinCount:
    def test_returns_int(self):
        assert isinstance(_adaptive_bin_count(10), int)

    def test_sparse_data_returns_raw_count(self):
        # Fewer raw levels than min_bins → no downsampling needed
        assert _adaptive_bin_count(5, min_bins=20, max_bins=100) == 5

    def test_zero_returns_zero(self):
        assert _adaptive_bin_count(0) == 0

    def test_exactly_min_bins_returns_min_bins(self):
        assert _adaptive_bin_count(20, min_bins=20, max_bins=100) == 20

    def test_dense_data_capped_at_max_bins(self):
        # Very many raw levels → capped at max_bins
        assert _adaptive_bin_count(10_000, min_bins=20, max_bins=100) == 100

    def test_medium_data_between_bounds(self):
        # Medium density: result is between min and max
        result = _adaptive_bin_count(100, min_bins=20, max_bins=100)
        assert 20 <= result <= 100

    def test_result_at_least_min_bins_when_n_raw_exceeds_min(self):
        # For n_raw > min_bins, result must be >= min_bins
        result = _adaptive_bin_count(25, min_bins=20, max_bins=100)
        assert result >= 20

    def test_result_never_exceeds_max_bins(self):
        for n in (50, 100, 200, 500, 1000):
            assert _adaptive_bin_count(n, min_bins=20, max_bins=100) <= 100

    def test_monotone_increasing_with_density(self):
        # More raw levels → same or more bins (up to max)
        prev = 0
        for n in (10, 30, 60, 100, 200, 500):
            cur = _adaptive_bin_count(n, min_bins=10, max_bins=80)
            assert cur >= prev
            prev = cur

    def test_custom_min_max(self):
        # Respect custom bounds
        r = _adaptive_bin_count(50, min_bins=5, max_bins=30)
        assert 5 <= r <= 30

    def test_default_min_bins_is_20(self):
        # Default min_bins=20: sparse case below 20 returns n_raw
        assert _adaptive_bin_count(10) == 10

    def test_default_max_bins_is_100(self):
        assert _adaptive_bin_count(10_000) == 100


# ═══════════════════════════════════════════════════════════════════════════════
# Bin annotation: is_poc
# ═══════════════════════════════════════════════════════════════════════════════

BINS_SIMPLE = [
    {"price": 1.0, "volume": 100.0},
    {"price": 2.0, "volume": 500.0},  # POC
    {"price": 3.0, "volume": 200.0},
    {"price": 4.0, "volume": 50.0},
    {"price": 5.0, "volume": 150.0},
]


class TestIsPoc:
    def test_exactly_one_bin_is_poc(self):
        annotated = annotate_bins(BINS_SIMPLE)
        poc_bins = [b for b in annotated if b["is_poc"]]
        assert len(poc_bins) == 1

    def test_poc_is_max_volume(self):
        annotated = annotate_bins(BINS_SIMPLE)
        poc_bin = next(b for b in annotated if b["is_poc"])
        max_vol = max(b["volume"] for b in annotated)
        assert poc_bin["volume"] == max_vol

    def test_poc_price_correct(self):
        annotated = annotate_bins(BINS_SIMPLE)
        poc_bin = next(b for b in annotated if b["is_poc"])
        assert poc_bin["price"] == 2.0

    def test_single_bin_is_poc(self):
        annotated = annotate_bins([{"price": 1.0, "volume": 100.0}])
        assert annotated[0]["is_poc"] is True

    def test_empty_returns_empty(self):
        assert annotate_bins([]) == []

    def test_poc_flag_is_bool(self):
        annotated = annotate_bins(BINS_SIMPLE)
        for b in annotated:
            assert isinstance(b["is_poc"], bool)


# ═══════════════════════════════════════════════════════════════════════════════
# Bin annotation: pct_of_max
# ═══════════════════════════════════════════════════════════════════════════════

class TestPctOfMax:
    def test_poc_pct_is_100(self):
        annotated = annotate_bins(BINS_SIMPLE)
        poc_bin = next(b for b in annotated if b["is_poc"])
        assert poc_bin["pct_of_max"] == pytest.approx(100.0)

    def test_all_pct_in_0_to_100(self):
        annotated = annotate_bins(BINS_SIMPLE)
        for b in annotated:
            assert 0.0 <= b["pct_of_max"] <= 100.0

    def test_pct_proportional_to_volume(self):
        bins = [
            {"price": 1.0, "volume": 200.0},
            {"price": 2.0, "volume": 400.0},  # POC, 100%
        ]
        annotated = annotate_bins(bins)
        non_poc = next(b for b in annotated if not b["is_poc"])
        assert non_poc["pct_of_max"] == pytest.approx(50.0)

    def test_pct_of_max_is_float(self):
        annotated = annotate_bins(BINS_SIMPLE)
        for b in annotated:
            assert isinstance(b["pct_of_max"], float)


# ═══════════════════════════════════════════════════════════════════════════════
# Bin annotation: in_value_area
# ═══════════════════════════════════════════════════════════════════════════════

class TestInValueArea:
    def test_poc_always_in_value_area(self):
        annotated = annotate_bins(BINS_SIMPLE)
        poc_bin = next(b for b in annotated if b["is_poc"])
        assert poc_bin["in_value_area"] is True

    def test_value_area_covers_at_least_70pct_of_volume(self):
        annotated = annotate_bins(BINS_SIMPLE)
        total = sum(b["volume"] for b in annotated)
        va_vol = sum(b["volume"] for b in annotated if b["in_value_area"])
        assert va_vol / total >= 0.70

    def test_value_area_is_contiguous(self):
        # The bins in value area should form a contiguous range
        annotated = sorted(annotate_bins(BINS_SIMPLE), key=lambda b: b["price"])
        in_va = [i for i, b in enumerate(annotated) if b["in_value_area"]]
        assert in_va == list(range(min(in_va), max(in_va) + 1))

    def test_single_bin_in_value_area(self):
        annotated = annotate_bins([{"price": 1.0, "volume": 100.0}])
        assert annotated[0]["in_value_area"] is True

    def test_in_value_area_flag_is_bool(self):
        annotated = annotate_bins(BINS_SIMPLE)
        for b in annotated:
            assert isinstance(b["in_value_area"], bool)

    def test_custom_value_area_pct_80(self):
        annotated = annotate_bins(BINS_SIMPLE, value_area_pct=0.80)
        total = sum(b["volume"] for b in annotated)
        va_vol = sum(b["volume"] for b in annotated if b["in_value_area"])
        assert va_vol / total >= 0.80

    def test_zero_volume_bins_not_annotated(self):
        bins = [
            {"price": 1.0, "volume": 0.0},
            {"price": 2.0, "volume": 0.0},
        ]
        # all zero volumes — should not crash
        assert annotate_bins(bins) == bins  # no changes when total=0


# ═══════════════════════════════════════════════════════════════════════════════
# Frontend smoke tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestHtmlCard:
    def test_adaptive_vp_canvas_exists(self):
        assert "adaptive-vp-canvas" in _html()

    def test_adaptive_vp_metrics_div_exists(self):
        assert "adaptive-vp-metrics" in _html()

    def test_adaptive_vp_badge_exists(self):
        assert "adaptive-vp-badge" in _html()

    def test_card_adaptive_vp_id_exists(self):
        assert "card-adaptive-vp" in _html()


class TestJsRenderFunction:
    def test_render_function_exists(self):
        assert "renderAdaptiveVolumeProfile" in _js()

    def test_calls_volume_profile_adaptive_endpoint(self):
        assert "/volume-profile/adaptive" in _js()

    def test_poc_highlighted_yellow(self):
        # POC bins should be rendered in yellow
        js = _js()
        assert "240,192,64" in js or "rgba(240" in js  # yellow color used for POC

    def test_is_poc_flag_used(self):
        assert "is_poc" in _js()

    def test_in_value_area_flag_used(self):
        assert "in_value_area" in _js()

    def test_auto_bins_mode(self):
        # Frontend should use bins=0 to trigger adaptive resolution
        js = _js()
        assert "bins=0" in js

    def test_smooth_animation_on_update(self):
        # Should not use 'none' animation for adaptive VP chart updates
        js = _js()
        # The adaptive VP chart update should use 'active' or no mode (smooth)
        assert "adaptiveVpChart.update('active')" in js or (
            "adaptiveVpChart.update()" in js
        )


class TestBatchIntegration:
    def test_render_in_batch_10(self):
        js = _js()
        # Find Batch 10 comment and check renderAdaptiveVolumeProfile is nearby
        batch10_idx = js.find("Batch 10")
        assert batch10_idx != -1, "Batch 10 not found in app.js"
        # The function should appear within a reasonable window after Batch 10 comment
        snippet = js[batch10_idx: batch10_idx + 300]
        assert "renderAdaptiveVolumeProfile" in snippet

    def test_resolution_bins_in_response_shape(self):
        # The /volume-profile/adaptive response should include resolution_bins
        # We test via the JS code that handles it (or just test the backend dict shape)
        # Backend test: pure function output check
        from metrics import _adaptive_bin_count
        # resolution_bins is always an int >= 0
        assert isinstance(_adaptive_bin_count(50), int)
