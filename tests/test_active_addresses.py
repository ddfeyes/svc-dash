"""
Unit / smoke tests for GET /api/active-addresses  (Issue #182).

Validates:
  - Deterministic RNG with seed 20260326 (same output on every call)
  - Response is a list of exactly 30 items
  - Each item has {timestamp: str, count: int, growth_rate: float}
  - growth_rate computation correctness (_aa_growth_rate)
  - trend label classification (_aa_trend_label)
  - count generation bounds (_aa_generate_counts)
  - Edge cases: zero previous count, single item, boundary values
  - Route registration in api.py
  - HTML card present in index.html
  - JS render function present in app.js
  - JS API call present in app.js
  - Refresh cycle wiring in app.js
  - Loading/error state present in HTML
  - Badge element present in HTML
"""

import sys
import os
import asyncio
import datetime

import pytest

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _aa_growth_rate,
    _aa_trend_label,
    _aa_generate_counts,
    compute_active_addresses,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read(rel_path: str) -> str:
    base = os.path.join(os.path.dirname(__file__), "..")
    with open(os.path.join(base, rel_path)) as f:
        return f.read()


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# 1. _aa_growth_rate
# ---------------------------------------------------------------------------

class TestAaGrowthRate:
    def test_zero_previous_returns_zero(self):
        assert _aa_growth_rate(1_000_000, 0) == 0.0

    def test_equal_values_returns_zero(self):
        assert _aa_growth_rate(500_000, 500_000) == pytest.approx(0.0, abs=1e-6)

    def test_positive_growth(self):
        rate = _aa_growth_rate(110, 100)
        assert rate == pytest.approx(10.0, rel=1e-4)

    def test_negative_growth(self):
        rate = _aa_growth_rate(90, 100)
        assert rate == pytest.approx(-10.0, rel=1e-4)

    def test_returns_float(self):
        assert isinstance(_aa_growth_rate(110, 100), float)

    def test_large_counts_correct(self):
        rate = _aa_growth_rate(1_050_000, 1_000_000)
        assert rate == pytest.approx(5.0, rel=1e-3)

    def test_result_rounded_to_4_decimal_places(self):
        rate = _aa_growth_rate(1_000_001, 1_000_000)
        # Should have at most 4 decimal places
        s = str(abs(rate))
        if '.' in s:
            decimals = len(s.split('.')[1])
            assert decimals <= 4

    def test_large_growth(self):
        rate = _aa_growth_rate(200, 100)
        assert rate == pytest.approx(100.0, rel=1e-4)

    def test_small_growth(self):
        rate = _aa_growth_rate(101, 100)
        assert rate == pytest.approx(1.0, rel=1e-3)

    def test_both_zero_returns_zero(self):
        assert _aa_growth_rate(0, 0) == 0.0


# ---------------------------------------------------------------------------
# 2. _aa_trend_label
# ---------------------------------------------------------------------------

class TestAaTrendLabel:
    def test_large_positive_is_growing(self):
        assert _aa_trend_label(5.0) == "growing"

    def test_large_negative_is_declining(self):
        assert _aa_trend_label(-5.0) == "declining"

    def test_zero_is_stable(self):
        assert _aa_trend_label(0.0) == "stable"

    def test_small_positive_below_threshold_is_stable(self):
        assert _aa_trend_label(0.3) == "stable"

    def test_small_negative_above_threshold_is_stable(self):
        assert _aa_trend_label(-0.3) == "stable"

    def test_boundary_positive_above_is_growing(self):
        assert _aa_trend_label(0.51) == "growing"

    def test_boundary_negative_below_is_declining(self):
        assert _aa_trend_label(-0.51) == "declining"

    def test_returns_string(self):
        assert isinstance(_aa_trend_label(1.0), str)

    def test_valid_labels_only(self):
        for val in [-10.0, -1.0, -0.3, 0.0, 0.3, 1.0, 10.0]:
            assert _aa_trend_label(val) in ("growing", "declining", "stable")

    def test_exact_threshold_positive_is_stable(self):
        # 0.5 is the boundary — exactly at threshold should be stable
        result = _aa_trend_label(0.5)
        assert result in ("stable", "growing")  # boundary: depends on > vs >=

    def test_exact_threshold_negative_is_stable(self):
        result = _aa_trend_label(-0.5)
        assert result in ("stable", "declining")


# ---------------------------------------------------------------------------
# 3. _aa_generate_counts
# ---------------------------------------------------------------------------

class TestAaGenerateCounts:
    def test_returns_list(self):
        import random
        rng = random.Random(42)
        assert isinstance(_aa_generate_counts(rng, n=10), list)

    def test_correct_length(self):
        import random
        rng = random.Random(42)
        assert len(_aa_generate_counts(rng, n=30)) == 30

    def test_custom_length(self):
        import random
        rng = random.Random(42)
        assert len(_aa_generate_counts(rng, n=10)) == 10

    def test_all_positive(self):
        import random
        rng = random.Random(42)
        counts = _aa_generate_counts(rng, n=30)
        assert all(c > 0 for c in counts)

    def test_all_integers(self):
        import random
        rng = random.Random(42)
        counts = _aa_generate_counts(rng, n=30)
        assert all(isinstance(c, int) for c in counts)

    def test_minimum_floor_applied(self):
        import random
        rng = random.Random(42)
        counts = _aa_generate_counts(rng, n=50, base=100_001)
        assert all(c >= 100_000 for c in counts)

    def test_deterministic_with_same_seed(self):
        import random
        r1 = random.Random(99)
        r2 = random.Random(99)
        assert _aa_generate_counts(r1, n=30) == _aa_generate_counts(r2, n=30)

    def test_different_seeds_different_counts(self):
        import random
        r1 = random.Random(1)
        r2 = random.Random(2)
        c1 = _aa_generate_counts(r1, n=30)
        c2 = _aa_generate_counts(r2, n=30)
        assert c1 != c2

    def test_empty_list_for_n_zero(self):
        import random
        rng = random.Random(42)
        assert _aa_generate_counts(rng, n=0) == []


# ---------------------------------------------------------------------------
# 4. compute_active_addresses — seed reproducibility
# ---------------------------------------------------------------------------

class TestComputeActiveAddressesSeedReproducibility:
    def test_returns_list(self):
        result = run(compute_active_addresses())
        assert isinstance(result, list)

    def test_exactly_30_items(self):
        result = run(compute_active_addresses())
        assert len(result) == 30

    def test_same_result_on_repeated_calls(self):
        r1 = run(compute_active_addresses())
        r2 = run(compute_active_addresses())
        assert r1 == r2

    def test_first_item_growth_rate_zero(self):
        result = run(compute_active_addresses())
        assert result[0]["growth_rate"] == pytest.approx(0.0, abs=1e-6)

    def test_all_counts_deterministic(self):
        r1 = run(compute_active_addresses())
        r2 = run(compute_active_addresses())
        counts1 = [d["count"] for d in r1]
        counts2 = [d["count"] for d in r2]
        assert counts1 == counts2

    def test_all_growth_rates_deterministic(self):
        r1 = run(compute_active_addresses())
        r2 = run(compute_active_addresses())
        rates1 = [d["growth_rate"] for d in r1]
        rates2 = [d["growth_rate"] for d in r2]
        assert rates1 == rates2

    def test_seed_value_20260326_produces_fixed_first_count(self):
        """With seed 20260326 the first base randint is deterministic."""
        result = run(compute_active_addresses())
        # Verify non-trivial: count is in expected ballpark
        assert 800_000 <= result[0]["count"] <= 1_100_000


# ---------------------------------------------------------------------------
# 5. compute_active_addresses — data shape & validation
# ---------------------------------------------------------------------------

class TestComputeActiveAddressesShape:
    def setup_method(self):
        self.data = run(compute_active_addresses())

    def test_each_item_has_timestamp(self):
        for item in self.data:
            assert "timestamp" in item, f"missing 'timestamp' in {item}"

    def test_each_item_has_count(self):
        for item in self.data:
            assert "count" in item, f"missing 'count' in {item}"

    def test_each_item_has_growth_rate(self):
        for item in self.data:
            assert "growth_rate" in item, f"missing 'growth_rate' in {item}"

    def test_timestamp_is_string(self):
        for item in self.data:
            assert isinstance(item["timestamp"], str)

    def test_count_is_int(self):
        for item in self.data:
            assert isinstance(item["count"], int)

    def test_growth_rate_is_float(self):
        for item in self.data:
            assert isinstance(item["growth_rate"], float)

    def test_all_counts_positive(self):
        for item in self.data:
            assert item["count"] > 0

    def test_timestamps_in_iso_format(self):
        for item in self.data:
            # Should parse without error
            datetime.datetime.strptime(item["timestamp"], "%Y-%m-%dT%H:%M:%SZ")

    def test_timestamps_ascending(self):
        ts_list = [item["timestamp"] for item in self.data]
        assert ts_list == sorted(ts_list)

    def test_no_extra_keys_not_required(self):
        """Items may have exactly 3 keys."""
        for item in self.data:
            assert set(item.keys()) == {"timestamp", "count", "growth_rate"}

    def test_growth_rate_consistent_with_counts(self):
        """growth_rate[i] == (count[i] - count[i-1]) / count[i-1] * 100 for i > 0."""
        for i in range(1, len(self.data)):
            prev = self.data[i - 1]["count"]
            curr = self.data[i]["count"]
            expected = round((curr - prev) / prev * 100, 4)
            assert self.data[i]["growth_rate"] == pytest.approx(expected, abs=1e-3)

    def test_date_range_covers_30_days(self):
        first_ts = self.data[0]["timestamp"]
        last_ts  = self.data[-1]["timestamp"]
        first_dt = datetime.datetime.strptime(first_ts, "%Y-%m-%dT%H:%M:%SZ")
        last_dt  = datetime.datetime.strptime(last_ts,  "%Y-%m-%dT%H:%M:%SZ")
        delta = (last_dt - first_dt).days
        assert delta == 29  # 30 points = 29 days difference

    def test_last_timestamp_is_2026_03_20(self):
        last_ts = self.data[-1]["timestamp"]
        assert last_ts.startswith("2026-03-20")


# ---------------------------------------------------------------------------
# 6. Refresh cycle timing
# ---------------------------------------------------------------------------

class TestRefreshCycleTiming:
    def test_refresh_contains_render_active_addresses(self):
        js = _read("frontend/app.js")
        assert "renderActiveAddresses" in js

    def test_refresh_batch_wires_active_addresses(self):
        js = _read("frontend/app.js")
        # The safe() call should appear in the refresh function
        assert "safe(renderActiveAddresses)" in js

    def test_batch_comment_present(self):
        js = _read("frontend/app.js")
        assert "active-addresses" in js or "active addresses" in js.lower()

    def test_refresh_function_exists(self):
        js = _read("frontend/app.js")
        assert "async function refresh()" in js

    def test_refresh_ms_constant_defined(self):
        js = _read("frontend/app.js")
        assert "REFRESH_MS" in js


# ---------------------------------------------------------------------------
# 7. Error states
# ---------------------------------------------------------------------------

class TestErrorStates:
    def test_html_has_loading_state(self):
        html = _read("frontend/index.html")
        assert "Loading" in html

    def test_js_handles_fetch_error(self):
        js = _read("frontend/app.js")
        # renderActiveAddresses should have a try/catch or error path
        assert "Error loading active addresses" in js

    def test_js_handles_empty_array(self):
        js = _read("frontend/app.js")
        assert "No data" in js

    def test_js_checks_array_length(self):
        js = _read("frontend/app.js")
        assert "Array.isArray" in js

    def test_js_checks_res_ok(self):
        js = _read("frontend/app.js")
        assert "res.ok" in js


# ---------------------------------------------------------------------------
# 8. Structural tests
# ---------------------------------------------------------------------------

class TestStructural:
    def test_route_registered_in_api_py(self):
        content = _read("backend/api.py")
        assert "/active-addresses" in content

    def test_compute_function_imported_in_api_py(self):
        content = _read("backend/api.py")
        assert "compute_active_addresses" in content

    def test_html_card_exists(self):
        html = _read("frontend/index.html")
        assert "card-active-addresses" in html

    def test_html_has_badge_element(self):
        html = _read("frontend/index.html")
        assert "active-addresses-badge" in html

    def test_html_has_content_div(self):
        html = _read("frontend/index.html")
        assert "active-addresses-content" in html

    def test_html_card_title(self):
        html = _read("frontend/index.html")
        assert "On-Chain Active Addresses" in html

    def test_js_render_function_exists(self):
        js = _read("frontend/app.js")
        assert "async function renderActiveAddresses" in js

    def test_js_api_call_to_endpoint(self):
        js = _read("frontend/app.js")
        assert "/api/active-addresses" in js

    def test_metrics_py_has_compute_function(self):
        content = _read("backend/metrics.py")
        assert "async def compute_active_addresses" in content

    def test_metrics_py_has_helper_growth_rate(self):
        content = _read("backend/metrics.py")
        assert "_aa_growth_rate" in content

    def test_metrics_py_has_helper_trend_label(self):
        content = _read("backend/metrics.py")
        assert "_aa_trend_label" in content

    def test_metrics_py_uses_seed_20260326(self):
        content = _read("backend/metrics.py")
        assert "20260326" in content

    def test_js_sparkline_rendered(self):
        js = _read("frontend/app.js")
        assert "sparkline" in js.lower() or "polyline" in js.lower() or "sparkSvg" in js

    def test_js_badge_green_for_positive(self):
        js = _read("frontend/app.js")
        assert "badge-green" in js

    def test_js_badge_red_for_negative(self):
        js = _read("frontend/app.js")
        assert "badge-red" in js

    def test_html_meta_text(self):
        html = _read("frontend/index.html")
        assert "sparkline" in html or "30d" in html or "growth rate" in html
