"""
Unit/smoke tests for the /api/liquidation-heatmap endpoint and
compute_liquidation_heatmap() 2-D heatmap function.

Validates bucket logic, response shape, intensity helpers, sorting,
edge cases, route registration, and the 50×288 matrix heatmap.
"""
import asyncio
import math
import os
import sys
import time

import pytest

_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(_ROOT, "backend"))

# ── Python mirrors of bucket/heatmap logic ────────────────────────────────────

def build_price_buckets(price_min: float, price_max: float, n: int) -> list[dict]:
    """Divide [price_min, price_max] into n equal-width buckets."""
    if n <= 0 or price_min >= price_max:
        return []
    step = (price_max - price_min) / n
    return [
        {
            "price_low":  round(price_min + i * step, 10),
            "price_high": round(price_min + (i + 1) * step, 10),
            "long_usd":   0.0,
            "short_usd":  0.0,
            "total_usd":  0.0,
        }
        for i in range(n)
    ]


def bucket_index(price: float, price_min: float, price_max: float, n: int) -> int:
    """Return which bucket a price falls into (clamped to [0, n-1])."""
    if price_max == price_min:
        return 0
    frac = (price - price_min) / (price_max - price_min)
    return max(0, min(n - 1, int(frac * n)))


def fill_buckets(liquidations: list[dict], price_min: float, price_max: float, n: int) -> list[dict]:
    """Accumulate liquidation USD values into price buckets."""
    buckets = build_price_buckets(price_min, price_max, n)
    for liq in liquidations:
        idx = bucket_index(float(liq["price"]), price_min, price_max, n)
        usd = float(liq.get("value") or liq["price"] * liq["qty"])
        if liq["side"] == "long":
            buckets[idx]["long_usd"] += usd
        else:
            buckets[idx]["short_usd"] += usd
        buckets[idx]["total_usd"] += usd
    return buckets


def intensity(total_usd: float, max_usd: float) -> float:
    """0.0–1.0 intensity for coloring (log-scaled)."""
    if max_usd <= 0 or total_usd <= 0:
        return 0.0
    return math.log1p(total_usd) / math.log1p(max_usd)


# ── Sample data ───────────────────────────────────────────────────────────────

SAMPLE_LIQS = [
    {"ts": 1e9, "symbol": "BANANAS31USDT", "side": "long",  "price": 0.002300, "qty": 1000, "value": 2.30},
    {"ts": 1e9, "symbol": "BANANAS31USDT", "side": "short", "price": 0.002350, "qty": 2000, "value": 4.70},
    {"ts": 1e9, "symbol": "BANANAS31USDT", "side": "long",  "price": 0.002400, "qty": 5000, "value": 12.00},
    {"ts": 1e9, "symbol": "BANANAS31USDT", "side": "long",  "price": 0.002500, "qty": 3000, "value": 7.50},
]
PRICE_MIN = 0.002200
PRICE_MAX = 0.002600
N_BUCKETS = 4

SAMPLE_RESPONSE = {
    "status": "ok",
    "ts": 1773600000.0,
    "window_s": 3600,
    "symbols": {
        "BANANAS31USDT": {
            "buckets": [
                {"price_low": 0.0022, "price_high": 0.0023, "long_usd": 0.0,  "short_usd": 0.0, "total_usd": 0.0},
                {"price_low": 0.0023, "price_high": 0.0024, "long_usd": 2.30, "short_usd": 4.70, "total_usd": 7.00},
                {"price_low": 0.0024, "price_high": 0.0025, "long_usd": 12.0, "short_usd": 0.0, "total_usd": 12.0},
                {"price_low": 0.0025, "price_high": 0.0026, "long_usd": 7.50, "short_usd": 0.0, "total_usd": 7.50},
            ],
            "price_min": 0.0022,
            "price_max": 0.0026,
            "total_usd": 26.5,
            "n_liquidations": 4,
        }
    },
}


# ── Bucket construction ───────────────────────────────────────────────────────

def test_build_buckets_count():
    buckets = build_price_buckets(100.0, 200.0, 5)
    assert len(buckets) == 5


def test_build_buckets_cover_range():
    buckets = build_price_buckets(100.0, 200.0, 4)
    assert buckets[0]["price_low"] == pytest.approx(100.0)
    assert buckets[-1]["price_high"] == pytest.approx(200.0)


def test_build_buckets_contiguous():
    buckets = build_price_buckets(0.0, 1.0, 5)
    for i in range(len(buckets) - 1):
        assert buckets[i]["price_high"] == pytest.approx(buckets[i + 1]["price_low"])


def test_build_buckets_empty_when_min_equals_max():
    assert build_price_buckets(100.0, 100.0, 5) == []


def test_build_buckets_zero_count():
    assert build_price_buckets(100.0, 200.0, 0) == []


def test_bucket_index_first():
    assert bucket_index(100.0, 100.0, 200.0, 10) == 0


def test_bucket_index_last():
    assert bucket_index(200.0, 100.0, 200.0, 10) == 9


def test_bucket_index_mid():
    idx = bucket_index(150.0, 100.0, 200.0, 10)
    assert idx == 5


def test_bucket_index_clamp_below():
    assert bucket_index(0.0, 100.0, 200.0, 10) == 0


def test_bucket_index_clamp_above():
    assert bucket_index(999.0, 100.0, 200.0, 10) == 9


# ── Fill logic ────────────────────────────────────────────────────────────────

def test_fill_buckets_total_usd():
    buckets = fill_buckets(SAMPLE_LIQS, PRICE_MIN, PRICE_MAX, N_BUCKETS)
    grand_total = sum(b["total_usd"] for b in buckets)
    assert grand_total == pytest.approx(2.30 + 4.70 + 12.00 + 7.50, rel=1e-3)


def test_fill_buckets_long_short_split():
    buckets = fill_buckets(SAMPLE_LIQS, PRICE_MIN, PRICE_MAX, N_BUCKETS)
    total_long  = sum(b["long_usd"]  for b in buckets)
    total_short = sum(b["short_usd"] for b in buckets)
    assert total_long  == pytest.approx(2.30 + 12.00 + 7.50, rel=1e-3)
    assert total_short == pytest.approx(4.70, rel=1e-3)


def test_fill_buckets_empty_input():
    buckets = fill_buckets([], PRICE_MIN, PRICE_MAX, N_BUCKETS)
    assert all(b["total_usd"] == 0.0 for b in buckets)


# ── Intensity helper ──────────────────────────────────────────────────────────

def test_intensity_zero_when_empty():
    assert intensity(0.0, 100.0) == pytest.approx(0.0)


def test_intensity_one_at_max():
    assert intensity(100.0, 100.0) == pytest.approx(1.0)


def test_intensity_between_zero_and_one():
    v = intensity(50.0, 200.0)
    assert 0.0 < v < 1.0


def test_intensity_zero_max():
    assert intensity(50.0, 0.0) == 0.0


# ── Response shape ────────────────────────────────────────────────────────────

def test_response_has_status():
    assert SAMPLE_RESPONSE["status"] == "ok"


def test_response_has_symbols_dict():
    assert isinstance(SAMPLE_RESPONSE["symbols"], dict)


def test_response_has_window_s():
    assert SAMPLE_RESPONSE["window_s"] == 3600


def test_response_has_ts():
    assert isinstance(SAMPLE_RESPONSE["ts"], float)


def test_symbol_entry_has_buckets():
    entry = SAMPLE_RESPONSE["symbols"]["BANANAS31USDT"]
    assert isinstance(entry["buckets"], list)
    assert len(entry["buckets"]) > 0


def test_symbol_entry_has_price_range():
    entry = SAMPLE_RESPONSE["symbols"]["BANANAS31USDT"]
    assert "price_min" in entry and "price_max" in entry
    assert entry["price_max"] > entry["price_min"]


def test_bucket_has_required_keys():
    for bucket in SAMPLE_RESPONSE["symbols"]["BANANAS31USDT"]["buckets"]:
        for key in ("price_low", "price_high", "long_usd", "short_usd", "total_usd"):
            assert key in bucket, f"Missing key '{key}'"


# ── Route registration ────────────────────────────────────────────────────────

def test_liquidation_heatmap_route_registered():
    import tempfile
    os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "t.db"))
    os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from api import router
    paths = [r.path for r in router.routes]
    assert any("liquidation-heatmap" in p for p in paths)


# ═══════════════════════════════════════════════════════════════════════════════
# compute_liquidation_heatmap() — 2-D heatmap (50 price levels × 288 time buckets)
# ═══════════════════════════════════════════════════════════════════════════════


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(scope="module")
def lhm():
    from metrics import compute_liquidation_heatmap
    return _run(compute_liquidation_heatmap())


@pytest.fixture(scope="module")
def lhm2():
    """Second call — for determinism checks."""
    from metrics import compute_liquidation_heatmap
    return _run(compute_liquidation_heatmap())


# ── Field presence ─────────────────────────────────────────────────────────────

class TestLHMFieldPresence:
    def test_has_heatmap_matrix(self, lhm):
        assert "heatmap_matrix" in lhm

    def test_has_zones(self, lhm):
        assert "zones" in lhm

    def test_has_peak_price_level(self, lhm):
        assert "peak_price_level" in lhm

    def test_has_peak_time(self, lhm):
        assert "peak_time" in lhm

    def test_has_price_levels(self, lhm):
        assert "price_levels" in lhm

    def test_has_n_price_levels(self, lhm):
        assert "n_price_levels" in lhm

    def test_has_n_time_buckets(self, lhm):
        assert "n_time_buckets" in lhm

    def test_has_current_price(self, lhm):
        assert "current_price" in lhm

    def test_has_price_min(self, lhm):
        assert "price_min" in lhm

    def test_has_price_max(self, lhm):
        assert "price_max" in lhm

    def test_has_total_liquidations(self, lhm):
        assert "total_liquidations" in lhm

    def test_has_symbol(self, lhm):
        assert "symbol" in lhm

    def test_all_required_fields(self, lhm):
        required = {
            "heatmap_matrix", "zones", "peak_price_level", "peak_time",
            "price_levels", "n_price_levels", "n_time_buckets",
            "current_price", "price_min", "price_max",
            "total_liquidations", "symbol",
        }
        assert required.issubset(lhm.keys())


# ── Matrix shape ───────────────────────────────────────────────────────────────

class TestLHMShape:
    def test_matrix_has_50_rows(self, lhm):
        assert len(lhm["heatmap_matrix"]) == 50

    def test_matrix_rows_have_288_cols(self, lhm):
        for row in lhm["heatmap_matrix"]:
            assert len(row) == 288

    def test_n_price_levels_is_50(self, lhm):
        assert lhm["n_price_levels"] == 50

    def test_n_time_buckets_is_288(self, lhm):
        assert lhm["n_time_buckets"] == 288

    def test_price_levels_length_matches(self, lhm):
        assert len(lhm["price_levels"]) == lhm["n_price_levels"]

    def test_matrix_row_count_matches_n_price_levels(self, lhm):
        assert len(lhm["heatmap_matrix"]) == lhm["n_price_levels"]

    def test_matrix_col_count_matches_n_time_buckets(self, lhm):
        for row in lhm["heatmap_matrix"]:
            assert len(row) == lhm["n_time_buckets"]


# ── Type correctness ───────────────────────────────────────────────────────────

class TestLHMTypes:
    def test_matrix_is_list(self, lhm):
        assert isinstance(lhm["heatmap_matrix"], list)

    def test_matrix_rows_are_lists(self, lhm):
        for row in lhm["heatmap_matrix"]:
            assert isinstance(row, list)

    def test_matrix_cells_are_int(self, lhm):
        for row in lhm["heatmap_matrix"]:
            for v in row:
                assert isinstance(v, int)

    def test_zones_is_list(self, lhm):
        assert isinstance(lhm["zones"], list)

    def test_peak_price_level_is_numeric(self, lhm):
        assert isinstance(lhm["peak_price_level"], (int, float))

    def test_peak_time_is_numeric(self, lhm):
        assert isinstance(lhm["peak_time"], (int, float))

    def test_price_levels_is_list(self, lhm):
        assert isinstance(lhm["price_levels"], list)

    def test_current_price_is_numeric(self, lhm):
        assert isinstance(lhm["current_price"], (int, float))

    def test_total_liquidations_is_int(self, lhm):
        assert isinstance(lhm["total_liquidations"], int)

    def test_symbol_is_str(self, lhm):
        assert isinstance(lhm["symbol"], str)


# ── Value ranges ───────────────────────────────────────────────────────────────

class TestLHMValueRanges:
    def test_matrix_cells_non_negative(self, lhm):
        for row in lhm["heatmap_matrix"]:
            for v in row:
                assert v >= 0

    def test_price_min_less_than_max(self, lhm):
        assert lhm["price_min"] < lhm["price_max"]

    def test_current_price_in_range(self, lhm):
        assert lhm["price_min"] <= lhm["current_price"] <= lhm["price_max"]

    def test_peak_price_level_positive(self, lhm):
        assert lhm["peak_price_level"] > 0

    def test_total_liquidations_non_negative(self, lhm):
        assert lhm["total_liquidations"] >= 0

    def test_peak_time_is_finite(self, lhm):
        assert math.isfinite(lhm["peak_time"])

    def test_window_seconds_is_86400(self, lhm):
        assert lhm.get("window_seconds") == 86400


# ── Zone structure ─────────────────────────────────────────────────────────────

class TestLHMZones:
    def test_zones_is_list(self, lhm):
        assert isinstance(lhm["zones"], list)

    def test_zones_have_price_level(self, lhm):
        for zone in lhm["zones"]:
            assert "price_level" in zone

    def test_zones_have_max_count(self, lhm):
        for zone in lhm["zones"]:
            assert "max_count" in zone

    def test_zones_max_count_above_threshold(self, lhm):
        threshold = lhm.get("zone_threshold", 10)
        for zone in lhm["zones"]:
            assert zone["max_count"] > threshold

    def test_zones_price_level_positive(self, lhm):
        for zone in lhm["zones"]:
            assert zone["price_level"] > 0

    def test_zones_have_price_low_and_high(self, lhm):
        for zone in lhm["zones"]:
            assert "price_low" in zone
            assert "price_high" in zone

    def test_zones_price_low_less_than_high(self, lhm):
        for zone in lhm["zones"]:
            assert zone["price_low"] < zone["price_high"]


# ── Determinism ────────────────────────────────────────────────────────────────

class TestLHMDeterminism:
    def test_n_price_levels_deterministic(self, lhm, lhm2):
        assert lhm["n_price_levels"] == lhm2["n_price_levels"]

    def test_n_time_buckets_deterministic(self, lhm, lhm2):
        assert lhm["n_time_buckets"] == lhm2["n_time_buckets"]

    def test_current_price_deterministic(self, lhm, lhm2):
        assert lhm["current_price"] == lhm2["current_price"]

    def test_peak_price_level_deterministic(self, lhm, lhm2):
        assert lhm["peak_price_level"] == lhm2["peak_price_level"]

    def test_zones_count_deterministic(self, lhm, lhm2):
        assert len(lhm["zones"]) == len(lhm2["zones"])

    def test_matrix_first_row_deterministic(self, lhm, lhm2):
        assert lhm["heatmap_matrix"][0] == lhm2["heatmap_matrix"][0]

    def test_total_liquidations_deterministic(self, lhm, lhm2):
        assert lhm["total_liquidations"] == lhm2["total_liquidations"]


# ── HTTP endpoint (structural) ─────────────────────────────────────────────────

class TestLHMHTTPEndpoint:
    def test_endpoint_in_api(self):
        api_path = os.path.join(_ROOT, "backend", "api.py")
        with open(api_path, encoding="utf-8") as f:
            content = f.read()
        assert "liquidation-heatmap-matrix" in content

    def test_function_called_in_api(self):
        api_path = os.path.join(_ROOT, "backend", "api.py")
        with open(api_path, encoding="utf-8") as f:
            content = f.read()
        assert "compute_liquidation_heatmap" in content

    def test_function_exists_in_metrics(self):
        metrics_path = os.path.join(_ROOT, "backend", "metrics.py")
        with open(metrics_path, encoding="utf-8") as f:
            content = f.read()
        assert "compute_liquidation_heatmap" in content

    def test_function_is_async(self):
        metrics_path = os.path.join(_ROOT, "backend", "metrics.py")
        with open(metrics_path, encoding="utf-8") as f:
            content = f.read()
        assert "async def compute_liquidation_heatmap" in content


# ── HTML card & JS render ──────────────────────────────────────────────────────

class TestLHMHTMLCard:
    def test_html_has_liquidation_heatmap_title(self):
        html_path = os.path.join(_ROOT, "frontend", "index.html")
        with open(html_path, encoding="utf-8") as f:
            html = f.read()
        assert "Liquidation Heatmap" in html

    def test_html_has_liq_heatmap_matrix_id(self):
        html_path = os.path.join(_ROOT, "frontend", "index.html")
        with open(html_path, encoding="utf-8") as f:
            html = f.read()
        assert "liq-heatmap-matrix" in html

    def test_html_has_content_div(self):
        html_path = os.path.join(_ROOT, "frontend", "index.html")
        with open(html_path, encoding="utf-8") as f:
            html = f.read()
        assert "liq-heatmap-matrix-content" in html

    def test_js_has_render_function(self):
        js_path = os.path.join(_ROOT, "frontend", "app.js")
        with open(js_path, encoding="utf-8") as f:
            js = f.read()
        assert "renderLiquidationHeatmap" in js

    def test_js_calls_api_endpoint(self):
        js_path = os.path.join(_ROOT, "frontend", "app.js")
        with open(js_path, encoding="utf-8") as f:
            js = f.read()
        assert "liquidation-heatmap-matrix" in js

    def test_html_card_has_meta(self):
        html_path = os.path.join(_ROOT, "frontend", "index.html")
        with open(html_path, encoding="utf-8") as f:
            html = f.read()
        # card meta should reference price levels or time buckets
        assert "price" in html.lower() or "heatmap" in html.lower()


# ── Additional / edge-case ─────────────────────────────────────────────────────

class TestLHMAdditional:
    def test_result_is_dict(self, lhm):
        assert isinstance(lhm, dict)

    def test_total_liquidations_equals_matrix_sum(self, lhm):
        computed = sum(sum(row) for row in lhm["heatmap_matrix"])
        assert lhm["total_liquidations"] == computed

    def test_price_levels_ascending(self, lhm):
        levels = lhm["price_levels"]
        for i in range(len(levels) - 1):
            assert levels[i] < levels[i + 1]

    def test_peak_price_level_in_price_levels(self, lhm):
        assert lhm["peak_price_level"] in lhm["price_levels"]

    def test_result_has_at_least_12_keys(self, lhm):
        assert len(lhm) >= 12

    def test_price_levels_all_positive(self, lhm):
        for lvl in lhm["price_levels"]:
            assert lvl > 0

    def test_current_price_is_finite(self, lhm):
        assert math.isfinite(lhm["current_price"])
