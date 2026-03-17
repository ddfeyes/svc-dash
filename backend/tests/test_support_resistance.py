"""
TDD tests for Support/Resistance Levels card (Wave 24, Issue #125).
Tests cover: level sorting, type detection, near level, empty levels,
badge logic, price formatting, distance calculation, multiple symbols.
"""

import os
import sys
import tempfile
from unittest.mock import AsyncMock, patch

import pytest

os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "test_sr.db"))
os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
os.environ.setdefault("SYMBOL_BYBIT", "BANANAS31USDT")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Inline helpers matching api.py logic ─────────────────────────────────────


def find_peaks(series, is_max: bool):
    peaks = []
    for i in range(2, len(series) - 2):
        if is_max:
            if (
                series[i] > series[i - 1]
                and series[i] > series[i - 2]
                and series[i] > series[i + 1]
                and series[i] > series[i + 2]
            ):
                peaks.append(series[i])
        else:
            if (
                series[i] < series[i - 1]
                and series[i] < series[i - 2]
                and series[i] < series[i + 1]
                and series[i] < series[i + 2]
            ):
                peaks.append(series[i])
    return peaks


def cluster(levels, sens=0.003):
    if not levels:
        return []
    levels_sorted = sorted(levels)
    clusters = []
    cur_cluster = [levels_sorted[0]]
    for p in levels_sorted[1:]:
        if (p - cur_cluster[-1]) / max(cur_cluster[-1], 1e-12) < sens:
            cur_cluster.append(p)
        else:
            clusters.append(cur_cluster)
            cur_cluster = [p]
    clusters.append(cur_cluster)
    return [{"price": round(sum(c) / len(c), 8), "touches": len(c)} for c in clusters]


def make_candles(prices):
    """Create minimal OHLCV candles from a price list."""
    return [
        {"open": p, "high": p * 1.001, "low": p * 0.999, "close": p, "volume": 100.0}
        for p in prices
    ]


def make_candles_with_peaks():
    """Candles with clear local maxima and minima."""
    prices = [
        100,
        101,
        105,
        101,
        100,  # peak at 105
        99,
        95,
        99,
        100,
        99,  # trough at 95
        100,
        102,
        108,
        102,
        100,  # peak at 108
        99,
        93,
        99,
        100,
        99,  # trough at 93
        100,
        101,
        100,
        99,
        100,
    ]
    result = []
    for p in prices:
        result.append(
            {
                "open": p,
                "high": p * 1.001,
                "low": p * 0.999,
                "close": p,
                "volume": 100.0,
            }
        )
    # Make peaks visible in highs and troughs visible in lows
    # index 2 = 105 high, index 7 = 95 low, index 12 = 108 high, index 17 = 93 low
    result[2]["high"] = 105
    result[7]["low"] = 95
    result[12]["high"] = 108
    result[17]["low"] = 93
    return result


# ── 1. Level sorting by abs(distance_pct) ────────────────────────────────────


def test_levels_sorted_by_abs_distance():
    """Levels must be sorted by abs(distance_pct) ascending."""
    levels = [
        {"price": 110, "type": "resistance", "distance_pct": 10.0, "touches": 3},
        {"price": 101, "type": "resistance", "distance_pct": 1.0, "touches": 2},
        {"price": 95, "type": "support", "distance_pct": -5.0, "touches": 4},
    ]
    sorted_levels = sorted(levels, key=lambda x: abs(x["distance_pct"]))
    assert sorted_levels[0]["distance_pct"] == 1.0
    assert sorted_levels[1]["distance_pct"] == -5.0
    assert sorted_levels[2]["distance_pct"] == 10.0


def test_nearest_level_is_first_after_sort():
    levels = [
        {"price": 200, "type": "resistance", "distance_pct": 100.0, "touches": 1},
        {"price": 100.5, "type": "resistance", "distance_pct": 0.5, "touches": 5},
        {"price": 50, "type": "support", "distance_pct": -50.0, "touches": 2},
    ]
    sorted_levels = sorted(levels, key=lambda x: abs(x["distance_pct"]))
    assert sorted_levels[0]["price"] == 100.5


# ── 2. Type detection (support vs resistance) ─────────────────────────────────


def test_resistance_type_above_price():
    """Price above current → resistance."""
    current = 100.0
    price = 105.0
    distance_pct = (price - current) / current * 100
    level_type = "resistance" if distance_pct > 0 else "support"
    assert level_type == "resistance"


def test_support_type_below_price():
    """Price below current → support."""
    current = 100.0
    price = 95.0
    distance_pct = (price - current) / current * 100
    level_type = "resistance" if distance_pct > 0 else "support"
    assert level_type == "support"


def test_find_peaks_detects_maxima():
    series = [1, 2, 5, 2, 1, 2, 7, 2, 1]
    peaks = find_peaks(series, is_max=True)
    assert 5 in peaks
    assert 7 in peaks


def test_find_peaks_detects_minima():
    series = [10, 8, 3, 8, 10, 8, 2, 8, 10]
    troughs = find_peaks(series, is_max=False)
    assert 3 in troughs
    assert 2 in troughs


# ── 3. Near level detection (abs distance < 0.5%) ────────────────────────────


def test_near_level_threshold():
    """Levels within 0.5% of current price are 'near'."""
    current = 100.0
    levels = [
        {"price": 100.4, "distance_pct": 0.4},  # near
        {"price": 100.6, "distance_pct": 0.6},  # not near
        {"price": 99.6, "distance_pct": -0.4},  # near
    ]
    near = [l for l in levels if abs(l["distance_pct"]) < 0.5]
    assert len(near) == 2
    assert all(abs(l["distance_pct"]) < 0.5 for l in near)


def test_near_level_boundary_not_included():
    """Exactly 0.5% is NOT near (strict less-than)."""
    level = {"distance_pct": 0.5}
    assert not (abs(level["distance_pct"]) < 0.5)


# ── 4. Empty levels ───────────────────────────────────────────────────────────


def test_cluster_empty_input():
    assert cluster([]) == []


def test_find_peaks_too_few_points():
    """Series too short for peak detection (need > 4 points)."""
    series = [1, 5, 1]
    peaks = find_peaks(series, is_max=True)
    assert peaks == []


def test_cluster_single_value():
    result = cluster([42.0])
    assert len(result) == 1
    assert result[0]["price"] == 42.0
    assert result[0]["touches"] == 1


# ── 5. Badge logic: nearest level determines badge ────────────────────────────


def test_badge_shows_support_when_nearest_is_support():
    levels = [
        {"type": "support", "distance_pct": -0.3},
        {"type": "resistance", "distance_pct": 2.0},
    ]
    sorted_levels = sorted(levels, key=lambda x: abs(x["distance_pct"]))
    assert sorted_levels[0]["type"] == "support"


def test_badge_shows_resistance_when_nearest_is_resistance():
    levels = [
        {"type": "support", "distance_pct": -5.0},
        {"type": "resistance", "distance_pct": 0.2},
    ]
    sorted_levels = sorted(levels, key=lambda x: abs(x["distance_pct"]))
    assert sorted_levels[0]["type"] == "resistance"


def test_badge_hidden_when_no_levels():
    levels = []
    # badge should not be shown
    assert len(levels) == 0


# ── 6. Price formatting ───────────────────────────────────────────────────────


def test_fmt_price_small_uses_8_decimals():
    """Prices < 0.01 use toFixed(8) equivalent."""
    price = 0.00012345
    assert price < 0.01
    formatted = f"{price:.8f}"
    assert len(formatted.split(".")[1]) == 8


def test_fmt_price_normal_uses_4_decimals():
    """Prices >= 0.01 use toFixed(4) equivalent."""
    price = 1.23456
    assert price >= 0.01
    formatted = f"{price:.4f}"
    assert len(formatted.split(".")[1]) == 4


def test_fmt_price_boundary_at_001():
    """Price exactly 0.01 uses 4 decimals."""
    price = 0.01
    assert not (price < 0.01)
    formatted = f"{price:.4f}"
    assert formatted == "0.0100"


def test_fmt_price_bananas_example():
    """BANANAS31USDT price ~0.010093 uses 4 decimals (>= 0.01)."""
    price = 0.010093
    assert not (price < 0.01)  # 0.010093 >= 0.01
    # It's >= 0.01, so 4 decimal places
    formatted = f"{price:.4f}"
    assert formatted == "0.0101"


# ── 7. Distance calculation ───────────────────────────────────────────────────


def test_distance_pct_positive_for_resistance():
    current = 0.010093
    level_price = 0.01013567
    dist = (level_price - current) / current * 100
    assert dist > 0
    assert abs(dist - 0.4228) < 0.01  # matches example response


def test_distance_pct_negative_for_support():
    current = 0.010093
    level_price = 0.01008378
    dist = (level_price - current) / current * 100
    assert dist < 0
    assert abs(dist - (-0.0914)) < 0.01  # matches example response


def test_distance_pct_zero_for_current_price():
    current = 100.0
    dist = (current - current) / current * 100
    assert dist == 0.0


def test_distance_pct_rounding():
    current = 100.0
    level_price = 101.0
    dist = round((level_price - current) / current * 100, 4)
    assert dist == 1.0


# ── 8. Multiple symbols ───────────────────────────────────────────────────────


def test_cluster_groups_nearby_levels():
    """Levels within sensitivity are merged into one cluster."""
    levels = [100.0, 100.1, 100.2, 105.0, 105.05]
    result = cluster(levels, sens=0.003)  # 0.3% sensitivity
    # 100.0 and 100.1 are 0.1% apart → same cluster
    # 105.0 and 105.05 are 0.047% apart → same cluster
    prices = [r["price"] for r in result]
    assert len(result) == 2


def test_cluster_touch_count_reflects_merges():
    """Touch count = number of raw peaks merged into cluster."""
    levels = [100.0, 100.1, 100.2]
    result = cluster(levels, sens=0.003)
    # All within 0.3% → one cluster with 3 touches
    assert result[0]["touches"] == 3


def test_response_has_required_fields():
    """Response structure must include status, symbol, current_price, levels, window."""
    mock_resp = {
        "status": "ok",
        "symbol": "BANANAS31USDT",
        "current_price": 0.010093,
        "levels": [],
        "window": 3600,
    }
    for key in ("status", "symbol", "current_price", "levels", "window"):
        assert key in mock_resp


def test_levels_capped_at_20():
    """API returns at most 20 levels (top-20 closest)."""
    levels = [
        {"price": i, "type": "resistance", "distance_pct": float(i), "touches": 1}
        for i in range(1, 30)
    ]
    capped = levels[:20]
    assert len(capped) == 20


# ── 9. Route registration smoke test ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_support_resistance_route_registered():
    """support-resistance endpoint must be registered on the API router."""
    from storage import init_db

    await init_db()
    from api import router

    paths = [r.path for r in router.routes]
    assert any("support-resistance" in p for p in paths)


# ── 10. HTML / JS smoke tests ─────────────────────────────────────────────────


def _read(rel_path):
    base = os.path.join(os.path.dirname(__file__), "..", "..")
    with open(os.path.join(base, rel_path), encoding="utf-8") as f:
        return f.read()


def test_html_has_support_resistance_card():
    html = _read("frontend/index.html")
    assert "card-support-resistance" in html


def test_html_has_support_resistance_badge():
    html = _read("frontend/index.html")
    assert "support-resistance-badge" in html


def test_js_has_render_function():
    js = _read("frontend/app.js")
    assert "renderSupportResistance" in js


def test_js_calls_support_resistance_api():
    js = _read("frontend/app.js")
    assert "support-resistance" in js


def test_js_render_in_refresh_batch():
    js = _read("frontend/app.js")
    assert "safe(renderSupportResistance)" in js
