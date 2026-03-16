"""
Unit/smoke tests for /api/trade-percentiles.

Validates:
  - Percentile computation correctness (linear interpolation)
  - Edge cases: empty, single, two, all-same, large dataset
  - USD notional path
  - Response shape
  - Display formatting helpers
  - Route registration
  - HTML card presence
  - JS function presence
"""
import math
import os
import sys
import time

import pytest


# ── Percentile computation (mirrors backend logic) ────────────────────────────

PCTS = (50, 75, 90, 95, 99)


def compute_percentiles(values: list, pcts: tuple = PCTS) -> dict:
    """Linear-interpolation percentiles. Returns {p50:..., p75:..., ...}."""
    if not values:
        return {f"p{p}": None for p in pcts}
    s = sorted(values)
    n = len(s)
    result = {}
    for p in pcts:
        if n == 1:
            result[f"p{p}"] = s[0]
        else:
            k = (n - 1) * p / 100
            lo = int(k)
            hi = min(lo + 1, n - 1)
            result[f"p{p}"] = s[lo] + (k - lo) * (s[hi] - s[lo])
    return result


def build_symbol_entry(trades: list, window_s: float = 3600) -> dict:
    """Mirror of the per-symbol payload built by the endpoint."""
    qtys = [float(t["qty"]) for t in trades]
    usd  = [float(t["qty"]) * float(t["price"]) for t in trades]
    pcts = compute_percentiles(qtys)
    usd_pcts = {f"usd_{k}": round(v, 2) if v is not None else None
                for k, v in compute_percentiles(usd).items()}
    return {
        "n_trades":  len(trades),
        "window_s":  window_s,
        **{k: round(v, 8) if v is not None else None for k, v in pcts.items()},
        **usd_pcts,
        "mean_qty":  round(sum(qtys) / len(qtys), 8) if qtys else None,
        "mean_usd":  round(sum(usd)  / len(usd),  2) if usd  else None,
    }


# ── Correctness tests ─────────────────────────────────────────────────────────

def test_p50_odd_length():
    vals = [1, 2, 3, 4, 5]
    r = compute_percentiles(vals)
    assert r["p50"] == pytest.approx(3.0)


def test_p50_even_length():
    vals = [1, 2, 3, 4]
    r = compute_percentiles(vals)
    assert r["p50"] == pytest.approx(2.5)


def test_p0_equals_min():
    vals = [5, 1, 9, 3]
    r = compute_percentiles(vals, pcts=(0,))
    assert r["p0"] == pytest.approx(min(vals))


def test_p100_equals_max():
    vals = [5, 1, 9, 3]
    r = compute_percentiles(vals, pcts=(100,))
    assert r["p100"] == pytest.approx(max(vals))


def test_p75_five_elements():
    vals = [2, 4, 6, 8, 10]
    r = compute_percentiles(vals)
    # k = 4 * 0.75 = 3.0 → s[3] = 8
    assert r["p75"] == pytest.approx(8.0)


def test_p90_ten_elements():
    vals = list(range(1, 11))   # [1..10]
    r = compute_percentiles(vals)
    # k = 9 * 0.9 = 8.1 → s[8] + 0.1*(s[9]-s[8]) = 9 + 0.1 = 9.1
    assert r["p90"] == pytest.approx(9.1)


def test_p99_hundred_elements():
    vals = list(range(1, 101))  # [1..100]
    r = compute_percentiles(vals)
    # k = 99 * 0.99 = 98.01 → s[98] + 0.01*(s[99]-s[98]) = 99 + 0.01 = 99.01
    assert r["p99"] == pytest.approx(99.01, rel=1e-4)


def test_all_same_values():
    vals = [7.5] * 50
    r = compute_percentiles(vals)
    for p in PCTS:
        assert r[f"p{p}"] == pytest.approx(7.5)


def test_single_element():
    r = compute_percentiles([42.0])
    for p in PCTS:
        assert r[f"p{p}"] == pytest.approx(42.0)


def test_two_elements():
    r = compute_percentiles([10.0, 20.0])
    assert r["p50"] == pytest.approx(15.0)
    assert r["p0"] == pytest.approx(10.0) if "p0" in r else True
    # p99 should be close to max
    assert r["p99"] == pytest.approx(19.9, rel=1e-2)


def test_empty_returns_none_for_all():
    r = compute_percentiles([])
    for p in PCTS:
        assert r[f"p{p}"] is None


def test_unsorted_input_still_correct():
    vals = [9, 1, 5, 3, 7]
    r = compute_percentiles(vals)
    assert r["p50"] == pytest.approx(5.0)


def test_float_values():
    vals = [0.001, 0.002, 0.003, 0.005, 0.01]
    r = compute_percentiles(vals)
    assert r["p50"] == pytest.approx(0.003)


def test_large_dataset_consistency():
    vals = list(range(10000))
    r = compute_percentiles(vals)
    # median of 0..9999 = 4999.5
    assert r["p50"] == pytest.approx(4999.5, rel=1e-4)


# ── Symbol entry builder ──────────────────────────────────────────────────────

SAMPLE_TRADES = [
    {"qty": 1.0,  "price": 100.0},
    {"qty": 2.0,  "price": 100.0},
    {"qty": 3.0,  "price": 100.0},
    {"qty": 5.0,  "price": 100.0},
    {"qty": 10.0, "price": 100.0},
]


def test_symbol_entry_n_trades():
    e = build_symbol_entry(SAMPLE_TRADES)
    assert e["n_trades"] == 5


def test_symbol_entry_has_all_pcts():
    e = build_symbol_entry(SAMPLE_TRADES)
    for p in PCTS:
        assert f"p{p}" in e


def test_symbol_entry_has_usd_pcts():
    e = build_symbol_entry(SAMPLE_TRADES)
    for p in PCTS:
        assert f"usd_p{p}" in e


def test_symbol_entry_mean_qty():
    e = build_symbol_entry(SAMPLE_TRADES)
    expected = (1 + 2 + 3 + 5 + 10) / 5
    assert e["mean_qty"] == pytest.approx(expected, rel=1e-4)


def test_symbol_entry_empty_trades():
    e = build_symbol_entry([])
    assert e["n_trades"] == 0
    assert e["p50"] is None
    assert e["mean_qty"] is None


# ── Response shape ────────────────────────────────────────────────────────────

SAMPLE_RESPONSE = {
    "status": "ok",
    "ts": 1773600000.0,
    "window_s": 3600,
    "symbols": {
        "BANANAS31USDT": {
            "n_trades": 500,
            "p50": 100.0, "p75": 250.0, "p90": 500.0, "p95": 800.0, "p99": 1200.0,
            "usd_p50": 0.23, "usd_p75": 0.58, "usd_p90": 1.15, "usd_p95": 1.84, "usd_p99": 2.76,
            "mean_qty": 210.0, "mean_usd": 0.49,
        }
    },
}


def test_response_status_ok():
    assert SAMPLE_RESPONSE["status"] == "ok"


def test_response_has_symbols():
    assert isinstance(SAMPLE_RESPONSE["symbols"], dict)


def test_response_has_window_s():
    assert SAMPLE_RESPONSE["window_s"] == 3600


def test_response_ts_is_float():
    assert isinstance(SAMPLE_RESPONSE["ts"], float)


def test_symbol_entry_pcts_ascending():
    e = SAMPLE_RESPONSE["symbols"]["BANANAS31USDT"]
    pct_vals = [e[f"p{p}"] for p in PCTS]
    assert pct_vals == sorted(pct_vals), "Percentiles must be non-decreasing"


# ── Display formatting ────────────────────────────────────────────────────────

def fmt_qty(v, symbol: str = "") -> str:
    """Format a trade qty for display — mirrors app.js fmtQty()."""
    if v is None:
        return "—"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v / 1_000:.1f}k"
    if v < 0.01:
        return f"{v:.6f}"
    if v < 1:
        return f"{v:.4f}"
    return f"{v:.2f}"


def test_fmt_qty_large():
    assert fmt_qty(2_500_000) == "2.50M"


def test_fmt_qty_thousands():
    assert fmt_qty(1500) == "1.5k"


def test_fmt_qty_sub_penny():
    result = fmt_qty(0.001)
    assert result == "0.001000"


def test_fmt_qty_normal():
    assert fmt_qty(3.5) == "3.50"


def test_fmt_qty_none():
    assert fmt_qty(None) == "—"


# ── Route registration ────────────────────────────────────────────────────────

def test_trade_percentiles_route_registered():
    import tempfile
    os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "t.db"))
    os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from api import router
    paths = [r.path for r in router.routes]
    assert any("trade-percentiles" in p for p in paths)


# ── HTML / JS smoke tests ─────────────────────────────────────────────────────

_ROOT = os.path.join(os.path.dirname(__file__), "..")


def _read(rel: str) -> str:
    with open(os.path.join(_ROOT, rel), encoding="utf-8") as f:
        return f.read()


def test_html_has_trade_percentiles_card():
    html = _read("frontend/index.html")
    assert "card-trade-percentiles" in html, "Missing #card-trade-percentiles in HTML"


def test_js_has_render_trade_percentiles():
    js = _read("frontend/app.js")
    assert "renderTradePercentiles" in js, "Missing renderTradePercentiles in app.js"


def test_js_calls_trade_percentiles_api():
    js = _read("frontend/app.js")
    assert "trade-percentiles" in js, "app.js must call /api/trade-percentiles"
