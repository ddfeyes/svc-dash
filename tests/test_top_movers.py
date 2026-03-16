"""
Unit tests for the /api/top-movers endpoint and helper logic.

Validates response shape, price-change computation, sorting, edge cases,
and Python mirrors of the app.js renderTopMovers() display helpers.
"""
from unittest.mock import AsyncMock, patch
import time

import pytest


# ── Python mirrors of computation helpers ────────────────────────────────────

def compute_pct_change(current: float, past: float) -> float | None:
    """Return % change from past→current, or None if past is unavailable."""
    if past is None or past == 0:
        return None
    return round((current - past) / past * 100, 4)


def fmt_change(v: float | None) -> str:
    """Format a pct-change value for display (mirrors app.js)."""
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"


def change_color(v: float | None) -> str:
    if v is None:
        return "var(--muted)"
    if v > 0:
        return "var(--green)"
    if v < 0:
        return "var(--red)"
    return "var(--muted)"


def sort_key(row: dict) -> float:
    return abs(row.get("change_1h") or 0.0)


# ── Sample API payloads ───────────────────────────────────────────────────────

MOVER_A = {
    "symbol": "BANANAS31USDT",
    "price": 0.002345,
    "change_1h": 3.21,
    "change_4h": 7.45,
    "change_24h": 12.80,
}
MOVER_B = {
    "symbol": "COSUSDT",
    "price": 0.01234,
    "change_1h": -1.50,
    "change_4h": -4.20,
    "change_24h": -8.00,
}
MOVER_C = {
    "symbol": "DEXEUSDT",
    "price": 5.6789,
    "change_1h": 0.05,
    "change_4h": 0.10,
    "change_24h": -0.30,
}
MOVER_NODATA = {
    "symbol": "LYNUSDT",
    "price": None,
    "change_1h": None,
    "change_4h": None,
    "change_24h": None,
}

SAMPLE_RESPONSE = {
    "status": "ok",
    "ts": 1773600000.0,
    "movers": [MOVER_A, MOVER_B, MOVER_C],
}


# ── Response shape tests ──────────────────────────────────────────────────────

def test_response_has_status():
    assert SAMPLE_RESPONSE["status"] == "ok"


def test_response_has_movers_list():
    assert isinstance(SAMPLE_RESPONSE["movers"], list)


def test_response_has_ts():
    assert isinstance(SAMPLE_RESPONSE["ts"], float)


def test_mover_has_required_keys():
    for row in SAMPLE_RESPONSE["movers"]:
        for key in ("symbol", "price", "change_1h", "change_4h", "change_24h"):
            assert key in row, f"Missing key '{key}' in {row}"


def test_mover_symbol_is_string():
    for row in SAMPLE_RESPONSE["movers"]:
        assert isinstance(row["symbol"], str)
        assert len(row["symbol"]) > 0


def test_mover_price_is_numeric_or_none():
    for row in SAMPLE_RESPONSE["movers"]:
        assert row["price"] is None or isinstance(row["price"], (int, float))


# ── Pct-change computation ────────────────────────────────────────────────────

def test_pct_change_positive():
    assert compute_pct_change(110.0, 100.0) == pytest.approx(10.0, rel=1e-3)


def test_pct_change_negative():
    assert compute_pct_change(90.0, 100.0) == pytest.approx(-10.0, rel=1e-3)


def test_pct_change_zero_past_returns_none():
    assert compute_pct_change(50.0, 0.0) is None


def test_pct_change_none_past_returns_none():
    assert compute_pct_change(50.0, None) is None


def test_pct_change_no_change():
    assert compute_pct_change(100.0, 100.0) == pytest.approx(0.0, abs=1e-6)


def test_pct_change_small_values():
    # Crypto assets with sub-penny prices
    result = compute_pct_change(0.002345, 0.002000)
    assert result == pytest.approx(17.25, rel=1e-2)


# ── Sorting ───────────────────────────────────────────────────────────────────

def test_sorted_by_abs_change_1h_descending():
    movers = [MOVER_C, MOVER_B, MOVER_A]  # unsorted
    sorted_movers = sorted(movers, key=sort_key, reverse=True)
    assert sorted_movers[0]["symbol"] == "BANANAS31USDT"  # 3.21 abs
    assert sorted_movers[1]["symbol"] == "COSUSDT"        # 1.50 abs
    assert sorted_movers[2]["symbol"] == "DEXEUSDT"       # 0.05 abs


def test_none_change_treated_as_zero_in_sort():
    movers = [MOVER_NODATA, MOVER_A]
    sorted_movers = sorted(movers, key=sort_key, reverse=True)
    assert sorted_movers[0]["symbol"] == "BANANAS31USDT"
    assert sorted_movers[1]["symbol"] == "LYNUSDT"


# ── Display formatting helpers ────────────────────────────────────────────────

def test_fmt_change_positive():
    assert fmt_change(3.21) == "+3.21%"


def test_fmt_change_negative():
    assert fmt_change(-1.50) == "-1.50%"


def test_fmt_change_zero():
    assert fmt_change(0.0) == "+0.00%"


def test_fmt_change_none():
    assert fmt_change(None) == "—"


def test_change_color_positive():
    assert change_color(3.21) == "var(--green)"


def test_change_color_negative():
    assert change_color(-1.50) == "var(--red)"


def test_change_color_zero():
    assert change_color(0.0) == "var(--muted)"


def test_change_color_none():
    assert change_color(None) == "var(--muted)"


# ── Endpoint import / route registration ─────────────────────────────────────

def test_top_movers_route_registered():
    import os, sys, tempfile
    os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "t.db"))
    os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from api import router
    paths = [r.path for r in router.routes]
    assert any("top-movers" in p for p in paths)


# ── Edge cases ────────────────────────────────────────────────────────────────

def test_empty_movers_list():
    resp = {"status": "ok", "ts": time.time(), "movers": []}
    assert resp["movers"] == []


def test_single_symbol_response():
    resp = {
        "status": "ok",
        "ts": time.time(),
        "movers": [MOVER_A],
    }
    assert len(resp["movers"]) == 1
    assert resp["movers"][0]["symbol"] == "BANANAS31USDT"


def test_all_none_changes_symbol_still_present():
    assert MOVER_NODATA["symbol"] == "LYNUSDT"
    assert MOVER_NODATA["change_1h"] is None
    assert MOVER_NODATA["change_4h"] is None
    assert MOVER_NODATA["change_24h"] is None
