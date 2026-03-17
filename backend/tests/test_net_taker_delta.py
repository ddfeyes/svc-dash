"""
TDD tests for compute_net_taker_delta (Wave 24 Task 3, Issue #127).

Tests cover:
  - Empty trade list
  - Single buy / sell trade
  - Mixed buy/sell same bucket
  - Multiple buckets, correct bucketing
  - is_buyer_aggressor flag overrides side field
  - total_net == total_buy - total_sell
  - Route registration in api.py
  - HTML card and JS function presence
"""
import os
import sys
import tempfile

import pytest

os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "test.db"))
os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
os.environ.setdefault("SYMBOL_BYBIT", "BANANAS31USDT")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from metrics import compute_net_taker_delta  # noqa: E402

_FRONTEND = os.path.join(os.path.dirname(__file__), "..", "..", "frontend")


def _html() -> str:
    with open(os.path.join(_FRONTEND, "index.html"), encoding="utf-8") as f:
        return f.read()


def _js() -> str:
    with open(os.path.join(_FRONTEND, "app.js"), encoding="utf-8") as f:
        return f.read()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _trade(ts: float, qty: float, is_buy: bool) -> dict:
    return {"ts": ts, "qty": qty, "is_buyer_aggressor": is_buy}


def _trade_side(ts: float, qty: float, side: str) -> dict:
    """Trade using 'side' field (no is_buyer_aggressor)."""
    return {"ts": ts, "qty": qty, "side": side}


# ── compute_net_taker_delta tests ─────────────────────────────────────────────

def test_empty_trades_returns_zero_totals():
    result = compute_net_taker_delta([])
    assert result["total_buy"] == 0.0
    assert result["total_sell"] == 0.0
    assert result["total_net"] == 0.0
    assert result["buckets"] == []


def test_single_buy_trade():
    trades = [_trade(1_000_000.0, 500.0, True)]
    result = compute_net_taker_delta(trades)
    assert result["total_buy"] == pytest.approx(500.0)
    assert result["total_sell"] == pytest.approx(0.0)
    assert result["total_net"] == pytest.approx(500.0)


def test_single_sell_trade():
    trades = [_trade(1_000_000.0, 300.0, False)]
    result = compute_net_taker_delta(trades)
    assert result["total_buy"] == pytest.approx(0.0)
    assert result["total_sell"] == pytest.approx(300.0)
    assert result["total_net"] == pytest.approx(-300.0)


def test_total_net_equals_buy_minus_sell():
    trades = [
        _trade(1_000_000.0, 1000.0, True),
        _trade(1_000_060.0, 400.0, False),
    ]
    result = compute_net_taker_delta(trades)
    assert result["total_net"] == pytest.approx(result["total_buy"] - result["total_sell"])


def test_same_bucket_aggregated():
    """Two trades in the same 60s bucket collapse into one bucket."""
    # 1_000_000 // 60 == 1_000_010 // 60 == 16666 → same slot
    trades = [
        _trade(1_000_000.0, 100.0, True),
        _trade(1_000_010.0, 200.0, True),   # same bucket
    ]
    result = compute_net_taker_delta(trades, bucket_seconds=60)
    assert len(result["buckets"]) == 1
    assert result["buckets"][0]["buy_vol"] == pytest.approx(300.0)


def test_different_buckets_produce_multiple_entries():
    trades = [
        _trade(1_000_000.0, 100.0, True),
        _trade(1_000_060.0, 200.0, False),
        _trade(1_000_120.0, 50.0, True),
    ]
    result = compute_net_taker_delta(trades, bucket_seconds=60)
    assert len(result["buckets"]) == 3


def test_bucket_net_delta_correct():
    trades = [
        _trade(1_000_000.0, 800.0, True),
        _trade(1_000_010.0, 300.0, False),
    ]
    result = compute_net_taker_delta(trades, bucket_seconds=60)
    b = result["buckets"][0]
    assert b["net_delta"] == pytest.approx(b["buy_vol"] - b["sell_vol"])


def test_is_buyer_aggressor_overrides_side_field():
    """is_buyer_aggressor=True should count as buy even if side='sell'."""
    trade = {"ts": 1_000_000.0, "qty": 100.0, "is_buyer_aggressor": True, "side": "sell"}
    result = compute_net_taker_delta([trade])
    assert result["total_buy"] == pytest.approx(100.0)
    assert result["total_sell"] == pytest.approx(0.0)


def test_side_field_used_when_no_is_buyer_aggressor():
    trades = [_trade_side(1_000_000.0, 250.0, "buy")]
    result = compute_net_taker_delta(trades)
    assert result["total_buy"] == pytest.approx(250.0)


def test_buckets_sorted_ascending():
    trades = [
        _trade(1_000_120.0, 100.0, True),
        _trade(1_000_000.0, 200.0, False),
    ]
    result = compute_net_taker_delta(trades, bucket_seconds=60)
    ts_list = [b["ts"] for b in result["buckets"]]
    assert ts_list == sorted(ts_list)


def test_each_bucket_has_required_keys():
    trades = [_trade(1_000_000.0, 100.0, True)]
    result = compute_net_taker_delta(trades)
    for b in result["buckets"]:
        for key in ("ts", "buy_vol", "sell_vol", "net_delta"):
            assert key in b, f"Missing key '{key}' in bucket"


# ── Route registration ────────────────────────────────────────────────────────

def test_net_taker_delta_route_registered():
    from api import router
    paths = [r.path for r in router.routes]
    assert any("net-taker-delta" in p for p in paths), (
        "/net-taker-delta route not found in router"
    )


# ── HTML / JS smoke tests ─────────────────────────────────────────────────────

def test_html_has_net_taker_delta_card():
    assert "card-net-taker-delta" in _html()


def test_js_has_render_function():
    assert "renderNetTakerDelta" in _js()


def test_js_calls_net_taker_delta_api():
    assert "net-taker-delta" in _js()


def test_js_uses_active_symbol():
    """renderNetTakerDelta must use getSymbols() for multi-symbol parallel fetch."""
    js = _js()
    # Find the function body
    start = js.find("async function renderNetTakerDelta()")
    end   = js.find("\nasync function ", start + 1)
    body  = js[start:end]
    assert "getSymbols" in body
    assert "Promise.all" in body
