"""
Unit / smoke tests for /api/net-taker-delta + net taker delta card.

Validates:
  - Response shape
  - Python mirrors of display-helper logic
  - Ranking logic (multi-symbol comparison)
  - Color coding helpers
  - HTML card / JS smoke tests
  - Route registration
"""
import os
import sys

import pytest

_ROOT = os.path.join(os.path.dirname(__file__), "..")


def _html() -> str:
    with open(os.path.join(_ROOT, "frontend", "index.html"), encoding="utf-8") as f:
        return f.read()


def _js() -> str:
    with open(os.path.join(_ROOT, "frontend", "app.js"), encoding="utf-8") as f:
        return f.read()


# ── Python mirrors of display helpers ────────────────────────────────────────

def fmt_vol(v: float) -> str:
    """Format volume (coin units) with K/M suffix."""
    if abs(v) >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if abs(v) >= 1_000:
        return f"{v / 1_000:.1f}k"
    return f"{v:.2f}"


def net_delta_badge(total_net: float) -> tuple[str, str]:
    """Returns (label, css_class) for the net delta direction badge."""
    if total_net > 0:
        return "buying", "badge-green"
    if total_net < 0:
        return "selling", "badge-red"
    return "neutral", "badge-blue"


def delta_pressure_pct(total_buy: float, total_sell: float) -> int:
    """Buy pressure as percentage of total volume (0–100)."""
    total = total_buy + total_sell
    if total <= 0:
        return 50
    return max(0, min(100, round(total_buy / total * 100)))


def rank_symbols(symbol_data: list[dict]) -> list[dict]:
    """
    Sort symbols by total_net descending, add rank field.
    symbol_data: [{"symbol": str, "total_net": float, "total_buy": float, "total_sell": float}]
    """
    sorted_data = sorted(symbol_data, key=lambda x: x["total_net"], reverse=True)
    for i, row in enumerate(sorted_data, start=1):
        row["rank"] = i
    return sorted_data


# ── Sample payloads ───────────────────────────────────────────────────────────

NET_TAKER_PAYLOAD = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "window_seconds": 3600,
    "bucket_seconds": 60,
    "buckets": [
        {"ts": 1700000060.0, "buy_vol": 1200.0, "sell_vol": 800.0, "net_delta": 400.0},
        {"ts": 1700000120.0, "buy_vol": 900.0,  "sell_vol": 1100.0, "net_delta": -200.0},
    ],
    "total_buy": 2100.0,
    "total_sell": 1900.0,
    "total_net": 200.0,
}

NET_TAKER_BEARISH = {
    "status": "ok",
    "symbol": "COSUSDT",
    "window_seconds": 3600,
    "bucket_seconds": 60,
    "buckets": [
        {"ts": 1700000060.0, "buy_vol": 300.0, "sell_vol": 700.0, "net_delta": -400.0},
    ],
    "total_buy": 300.0,
    "total_sell": 700.0,
    "total_net": -400.0,
}

NET_TAKER_NEUTRAL = {
    "status": "ok",
    "symbol": "DEXEUSDT",
    "window_seconds": 3600,
    "bucket_seconds": 60,
    "buckets": [],
    "total_buy": 0.0,
    "total_sell": 0.0,
    "total_net": 0.0,
}


# ── Response shape tests ──────────────────────────────────────────────────────

def test_net_taker_response_has_status():
    assert NET_TAKER_PAYLOAD["status"] == "ok"


def test_net_taker_has_required_keys():
    for key in ("symbol", "window_seconds", "bucket_seconds", "buckets",
                "total_buy", "total_sell", "total_net"):
        assert key in NET_TAKER_PAYLOAD


def test_net_taker_buckets_is_list():
    assert isinstance(NET_TAKER_PAYLOAD["buckets"], list)


def test_net_taker_each_bucket_has_required_keys():
    for bucket in NET_TAKER_PAYLOAD["buckets"]:
        for key in ("ts", "buy_vol", "sell_vol", "net_delta"):
            assert key in bucket


def test_net_taker_net_equals_buy_minus_sell():
    p = NET_TAKER_PAYLOAD
    assert p["total_net"] == pytest.approx(p["total_buy"] - p["total_sell"])


def test_net_taker_bucket_net_delta_correct():
    b = NET_TAKER_PAYLOAD["buckets"][0]
    assert b["net_delta"] == pytest.approx(b["buy_vol"] - b["sell_vol"])


# ── fmt_vol tests ─────────────────────────────────────────────────────────────

def test_fmt_vol_millions():
    assert fmt_vol(2_500_000.0) == "2.50M"


def test_fmt_vol_thousands():
    assert fmt_vol(1_500.0) == "1.5k"


def test_fmt_vol_small():
    assert fmt_vol(999.0) == "999.00"


def test_fmt_vol_negative_thousands():
    assert fmt_vol(-1_200.0) == "-1.2k"


def test_fmt_vol_zero():
    assert fmt_vol(0.0) == "0.00"


# ── net_delta_badge tests ─────────────────────────────────────────────────────

def test_badge_positive_net():
    label, cls = net_delta_badge(200.0)
    assert label == "buying"
    assert cls == "badge-green"


def test_badge_negative_net():
    label, cls = net_delta_badge(-400.0)
    assert label == "selling"
    assert cls == "badge-red"


def test_badge_zero_net():
    label, cls = net_delta_badge(0.0)
    assert label == "neutral"
    assert cls == "badge-blue"


# ── delta_pressure_pct tests ──────────────────────────────────────────────────

def test_pressure_pct_equal():
    assert delta_pressure_pct(500.0, 500.0) == 50


def test_pressure_pct_all_buy():
    assert delta_pressure_pct(1000.0, 0.0) == 100


def test_pressure_pct_all_sell():
    assert delta_pressure_pct(0.0, 1000.0) == 0


def test_pressure_pct_zero_total():
    assert delta_pressure_pct(0.0, 0.0) == 50


def test_pressure_pct_clamped():
    assert delta_pressure_pct(200.0, 100.0) == 67


def test_pressure_pct_bullish_payload():
    p = NET_TAKER_PAYLOAD
    pct = delta_pressure_pct(p["total_buy"], p["total_sell"])
    assert pct > 50  # more buying than selling


def test_pressure_pct_bearish_payload():
    p = NET_TAKER_BEARISH
    pct = delta_pressure_pct(p["total_buy"], p["total_sell"])
    assert pct < 50  # more selling than buying


# ── rank_symbols tests ────────────────────────────────────────────────────────

FOUR_SYMBOLS_DELTA = [
    {"symbol": "BANANAS31USDT", "total_net": 200.0,   "total_buy": 2100.0, "total_sell": 1900.0},
    {"symbol": "COSUSDT",       "total_net": -400.0,  "total_buy": 300.0,  "total_sell": 700.0},
    {"symbol": "DEXEUSDT",      "total_net": 0.0,     "total_buy": 0.0,    "total_sell": 0.0},
    {"symbol": "LYNUSDT",       "total_net": 1500.0,  "total_buy": 3000.0, "total_sell": 1500.0},
]


def test_rank_1_is_highest_net():
    ranked = rank_symbols([d.copy() for d in FOUR_SYMBOLS_DELTA])
    assert ranked[0]["symbol"] == "LYNUSDT"
    assert ranked[0]["rank"] == 1


def test_rank_last_is_most_negative():
    ranked = rank_symbols([d.copy() for d in FOUR_SYMBOLS_DELTA])
    assert ranked[-1]["symbol"] == "COSUSDT"
    assert ranked[-1]["rank"] == 4


def test_ranks_are_sequential():
    ranked = rank_symbols([d.copy() for d in FOUR_SYMBOLS_DELTA])
    assert [r["rank"] for r in ranked] == [1, 2, 3, 4]


def test_ranked_in_descending_net_order():
    ranked = rank_symbols([d.copy() for d in FOUR_SYMBOLS_DELTA])
    nets = [r["total_net"] for r in ranked]
    assert nets == sorted(nets, reverse=True)


def test_all_symbols_present_after_ranking():
    ranked = rank_symbols([d.copy() for d in FOUR_SYMBOLS_DELTA])
    syms = {r["symbol"] for r in ranked}
    assert syms == {"BANANAS31USDT", "COSUSDT", "DEXEUSDT", "LYNUSDT"}


def test_no_duplicate_ranks():
    ranked = rank_symbols([d.copy() for d in FOUR_SYMBOLS_DELTA])
    ranks = [r["rank"] for r in ranked]
    assert len(ranks) == len(set(ranks))


def test_single_symbol_rank_is_one():
    ranked = rank_symbols([{"symbol": "X", "total_net": 100.0, "total_buy": 200.0, "total_sell": 100.0}])
    assert ranked[0]["rank"] == 1


# ── Route registration ────────────────────────────────────────────────────────

def test_net_taker_delta_route_registered():
    import tempfile
    os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "t.db"))
    os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from api import router
    paths = [r.path for r in router.routes]
    assert any("net-taker-delta" in p for p in paths)


# ── HTML / JS smoke tests ─────────────────────────────────────────────────────

def test_html_has_net_taker_delta_card():
    assert "card-net-taker-delta" in _html()


def test_js_has_render_net_taker_delta():
    assert "renderNetTakerDelta" in _js()


def test_js_calls_net_taker_delta_api():
    assert "net-taker-delta" in _js()
