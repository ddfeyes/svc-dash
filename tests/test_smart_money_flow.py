"""
Unit / smoke tests for /api/smart-money-flow.

Smart Money Flow Index: institutional vs retail flow with divergence signals.

Distinct from the existing /smart-money-divergence (raw divergence score):
  - Normalized 0-100 SMF Index (50 = neutral)
  - Separate retail flow index
  - Trend bias (bullish/bearish/neutral) with strength 1-3
  - Multi-window (15m / 1h) breakdown
  - Time series for sparkline rendering

Covers:
  - flow_ratio computation
  - flow_to_index normalization
  - bias_from_index classification
  - bias_strength classification
  - signal_from_divergence
  - flow series building
  - Response shape validation
  - Edge cases (no trades, all buys, all sells, threshold edge)
  - Route registration
  - HTML card / JS smoke tests
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


# ── Python mirrors of backend logic ──────────────────────────────────────────

def flow_ratio(buy_usd: float, sell_usd: float) -> float | None:
    """
    Net flow ratio: (buy - sell) / (buy + sell).
    Returns None if both are 0. Range: -1.0 to +1.0.
    """
    total = buy_usd + sell_usd
    if total <= 0:
        return None
    return round((buy_usd - sell_usd) / total, 6)


def flow_to_index(ratio: float | None) -> float:
    """
    Convert flow ratio [-1, +1] to SMF Index [0, 100].
    None → 50 (neutral).
    -1 → 0, 0 → 50, +1 → 100.
    """
    if ratio is None:
        return 50.0
    return round((ratio + 1.0) / 2.0 * 100.0, 2)


def bias_from_index(smf_index: float, retail_index: float,
                    bull_threshold: float = 55.0,
                    bear_threshold: float = 45.0) -> str:
    """
    Trend bias: smart money dominant direction.
    bullish:  smf_index >= bull_threshold AND smf_index > retail_index
    bearish:  smf_index <= bear_threshold AND smf_index < retail_index
    neutral:  otherwise
    """
    if smf_index >= bull_threshold and smf_index > retail_index:
        return "bullish"
    if smf_index <= bear_threshold and smf_index < retail_index:
        return "bearish"
    return "neutral"


def bias_strength(divergence: float) -> int:
    """
    Divergence strength 1-3 based on |smf_index - retail_index|.
    weak (1):     |div| < 10
    moderate (2): |div| < 25
    strong (3):   |div| >= 25
    """
    d = abs(divergence)
    if d >= 25:
        return 3
    if d >= 10:
        return 2
    return 1


def signal_from_divergence(divergence: float, threshold: float = 10.0) -> str:
    """
    Classify signal from SMF-retail divergence.
    accumulation:  divergence >= threshold  (smart buying > retail buying)
    distribution:  divergence <= -threshold (smart selling > retail selling)
    neutral:       |divergence| < threshold
    """
    if divergence >= threshold:
        return "accumulation"
    if divergence <= -threshold:
        return "distribution"
    return "neutral"


def build_flow_series(
    trades: list[dict],
    threshold_usd: float,
    bucket_s: int,
    since: float,
    window_s: int,
) -> list[dict]:
    """
    Time-series of smart_flow and retail_flow per bucket.
    Each point: {ts, smart_flow, retail_flow, smf_index, retail_index}
    smart_flow / retail_flow are flow_ratio values per bucket.
    """
    n_buckets = max(1, window_s // bucket_s)
    buckets: list[dict] = [
        {"ts": since + i * bucket_s,
         "sb": 0.0, "ss": 0.0, "rb": 0.0, "rs": 0.0}
        for i in range(n_buckets)
    ]

    for t in trades:
        ts = float(t.get("ts") or 0)
        price = float(t.get("price") or 0)
        qty = float(t.get("qty") or 0)
        val = price * qty
        side = (t.get("side") or "").lower()
        is_buy = side == "buy"

        elapsed = ts - since
        if elapsed < 0 or elapsed >= window_s:
            continue
        idx = min(n_buckets - 1, int(elapsed / bucket_s))

        if val >= threshold_usd:
            if is_buy:
                buckets[idx]["sb"] += val
            else:
                buckets[idx]["ss"] += val
        else:
            if is_buy:
                buckets[idx]["rb"] += val
            else:
                buckets[idx]["rs"] += val

    series = []
    for b in buckets:
        sr = flow_ratio(b["sb"], b["ss"])
        rr = flow_ratio(b["rb"], b["rs"])
        series.append({
            "ts": b["ts"],
            "smart_flow": sr,
            "retail_flow": rr,
            "smf_index": flow_to_index(sr),
            "retail_index": flow_to_index(rr),
        })
    return series


# ── flow_ratio tests ──────────────────────────────────────────────────────────

def test_flow_ratio_all_buy():
    assert flow_ratio(1000.0, 0.0) == pytest.approx(1.0)


def test_flow_ratio_all_sell():
    assert flow_ratio(0.0, 1000.0) == pytest.approx(-1.0)


def test_flow_ratio_balanced():
    assert flow_ratio(500.0, 500.0) == pytest.approx(0.0)


def test_flow_ratio_none_both_zero():
    assert flow_ratio(0.0, 0.0) is None


def test_flow_ratio_buy_dominant():
    r = flow_ratio(800.0, 200.0)
    assert r is not None
    assert 0.0 < r < 1.0
    assert r == pytest.approx(0.6)


def test_flow_ratio_sell_dominant():
    r = flow_ratio(200.0, 800.0)
    assert r is not None
    assert r == pytest.approx(-0.6)


def test_flow_ratio_range():
    for buy, sell in [(100, 0), (0, 100), (50, 50), (75, 25)]:
        r = flow_ratio(buy, sell)
        if r is not None:
            assert -1.0 <= r <= 1.0


# ── flow_to_index tests ───────────────────────────────────────────────────────

def test_flow_to_index_max():
    assert flow_to_index(1.0) == pytest.approx(100.0)


def test_flow_to_index_min():
    assert flow_to_index(-1.0) == pytest.approx(0.0)


def test_flow_to_index_neutral():
    assert flow_to_index(0.0) == pytest.approx(50.0)


def test_flow_to_index_none():
    assert flow_to_index(None) == pytest.approx(50.0)


def test_flow_to_index_positive():
    idx = flow_to_index(0.6)
    assert idx == pytest.approx(80.0)


def test_flow_to_index_negative():
    idx = flow_to_index(-0.6)
    assert idx == pytest.approx(20.0)


def test_flow_to_index_range():
    for r in [-1.0, -0.5, 0.0, 0.5, 1.0]:
        idx = flow_to_index(r)
        assert 0.0 <= idx <= 100.0


def test_flow_to_index_monotone():
    vals = [-1.0, -0.5, 0.0, 0.5, 1.0]
    idxs = [flow_to_index(v) for v in vals]
    assert idxs == sorted(idxs)


# ── bias_from_index tests ─────────────────────────────────────────────────────

def test_bias_bullish():
    assert bias_from_index(70.0, 50.0) == "bullish"


def test_bias_bearish():
    assert bias_from_index(30.0, 50.0) == "bearish"


def test_bias_neutral_smf_above_but_below_retail():
    # SMF > threshold but retail also higher
    assert bias_from_index(60.0, 65.0) == "neutral"


def test_bias_neutral_smf_low_but_above_retail():
    # SMF low but retail even lower
    assert bias_from_index(40.0, 35.0) == "neutral"


def test_bias_neutral_at_50():
    assert bias_from_index(50.0, 50.0) == "neutral"


def test_bias_bullish_strong():
    assert bias_from_index(80.0, 40.0) == "bullish"


def test_bias_bearish_strong():
    assert bias_from_index(20.0, 60.0) == "bearish"


def test_bias_valid_values():
    for smf, ret in [(70, 50), (30, 50), (50, 50), (80, 40), (20, 60)]:
        b = bias_from_index(float(smf), float(ret))
        assert b in ("bullish", "bearish", "neutral")


# ── bias_strength tests ───────────────────────────────────────────────────────

def test_strength_weak():
    assert bias_strength(5.0) == 1
    assert bias_strength(-9.9) == 1


def test_strength_moderate():
    assert bias_strength(10.0) == 2
    assert bias_strength(-24.9) == 2


def test_strength_strong():
    assert bias_strength(25.0) == 3
    assert bias_strength(-50.0) == 3


def test_strength_zero():
    assert bias_strength(0.0) == 1


def test_strength_boundary_10():
    assert bias_strength(10.0) == 2
    assert bias_strength(9.9) == 1


def test_strength_boundary_25():
    assert bias_strength(25.0) == 3
    assert bias_strength(24.9) == 2


# ── signal_from_divergence tests ──────────────────────────────────────────────

def test_signal_accumulation():
    assert signal_from_divergence(15.0, threshold=10.0) == "accumulation"


def test_signal_distribution():
    assert signal_from_divergence(-15.0, threshold=10.0) == "distribution"


def test_signal_neutral_inside_threshold():
    assert signal_from_divergence(5.0, threshold=10.0) == "neutral"
    assert signal_from_divergence(-5.0, threshold=10.0) == "neutral"


def test_signal_at_boundary():
    # exactly at threshold → accumulation
    assert signal_from_divergence(10.0, threshold=10.0) == "accumulation"
    assert signal_from_divergence(-10.0, threshold=10.0) == "distribution"


def test_signal_valid_values():
    for d in [-20, -5, 0, 5, 20]:
        s = signal_from_divergence(float(d))
        assert s in ("accumulation", "distribution", "neutral")


# ── build_flow_series tests ───────────────────────────────────────────────────

BASE_TS = 1700000000.0
TRADES_MIXED = [
    # Smart money (val >= 10000)
    {"ts": BASE_TS + 60,  "price": 100.0, "qty": 200.0, "side": "buy"},   # val=20000
    {"ts": BASE_TS + 120, "price": 100.0, "qty": 150.0, "side": "sell"},  # val=15000
    # Retail (val < 10000)
    {"ts": BASE_TS + 180, "price": 100.0, "qty": 50.0, "side": "buy"},    # val=5000
    {"ts": BASE_TS + 240, "price": 100.0, "qty": 30.0, "side": "sell"},   # val=3000
    # Second bucket
    {"ts": BASE_TS + 360, "price": 100.0, "qty": 300.0, "side": "buy"},   # val=30000 (smart)
]


def test_flow_series_length():
    series = build_flow_series(TRADES_MIXED, 10000.0, 300, BASE_TS, 600)
    assert len(series) == 2  # 600 / 300 = 2 buckets


def test_flow_series_has_required_keys():
    series = build_flow_series(TRADES_MIXED, 10000.0, 300, BASE_TS, 600)
    for pt in series:
        for key in ("ts", "smart_flow", "retail_flow", "smf_index", "retail_index"):
            assert key in pt


def test_flow_series_smf_index_range():
    series = build_flow_series(TRADES_MIXED, 10000.0, 300, BASE_TS, 600)
    for pt in series:
        assert 0.0 <= pt["smf_index"] <= 100.0
        assert 0.0 <= pt["retail_index"] <= 100.0


def test_flow_series_neutral_for_empty_buckets():
    series = build_flow_series([], 10000.0, 300, BASE_TS, 600)
    for pt in series:
        assert pt["smf_index"] == pytest.approx(50.0)
        assert pt["retail_index"] == pytest.approx(50.0)


def test_flow_series_bucket0_smart_has_both_sides():
    series = build_flow_series(TRADES_MIXED, 10000.0, 300, BASE_TS, 600)
    # Bucket 0: smart buy 20000 + smart sell 15000 → flow_ratio=(20k-15k)/(35k)=5/35≈0.143
    b0 = series[0]
    assert b0["smart_flow"] is not None
    assert b0["smf_index"] > 50.0  # net buy dominant


def test_flow_series_bucket1_all_smart_buy():
    series = build_flow_series(TRADES_MIXED, 10000.0, 300, BASE_TS, 600)
    b1 = series[1]
    # Only smart buy in bucket 1 → ratio = 1.0 → index = 100
    assert b1["smf_index"] == pytest.approx(100.0)


def test_flow_series_out_of_window_excluded():
    out_of_window = TRADES_MIXED + [
        {"ts": BASE_TS - 100, "price": 100.0, "qty": 9999.0, "side": "buy"},
        {"ts": BASE_TS + 700, "price": 100.0, "qty": 9999.0, "side": "buy"},
    ]
    s1 = build_flow_series(TRADES_MIXED, 10000.0, 300, BASE_TS, 600)
    s2 = build_flow_series(out_of_window, 10000.0, 300, BASE_TS, 600)
    # Both series should have same smart_flow (out-of-window excluded)
    for b1, b2 in zip(s1, s2):
        assert b1["smart_flow"] == b2["smart_flow"]
        assert b1["retail_flow"] == b2["retail_flow"]


# ── Response shape ────────────────────────────────────────────────────────────

SAMPLE_RESPONSE = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "window_seconds": 3600,
    "threshold_usd": 10000.0,
    "smf_index": 64.3,
    "retail_index": 46.8,
    "divergence": 17.5,
    "bias": "bullish",
    "bias_strength": 2,
    "signal": "accumulation",
    "smart_buy_usd": 85000.0,
    "smart_sell_usd": 35000.0,
    "retail_buy_usd": 12000.0,
    "retail_sell_usd": 15000.0,
    "smart_trade_count": 12,
    "retail_trade_count": 430,
    "smart_pct_volume": 0.87,
    "windows": {
        "15m": {
            "smf_index": 58.0,
            "retail_index": 50.0,
            "divergence": 8.0,
            "signal": "neutral",
        },
        "1h": {
            "smf_index": 64.3,
            "retail_index": 46.8,
            "divergence": 17.5,
            "signal": "accumulation",
        },
    },
    "series": [
        {"ts": 1700000000.0, "smart_flow": 0.3, "retail_flow": -0.1,
         "smf_index": 65.0, "retail_index": 45.0},
        {"ts": 1700000300.0, "smart_flow": 0.2, "retail_flow": -0.05,
         "smf_index": 60.0, "retail_index": 47.5},
    ],
    "description": "Smart money buying: institutional flow +17.5pts above retail",
}


def test_response_status_ok():
    assert SAMPLE_RESPONSE["status"] == "ok"


def test_response_has_required_keys():
    for key in (
        "symbol", "window_seconds", "threshold_usd",
        "smf_index", "retail_index", "divergence",
        "bias", "bias_strength", "signal",
        "smart_buy_usd", "smart_sell_usd",
        "retail_buy_usd", "retail_sell_usd",
        "smart_trade_count", "retail_trade_count",
        "smart_pct_volume", "windows", "series", "description",
    ):
        assert key in SAMPLE_RESPONSE


def test_response_smf_index_range():
    assert 0.0 <= SAMPLE_RESPONSE["smf_index"] <= 100.0


def test_response_retail_index_range():
    assert 0.0 <= SAMPLE_RESPONSE["retail_index"] <= 100.0


def test_response_divergence_matches():
    r = SAMPLE_RESPONSE
    assert r["divergence"] == pytest.approx(r["smf_index"] - r["retail_index"], rel=0.01)


def test_response_bias_valid():
    assert SAMPLE_RESPONSE["bias"] in ("bullish", "bearish", "neutral")


def test_response_bias_strength_valid():
    assert SAMPLE_RESPONSE["bias_strength"] in (1, 2, 3)


def test_response_signal_valid():
    assert SAMPLE_RESPONSE["signal"] in ("accumulation", "distribution", "neutral")


def test_response_windows_has_15m_and_1h():
    assert "15m" in SAMPLE_RESPONSE["windows"]
    assert "1h"  in SAMPLE_RESPONSE["windows"]


def test_response_window_has_required_keys():
    for w in SAMPLE_RESPONSE["windows"].values():
        for key in ("smf_index", "retail_index", "divergence", "signal"):
            assert key in w


def test_response_series_is_list():
    assert isinstance(SAMPLE_RESPONSE["series"], list)
    assert len(SAMPLE_RESPONSE["series"]) > 0


def test_response_series_has_required_keys():
    for pt in SAMPLE_RESPONSE["series"]:
        for key in ("ts", "smart_flow", "retail_flow", "smf_index", "retail_index"):
            assert key in pt


def test_response_series_index_range():
    for pt in SAMPLE_RESPONSE["series"]:
        assert 0.0 <= pt["smf_index"] <= 100.0
        assert 0.0 <= pt["retail_index"] <= 100.0


def test_response_smart_pct_range():
    assert 0.0 <= SAMPLE_RESPONSE["smart_pct_volume"] <= 1.0


def test_response_trade_counts_positive():
    assert SAMPLE_RESPONSE["smart_trade_count"] >= 0
    assert SAMPLE_RESPONSE["retail_trade_count"] >= 0


def test_response_bias_consistent():
    r = SAMPLE_RESPONSE
    expected = bias_from_index(r["smf_index"], r["retail_index"])
    assert r["bias"] == expected


def test_response_signal_consistent():
    r = SAMPLE_RESPONSE
    expected = signal_from_divergence(r["divergence"])
    assert r["signal"] == expected


def test_response_strength_consistent():
    r = SAMPLE_RESPONSE
    expected = bias_strength(r["divergence"])
    assert r["bias_strength"] == expected


def test_response_has_description():
    assert isinstance(SAMPLE_RESPONSE["description"], str)
    assert len(SAMPLE_RESPONSE["description"]) > 0


# ── Route registration ────────────────────────────────────────────────────────

def test_smart_money_flow_route_registered():
    import tempfile
    os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "t.db"))
    os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from api import router
    paths = [r.path for r in router.routes]
    assert any("smart-money-flow" in p for p in paths)


# ── HTML / JS smoke tests ─────────────────────────────────────────────────────

def test_html_has_smart_money_flow_card():
    assert "card-smart-money-flow" in _html()


def test_js_has_render_smart_money_flow():
    assert "renderSmartMoneyFlow" in _js()


def test_js_calls_smart_money_flow_api():
    assert "smart-money-flow" in _js()
