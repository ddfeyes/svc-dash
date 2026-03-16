"""
Unit / smoke tests for /api/session-volume-profile.

Covers:
  - Session boundary logic (Asia/EU/US UTC hours)
  - Volume profile helpers (POC, VAH, VAL computation)
  - "active session" detection
  - Response shape validation
  - Display helpers mirrored from app.js
  - HTML card / JS smoke tests
  - Route registration
"""
import os
import sys
import time
import datetime

import pytest

_ROOT = os.path.join(os.path.dirname(__file__), "..")


def _html() -> str:
    with open(os.path.join(_ROOT, "frontend", "index.html"), encoding="utf-8") as f:
        return f.read()


def _js() -> str:
    with open(os.path.join(_ROOT, "frontend", "app.js"), encoding="utf-8") as f:
        return f.read()


# ── Session definitions ───────────────────────────────────────────────────────

SESSIONS = {
    "asia": {"name": "Asia",  "start_utc": 0,  "end_utc": 8},
    "eu":   {"name": "EU",    "start_utc": 7,  "end_utc": 16},
    "us":   {"name": "US",    "start_utc": 13, "end_utc": 22},
}


def current_session(utc_hour: int) -> str:
    """Return the most recently opened session at the given UTC hour."""
    active = []
    for key, s in SESSIONS.items():
        if s["start_utc"] <= utc_hour < s["end_utc"]:
            active.append(key)
    if len(active) > 1:
        return "overlap"
    if len(active) == 1:
        return active[0]
    return "none"


def session_window(session_key: str, reference_ts: float) -> tuple[float, float]:
    """Return (start_ts, end_ts) for the most recent occurrence of session_key."""
    s = SESSIONS[session_key]
    dt = datetime.datetime.utcfromtimestamp(reference_ts)
    day_start = datetime.datetime(dt.year, dt.month, dt.day)
    start_ts = day_start.timestamp() + s["start_utc"] * 3600
    end_ts   = day_start.timestamp() + s["end_utc"]   * 3600
    # if session hasn't started yet today roll back to yesterday
    if start_ts > reference_ts:
        start_ts -= 86400
        end_ts   -= 86400
    return start_ts, end_ts


# ── Python mirrors of volume profile helpers ──────────────────────────────────

def build_profile(bins: list[dict]) -> dict:
    """
    Given bins [{price, volume}], compute poc, vah, val (70% value area).
    Returns {"poc", "vah", "val", "total_volume"}.
    """
    if not bins:
        return {"poc": None, "vah": None, "val": None, "total_volume": 0.0}

    total = sum(b["volume"] for b in bins)
    if total <= 0:
        return {"poc": None, "vah": None, "val": None, "total_volume": 0.0}

    poc_bin = max(bins, key=lambda b: b["volume"])
    poc_price = poc_bin["price"]
    poc_idx = next(i for i, b in enumerate(bins) if b["price"] == poc_price)

    target = total * 0.70
    lo, hi = poc_idx, poc_idx
    accumulated = poc_bin["volume"]

    while accumulated < target:
        can_up   = hi + 1 < len(bins)
        can_down = lo - 1 >= 0
        if not can_up and not can_down:
            break
        vol_up   = bins[hi + 1]["volume"] if can_up   else -1
        vol_down = bins[lo - 1]["volume"] if can_down else -1
        if vol_up >= vol_down:
            hi += 1
            accumulated += vol_up
        else:
            lo -= 1
            accumulated += vol_down

    return {
        "poc":          poc_price,
        "vah":          bins[hi]["price"],
        "val":          bins[lo]["price"],
        "total_volume": round(total, 6),
    }


def pct_of_poc(volume: float, poc_volume: float) -> int:
    """Bar width as 0–100% of POC volume."""
    if poc_volume <= 0:
        return 0
    return max(0, min(100, round(volume / poc_volume * 100)))


def fmt_price(p: float | None) -> str:
    if p is None:
        return "—"
    if p < 0.01:
        return f"{p:.6f}"
    if p < 1:
        return f"{p:.4f}"
    return f"{p:.2f}"


def fmt_vol_short(v: float) -> str:
    if v >= 1_000_000:
        return f"{v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v/1_000:.1f}k"
    return f"{v:.2f}"


def session_badge(session_key: str) -> tuple[str, str]:
    """(label, css_class) for the active session badge."""
    mapping = {
        "asia":    ("ASIA",    "badge-blue"),
        "eu":      ("EU",      "badge-yellow"),
        "us":      ("US",      "badge-green"),
        "overlap": ("OVERLAP", "badge-red"),
        "none":    ("—",       "badge-blue"),
    }
    return mapping.get(session_key, ("—", "badge-blue"))


# ── Session boundary tests ────────────────────────────────────────────────────

def test_current_session_asia_only():
    assert current_session(3) == "asia"


def test_current_session_us_only():
    assert current_session(20) == "us"


def test_current_session_eu_only():
    assert current_session(10) == "eu"


def test_current_session_asia_eu_overlap():
    # hour 7 = both asia (0-8) and EU (7-16)
    assert current_session(7) == "overlap"


def test_current_session_eu_us_overlap():
    # hour 13 = both EU (7-16) and US (13-22)
    assert current_session(13) == "overlap"


def test_current_session_none():
    # hour 23 = after US (ends at 22)
    assert current_session(23) == "none"


def test_session_window_returns_two_floats():
    ref = 1700000000.0
    start, end = session_window("asia", ref)
    assert isinstance(start, float)
    assert isinstance(end, float)


def test_session_window_asia_span():
    ref = 1700000000.0
    start, end = session_window("asia", ref)
    assert end - start == 8 * 3600


def test_session_window_eu_span():
    ref = 1700000000.0
    start, end = session_window("eu", ref)
    assert end - start == 9 * 3600


def test_session_window_us_span():
    ref = 1700000000.0
    start, end = session_window("us", ref)
    assert end - start == 9 * 3600


def test_session_window_start_before_end():
    for key in ("asia", "eu", "us"):
        s, e = session_window(key, 1700000000.0)
        assert s < e


# ── Volume profile helper tests ───────────────────────────────────────────────

BINS_SIMPLE = [
    {"price": 1.0, "volume": 100.0},
    {"price": 2.0, "volume": 500.0},
    {"price": 3.0, "volume": 200.0},
    {"price": 4.0, "volume": 50.0},
    {"price": 5.0, "volume": 150.0},
]


def test_build_profile_poc_is_max_volume():
    p = build_profile(BINS_SIMPLE)
    assert p["poc"] == 2.0


def test_build_profile_total_volume():
    p = build_profile(BINS_SIMPLE)
    assert p["total_volume"] == pytest.approx(1000.0)


def test_build_profile_vah_gte_poc():
    p = build_profile(BINS_SIMPLE)
    assert p["vah"] >= p["poc"]


def test_build_profile_val_lte_poc():
    p = build_profile(BINS_SIMPLE)
    assert p["val"] <= p["poc"]


def test_build_profile_val_lte_vah():
    p = build_profile(BINS_SIMPLE)
    assert p["val"] <= p["vah"]


def test_build_profile_empty():
    p = build_profile([])
    assert p["poc"] is None
    assert p["total_volume"] == 0.0


def test_build_profile_single_bin():
    p = build_profile([{"price": 1.5, "volume": 100.0}])
    assert p["poc"] == 1.5
    assert p["vah"] == 1.5
    assert p["val"] == 1.5


def test_pct_of_poc_full():
    assert pct_of_poc(500.0, 500.0) == 100


def test_pct_of_poc_half():
    assert pct_of_poc(250.0, 500.0) == 50


def test_pct_of_poc_zero_poc():
    assert pct_of_poc(100.0, 0.0) == 0


def test_pct_of_poc_clamped():
    assert pct_of_poc(999.0, 100.0) == 100


# ── Display helper tests ──────────────────────────────────────────────────────

def test_fmt_price_large():
    assert fmt_price(1234.56) == "1234.56"


def test_fmt_price_sub_penny():
    assert fmt_price(0.002345) == "0.002345"


def test_fmt_price_none():
    assert fmt_price(None) == "—"


def test_fmt_vol_short_millions():
    assert fmt_vol_short(2_500_000.0) == "2.5M"


def test_fmt_vol_short_thousands():
    assert fmt_vol_short(1_500.0) == "1.5k"


def test_fmt_vol_short_small():
    assert fmt_vol_short(42.5) == "42.50"


def test_session_badge_asia():
    label, cls = session_badge("asia")
    assert label == "ASIA"
    assert cls == "badge-blue"


def test_session_badge_eu():
    label, cls = session_badge("eu")
    assert label == "EU"
    assert cls == "badge-yellow"


def test_session_badge_us():
    label, cls = session_badge("us")
    assert label == "US"
    assert cls == "badge-green"


def test_session_badge_overlap():
    label, cls = session_badge("overlap")
    assert label == "OVERLAP"
    assert cls == "badge-red"


# ── Response shape ────────────────────────────────────────────────────────────

SAMPLE_RESPONSE = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "ts": 1700050000.0,
    "current_session": "us",
    "sessions": {
        "asia": {
            "name": "Asia", "hours": "00-08 UTC",
            "poc": 0.002345, "vah": 0.002400, "val": 0.002300,
            "total_volume": 12000.0,
            "bins": [
                {"price": 0.002300, "volume": 2000.0, "buy_vol": 1200.0, "sell_vol": 800.0,
                 "pct_of_max": 40, "in_value_area": True},
                {"price": 0.002345, "volume": 5000.0, "buy_vol": 3000.0, "sell_vol": 2000.0,
                 "pct_of_max": 100, "in_value_area": True},
                {"price": 0.002400, "volume": 3000.0, "buy_vol": 1800.0, "sell_vol": 1200.0,
                 "pct_of_max": 60, "in_value_area": True},
            ],
            "active": False,
        },
        "eu": {
            "name": "EU", "hours": "07-16 UTC",
            "poc": 0.002360, "vah": 0.002410, "val": 0.002310,
            "total_volume": 18000.0, "bins": [], "active": False,
        },
        "us": {
            "name": "US", "hours": "13-22 UTC",
            "poc": 0.002380, "vah": 0.002430, "val": 0.002330,
            "total_volume": 25000.0, "bins": [], "active": True,
        },
    },
}


def test_response_status_ok():
    assert SAMPLE_RESPONSE["status"] == "ok"


def test_response_has_sessions():
    assert "sessions" in SAMPLE_RESPONSE
    for key in ("asia", "eu", "us"):
        assert key in SAMPLE_RESPONSE["sessions"]


def test_response_has_current_session():
    assert SAMPLE_RESPONSE["current_session"] in ("asia", "eu", "us", "overlap", "none")


def test_response_has_ts():
    assert isinstance(SAMPLE_RESPONSE["ts"], float)


def test_each_session_has_required_keys():
    for key, s in SAMPLE_RESPONSE["sessions"].items():
        for field in ("name", "hours", "poc", "vah", "val", "total_volume", "bins", "active"):
            assert field in s, f"Missing '{field}' in session '{key}'"


def test_each_bin_has_required_keys():
    for b in SAMPLE_RESPONSE["sessions"]["asia"]["bins"]:
        for field in ("price", "volume", "pct_of_max", "in_value_area"):
            assert field in b


def test_vah_gte_poc():
    for s in SAMPLE_RESPONSE["sessions"].values():
        if s["poc"] and s["vah"]:
            assert s["vah"] >= s["poc"]


def test_val_lte_poc():
    for s in SAMPLE_RESPONSE["sessions"].values():
        if s["poc"] and s["val"]:
            assert s["val"] <= s["poc"]


def test_active_session_is_us():
    active = [k for k, s in SAMPLE_RESPONSE["sessions"].items() if s["active"]]
    assert "us" in active


def test_bins_pct_of_max_clamped():
    for b in SAMPLE_RESPONSE["sessions"]["asia"]["bins"]:
        assert 0 <= b["pct_of_max"] <= 100


# ── Route registration ────────────────────────────────────────────────────────

def test_session_volume_profile_route_registered():
    import tempfile
    os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "t.db"))
    os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from api import router
    paths = [r.path for r in router.routes]
    assert any("session-volume-profile" in p for p in paths)


# ── HTML / JS smoke tests ─────────────────────────────────────────────────────

def test_html_has_session_volume_profile_card():
    assert "card-session-volume-profile" in _html()


def test_js_has_render_session_volume_profile():
    assert "renderSessionVolumeProfile" in _js()


def test_js_calls_session_volume_profile_api():
    assert "session-volume-profile" in _js()
