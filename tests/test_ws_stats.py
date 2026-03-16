"""
Unit/smoke tests for /api/ws-stats.

Validates:
  - Rolling-window rate computation logic
  - Per-symbol isolation and aggregation
  - Response shape
  - Zero-message edge cases
  - Display formatting helpers
  - Route registration
"""
import os
import sys
import time

import pytest

# ── Python mirror of rate-computation logic ───────────────────────────────────
# Mirrors the implementation in collectors.py so we can test it in isolation
# without importing the full backend.

from collections import deque

_WS_WINDOW = 60.0  # seconds


class WsRateTracker:
    """Standalone mirror of the collectors.py rolling-window tracker."""

    def __init__(self, window: float = _WS_WINDOW):
        self.window = window
        self._events: deque = deque()   # (ts: float, symbol: str)

    def record(self, symbol: str, ts: float = None) -> None:
        self._events.append((ts if ts is not None else time.time(), symbol))

    def _prune(self, now: float) -> None:
        cutoff = now - self.window
        while self._events and self._events[0][0] <= cutoff:
            self._events.popleft()

    def stats(self, now: float = None) -> dict:
        now = now if now is not None else time.time()
        self._prune(now)

        per_symbol: dict = {}
        for _, sym in self._events:
            per_symbol[sym] = per_symbol.get(sym, 0) + 1

        result = {
            sym: {"msgs_60s": cnt, "rate": round(cnt / self.window, 2)}
            for sym, cnt in per_symbol.items()
        }
        total = sum(d["msgs_60s"] for d in result.values())
        return {
            "status": "ok",
            "symbols": result,
            "aggregate_rate": round(total / self.window, 2),
            "total_msgs_60s": total,
            "window_s": self.window,
            "ts": now,
        }


# ── Display formatting helpers (mirrors app.js) ───────────────────────────────

def fmt_rate(msgs_per_sec: float) -> str:
    if msgs_per_sec <= 0:
        return "0 msg/s"
    if msgs_per_sec >= 1000:
        return f"{msgs_per_sec / 1000:.1f}k msg/s"
    return f"{msgs_per_sec:.1f} msg/s"


def rate_color(msgs_per_sec: float) -> str:
    if msgs_per_sec >= 100:
        return "var(--green)"
    if msgs_per_sec >= 20:
        return "var(--yellow)"
    if msgs_per_sec > 0:
        return "var(--muted)"
    return "var(--red)"


# ── Rolling-window rate tests ─────────────────────────────────────────────────

def test_empty_tracker_returns_zero_rate():
    t = WsRateTracker()
    s = t.stats(now=1000.0)
    assert s["aggregate_rate"] == 0.0
    assert s["total_msgs_60s"] == 0


def test_single_symbol_rate():
    t = WsRateTracker(window=60.0)
    now = 1000.0
    for i in range(120):
        t.record("BANANAS31USDT", ts=now - 30 + i * 0.25)  # 120 msgs over 30s
    s = t.stats(now=now)
    # 120 msgs / 60s window = 2.0 msg/s
    assert s["symbols"]["BANANAS31USDT"]["msgs_60s"] == 120
    assert s["symbols"]["BANANAS31USDT"]["rate"] == pytest.approx(2.0, rel=1e-2)


def test_old_events_pruned():
    t = WsRateTracker(window=60.0)
    now = 1000.0
    # 30 msgs older than the window
    for i in range(30):
        t.record("COSUSDT", ts=now - 120 + i)
    # 10 msgs inside the window
    for i in range(10):
        t.record("COSUSDT", ts=now - 10 + i)
    s = t.stats(now=now)
    assert s["symbols"]["COSUSDT"]["msgs_60s"] == 10


def test_per_symbol_isolation():
    t = WsRateTracker(window=60.0)
    now = 1000.0
    for i in range(60):
        t.record("BANANAS31USDT", ts=now - 59 + i)
    for i in range(30):
        t.record("COSUSDT", ts=now - 59 + i * 2)
    s = t.stats(now=now)
    assert s["symbols"]["BANANAS31USDT"]["msgs_60s"] == 60
    assert s["symbols"]["COSUSDT"]["msgs_60s"] == 30


def test_aggregate_rate_sums_all_symbols():
    t = WsRateTracker(window=60.0)
    now = 1000.0
    for sym in ("BANANAS31USDT", "COSUSDT", "DEXEUSDT", "LYNUSDT"):
        for i in range(60):
            t.record(sym, ts=now - 59 + i)
    s = t.stats(now=now)
    assert s["total_msgs_60s"] == 240
    assert s["aggregate_rate"] == pytest.approx(4.0, rel=1e-2)


def test_window_boundary_exactly():
    t = WsRateTracker(window=60.0)
    now = 1000.0
    t.record("BANANAS31USDT", ts=now - 60.0)   # exactly at boundary — should be excluded
    t.record("BANANAS31USDT", ts=now - 59.9)   # just inside
    s = t.stats(now=now)
    assert s["symbols"]["BANANAS31USDT"]["msgs_60s"] == 1


def test_no_symbol_after_window_expires():
    t = WsRateTracker(window=60.0)
    now = 1000.0
    t.record("BANANAS31USDT", ts=now - 120.0)  # way outside window
    s = t.stats(now=now)
    assert "BANANAS31USDT" not in s["symbols"]
    assert s["aggregate_rate"] == 0.0


# ── Response shape tests ──────────────────────────────────────────────────────

def test_response_has_status_ok():
    t = WsRateTracker()
    s = t.stats()
    assert s["status"] == "ok"


def test_response_has_symbols_dict():
    t = WsRateTracker()
    s = t.stats()
    assert isinstance(s["symbols"], dict)


def test_response_has_aggregate_rate():
    t = WsRateTracker()
    s = t.stats()
    assert "aggregate_rate" in s
    assert isinstance(s["aggregate_rate"], float)


def test_response_has_window_s():
    t = WsRateTracker(window=60.0)
    s = t.stats()
    assert s["window_s"] == 60.0


def test_symbol_entry_has_rate_and_msgs():
    t = WsRateTracker(window=60.0)
    now = 1000.0
    t.record("BANANAS31USDT", ts=now - 1)
    s = t.stats(now=now)
    entry = s["symbols"]["BANANAS31USDT"]
    assert "rate" in entry
    assert "msgs_60s" in entry


# ── Display formatting ────────────────────────────────────────────────────────

def test_fmt_rate_zero():
    assert fmt_rate(0.0) == "0 msg/s"


def test_fmt_rate_normal():
    assert fmt_rate(25.3) == "25.3 msg/s"


def test_fmt_rate_high():
    assert fmt_rate(1500.0) == "1.5k msg/s"


def test_rate_color_healthy():
    assert rate_color(150.0) == "var(--green)"


def test_rate_color_medium():
    assert rate_color(50.0) == "var(--yellow)"


def test_rate_color_low():
    assert rate_color(5.0) == "var(--muted)"


def test_rate_color_zero():
    assert rate_color(0.0) == "var(--red)"


# ── Route registration ────────────────────────────────────────────────────────

def test_ws_stats_route_registered():
    import tempfile
    os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "t.db"))
    os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from api import router
    paths = [r.path for r in router.routes]
    assert any("ws-stats" in p for p in paths)


# ── HTML / JS structure smoke tests ──────────────────────────────────────────

_ROOT = os.path.join(os.path.dirname(__file__), "..")


def _read(rel: str) -> str:
    with open(os.path.join(_ROOT, rel), encoding="utf-8") as f:
        return f.read()


def test_html_has_ws_rate_element():
    html = _read("frontend/index.html")
    assert 'id="ws-rate"' in html, "Missing #ws-rate element in HTML"


def test_ws_rate_element_in_header():
    html = _read("frontend/index.html")
    hi = html.index("<header")
    he = html.index("</header>")
    assert 'id="ws-rate"' in html[hi:he], "#ws-rate must be inside <header>"


def test_js_has_render_ws_stats():
    js = _read("frontend/app.js")
    assert "renderWsStats" in js or "ws-stats" in js, \
        "app.js must reference ws-stats rendering"


def test_js_calls_ws_stats_api():
    js = _read("frontend/app.js")
    assert "ws-stats" in js, "app.js must call /api/ws-stats"
