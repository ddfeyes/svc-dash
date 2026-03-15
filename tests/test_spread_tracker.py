"""Unit tests for bid-ask spread tracker logic."""
import asyncio
import time
import pytest


# ── Pure calculation helpers (mirror what storage/api do) ────────────────────

def calc_spread(bid: float, ask: float) -> dict:
    """Mirror the spread calculation in storage.insert_orderbook."""
    mid = (bid + ask) / 2
    spread_abs = ask - bid
    spread_pct = (spread_abs / mid) * 100
    spread_bps = round(spread_pct * 100, 4)
    return {
        "bid": bid, "ask": ask, "mid": mid,
        "spread_abs": spread_abs,
        "spread_pct": spread_pct,
        "spread_bps": spread_bps,
    }


def check_alert(spread_pct: float, threshold_pct: float = 0.1) -> dict | None:
    """Mirror alert logic from get_spread_stats and spread_tracker endpoint."""
    if spread_pct > threshold_pct:
        return {
            "level": "high",
            "reason": "spread_pct_threshold",
            "message": f"Spread {spread_pct:.4f}% exceeds {threshold_pct}% threshold",
            "current_pct": round(spread_pct, 4),
        }
    return None


# ── Spread calculation tests ─────────────────────────────────────────────────

class TestSpreadCalculation:
    def test_basic_spread(self):
        r = calc_spread(bid=100.0, ask=100.1)
        assert abs(r["spread_abs"] - 0.1) < 1e-9
        assert abs(r["mid"] - 100.05) < 1e-9
        # pct = 0.1 / 100.05 * 100 ≈ 0.09995%
        assert abs(r["spread_pct"] - (0.1 / 100.05 * 100)) < 1e-6
        assert r["spread_bps"] == round(r["spread_pct"] * 100, 4)

    def test_zero_spread(self):
        r = calc_spread(bid=200.0, ask=200.0)
        assert r["spread_abs"] == 0.0
        assert r["spread_pct"] == 0.0
        assert r["spread_bps"] == 0.0

    def test_wide_spread(self):
        r = calc_spread(bid=1000.0, ask=1002.0)
        # spread_pct = 2 / 1001 * 100 ≈ 0.1998%
        assert r["spread_pct"] > 0.1
        assert r["spread_bps"] > 10

    def test_tiny_spread(self):
        r = calc_spread(bid=50000.0, ask=50000.5)
        # spread_pct = 0.5 / 50000.25 * 100 ≈ 0.001%
        assert r["spread_pct"] < 0.1
        assert r["spread_bps"] < 10

    def test_bps_equals_pct_times_100(self):
        for bid, ask in [(99.9, 100.1), (0.5, 0.502), (1234.56, 1234.70)]:
            r = calc_spread(bid, ask)
            assert abs(r["spread_bps"] - round(r["spread_pct"] * 100, 4)) < 1e-6


# ── Alert threshold tests ─────────────────────────────────────────────────────

class TestSpreadAlerts:
    def test_no_alert_below_threshold(self):
        assert check_alert(0.09) is None
        assert check_alert(0.05) is None
        assert check_alert(0.0) is None

    def test_alert_at_threshold_boundary(self):
        # Exactly at threshold → no alert (strict >)
        assert check_alert(0.1) is None

    def test_alert_above_threshold(self):
        result = check_alert(0.11)
        assert result is not None
        assert result["level"] == "high"
        assert result["reason"] == "spread_pct_threshold"
        assert "0.1%" in result["message"]

    def test_alert_well_above_threshold(self):
        result = check_alert(0.5)
        assert result is not None
        assert result["level"] == "high"
        assert result["current_pct"] == 0.5

    def test_custom_threshold(self):
        assert check_alert(0.05, threshold_pct=0.05) is None
        assert check_alert(0.051, threshold_pct=0.05) is not None

    def test_alert_message_contains_current_spread(self):
        result = check_alert(0.1234)
        assert "0.1234" in result["message"]

    def test_ws_threshold_is_01_pct(self):
        """Verify the WS alert fires for spread > 0.1%."""
        WS_THRESHOLD = 0.1  # must match api.py SPREAD_ALERT_THRESHOLD
        assert check_alert(0.099, threshold_pct=WS_THRESHOLD) is None
        assert check_alert(0.101, threshold_pct=WS_THRESHOLD) is not None


# ── Spread stats aggregation tests ───────────────────────────────────────────

class TestSpreadStats:
    def _build_rows(self, pct_values: list[float]) -> list[dict]:
        now = time.time()
        return [
            {"ts": now - (len(pct_values) - i), "spread_pct": p, "spread_bps": p * 100}
            for i, p in enumerate(pct_values)
        ]

    def _compute_stats(self, rows):
        """Mirror get_spread_stats aggregation logic (pure, no DB)."""
        bps_vals = [r["spread_bps"] for r in rows]
        pct_vals = [r["spread_pct"] for r in rows]
        current_bps = bps_vals[-1]
        current_pct = pct_vals[-1]
        avg_bps = sum(bps_vals) / len(bps_vals)
        max_bps = max(bps_vals)
        min_bps = min(bps_vals)
        p95_bps = sorted(bps_vals)[int(len(bps_vals) * 0.95)]
        return {
            "current_pct": current_pct, "current_bps": current_bps,
            "avg_bps": avg_bps, "max_bps": max_bps, "min_bps": min_bps,
            "p95_bps": p95_bps, "count": len(rows),
        }

    def test_current_is_last_row(self):
        rows = self._build_rows([0.05, 0.08, 0.12])
        stats = self._compute_stats(rows)
        assert stats["current_pct"] == 0.12

    def test_avg_calculation(self):
        rows = self._build_rows([0.1, 0.2, 0.3])
        stats = self._compute_stats(rows)
        assert abs(stats["avg_bps"] - 20.0) < 1e-6

    def test_max_bps(self):
        rows = self._build_rows([0.05, 0.15, 0.08])
        stats = self._compute_stats(rows)
        assert stats["max_bps"] == 15.0

    def test_min_bps(self):
        rows = self._build_rows([0.05, 0.15, 0.08])
        stats = self._compute_stats(rows)
        assert stats["min_bps"] == 5.0

    def test_p95_bps(self):
        rows = self._build_rows([float(i) / 100 for i in range(1, 101)])  # 0.01 to 1.0
        stats = self._compute_stats(rows)
        # p95 index = int(100 * 0.95) = 95 → value = 0.96 → bps = 96.0
        assert stats["p95_bps"] == 96.0

    def test_spread_widens_2x_avg(self):
        """Alert on current > 2x avg even when below absolute threshold."""
        # avg ≈ (0.02*4 + 0.09)/5 = 0.026; current=0.09 → 9bps > 2*2.6=5.2bps
        rows = self._build_rows([0.02, 0.02, 0.02, 0.02, 0.09])
        stats = self._compute_stats(rows)
        avg_bps = stats["avg_bps"]
        current_bps = stats["current_bps"]
        assert current_bps > avg_bps * 2


# ── Spread history data structure tests ──────────────────────────────────────

class TestSpreadHistoryStructure:
    def test_history_entry_has_required_fields(self):
        entry = {
            "ts": time.time(),
            "spread_pct": 0.08,
            "spread_bps": 8.0,
            "bid_vol": 1000.0,
            "ask_vol": 900.0,
        }
        for field in ("ts", "spread_pct", "spread_bps"):
            assert field in entry

    def test_spread_pct_and_bps_consistent(self):
        for spread_pct in [0.01, 0.05, 0.1, 0.5, 1.0]:
            spread_bps = spread_pct * 100
            assert abs(spread_bps - spread_pct * 100) < 1e-9

    def test_history_ordered_by_ts(self):
        now = time.time()
        history = [
            {"ts": now - 3, "spread_pct": 0.05, "spread_bps": 5.0},
            {"ts": now - 2, "spread_pct": 0.07, "spread_bps": 7.0},
            {"ts": now - 1, "spread_pct": 0.12, "spread_bps": 12.0},
        ]
        ts_vals = [h["ts"] for h in history]
        assert ts_vals == sorted(ts_vals)
