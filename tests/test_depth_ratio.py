"""
TDD tests for top-of-book depth ratio tracker.

Spec:
  - depth_ratio = sum(bid qty at best N levels) / sum(ask qty at best N levels)
  - default N = 5
  - ratio > 1.0 → bid-heavy (buyers stacking), < 1.0 → ask-heavy
  - rolling 10-minute series from orderbook_snapshots
"""
import json
import sys
import os
import time
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import compute_depth_ratio, compute_depth_ratio_series  # noqa: E402


# ── Helper builders ───────────────────────────────────────────────────────────

def _snapshot(ts, bids, asks, bid_volume=None, ask_volume=None):
    """Build a minimal orderbook snapshot dict (mirrors DB row)."""
    bids_j = json.dumps(bids) if isinstance(bids, list) else bids
    asks_j = json.dumps(asks) if isinstance(asks, list) else asks
    return {
        "ts": float(ts),
        "bids": bids_j,
        "asks": asks_j,
        "bid_volume": bid_volume,
        "ask_volume": ask_volume,
    }


def _levels(base_price, direction, n=10, qty=1.0):
    """Generate n price levels. direction='bid' → descending, 'ask' → ascending."""
    if direction == "bid":
        return [[base_price - i * 0.01, qty] for i in range(n)]
    return [[base_price + i * 0.01, qty] for i in range(n)]


# ── compute_depth_ratio: pure function ───────────────────────────────────────

class TestComputeDepthRatio:
    def test_equal_depth_gives_ratio_one(self):
        snap = _snapshot(
            ts=1000,
            bids=_levels(100.0, "bid", n=5, qty=10.0),
            asks=_levels(100.1, "ask", n=5, qty=10.0),
        )
        ratio = compute_depth_ratio(snap, levels=5)
        assert ratio == pytest.approx(1.0)

    def test_double_bid_depth_gives_ratio_two(self):
        snap = _snapshot(
            ts=1000,
            bids=_levels(100.0, "bid", n=5, qty=20.0),
            asks=_levels(100.1, "ask", n=5, qty=10.0),
        )
        ratio = compute_depth_ratio(snap, levels=5)
        assert ratio == pytest.approx(2.0)

    def test_half_bid_depth_gives_ratio_half(self):
        snap = _snapshot(
            ts=1000,
            bids=_levels(100.0, "bid", n=5, qty=5.0),
            asks=_levels(100.1, "ask", n=5, qty=10.0),
        )
        ratio = compute_depth_ratio(snap, levels=5)
        assert ratio == pytest.approx(0.5)

    def test_only_top_n_levels_used(self):
        """Levels 6-10 must not contribute to the ratio."""
        # First 5 bids: qty=10, rest: qty=1000 → should not matter
        bids = [[100.0 - i * 0.01, 10.0 if i < 5 else 1000.0] for i in range(10)]
        asks = [[100.1 + i * 0.01, 10.0] for i in range(10)]
        snap = _snapshot(ts=1000, bids=bids, asks=asks)
        ratio = compute_depth_ratio(snap, levels=5)
        # bid5 = 5*10 = 50, ask5 = 5*10 = 50 → 1.0
        assert ratio == pytest.approx(1.0)

    def test_levels_clamped_when_fewer_available(self):
        """Only 3 bid levels available, levels=5 → uses 3."""
        bids = [[100.0 - i * 0.01, 10.0] for i in range(3)]
        asks = [[100.1 + i * 0.01, 10.0] for i in range(5)]
        snap = _snapshot(ts=1000, bids=bids, asks=asks)
        ratio = compute_depth_ratio(snap, levels=5)
        # bid3 = 30, ask5 = 50 → 0.6
        assert ratio == pytest.approx(0.6)

    def test_zero_ask_depth_returns_none(self):
        """Avoid division by zero when asks are empty."""
        bids = [[100.0, 10.0]]
        asks = []
        snap = _snapshot(ts=1000, bids=bids, asks=asks)
        result = compute_depth_ratio(snap, levels=5)
        assert result is None

    def test_zero_bid_depth_returns_zero(self):
        """No bids → ratio is 0."""
        bids = []
        asks = [[100.1, 10.0]] * 5
        snap = _snapshot(ts=1000, bids=bids, asks=asks)
        ratio = compute_depth_ratio(snap, levels=5)
        assert ratio == pytest.approx(0.0)

    def test_handles_string_json_bids_asks(self):
        """DB stores bids/asks as JSON strings."""
        bids = json.dumps([[100.0, 5.0], [99.9, 3.0]])
        asks = json.dumps([[100.1, 4.0], [100.2, 2.0]])
        snap = {"ts": 1000.0, "bids": bids, "asks": asks}
        ratio = compute_depth_ratio(snap, levels=5)
        # bid2 = 8, ask2 = 6 → 8/6 ≈ 1.333
        assert ratio == pytest.approx(8.0 / 6.0)

    def test_handles_list_bids_asks(self):
        """Already-parsed list is also accepted."""
        bids = [[100.0, 5.0], [99.9, 3.0]]
        asks = [[100.1, 4.0], [100.2, 2.0]]
        snap = {"ts": 1000.0, "bids": bids, "asks": asks}
        ratio = compute_depth_ratio(snap, levels=5)
        assert ratio == pytest.approx(8.0 / 6.0)

    def test_returns_float_not_none_for_valid_data(self):
        snap = _snapshot(1000, _levels(100, "bid"), _levels(100.1, "ask"))
        result = compute_depth_ratio(snap)
        assert isinstance(result, float)

    def test_default_levels_is_5(self):
        """Calling without levels= uses 5 levels by default."""
        bids = [[100.0 - i * 0.01, 1.0] for i in range(10)]
        asks = [[100.1 + i * 0.01, 2.0] for i in range(10)]
        snap = _snapshot(1000, bids, asks)
        ratio_default = compute_depth_ratio(snap)
        ratio_5 = compute_depth_ratio(snap, levels=5)
        assert ratio_default == ratio_5

    def test_levels_1_uses_only_best_bid_ask(self):
        bids = [[100.0, 7.0], [99.9, 100.0]]  # best bid qty=7, rest should not count
        asks = [[100.1, 3.0], [100.2, 100.0]]
        snap = _snapshot(1000, bids, asks)
        ratio = compute_depth_ratio(snap, levels=1)
        assert ratio == pytest.approx(7.0 / 3.0)

    def test_bids_must_be_sorted_descending(self):
        """Best bid = highest price. Verify first entry is highest."""
        bids = [[100.0, 10.0], [99.9, 1.0], [99.8, 1.0], [99.7, 1.0], [99.6, 1.0]]
        asks = [[100.1, 5.0]] * 5
        snap = _snapshot(1000, bids, asks)
        # bid5 = 10+1+1+1+1 = 14, ask5 = 25 → ratio = 14/25 = 0.56
        ratio = compute_depth_ratio(snap, levels=5)
        assert ratio == pytest.approx(14.0 / 25.0)


# ── compute_depth_ratio_series: pure function ────────────────────────────────

class TestComputeDepthRatioSeries:
    def test_empty_snapshots_returns_empty(self):
        result = compute_depth_ratio_series([], levels=5)
        assert result == []

    def test_single_snapshot_returns_one_point(self):
        snap = _snapshot(1000, _levels(100, "bid"), _levels(100.1, "ask"))
        result = compute_depth_ratio_series([snap])
        assert len(result) == 1

    def test_series_point_has_required_fields(self):
        snap = _snapshot(1000, _levels(100, "bid"), _levels(100.1, "ask"))
        result = compute_depth_ratio_series([snap])
        pt = result[0]
        assert "ts" in pt
        assert "ratio" in pt

    def test_ts_matches_snapshot_ts(self):
        snap = _snapshot(ts=12345.0, bids=_levels(100, "bid"), asks=_levels(100.1, "ask"))
        result = compute_depth_ratio_series([snap])
        assert result[0]["ts"] == 12345.0

    def test_ratio_values_match_per_snapshot(self):
        snaps = [
            _snapshot(1000, _levels(100, "bid", qty=10.0), _levels(100.1, "ask", qty=10.0)),  # 1.0
            _snapshot(2000, _levels(100, "bid", qty=20.0), _levels(100.1, "ask", qty=10.0)),  # 2.0
        ]
        result = compute_depth_ratio_series(snaps)
        assert result[0]["ratio"] == pytest.approx(1.0)
        assert result[1]["ratio"] == pytest.approx(2.0)

    def test_snapshots_with_none_ratio_excluded(self):
        """Snapshots with empty asks produce None ratio → excluded from series."""
        snaps = [
            _snapshot(1000, _levels(100, "bid"), _levels(100.1, "ask")),  # valid
            _snapshot(2000, [[100.0, 5.0]], []),  # asks empty → None → skip
            _snapshot(3000, _levels(100, "bid"), _levels(100.1, "ask")),  # valid
        ]
        result = compute_depth_ratio_series(snaps)
        assert len(result) == 2
        ts_values = [r["ts"] for r in result]
        assert 2000.0 not in ts_values

    def test_series_ordered_by_ts(self):
        snaps = [
            _snapshot(3000, _levels(100, "bid"), _levels(100.1, "ask")),
            _snapshot(1000, _levels(100, "bid"), _levels(100.1, "ask")),
            _snapshot(2000, _levels(100, "bid"), _levels(100.1, "ask")),
        ]
        result = compute_depth_ratio_series(snaps, levels=5)
        ts_vals = [r["ts"] for r in result]
        assert ts_vals == sorted(ts_vals)

    def test_levels_parameter_passed_through(self):
        """levels=1 vs levels=5 should produce different ratios when levels matter."""
        bids = [[100.0, 100.0]] + [[99.0 - i, 1.0] for i in range(9)]
        asks = [[100.1 + i, 1.0] for i in range(10)]
        snap = _snapshot(1000, bids, asks)
        r1 = compute_depth_ratio_series([snap], levels=1)[0]["ratio"]
        r5 = compute_depth_ratio_series([snap], levels=5)[0]["ratio"]
        # levels=1: 100/1 = 100; levels=5: (100+1+1+1+1)/5 = 104/5
        assert r1 != r5


# ── Edge cases ────────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_malformed_json_bids_returns_none(self):
        snap = {"ts": 1000.0, "bids": "not-json", "asks": "[[100.1, 1.0]]"}
        result = compute_depth_ratio(snap, levels=5)
        assert result is None

    def test_malformed_json_asks_returns_none(self):
        snap = {"ts": 1000.0, "bids": "[[100.0, 1.0]]", "asks": "bad"}
        result = compute_depth_ratio(snap, levels=5)
        assert result is None

    def test_none_bids_returns_none(self):
        snap = {"ts": 1000.0, "bids": None, "asks": "[[100.1, 1.0]]"}
        result = compute_depth_ratio(snap, levels=5)
        assert result is None

    def test_none_asks_returns_none(self):
        snap = {"ts": 1000.0, "bids": "[[100.0, 1.0]]", "asks": None}
        result = compute_depth_ratio(snap, levels=5)
        assert result is None

    def test_qty_as_string_still_works(self):
        """Binance sends prices/qtys as strings in the WS feed."""
        bids = [["100.00", "5.0"], ["99.99", "3.0"]]
        asks = [["100.10", "4.0"], ["100.20", "2.0"]]
        snap = _snapshot(1000, bids, asks)
        ratio = compute_depth_ratio(snap, levels=5)
        assert ratio == pytest.approx(8.0 / 6.0)
