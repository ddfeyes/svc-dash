"""
TDD tests for tape speed TPS indicator.

compute_tape_speed_tps(
    trades,           # list[{ts, side}] — unix timestamps + "buy"/"sell"
    reference_ts,     # float "now" (injectable for tests)
)

Returns:
    ts            float  — reference timestamp
    buy_tps_1s    float  — buy trades/second in last 1s
    sell_tps_1s   float  — sell trades/second in last 1s
    buy_tps_5s    float  — buy TPS over last 5s
    sell_tps_5s   float  — sell TPS over last 5s
    buy_tps_30s   float  — buy TPS over last 30s
    sell_tps_30s  float  — sell TPS over last 30s
    ratio         float  — buy/(buy+sell) over 5s; 0.5 when no trades
    speed_label   str    — "slow" | "normal" | "fast" | "blazing"
    buckets       list[{ts, buy_tps, sell_tps, total_tps}]  — 1s buckets, last 60s
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
from metrics import compute_tape_speed_tps  # noqa: E402


BASE = 1_700_000_000.0  # fixed "now" for all tests


def _buy(offset: float) -> dict:
    return {"ts": BASE + offset, "side": "buy"}


def _sell(offset: float) -> dict:
    return {"ts": BASE + offset, "side": "sell"}


def _run(trades, ref=BASE):
    return compute_tape_speed_tps(trades, reference_ts=ref)


# ═══════════════════════════════════════════════════════════════════════════════
# Structure
# ═══════════════════════════════════════════════════════════════════════════════

class TestStructure:
    def test_returns_dict(self):
        assert isinstance(_run([]), dict)

    def test_required_keys(self):
        r = _run([])
        for k in (
            "ts", "buy_tps_1s", "sell_tps_1s",
            "buy_tps_5s", "sell_tps_5s",
            "buy_tps_30s", "sell_tps_30s",
            "ratio", "speed_label", "buckets",
        ):
            assert k in r, f"missing key: {k}"

    def test_empty_gives_zeros(self):
        r = _run([])
        assert r["buy_tps_1s"] == 0.0
        assert r["sell_tps_1s"] == 0.0
        assert r["buy_tps_5s"] == 0.0
        assert r["sell_tps_5s"] == 0.0
        assert r["buy_tps_30s"] == 0.0
        assert r["sell_tps_30s"] == 0.0

    def test_empty_ratio_is_half(self):
        assert _run([])["ratio"] == 0.5

    def test_empty_speed_label_slow(self):
        assert _run([])["speed_label"] == "slow"

    def test_ts_equals_reference(self):
        r = _run([], ref=BASE)
        assert r["ts"] == pytest.approx(BASE)

    def test_buckets_is_list(self):
        assert isinstance(_run([])["buckets"], list)

    def test_bucket_has_required_keys(self):
        r = _run([_buy(-0.5)])
        assert r["buckets"]
        b = r["buckets"][0]
        for k in ("ts", "buy_tps", "sell_tps", "total_tps"):
            assert k in b, f"bucket missing key: {k}"


# ═══════════════════════════════════════════════════════════════════════════════
# TPS windows
# ═══════════════════════════════════════════════════════════════════════════════

class TestTpsWindows:
    def test_buy_tps_1s_single_trade(self):
        # 1 buy trade 0.5s ago → 1.0 buy_tps_1s
        r = _run([_buy(-0.5)])
        assert r["buy_tps_1s"] == pytest.approx(1.0)

    def test_sell_tps_1s_single_trade(self):
        r = _run([_sell(-0.5)])
        assert r["sell_tps_1s"] == pytest.approx(1.0)
        assert r["buy_tps_1s"] == pytest.approx(0.0)

    def test_trade_outside_1s_not_counted(self):
        # trade at -2s, beyond the 1s window
        r = _run([_buy(-2.0)])
        assert r["buy_tps_1s"] == 0.0

    def test_buy_tps_5s_with_five_trades(self):
        # 5 buys spread over last 5s → 5/5 = 1.0 tps
        trades = [_buy(-i * 0.9 - 0.1) for i in range(5)]
        r = _run(trades)
        assert r["buy_tps_5s"] == pytest.approx(1.0)

    def test_sell_tps_5s_separate_from_buy(self):
        buys  = [_buy(-1.0), _buy(-2.0)]
        sells = [_sell(-3.0), _sell(-4.0)]
        r = _run(buys + sells)
        assert r["buy_tps_5s"] == pytest.approx(2 / 5.0)
        assert r["sell_tps_5s"] == pytest.approx(2 / 5.0)

    def test_trade_outside_5s_not_in_5s_window(self):
        r = _run([_buy(-6.0)])
        assert r["buy_tps_5s"] == 0.0

    def test_trade_outside_5s_counted_in_30s(self):
        r = _run([_buy(-10.0)])
        assert r["buy_tps_30s"] == pytest.approx(1 / 30.0, abs=1e-3)

    def test_trade_outside_30s_not_counted(self):
        r = _run([_buy(-40.0)])
        assert r["buy_tps_30s"] == 0.0
        assert r["sell_tps_30s"] == 0.0

    def test_boundary_strictly_excluded(self):
        # trade at exactly -1.0 is on the boundary; strict > so excluded from 1s
        r = _run([_buy(-1.0)])
        assert r["buy_tps_1s"] == 0.0

    def test_mix_buy_sell_30s(self):
        trades = [_buy(-5.0), _buy(-10.0), _sell(-15.0)]
        r = _run(trades)
        assert r["buy_tps_30s"] == pytest.approx(2 / 30.0, abs=1e-3)
        assert r["sell_tps_30s"] == pytest.approx(1 / 30.0, abs=1e-3)


# ═══════════════════════════════════════════════════════════════════════════════
# ratio
# ═══════════════════════════════════════════════════════════════════════════════

class TestRatio:
    def test_all_buys_5s_ratio_one(self):
        r = _run([_buy(-1.0), _buy(-2.0), _buy(-3.0)])
        assert r["ratio"] == pytest.approx(1.0)

    def test_all_sells_5s_ratio_zero(self):
        r = _run([_sell(-1.0), _sell(-2.0)])
        assert r["ratio"] == pytest.approx(0.0)

    def test_equal_buys_sells_ratio_half(self):
        r = _run([_buy(-1.0), _sell(-2.0)])
        assert r["ratio"] == pytest.approx(0.5)

    def test_ratio_uses_5s_window(self):
        # buy inside 5s, sell outside 5s → ratio = 1.0
        r = _run([_buy(-1.0), _sell(-10.0)])
        assert r["ratio"] == pytest.approx(1.0)

    def test_ratio_is_float(self):
        r = _run([_buy(-1.0)])
        assert isinstance(r["ratio"], float)


# ═══════════════════════════════════════════════════════════════════════════════
# speed_label
# ═══════════════════════════════════════════════════════════════════════════════

class TestSpeedLabel:
    def test_slow_when_no_trades(self):
        assert _run([])["speed_label"] == "slow"

    def test_slow_threshold(self):
        # 4 total trades in 5s → 4/5 = 0.8 tps < 1.0 → slow
        trades = [_buy(-1.0), _buy(-2.0), _sell(-3.0), _sell(-4.0)]
        r = _run(trades)
        assert r["speed_label"] == "slow"

    def test_normal_threshold(self):
        # 10 trades in 5s → 2.0 tps → normal
        trades = [_buy(-float(i) * 0.45 - 0.1) for i in range(10)]
        r = _run(trades)
        assert r["speed_label"] == "normal"

    def test_fast_threshold(self):
        # 30 trades in 5s → 6.0 tps → fast
        trades = [_buy(-float(i) * 0.15 - 0.05) for i in range(30)]
        r = _run(trades)
        assert r["speed_label"] == "fast"

    def test_blazing_threshold(self):
        # 80 trades in 5s → 16 tps → blazing
        trades = [_buy(-float(i) * 0.06 - 0.01) for i in range(80)]
        r = _run(trades)
        assert r["speed_label"] == "blazing"

    def test_valid_labels(self):
        for trades in ([], [_buy(-1.0)]):
            assert _run(trades)["speed_label"] in ("slow", "normal", "fast", "blazing")


# ═══════════════════════════════════════════════════════════════════════════════
# buckets
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuckets:
    def test_buckets_cover_60s(self):
        r = _run([_buy(-30.0)])
        tss = [b["ts"] for b in r["buckets"]]
        assert min(tss) <= BASE - 59
        assert max(tss) <= BASE

    def test_buckets_sorted_ascending(self):
        r = _run([_buy(-10.0), _buy(-50.0)])
        tss = [b["ts"] for b in r["buckets"]]
        assert tss == sorted(tss)

    def test_bucket_counts_correct(self):
        # One buy 0.5s ago — exactly one buy trade should appear across all buckets
        r = _run([_buy(-0.5)])
        total_buy  = sum(b["buy_tps"]   for b in r["buckets"])
        total_sell = sum(b["sell_tps"]  for b in r["buckets"])
        total_all  = sum(b["total_tps"] for b in r["buckets"])
        assert total_buy  == pytest.approx(1.0)
        assert total_sell == pytest.approx(0.0)
        assert total_all  == pytest.approx(1.0)

    def test_old_trades_excluded_from_buckets(self):
        # trade 100s ago should not appear in 60s buckets
        r = _run([_buy(-100.0)])
        for b in r["buckets"]:
            assert b["total_tps"] == 0.0

    def test_bucket_values_non_negative(self):
        r = _run([_buy(-5.0), _sell(-10.0)])
        for b in r["buckets"]:
            assert b["buy_tps"] >= 0.0
            assert b["sell_tps"] >= 0.0
            assert b["total_tps"] >= 0.0

    def test_empty_trades_all_zero_buckets(self):
        r = _run([])
        for b in r["buckets"]:
            assert b["total_tps"] == 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# API route
# ═══════════════════════════════════════════════════════════════════════════════

class TestApiRoute:
    def test_route_registered(self):
        import importlib
        import sys as _sys
        # Ensure api module path is available
        backend_path = os.path.join(os.path.dirname(__file__), "..", "backend")
        if backend_path not in _sys.path:
            _sys.path.insert(0, backend_path)
        import api
        routes = [r.path for r in api.router.routes]
        assert "/api/tape-speed-tps" in routes, f"route missing; routes={routes}"
