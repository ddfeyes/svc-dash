"""
TDD tests for session statistics.

Spec:
  compute_session_stats(trades, session_start=None)

  trades: [{ts, price, qty, side}]  — may be unsorted
  session_start: float|None  — Unix timestamp; if None, use floor(latest_ts / 86400) * 86400
                               (start of the UTC day of the latest trade)

  Computes statistics over trades with ts >= session_start.

  Returns:
    total_volume_usd:    float   sum(price * qty) for session trades
    total_qty:           float   sum(qty)
    trade_count:         int     number of trades
    avg_trade_size_usd:  float   total_volume_usd / trade_count (0 if no trades)
    max_trade_usd:       float   max single price*qty (0 if no trades)
    max_trade_price:     float   price of the largest trade by USD (0 if no trades)
    buy_volume_usd:      float   sum(price*qty) where side=='buy'
    sell_volume_usd:     float   sum(price*qty) where side=='sell'
    buy_qty:             float   sum(qty) where side=='buy'
    sell_qty:            float   sum(qty) where side=='sell'
    buy_sell_ratio:      float   buy_volume_usd / (buy_volume_usd + sell_volume_usd)
                                 0.5 if both zero
    buy_count:           int     number of buy trades
    sell_count:          int     number of sell trades
    session_start:       float   the session_start actually used
    first_trade_ts:      float|None  ts of earliest session trade
    last_trade_ts:       float|None  ts of latest session trade
    vwap:                float   sum(price*qty) / sum(qty)  (0 if no trades)
    price_high:          float   max price in session (0 if no trades)
    price_low:           float   min price in session (0 if no trades; use inf guard)
"""
import sys, os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
from metrics import compute_session_stats  # noqa: E402


# ── helpers ────────────────────────────────────────────────────────────────────

DAY = 86400

def _t(ts, price, qty=1.0, side="buy"):
    return {"ts": float(ts), "price": float(price), "qty": float(qty), "side": side}

def _buy(ts, price=100.0, qty=1.0):
    return _t(ts, price, qty, "buy")

def _sell(ts, price=100.0, qty=1.0):
    return _t(ts, price, qty, "sell")


# ═══════════════════════════════════════════════════════════════════════════════
# Structure
# ═══════════════════════════════════════════════════════════════════════════════

class TestStructure:
    def test_empty_returns_dict(self):
        r = compute_session_stats([], session_start=0.0)
        assert isinstance(r, dict)

    def test_required_fields_present(self):
        r = compute_session_stats([], session_start=0.0)
        for f in (
            "total_volume_usd", "total_qty", "trade_count",
            "avg_trade_size_usd", "max_trade_usd", "max_trade_price",
            "buy_volume_usd", "sell_volume_usd", "buy_qty", "sell_qty",
            "buy_sell_ratio", "buy_count", "sell_count",
            "session_start", "first_trade_ts", "last_trade_ts",
            "vwap", "price_high", "price_low",
        ):
            assert f in r, f"missing: {f}"

    def test_empty_zero_state(self):
        r = compute_session_stats([], session_start=0.0)
        assert r["trade_count"]        == 0
        assert r["total_volume_usd"]   == pytest.approx(0.0)
        assert r["total_qty"]          == pytest.approx(0.0)
        assert r["avg_trade_size_usd"] == pytest.approx(0.0)
        assert r["max_trade_usd"]      == pytest.approx(0.0)
        assert r["buy_sell_ratio"]     == pytest.approx(0.5)
        assert r["first_trade_ts"]     is None
        assert r["last_trade_ts"]      is None
        assert r["vwap"]               == pytest.approx(0.0)
        assert r["price_high"]         == pytest.approx(0.0)
        assert r["price_low"]          == pytest.approx(0.0)

    def test_session_start_echoed(self):
        r = compute_session_stats([], session_start=12345.0)
        assert r["session_start"] == pytest.approx(12345.0)


# ═══════════════════════════════════════════════════════════════════════════════
# Session start filtering
# ═══════════════════════════════════════════════════════════════════════════════

class TestSessionFiltering:
    def test_trades_before_session_excluded(self):
        trades = [_buy(ts=50, price=100.0), _buy(ts=200, price=100.0)]
        r = compute_session_stats(trades, session_start=100.0)
        assert r["trade_count"] == 1

    def test_trades_at_session_start_included(self):
        trades = [_buy(ts=100, price=100.0), _buy(ts=200, price=100.0)]
        r = compute_session_stats(trades, session_start=100.0)
        assert r["trade_count"] == 2

    def test_all_trades_before_session_gives_zero(self):
        trades = [_buy(ts=10), _buy(ts=20)]
        r = compute_session_stats(trades, session_start=100.0)
        assert r["trade_count"] == 0
        assert r["total_volume_usd"] == pytest.approx(0.0)

    def test_session_start_none_uses_utc_day_of_latest_trade(self):
        """session_start=None → floor(max_ts / 86400) * 86400."""
        # Trade 1 at ts=1000 (day 0), trade 2 at ts=86401 (day 1)
        trades = [_buy(ts=1000.0), _buy(ts=86401.0)]
        r = compute_session_stats(trades, session_start=None)
        # latest ts = 86401 → day start = floor(86401/86400)*86400 = 86400
        assert r["session_start"] == pytest.approx(86400.0)
        assert r["trade_count"] == 1   # only the ts=86401 trade

    def test_session_start_none_empty_uses_zero(self):
        r = compute_session_stats([], session_start=None)
        assert r["session_start"] == pytest.approx(0.0)

    def test_unsorted_input_handled(self):
        trades = [_buy(ts=200), _buy(ts=50), _buy(ts=150)]
        r = compute_session_stats(trades, session_start=100.0)
        assert r["trade_count"] == 2   # ts=200 and ts=150


# ═══════════════════════════════════════════════════════════════════════════════
# Volume and trade size
# ═══════════════════════════════════════════════════════════════════════════════

class TestVolumeStats:
    def test_total_volume_usd(self):
        # 100 * 2 + 200 * 1 = 400
        trades = [_buy(1, price=100.0, qty=2.0), _buy(2, price=200.0, qty=1.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["total_volume_usd"] == pytest.approx(400.0)

    def test_total_qty(self):
        trades = [_buy(1, qty=2.0), _buy(2, qty=3.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["total_qty"] == pytest.approx(5.0)

    def test_trade_count(self):
        trades = [_buy(i) for i in range(7)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["trade_count"] == 7

    def test_avg_trade_size_usd(self):
        # total_usd=300, count=3 → avg=100
        trades = [_buy(i, price=100.0, qty=1.0) for i in range(3)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["avg_trade_size_usd"] == pytest.approx(100.0)

    def test_avg_trade_size_usd_zero_when_no_trades(self):
        r = compute_session_stats([], session_start=0.0)
        assert r["avg_trade_size_usd"] == pytest.approx(0.0)

    def test_max_trade_usd(self):
        trades = [_buy(0, price=100.0, qty=1.0),
                  _buy(1, price=500.0, qty=2.0),   # 1000 USD — max
                  _buy(2, price=200.0, qty=3.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["max_trade_usd"] == pytest.approx(1000.0)

    def test_max_trade_price(self):
        """max_trade_price is the price of the single largest USD trade."""
        trades = [_buy(0, price=100.0, qty=1.0),
                  _buy(1, price=500.0, qty=2.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["max_trade_price"] == pytest.approx(500.0)

    def test_max_trade_zero_when_no_trades(self):
        r = compute_session_stats([], session_start=0.0)
        assert r["max_trade_usd"]   == pytest.approx(0.0)
        assert r["max_trade_price"] == pytest.approx(0.0)

    def test_single_trade(self):
        r = compute_session_stats([_buy(0, price=250.0, qty=4.0)], session_start=0.0)
        assert r["total_volume_usd"]   == pytest.approx(1000.0)
        assert r["max_trade_usd"]      == pytest.approx(1000.0)
        assert r["avg_trade_size_usd"] == pytest.approx(1000.0)
        assert r["trade_count"]        == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Buy / sell split
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuySell:
    def test_buy_volume_usd(self):
        trades = [_buy(0, price=100.0, qty=3.0), _sell(1, price=100.0, qty=1.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["buy_volume_usd"] == pytest.approx(300.0)

    def test_sell_volume_usd(self):
        trades = [_buy(0, price=100.0, qty=3.0), _sell(1, price=100.0, qty=1.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["sell_volume_usd"] == pytest.approx(100.0)

    def test_buy_qty(self):
        trades = [_buy(0, qty=2.0), _sell(1, qty=5.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["buy_qty"] == pytest.approx(2.0)

    def test_sell_qty(self):
        trades = [_buy(0, qty=2.0), _sell(1, qty=5.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["sell_qty"] == pytest.approx(5.0)

    def test_buy_count(self):
        trades = [_buy(i) for i in range(4)] + [_sell(10 + i) for i in range(3)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["buy_count"]  == 4
        assert r["sell_count"] == 3

    def test_buy_sell_ratio_fifty_fifty(self):
        trades = [_buy(0, qty=1.0), _sell(1, qty=1.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["buy_sell_ratio"] == pytest.approx(0.5)

    def test_buy_sell_ratio_all_buys(self):
        trades = [_buy(i) for i in range(5)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["buy_sell_ratio"] == pytest.approx(1.0)

    def test_buy_sell_ratio_all_sells(self):
        trades = [_sell(i) for i in range(5)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["buy_sell_ratio"] == pytest.approx(0.0)

    def test_buy_sell_ratio_default_when_no_trades(self):
        r = compute_session_stats([], session_start=0.0)
        assert r["buy_sell_ratio"] == pytest.approx(0.5)

    def test_buy_plus_sell_vol_equals_total(self):
        trades = [_buy(0, price=100.0, qty=3.0), _sell(1, price=200.0, qty=2.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["buy_volume_usd"] + r["sell_volume_usd"] == pytest.approx(r["total_volume_usd"])

    def test_buy_plus_sell_qty_equals_total(self):
        trades = [_buy(0, qty=3.0), _sell(1, qty=2.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["buy_qty"] + r["sell_qty"] == pytest.approx(r["total_qty"])


# ═══════════════════════════════════════════════════════════════════════════════
# Timestamps
# ═══════════════════════════════════════════════════════════════════════════════

class TestTimestamps:
    def test_first_trade_ts(self):
        trades = [_buy(ts=300), _buy(ts=100), _buy(ts=200)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["first_trade_ts"] == pytest.approx(100.0)

    def test_last_trade_ts(self):
        trades = [_buy(ts=300), _buy(ts=100), _buy(ts=200)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["last_trade_ts"] == pytest.approx(300.0)

    def test_first_last_same_when_single_trade(self):
        r = compute_session_stats([_buy(ts=42)], session_start=0.0)
        assert r["first_trade_ts"] == pytest.approx(42.0)
        assert r["last_trade_ts"]  == pytest.approx(42.0)

    def test_first_last_none_when_no_trades(self):
        r = compute_session_stats([], session_start=0.0)
        assert r["first_trade_ts"] is None
        assert r["last_trade_ts"]  is None


# ═══════════════════════════════════════════════════════════════════════════════
# VWAP and price range
# ═══════════════════════════════════════════════════════════════════════════════

class TestVwapAndRange:
    def test_vwap_formula(self):
        # (100*2 + 200*1) / 3 = 400/3 ≈ 133.33
        trades = [_buy(0, price=100.0, qty=2.0), _buy(1, price=200.0, qty=1.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["vwap"] == pytest.approx(400.0 / 3.0)

    def test_vwap_uniform_price(self):
        trades = [_buy(i, price=150.0, qty=1.0) for i in range(5)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["vwap"] == pytest.approx(150.0)

    def test_vwap_zero_when_no_trades(self):
        r = compute_session_stats([], session_start=0.0)
        assert r["vwap"] == pytest.approx(0.0)

    def test_price_high(self):
        trades = [_buy(0, price=100.0), _buy(1, price=150.0), _buy(2, price=80.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["price_high"] == pytest.approx(150.0)

    def test_price_low(self):
        trades = [_buy(0, price=100.0), _buy(1, price=150.0), _buy(2, price=80.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["price_low"] == pytest.approx(80.0)

    def test_price_high_low_same_single_trade(self):
        r = compute_session_stats([_buy(0, price=123.0)], session_start=0.0)
        assert r["price_high"] == pytest.approx(123.0)
        assert r["price_low"]  == pytest.approx(123.0)

    def test_price_high_low_zero_when_no_trades(self):
        r = compute_session_stats([], session_start=0.0)
        assert r["price_high"] == pytest.approx(0.0)
        assert r["price_low"]  == pytest.approx(0.0)

    def test_vwap_with_mixed_sides(self):
        """VWAP uses all trades regardless of side."""
        trades = [_buy(0, price=100.0, qty=1.0), _sell(1, price=200.0, qty=1.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["vwap"] == pytest.approx(150.0)


# ═══════════════════════════════════════════════════════════════════════════════
# Edge cases
# ═══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    def test_large_dataset(self):
        import random; random.seed(99)
        trades = [
            _t(i, price=100 + random.gauss(0, 5),
               qty=random.uniform(0.1, 10.0),
               side=random.choice(["buy", "sell"]))
            for i in range(1000)
        ]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["trade_count"] == 1000
        assert r["total_volume_usd"] > 0

    def test_all_same_price(self):
        trades = [_buy(i, price=50.0, qty=2.0) for i in range(10)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["price_high"] == pytest.approx(50.0)
        assert r["price_low"]  == pytest.approx(50.0)
        assert r["vwap"]       == pytest.approx(50.0)

    def test_buy_sell_ratio_weighted_by_usd_not_count(self):
        """Ratio is by USD volume, not trade count."""
        # 1 buy of $900, 9 sells of $100 each → buy_usd=900, sell_usd=900 → ratio=0.5
        trades = [_buy(0, price=900.0, qty=1.0)] + [_sell(i+1, price=100.0, qty=1.0) for i in range(9)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["buy_sell_ratio"] == pytest.approx(0.5)

    def test_session_start_exactly_filters_boundary(self):
        """Trade at exactly session_start IS included."""
        t = _buy(ts=1000.0, price=100.0, qty=1.0)
        r = compute_session_stats([t], session_start=1000.0)
        assert r["trade_count"] == 1

    def test_zero_qty_trade_not_crash(self):
        """A trade with qty=0 doesn't crash (zero-volume trade)."""
        trades = [_buy(0, price=100.0, qty=0.0), _buy(1, price=100.0, qty=1.0)]
        r = compute_session_stats(trades, session_start=0.0)
        assert r["trade_count"] == 2
        assert r["total_qty"] == pytest.approx(1.0)
