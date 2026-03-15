"""
TDD tests for Aggressor Imbalance Streak Counter.

Pure function under test:

compute_aggressor_imbalance_streak(
    trades,                    # [{ts, price, qty, side}]
    bucket_size=60,            # seconds per candle (default: 1m)
    threshold_pct=70.0,        # buy% or sell% threshold for imbalance
    alert_streak=3,            # consecutive candles to trigger alert
)
    - Buckets trades into time windows of bucket_size seconds
    - For each bucket: buy_pct = buy_count / total_count * 100
    - Candle direction: "buy" if buy_pct >= threshold_pct
                        "sell" if buy_pct <= (100 - threshold_pct)
                        None otherwise (balanced)
    - Streak: count consecutive trailing candles with the same non-None direction
    - Returns:
        candles:           [{ts, buy_pct, sell_pct, total, direction}]
        streak:            int   — current streak length (0 if no imbalance candle at end)
        streak_direction:  str | None  — "buy" | "sell" | None
        alert:             bool  — streak >= alert_streak
        alert_streak:      int   — the configured threshold (default 3)
        description:       str   — human-readable summary
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import compute_aggressor_imbalance_streak  # noqa: E402


# ── helpers ────────────────────────────────────────────────────────────────────

def _trade(ts, side="buy", qty=1.0, price=100.0):
    return {"ts": float(ts), "price": float(price), "qty": float(qty), "side": side}


def _buy(ts, qty=1.0):
    return _trade(ts, side="buy", qty=qty)


def _sell(ts, qty=1.0):
    return _trade(ts, side="sell", qty=qty)


def _candle_trades(ts_start, buy_count, sell_count, bucket=60):
    """Fill a bucket starting at ts_start with buy_count buys and sell_count sells."""
    trades = []
    for i in range(buy_count):
        trades.append(_buy(ts=ts_start + i * 0.1))
    for i in range(sell_count):
        trades.append(_sell(ts=ts_start + buy_count * 0.1 + i * 0.1))
    return trades


# ═══════════════════════════════════════════════════════════════════════════════
# Structure
# ═══════════════════════════════════════════════════════════════════════════════

class TestStructure:
    def test_empty_returns_valid_dict(self):
        r = compute_aggressor_imbalance_streak([])
        assert isinstance(r, dict)

    def test_required_fields_present(self):
        r = compute_aggressor_imbalance_streak([])
        for f in ("candles", "streak", "streak_direction", "alert", "alert_streak", "description"):
            assert f in r, f"missing field: {f}"

    def test_empty_gives_zero_streak(self):
        r = compute_aggressor_imbalance_streak([])
        assert r["streak"] == 0
        assert r["streak_direction"] is None
        assert r["alert"] is False
        assert r["candles"] == []

    def test_alert_streak_field_reflects_parameter(self):
        r = compute_aggressor_imbalance_streak([], alert_streak=5)
        assert r["alert_streak"] == 5

    def test_description_is_string(self):
        r = compute_aggressor_imbalance_streak([])
        assert isinstance(r["description"], str)

    def test_candle_has_required_fields(self):
        trades = _candle_trades(0, buy_count=8, sell_count=2)
        r = compute_aggressor_imbalance_streak(trades)
        assert len(r["candles"]) == 1
        c = r["candles"][0]
        for f in ("ts", "buy_pct", "sell_pct", "total", "direction"):
            assert f in c, f"missing candle field: {f}"

    def test_candle_buy_pct_sell_pct_sum_to_100(self):
        trades = _candle_trades(0, buy_count=7, sell_count=3)
        r = compute_aggressor_imbalance_streak(trades)
        c = r["candles"][0]
        assert abs(c["buy_pct"] + c["sell_pct"] - 100.0) < 0.01

    def test_candle_total_reflects_trade_count(self):
        trades = _candle_trades(0, buy_count=6, sell_count=4)
        r = compute_aggressor_imbalance_streak(trades)
        assert r["candles"][0]["total"] == 10


# ═══════════════════════════════════════════════════════════════════════════════
# Bucketing
# ═══════════════════════════════════════════════════════════════════════════════

class TestBucketing:
    def test_single_minute_one_candle(self):
        trades = [_buy(0), _buy(30), _sell(59)]
        r = compute_aggressor_imbalance_streak(trades, bucket_size=60)
        assert len(r["candles"]) == 1

    def test_two_minutes_two_candles(self):
        trades = [_buy(0), _buy(60)]
        r = compute_aggressor_imbalance_streak(trades, bucket_size=60)
        assert len(r["candles"]) == 2

    def test_bucket_ts_is_floor_of_window(self):
        r = compute_aggressor_imbalance_streak([_buy(45)], bucket_size=60)
        assert r["candles"][0]["ts"] == 0.0

    def test_candles_sorted_ascending_by_ts(self):
        trades = [_buy(120), _buy(0), _buy(60)]
        r = compute_aggressor_imbalance_streak(trades, bucket_size=60)
        ts_vals = [c["ts"] for c in r["candles"]]
        assert ts_vals == sorted(ts_vals)


# ═══════════════════════════════════════════════════════════════════════════════
# Direction classification
# ═══════════════════════════════════════════════════════════════════════════════

class TestDirectionClassification:
    def test_buy_direction_when_buy_pct_above_threshold(self):
        """8 buys, 2 sells → 80% buy → direction=buy."""
        trades = _candle_trades(0, buy_count=8, sell_count=2)
        r = compute_aggressor_imbalance_streak(trades, threshold_pct=70.0)
        assert r["candles"][0]["direction"] == "buy"

    def test_sell_direction_when_sell_pct_above_threshold(self):
        """2 buys, 8 sells → 20% buy → direction=sell."""
        trades = _candle_trades(0, buy_count=2, sell_count=8)
        r = compute_aggressor_imbalance_streak(trades, threshold_pct=70.0)
        assert r["candles"][0]["direction"] == "sell"

    def test_no_direction_when_balanced(self):
        """5 buys, 5 sells → 50% buy → direction=None."""
        trades = _candle_trades(0, buy_count=5, sell_count=5)
        r = compute_aggressor_imbalance_streak(trades, threshold_pct=70.0)
        assert r["candles"][0]["direction"] is None

    def test_exactly_at_threshold_is_buy(self):
        """7 buys, 3 sells → exactly 70% → direction=buy."""
        trades = _candle_trades(0, buy_count=7, sell_count=3)
        r = compute_aggressor_imbalance_streak(trades, threshold_pct=70.0)
        assert r["candles"][0]["direction"] == "buy"

    def test_exactly_at_sell_threshold_is_sell(self):
        """3 buys, 7 sells → exactly 30% buy → direction=sell."""
        trades = _candle_trades(0, buy_count=3, sell_count=7)
        r = compute_aggressor_imbalance_streak(trades, threshold_pct=70.0)
        assert r["candles"][0]["direction"] == "sell"

    def test_custom_threshold(self):
        """6 buys, 4 sells → 60% buy, threshold=60 → direction=buy."""
        trades = _candle_trades(0, buy_count=6, sell_count=4)
        r = compute_aggressor_imbalance_streak(trades, threshold_pct=60.0)
        assert r["candles"][0]["direction"] == "buy"

    def test_buy_pct_computed_correctly(self):
        trades = _candle_trades(0, buy_count=8, sell_count=2)
        r = compute_aggressor_imbalance_streak(trades)
        assert r["candles"][0]["buy_pct"] == pytest.approx(80.0)

    def test_sell_pct_computed_correctly(self):
        trades = _candle_trades(0, buy_count=8, sell_count=2)
        r = compute_aggressor_imbalance_streak(trades)
        assert r["candles"][0]["sell_pct"] == pytest.approx(20.0)


# ═══════════════════════════════════════════════════════════════════════════════
# Streak counting
# ═══════════════════════════════════════════════════════════════════════════════

class TestStreakCounting:
    def test_single_imbalanced_candle_streak_one(self):
        trades = _candle_trades(0, buy_count=8, sell_count=2)
        r = compute_aggressor_imbalance_streak(trades)
        assert r["streak"] == 1
        assert r["streak_direction"] == "buy"

    def test_two_consecutive_buy_candles_streak_two(self):
        trades = (
            _candle_trades(0, buy_count=8, sell_count=2) +   # candle 0: buy
            _candle_trades(60, buy_count=9, sell_count=1)    # candle 1: buy
        )
        r = compute_aggressor_imbalance_streak(trades)
        assert r["streak"] == 2
        assert r["streak_direction"] == "buy"

    def test_three_consecutive_sell_candles_streak_three(self):
        trades = (
            _candle_trades(0,   buy_count=2, sell_count=8) +   # sell
            _candle_trades(60,  buy_count=1, sell_count=9) +   # sell
            _candle_trades(120, buy_count=2, sell_count=8)     # sell
        )
        r = compute_aggressor_imbalance_streak(trades)
        assert r["streak"] == 3
        assert r["streak_direction"] == "sell"

    def test_streak_resets_on_balanced_candle(self):
        """buy, balanced, buy → streak=1 (not 2)."""
        trades = (
            _candle_trades(0,   buy_count=8, sell_count=2) +   # buy
            _candle_trades(60,  buy_count=5, sell_count=5) +   # balanced → resets
            _candle_trades(120, buy_count=8, sell_count=2)     # buy → streak=1
        )
        r = compute_aggressor_imbalance_streak(trades)
        assert r["streak"] == 1
        assert r["streak_direction"] == "buy"

    def test_streak_resets_on_direction_change(self):
        """buy, sell → streak=1 sell (previous buy streak broken)."""
        trades = (
            _candle_trades(0,  buy_count=8, sell_count=2) +   # buy
            _candle_trades(60, buy_count=2, sell_count=8)     # sell → new streak=1
        )
        r = compute_aggressor_imbalance_streak(trades)
        assert r["streak"] == 1
        assert r["streak_direction"] == "sell"

    def test_streak_zero_when_last_candle_is_balanced(self):
        """buy, buy, balanced → streak=0."""
        trades = (
            _candle_trades(0,   buy_count=8, sell_count=2) +
            _candle_trades(60,  buy_count=8, sell_count=2) +
            _candle_trades(120, buy_count=5, sell_count=5)    # balanced end
        )
        r = compute_aggressor_imbalance_streak(trades)
        assert r["streak"] == 0
        assert r["streak_direction"] is None

    def test_longer_streak_counted_correctly(self):
        trades = []
        for i in range(6):
            trades += _candle_trades(i * 60, buy_count=9, sell_count=1)
        r = compute_aggressor_imbalance_streak(trades)
        assert r["streak"] == 6
        assert r["streak_direction"] == "buy"


# ═══════════════════════════════════════════════════════════════════════════════
# Alert logic
# ═══════════════════════════════════════════════════════════════════════════════

class TestAlertLogic:
    def test_no_alert_below_threshold(self):
        """2 consecutive buy candles → no alert (default alert_streak=3)."""
        trades = (
            _candle_trades(0,  buy_count=8, sell_count=2) +
            _candle_trades(60, buy_count=8, sell_count=2)
        )
        r = compute_aggressor_imbalance_streak(trades, alert_streak=3)
        assert r["alert"] is False

    def test_alert_fires_at_exactly_three(self):
        trades = (
            _candle_trades(0,   buy_count=8, sell_count=2) +
            _candle_trades(60,  buy_count=8, sell_count=2) +
            _candle_trades(120, buy_count=8, sell_count=2)
        )
        r = compute_aggressor_imbalance_streak(trades, alert_streak=3)
        assert r["alert"] is True
        assert r["streak"] == 3

    def test_alert_fires_for_sell_streak(self):
        trades = (
            _candle_trades(0,   buy_count=2, sell_count=8) +
            _candle_trades(60,  buy_count=2, sell_count=8) +
            _candle_trades(120, buy_count=2, sell_count=8)
        )
        r = compute_aggressor_imbalance_streak(trades, alert_streak=3)
        assert r["alert"] is True
        assert r["streak_direction"] == "sell"

    def test_alert_fires_above_threshold(self):
        """4 consecutive buy candles, alert_streak=3 → should alert."""
        trades = []
        for i in range(4):
            trades += _candle_trades(i * 60, buy_count=8, sell_count=2)
        r = compute_aggressor_imbalance_streak(trades, alert_streak=3)
        assert r["alert"] is True
        assert r["streak"] == 4

    def test_custom_alert_streak(self):
        """With alert_streak=2, 2 candles is enough to alert."""
        trades = (
            _candle_trades(0,  buy_count=8, sell_count=2) +
            _candle_trades(60, buy_count=8, sell_count=2)
        )
        r = compute_aggressor_imbalance_streak(trades, alert_streak=2)
        assert r["alert"] is True

    def test_alert_false_when_streak_broken_before_threshold(self):
        """buy, buy, balanced, buy → streak=1 → no alert."""
        trades = (
            _candle_trades(0,   buy_count=8, sell_count=2) +
            _candle_trades(60,  buy_count=8, sell_count=2) +
            _candle_trades(120, buy_count=5, sell_count=5) +
            _candle_trades(180, buy_count=8, sell_count=2)
        )
        r = compute_aggressor_imbalance_streak(trades, alert_streak=3)
        assert r["alert"] is False
        assert r["streak"] == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Description
# ═══════════════════════════════════════════════════════════════════════════════

class TestDescription:
    def test_description_mentions_buy_on_buy_streak(self):
        trades = (
            _candle_trades(0,   buy_count=8, sell_count=2) +
            _candle_trades(60,  buy_count=8, sell_count=2) +
            _candle_trades(120, buy_count=8, sell_count=2)
        )
        r = compute_aggressor_imbalance_streak(trades, alert_streak=3)
        assert "buy" in r["description"].lower()

    def test_description_mentions_sell_on_sell_streak(self):
        trades = (
            _candle_trades(0,   buy_count=2, sell_count=8) +
            _candle_trades(60,  buy_count=2, sell_count=8) +
            _candle_trades(120, buy_count=2, sell_count=8)
        )
        r = compute_aggressor_imbalance_streak(trades, alert_streak=3)
        assert "sell" in r["description"].lower()

    def test_description_mentions_streak_length(self):
        trades = []
        for i in range(4):
            trades += _candle_trades(i * 60, buy_count=8, sell_count=2)
        r = compute_aggressor_imbalance_streak(trades, alert_streak=3)
        assert "4" in r["description"]

    def test_description_non_empty_when_no_streak(self):
        r = compute_aggressor_imbalance_streak([])
        assert len(r["description"]) > 0
