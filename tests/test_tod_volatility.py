"""
TDD tests for time-of-day volatility pattern.

Spec:
  compute_tod_volatility(candles, elevation_threshold=1.5)

  candles: [{ts, high, low, close, ...}]  (Unix seconds UTC, sorted or unsorted)

  Per-candle volatility = (high - low) / close * 100  (hl_pct)

  current_hour_start = floor(latest_ts / 3600) * 3600
  current_hour_candles     = candles where ts >= current_hour_start
  historical_candles       = candles where hour_of_day == current_hour
                                        AND ts < current_hour_start

  current_vol    = mean hl_pct of current_hour_candles    (0.0 if none)
  historical_avg = mean hl_pct of historical_candles      (0.0 if none)
  ratio          = current_vol / historical_avg            (0.0 if historical_avg == 0)
  elevated       = ratio >= elevation_threshold

  hours profile: for every hour_of_day present in candles:
    {hour, avg_vol, sample_count}   (avg of ALL candles at that hour)

Returns:
  {current_hour, current_vol, historical_avg, ratio, elevated, hours}
"""
import sys, os, math
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
from metrics import compute_tod_volatility  # noqa: E402


# ── helpers ────────────────────────────────────────────────────────────────────

HOUR = 3600   # seconds

def _candle(ts, high, low, close=100.0):
    """Minimal candle dict."""
    return {"ts": float(ts), "high": float(high), "low": float(low), "close": float(close)}

def _flat(ts, close=100.0, hl_spread_pct=1.0):
    """Candle with given hl_spread_pct = (high-low)/close*100."""
    half = close * hl_spread_pct / 100 / 2
    return _candle(ts, high=close + half, low=close - half, close=close)

def _candles_at_hour(day_offsets, hour_utc, hl_pct=1.0, base_ts=0.0):
    """
    Create one candle per day at the given UTC hour.
    day_offsets: list of day numbers (0=today, 1=yesterday, ...)
    hour_utc: 0-23
    base_ts: anchor timestamp for 'today 00:00 UTC'
    """
    hour_start = base_ts + hour_utc * HOUR
    return [_flat(hour_start - d * 86400, hl_spread_pct=hl_pct) for d in day_offsets]


# Reference: today's 14:00 UTC anchor
TODAY_14H = 14 * HOUR   # ts = 50400


# ═══════════════════════════════════════════════════════════════════════════════
# Structure
# ═══════════════════════════════════════════════════════════════════════════════

class TestStructure:
    def test_empty_returns_valid_dict(self):
        r = compute_tod_volatility([])
        assert isinstance(r, dict)

    def test_result_has_required_fields(self):
        r = compute_tod_volatility([])
        for f in ("current_hour", "current_vol", "historical_avg", "ratio",
                  "elevated", "hours"):
            assert f in r, f"missing: {f}"

    def test_hours_entry_has_required_fields(self):
        candles = [_flat(TODAY_14H + 60)]
        r = compute_tod_volatility(candles)
        assert r["hours"]
        h = r["hours"][0]
        for f in ("hour", "avg_vol", "sample_count"):
            assert f in h, f"missing hours field: {f}"

    def test_empty_gives_safe_defaults(self):
        r = compute_tod_volatility([])
        assert r["current_vol"] == pytest.approx(0.0)
        assert r["historical_avg"] == pytest.approx(0.0)
        assert r["ratio"] == pytest.approx(0.0)
        assert r["elevated"] is False
        assert r["hours"] == []


# ═══════════════════════════════════════════════════════════════════════════════
# Current hour detection
# ═══════════════════════════════════════════════════════════════════════════════

class TestCurrentHour:
    def test_current_hour_from_latest_candle(self):
        """current_hour = UTC hour of the most recent candle."""
        candles = [_flat(ts=14 * HOUR + 1800)]   # 14:30 UTC
        r = compute_tod_volatility(candles)
        assert r["current_hour"] == 14

    def test_current_hour_zero(self):
        candles = [_flat(ts=0 * HOUR + 300)]     # 00:05 UTC
        r = compute_tod_volatility(candles)
        assert r["current_hour"] == 0

    def test_current_hour_23(self):
        candles = [_flat(ts=23 * HOUR + 1)]      # 23:00 UTC
        r = compute_tod_volatility(candles)
        assert r["current_hour"] == 23

    def test_current_hour_from_latest_not_earliest(self):
        """Uses max ts, not min ts."""
        candles = [_flat(ts=10 * HOUR), _flat(ts=22 * HOUR + 100)]
        r = compute_tod_volatility(candles)
        assert r["current_hour"] == 22

    def test_unsorted_candles_correct_current_hour(self):
        candles = [_flat(ts=16 * HOUR), _flat(ts=14 * HOUR), _flat(ts=15 * HOUR)]
        r = compute_tod_volatility(candles)
        assert r["current_hour"] == 16


# ═══════════════════════════════════════════════════════════════════════════════
# hl_pct computation
# ═══════════════════════════════════════════════════════════════════════════════

class TestHlPct:
    def test_hl_pct_formula(self):
        """hl_pct = (high - low) / close * 100."""
        # high=102, low=98, close=100 → hl_pct = 4/100*100 = 4.0
        c = _candle(ts=14 * HOUR + 60, high=102, low=98, close=100)
        r = compute_tod_volatility([c])
        assert r["current_vol"] == pytest.approx(4.0)

    def test_zero_range_candle_is_zero(self):
        c = _candle(ts=14 * HOUR + 60, high=100, low=100, close=100)
        r = compute_tod_volatility([c])
        assert r["current_vol"] == pytest.approx(0.0)

    def test_current_vol_is_mean_of_current_hour_candles(self):
        """Two candles in current hour: hl_pct 2% and 4% → mean = 3%."""
        ts_start = 14 * HOUR   # hour 14
        candles = [
            _flat(ts_start + 600,  hl_spread_pct=2.0),
            _flat(ts_start + 1200, hl_spread_pct=4.0),
        ]
        r = compute_tod_volatility(candles)
        assert r["current_vol"] == pytest.approx(3.0)

    def test_hl_pct_uses_close_not_constant(self):
        """hl_pct normalises by close, not a fixed price."""
        # close=200, range=4 → hl_pct = 4/200*100 = 2.0
        c = _candle(ts=14 * HOUR + 60, high=202, low=198, close=200)
        r = compute_tod_volatility([c])
        assert r["current_vol"] == pytest.approx(2.0)


# ═══════════════════════════════════════════════════════════════════════════════
# Historical average
# ═══════════════════════════════════════════════════════════════════════════════

class TestHistoricalAverage:
    def test_historical_avg_uses_same_hour_past_days(self):
        """
        Historical candles: same UTC hour on previous days.
        current_hour=14, current window starts at ts=14*HOUR (today).
        Historical = candles at hour 14 on previous days.
        """
        current_ts = TODAY_14H + 300   # 14:05 today
        hist1_ts   = TODAY_14H - 86400 + 300   # 14:05 yesterday
        hist2_ts   = TODAY_14H - 2*86400 + 300 # 14:05 two days ago

        candles = [
            _flat(current_ts, hl_spread_pct=6.0),   # current hour: 6%
            _flat(hist1_ts,   hl_spread_pct=2.0),   # hist day 1: 2%
            _flat(hist2_ts,   hl_spread_pct=4.0),   # hist day 2: 4%
        ]
        r = compute_tod_volatility(candles)
        assert r["current_hour"] == 14
        assert r["current_vol"] == pytest.approx(6.0)
        assert r["historical_avg"] == pytest.approx(3.0)   # mean(2, 4)

    def test_historical_excludes_other_hours(self):
        """Candles at different hours don't affect historical_avg for hour 14."""
        current_ts = TODAY_14H + 300
        other_hour_ts = TODAY_14H - 86400 + HOUR   # yesterday at hour 15

        candles = [
            _flat(current_ts,    hl_spread_pct=5.0),
            _flat(other_hour_ts, hl_spread_pct=99.0),  # hour 15 — ignored
        ]
        r = compute_tod_volatility(candles)
        assert r["historical_avg"] == pytest.approx(0.0)   # no hist data at hour 14

    def test_historical_excludes_current_window(self):
        """Candles in the current 1h window don't count as historical."""
        current_ts_early = TODAY_14H + 100
        current_ts_late  = TODAY_14H + 200
        candles = [
            _flat(current_ts_early, hl_spread_pct=2.0),
            _flat(current_ts_late,  hl_spread_pct=8.0),
        ]
        r = compute_tod_volatility(candles)
        # Both candles in same 1h window → historical_avg = 0 (no prior data)
        assert r["historical_avg"] == pytest.approx(0.0)
        assert r["current_vol"] == pytest.approx(5.0)

    def test_multiple_hist_candles_per_day_all_counted(self):
        """3 historical candles at same hour (different days) → mean of all 3."""
        base = TODAY_14H
        candles = [
            _flat(base + 300,         hl_spread_pct=10.0),  # today (current)
            _flat(base - 86400 + 100, hl_spread_pct=2.0),   # yesterday
            _flat(base - 86400 + 200, hl_spread_pct=4.0),   # yesterday (2nd)
            _flat(base - 2*86400 + 300, hl_spread_pct=6.0), # 2 days ago
        ]
        r = compute_tod_volatility(candles)
        # historical = 3 candles: 2.0, 4.0, 6.0 → mean = 4.0
        assert r["historical_avg"] == pytest.approx(4.0)

    def test_no_historical_data_historical_avg_zero(self):
        candles = [_flat(TODAY_14H + 300, hl_spread_pct=5.0)]
        r = compute_tod_volatility(candles)
        assert r["historical_avg"] == pytest.approx(0.0)


# ═══════════════════════════════════════════════════════════════════════════════
# Ratio and elevation
# ═══════════════════════════════════════════════════════════════════════════════

class TestRatioAndElevation:
    def test_ratio_formula(self):
        """ratio = current_vol / historical_avg."""
        base = TODAY_14H
        candles = [
            _flat(base + 300,         hl_spread_pct=3.0),  # current: 3%
            _flat(base - 86400 + 300, hl_spread_pct=1.0),  # hist: 1%
        ]
        r = compute_tod_volatility(candles)
        assert r["ratio"] == pytest.approx(3.0)

    def test_ratio_zero_when_no_historical(self):
        candles = [_flat(TODAY_14H + 300, hl_spread_pct=5.0)]
        r = compute_tod_volatility(candles)
        assert r["ratio"] == pytest.approx(0.0)

    def test_elevated_true_when_ratio_meets_threshold(self):
        base = TODAY_14H
        candles = [
            _flat(base + 300,         hl_spread_pct=3.0),  # current
            _flat(base - 86400 + 300, hl_spread_pct=2.0),  # hist → ratio=1.5
        ]
        r = compute_tod_volatility(candles, elevation_threshold=1.5)
        assert r["elevated"] is True

    def test_elevated_false_when_ratio_below_threshold(self):
        base = TODAY_14H
        candles = [
            _flat(base + 300,         hl_spread_pct=2.0),
            _flat(base - 86400 + 300, hl_spread_pct=2.0),  # ratio = 1.0
        ]
        r = compute_tod_volatility(candles, elevation_threshold=1.5)
        assert r["elevated"] is False

    def test_elevated_false_when_no_historical(self):
        r = compute_tod_volatility([_flat(TODAY_14H + 1)])
        assert r["elevated"] is False

    def test_custom_threshold_applied(self):
        base = TODAY_14H
        candles = [
            _flat(base + 300,         hl_spread_pct=1.2),
            _flat(base - 86400 + 300, hl_spread_pct=1.0),  # ratio = 1.2
        ]
        r_strict = compute_tod_volatility(candles, elevation_threshold=1.1)
        r_loose  = compute_tod_volatility(candles, elevation_threshold=1.5)
        assert r_strict["elevated"] is True
        assert r_loose["elevated"]  is False

    def test_ratio_below_1_not_elevated(self):
        """Current vol quieter than historical → ratio < 1 → not elevated."""
        base = TODAY_14H
        candles = [
            _flat(base + 300,         hl_spread_pct=1.0),
            _flat(base - 86400 + 300, hl_spread_pct=3.0),
        ]
        r = compute_tod_volatility(candles, elevation_threshold=1.5)
        assert r["ratio"] == pytest.approx(1 / 3, rel=1e-4)
        assert r["elevated"] is False


# ═══════════════════════════════════════════════════════════════════════════════
# Hours profile
# ═══════════════════════════════════════════════════════════════════════════════

class TestHoursProfile:
    def test_hours_contains_entry_for_each_present_hour(self):
        candles = [
            _flat(10 * HOUR + 60),
            _flat(14 * HOUR + 60),
            _flat(22 * HOUR + 60),
        ]
        r = compute_tod_volatility(candles)
        present = {e["hour"] for e in r["hours"]}
        assert present == {10, 14, 22}

    def test_hours_avg_vol_is_mean_of_all_candles_in_hour(self):
        """Profile uses ALL candles at that hour (current + historical)."""
        base = TODAY_14H
        candles = [
            _flat(base + 60,           hl_spread_pct=2.0),   # today 14h
            _flat(base - 86400 + 60,   hl_spread_pct=4.0),   # yesterday 14h
        ]
        r = compute_tod_volatility(candles)
        entry = next(e for e in r["hours"] if e["hour"] == 14)
        assert entry["avg_vol"] == pytest.approx(3.0)

    def test_hours_sample_count_correct(self):
        base = TODAY_14H
        candles = [
            _flat(base + 60),
            _flat(base - 86400 + 60),
            _flat(base - 2*86400 + 60),
        ]
        r = compute_tod_volatility(candles)
        entry = next(e for e in r["hours"] if e["hour"] == 14)
        assert entry["sample_count"] == 3

    def test_hours_excludes_empty_hours(self):
        """Hours with zero candles are not in the profile."""
        candles = [_flat(10 * HOUR + 60), _flat(20 * HOUR + 60)]
        r = compute_tod_volatility(candles)
        hours_present = [e["hour"] for e in r["hours"]]
        # Only hours 10 and 20 should appear
        assert 11 not in hours_present
        assert 15 not in hours_present

    def test_hours_sorted_by_hour(self):
        candles = [_flat(22 * HOUR + 60), _flat(5 * HOUR + 60), _flat(14 * HOUR + 60)]
        r = compute_tod_volatility(candles)
        hours = [e["hour"] for e in r["hours"]]
        assert hours == sorted(hours)


# ═══════════════════════════════════════════════════════════════════════════════
# Edge cases
# ═══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    def test_single_candle_no_history(self):
        r = compute_tod_volatility([_flat(TODAY_14H + 60, hl_spread_pct=3.0)])
        assert r["current_vol"] == pytest.approx(3.0)
        assert r["historical_avg"] == pytest.approx(0.0)
        assert r["ratio"] == pytest.approx(0.0)

    def test_all_candles_same_hour_same_day(self):
        """Multiple candles all in the same 1h window → all current, no history."""
        candles = [_flat(TODAY_14H + i * 60, hl_spread_pct=2.0) for i in range(5)]
        r = compute_tod_volatility(candles)
        assert r["current_vol"] == pytest.approx(2.0)
        assert r["historical_avg"] == pytest.approx(0.0)

    def test_boundary_candle_exactly_at_hour_start_is_current(self):
        """A candle exactly at the start of the current hour is in current window."""
        ts = TODAY_14H  # exact boundary
        candles = [_flat(ts, hl_spread_pct=5.0)]
        r = compute_tod_volatility(candles)
        assert r["current_vol"] == pytest.approx(5.0)

    def test_large_dataset_no_crash(self):
        """7 days × 24h × 4 candles/h = 672 candles — function must not crash."""
        import random; random.seed(99)
        candles = []
        base = TODAY_14H
        for day in range(7):
            for h in range(24):
                for m in range(0, 60, 15):
                    ts = base - day * 86400 + h * HOUR + m * 60
                    vol = random.uniform(0.5, 3.0)
                    candles.append(_flat(ts, hl_spread_pct=vol))
        r = compute_tod_volatility(candles)
        assert 0 <= r["current_hour"] <= 23
        assert r["current_vol"] >= 0
        assert len(r["hours"]) == 24
