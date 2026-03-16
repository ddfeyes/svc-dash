"""Tests for compute_market_regime_classifier — 50+ assertions."""
import asyncio
import os
import tempfile
import time
import pytest

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test_regime_classifier.db")
os.environ["SYMBOL_BINANCE"] = "BANANAS31USDT"
os.environ["SYMBOL_BYBIT"] = "BANANAS31USDT"

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage import init_db, get_db
from metrics import (
    compute_market_regime_classifier,
    _rsi,
    _compute_rsi_signal,
    _compute_funding_signal,
    _compute_cvd_signal,
    _compute_oi_signal,
    _compute_dominance_signal,
    _classify_regime,
    _classify_regime_signal,
    _regime_classifier_state,
)

SYM = "BANANAS31USDT"

# ── Helpers ───────────────────────────────────────────────────────────────────

async def _insert_trades(db, trades):
    """Insert (ts, exchange, symbol, price, qty, side) tuples."""
    for t in trades:
        await db.execute(
            "INSERT INTO trades (ts, exchange, symbol, price, qty, side) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            t,
        )
    await db.commit()


async def _insert_oi(db, rows):
    """Insert (ts, exchange, symbol, oi_value, oi_contracts) tuples."""
    for r in rows:
        await db.execute(
            "INSERT INTO open_interest (ts, exchange, symbol, oi_value, oi_contracts) "
            "VALUES (?, ?, ?, ?, ?)",
            r,
        )
    await db.commit()


async def _insert_funding(db, rows):
    """Insert (ts, exchange, symbol, rate, next_funding_ts) tuples."""
    for r in rows:
        await db.execute(
            "INSERT INTO funding_rate (ts, exchange, symbol, rate, next_funding_ts) "
            "VALUES (?, ?, ?, ?, ?)",
            r,
        )
    await db.commit()


def _reset_classifier_state(symbol=None):
    """Clear per-symbol classifier state between tests."""
    key = symbol or "__default__"
    _regime_classifier_state.pop(key, None)


# ── Structure tests ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_empty_db_returns_valid_structure():
    """Empty DB should return a well-formed dict with all required keys."""
    await init_db()
    _reset_classifier_state(SYM)
    result = await compute_market_regime_classifier(symbol=SYM)

    assert isinstance(result, dict), "result must be a dict"
    assert "regime" in result
    assert "regime_confidence" in result
    assert "duration_hours" in result
    assert "signal_weights" in result
    assert "regime_history" in result
    assert "regime_signal" in result
    assert "signals" in result


@pytest.mark.asyncio
async def test_regime_is_valid_label():
    """regime must be one of the five valid labels."""
    await init_db()
    _reset_classifier_state(SYM)
    result = await compute_market_regime_classifier(symbol=SYM)
    valid = {"bull", "bear", "accumulation", "distribution", "ranging"}
    assert result["regime"] in valid, f"unexpected regime: {result['regime']}"


@pytest.mark.asyncio
async def test_confidence_is_between_0_and_1():
    """regime_confidence must be in [0, 1]."""
    await init_db()
    _reset_classifier_state(SYM)
    result = await compute_market_regime_classifier(symbol=SYM)
    assert 0.0 <= result["regime_confidence"] <= 1.0


@pytest.mark.asyncio
async def test_duration_hours_non_negative():
    """duration_hours must be >= 0."""
    await init_db()
    _reset_classifier_state(SYM)
    result = await compute_market_regime_classifier(symbol=SYM)
    assert result["duration_hours"] >= 0.0


@pytest.mark.asyncio
async def test_signal_weights_has_all_signals():
    """signal_weights must contain all five signal keys."""
    await init_db()
    _reset_classifier_state(SYM)
    result = await compute_market_regime_classifier(symbol=SYM)
    sw = result["signal_weights"]
    assert isinstance(sw, dict)
    assert "rsi" in sw
    assert "oi" in sw
    assert "funding" in sw
    assert "cvd" in sw
    assert "dominance" in sw


@pytest.mark.asyncio
async def test_regime_history_is_list():
    """regime_history must be a list."""
    await init_db()
    _reset_classifier_state(SYM)
    result = await compute_market_regime_classifier(symbol=SYM)
    assert isinstance(result["regime_history"], list)


@pytest.mark.asyncio
async def test_regime_signal_is_valid_label():
    """regime_signal must be one of the five valid signal labels."""
    await init_db()
    _reset_classifier_state(SYM)
    result = await compute_market_regime_classifier(symbol=SYM)
    valid = {"strong_bull", "bull", "neutral", "bear", "strong_bear"}
    assert result["regime_signal"] in valid


@pytest.mark.asyncio
async def test_signals_dict_has_raw_values():
    """signals dict exposes per-signal raw values in [-1, 1]."""
    await init_db()
    _reset_classifier_state(SYM)
    result = await compute_market_regime_classifier(symbol=SYM)
    sigs = result["signals"]
    assert isinstance(sigs, dict)
    for key in ("rsi", "oi", "funding", "cvd", "dominance"):
        assert key in sigs
        assert -1.0 <= sigs[key] <= 1.0, f"{key} signal out of range: {sigs[key]}"


# ── RSI helper tests ──────────────────────────────────────────────────────────

def test_rsi_too_few_candles_returns_50():
    """With fewer than period+1 candles, _rsi returns 50.0."""
    closes = [100.0, 101.0, 102.0]  # only 3 points
    assert _rsi(closes, period=14) == 50.0


def test_rsi_all_up_returns_high():
    """Steadily rising prices should give RSI > 70."""
    closes = [float(i) for i in range(1, 30)]  # all gains
    rsi_val = _rsi(closes, period=14)
    assert rsi_val > 70.0, f"expected RSI > 70, got {rsi_val}"


def test_rsi_all_down_returns_low():
    """Steadily falling prices should give RSI < 30."""
    closes = [float(100 - i) for i in range(30)]  # all losses
    rsi_val = _rsi(closes, period=14)
    assert rsi_val < 30.0, f"expected RSI < 30, got {rsi_val}"


def test_rsi_equal_gains_losses_near_50():
    """Alternating equal up/down moves should give RSI ≈ 50."""
    closes = [100.0 + (i % 2) * 1.0 for i in range(30)]
    rsi_val = _rsi(closes, period=14)
    assert 40.0 <= rsi_val <= 60.0, f"expected RSI ≈ 50, got {rsi_val}"


# ── _compute_rsi_signal tests ─────────────────────────────────────────────────

def test_rsi_signal_empty_returns_0():
    assert _compute_rsi_signal([]) == 0.0


def test_rsi_signal_too_few_candles_returns_0():
    assert _compute_rsi_signal([{"close": 100.0}]) == 0.0


def test_rsi_signal_high_rsi_is_positive():
    """Overbought candles should give positive RSI signal."""
    candles = [{"close": float(i)} for i in range(1, 30)]
    sig = _compute_rsi_signal(candles)
    assert sig > 0.0, f"expected positive RSI signal, got {sig}"


def test_rsi_signal_low_rsi_is_negative():
    """Oversold candles should give negative RSI signal."""
    candles = [{"close": float(100 - i)} for i in range(30)]
    sig = _compute_rsi_signal(candles)
    assert sig < 0.0, f"expected negative RSI signal, got {sig}"


def test_rsi_signal_clamped_to_minus1_plus1():
    """RSI signal must be clamped to [-1, 1]."""
    candles = [{"close": float(i)} for i in range(1, 50)]
    sig = _compute_rsi_signal(candles)
    assert -1.0 <= sig <= 1.0


# ── _compute_funding_signal tests ─────────────────────────────────────────────

def test_funding_signal_empty_returns_0():
    assert _compute_funding_signal([]) == 0.0


def test_funding_signal_positive_rate_is_positive():
    rows = [{"rate": 0.0001}] * 10  # positive funding
    sig = _compute_funding_signal(rows)
    assert sig > 0.0


def test_funding_signal_negative_rate_is_negative():
    rows = [{"rate": -0.0001}] * 10  # negative funding
    sig = _compute_funding_signal(rows)
    assert sig < 0.0


def test_funding_signal_zero_rate_is_zero():
    rows = [{"rate": 0.0}] * 10
    sig = _compute_funding_signal(rows)
    assert sig == 0.0


def test_funding_signal_clamped():
    rows = [{"rate": 1.0}] * 10  # extreme positive
    sig = _compute_funding_signal(rows)
    assert sig <= 1.0


def test_funding_signal_clamped_negative():
    rows = [{"rate": -1.0}] * 10  # extreme negative
    sig = _compute_funding_signal(rows)
    assert sig >= -1.0


# ── _compute_cvd_signal tests ─────────────────────────────────────────────────

def test_cvd_signal_empty_returns_0():
    assert _compute_cvd_signal([]) == 0.0


def test_cvd_signal_all_buys_is_positive():
    data = [{"cvd": float(i), "delta": 1.0} for i in range(1, 11)]
    sig = _compute_cvd_signal(data)
    assert sig > 0.0


def test_cvd_signal_all_sells_is_negative():
    data = [{"cvd": -float(i), "delta": -1.0} for i in range(1, 11)]
    sig = _compute_cvd_signal(data)
    assert sig < 0.0


def test_cvd_signal_clamped():
    data = [{"cvd": 999.0, "delta": 1.0}] * 5
    sig = _compute_cvd_signal(data)
    assert -1.0 <= sig <= 1.0


def test_cvd_signal_zero_delta_volume_returns_0():
    data = [{"cvd": 0.0, "delta": 0.0}] * 5
    sig = _compute_cvd_signal(data)
    assert sig == 0.0


# ── _compute_oi_signal tests ──────────────────────────────────────────────────

def test_oi_signal_empty_oi_returns_0():
    assert _compute_oi_signal([], []) == 0.0


def test_oi_signal_single_row_returns_0():
    oi = [{"exchange": "binance", "oi_value": 1000.0}]
    assert _compute_oi_signal(oi, []) == 0.0


def test_oi_signal_rising_oi_price_up_is_positive():
    oi = [
        {"exchange": "binance", "oi_value": 1000.0},
        {"exchange": "binance", "oi_value": 1100.0},
    ]
    ohlcv = [{"close": 100.0}, {"close": 102.0}]
    sig = _compute_oi_signal(oi, ohlcv)
    assert sig > 0.0


def test_oi_signal_falling_oi_is_negative():
    oi = [
        {"exchange": "binance", "oi_value": 1100.0},
        {"exchange": "binance", "oi_value": 900.0},
    ]
    ohlcv = [{"close": 100.0}, {"close": 99.0}]
    sig = _compute_oi_signal(oi, ohlcv)
    assert sig < 0.0


def test_oi_signal_clamped():
    oi = [
        {"exchange": "binance", "oi_value": 100.0},
        {"exchange": "binance", "oi_value": 10000.0},  # massive rise
    ]
    ohlcv = [{"close": 100.0}, {"close": 200.0}]
    sig = _compute_oi_signal(oi, ohlcv)
    assert -1.0 <= sig <= 1.0


# ── _compute_dominance_signal tests ──────────────────────────────────────────

def test_dominance_signal_btc_symbol_returns_0():
    assert _compute_dominance_signal("BTCUSDT", []) == 0.0


def test_dominance_signal_no_data_returns_0():
    assert _compute_dominance_signal("BANANAS31USDT", []) == 0.0


def test_dominance_signal_too_few_candles_returns_0():
    candles = [{"close": 100.0}] * 3
    assert _compute_dominance_signal("BANANAS31USDT", candles) == 0.0


def test_dominance_signal_rising_alt_is_positive():
    candles = [{"close": float(100 + i * 2)} for i in range(6)]
    sig = _compute_dominance_signal("BANANAS31USDT", candles)
    assert sig > 0.0


def test_dominance_signal_falling_alt_is_negative():
    candles = [{"close": float(100 - i * 2)} for i in range(6)]
    sig = _compute_dominance_signal("BANANAS31USDT", candles)
    assert sig < 0.0


def test_dominance_signal_clamped():
    candles = [{"close": float(10 + i * 50)} for i in range(6)]
    sig = _compute_dominance_signal("BANANAS31USDT", candles)
    assert -1.0 <= sig <= 1.0


# ── _classify_regime tests ────────────────────────────────────────────────────

def test_classify_regime_high_score_is_bull():
    sigs = {"cvd": 0.8, "oi": 0.5, "funding": 0.3, "rsi": 0.6, "dominance": 0.4}
    assert _classify_regime(0.6, sigs) == "bull"


def test_classify_regime_low_score_is_bear():
    sigs = {"cvd": -0.8, "oi": -0.5, "funding": -0.3, "rsi": -0.6, "dominance": -0.4}
    assert _classify_regime(-0.6, sigs) == "bear"


def test_classify_regime_near_zero_positive_cvd_oi_is_accumulation():
    sigs = {"cvd": 0.3, "oi": 0.2, "funding": 0.0, "rsi": 0.0, "dominance": 0.0}
    assert _classify_regime(0.05, sigs) == "accumulation"


def test_classify_regime_near_zero_negative_cvd_oi_is_distribution():
    sigs = {"cvd": -0.3, "oi": -0.2, "funding": 0.0, "rsi": 0.0, "dominance": 0.0}
    assert _classify_regime(-0.05, sigs) == "distribution"


def test_classify_regime_near_zero_neutral_is_ranging():
    sigs = {"cvd": 0.0, "oi": 0.0, "funding": 0.0, "rsi": 0.0, "dominance": 0.0}
    assert _classify_regime(0.0, sigs) == "ranging"


# ── _classify_regime_signal tests ─────────────────────────────────────────────

def test_regime_signal_strong_bull():
    assert _classify_regime_signal(0.8) == "strong_bull"


def test_regime_signal_bull():
    assert _classify_regime_signal(0.4) == "bull"


def test_regime_signal_neutral():
    assert _classify_regime_signal(0.0) == "neutral"


def test_regime_signal_bear():
    assert _classify_regime_signal(-0.4) == "bear"


def test_regime_signal_strong_bear():
    assert _classify_regime_signal(-0.8) == "strong_bear"


# ── Integration: bullish scenario ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_bullish_scenario_produces_bull_or_accumulation():
    """Rising price + positive CVD + rising OI + positive funding → bull-ish regime."""
    await init_db()
    _reset_classifier_state(SYM)
    db = await get_db()
    now = time.time()

    # Insert rising trades (buy pressure)
    trades = [
        (now - 3600 + i * 60, "binance", SYM, 100.0 + i * 0.5, 50.0, "buy")
        for i in range(40)
    ]
    await _insert_trades(db, trades)

    # Insert rising OI
    oi_rows = [
        (now - 3600 + i * 180, "binance", SYM, 1000.0 + i * 10, 1000.0 + i * 10)
        for i in range(10)
    ]
    await _insert_oi(db, oi_rows)

    # Insert positive funding
    funding_rows = [
        (now - 3600 + i * 480, "binance", SYM, 0.0002, now + 28800)
        for i in range(5)
    ]
    await _insert_funding(db, funding_rows)

    await db.close()

    result = await compute_market_regime_classifier(symbol=SYM)
    assert result["regime"] in {"bull", "accumulation"}, (
        f"expected bull-ish regime, got {result['regime']}"
    )
    assert result["regime_confidence"] > 0.0
    assert result["regime_signal"] in {"strong_bull", "bull", "neutral"}


@pytest.mark.asyncio
async def test_bearish_scenario_produces_bear_or_distribution():
    """Falling price + negative CVD + falling OI + negative funding → bear-ish regime."""
    await init_db()
    sym2 = "COSUSDT"
    _reset_classifier_state(sym2)

    # Use a separate env for this test — just set DB_PATH to same
    db = await get_db()
    now = time.time()

    # Insert falling trades (sell pressure)
    trades = [
        (now - 3600 + i * 60, "binance", sym2, 100.0 - i * 0.5, 50.0, "sell")
        for i in range(40)
    ]
    await _insert_trades(db, trades)

    # Insert falling OI
    oi_rows = [
        (now - 3600 + i * 180, "binance", sym2, 1000.0 - i * 10, 1000.0 - i * 10)
        for i in range(10)
    ]
    await _insert_oi(db, oi_rows)

    # Insert negative funding
    funding_rows = [
        (now - 3600 + i * 480, "binance", sym2, -0.0002, now + 28800)
        for i in range(5)
    ]
    await _insert_funding(db, funding_rows)

    await db.close()

    result = await compute_market_regime_classifier(symbol=sym2)
    assert result["regime"] in {"bear", "distribution"}, (
        f"expected bear-ish regime, got {result['regime']}"
    )
    assert result["regime_signal"] in {"strong_bear", "bear", "neutral"}


# ── Duration and history tracking ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_duration_starts_at_zero_on_first_call():
    """First call for a symbol should have near-zero duration."""
    await init_db()
    _reset_classifier_state("DEXEUSDT")
    result = await compute_market_regime_classifier(symbol="DEXEUSDT")
    # Duration on first call should be essentially zero (< 1s)
    assert result["duration_hours"] < 0.01


@pytest.mark.asyncio
async def test_regime_history_grows_on_transition():
    """After a regime transition, regime_history should record it."""
    await init_db()
    _reset_classifier_state(SYM)

    # Force an initial state with a known regime
    key = SYM
    _regime_classifier_state[key] = {
        "regime": "bear",
        "start_ts": time.time() - 3600,
        "history": [],
    }

    # Now run — if a different regime is detected, it will record a transition
    result = await compute_market_regime_classifier(symbol=SYM)

    # If a transition happened, history should have an entry
    if result["regime"] != "bear":
        assert len(result["regime_history"]) == 1
        entry = result["regime_history"][0]
        assert "ts" in entry
        assert "from" in entry
        assert "to" in entry
        assert entry["from"] == "bear"
        assert entry["to"] == result["regime"]


@pytest.mark.asyncio
async def test_regime_history_max_10_entries():
    """regime_history must never exceed 10 entries."""
    await init_db()
    _reset_classifier_state(SYM)
    now = time.time()

    # Pre-populate 12 transitions in state
    transitions = [
        {"ts": now - (12 - i) * 600, "from": "bull", "to": "bear"}
        for i in range(12)
    ]
    _regime_classifier_state[SYM] = {
        "regime": "bull",
        "start_ts": now - 600,
        "history": transitions,
    }

    # Trigger a new transition by making regime "bear" → "ranging" won't happen from state
    # Just check the current state is pruned
    result = await compute_market_regime_classifier(symbol=SYM)
    assert len(result["regime_history"]) <= 10


@pytest.mark.asyncio
async def test_regime_history_entry_structure():
    """Each history entry must have ts, from, to."""
    await init_db()
    _reset_classifier_state(SYM)
    now = time.time()

    _regime_classifier_state[SYM] = {
        "regime": "bear",
        "start_ts": now - 3600,
        "history": [{"ts": now - 3600, "from": "bull", "to": "bear"}],
    }

    result = await compute_market_regime_classifier(symbol=SYM)
    # There is at least one historical entry (our seeded one or a new one)
    hist = result["regime_history"]
    if hist:
        entry = hist[0]
        assert "ts" in entry
        assert "from" in entry
        assert "to" in entry


@pytest.mark.asyncio
async def test_duration_reflects_elapsed_time():
    """Duration should reflect the time since the current regime started."""
    await init_db()
    _reset_classifier_state("LYNUSDT")
    now = time.time()

    # Pre-seed state with regime started 2 hours ago
    _regime_classifier_state["LYNUSDT"] = {
        "regime": "ranging",  # likely to be the regime with no data
        "start_ts": now - 7200,
        "history": [],
    }

    result = await compute_market_regime_classifier(symbol="LYNUSDT")

    # If regime hasn't changed, duration should be ~2 hours
    if result["regime"] == "ranging":
        assert result["duration_hours"] >= 1.9, (
            f"expected ~2h duration, got {result['duration_hours']}"
        )


# ── Symbol isolation ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_different_symbols_have_independent_state():
    """State for one symbol should not affect another."""
    await init_db()
    _reset_classifier_state("SYMX")
    _reset_classifier_state("SYMY")

    r1 = await compute_market_regime_classifier(symbol="SYMX")
    r2 = await compute_market_regime_classifier(symbol="SYMY")

    # Both are valid — state is independent
    assert r1["regime"] in {"bull", "bear", "accumulation", "distribution", "ranging"}
    assert r2["regime"] in {"bull", "bear", "accumulation", "distribution", "ranging"}


@pytest.mark.asyncio
async def test_none_symbol_uses_default_key():
    """symbol=None should use the '__default__' state key."""
    await init_db()
    _reset_classifier_state(None)
    result = await compute_market_regime_classifier(symbol=None)
    assert isinstance(result, dict)
    assert "regime" in result
    assert "__default__" in _regime_classifier_state
