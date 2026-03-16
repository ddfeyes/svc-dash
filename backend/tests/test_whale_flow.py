"""
TDD tests for Whale Wallet Flow Tracker (Wave 23 Task 3, Issue #117).
30+ tests covering: mock data generation, inflow/outflow, accumulation score,
edge cases, API response structure/validation.
"""
import os
import sys
import tempfile

import pytest

os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "test.db"))
os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
os.environ.setdefault("SYMBOL_BYBIT", "BANANAS31USDT")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from whale_flow import (  # noqa: E402
    generate_whale_trades,
    compute_inflow_outflow,
    compute_accumulation_score,
    compute_flow_signal,
    compute_trend_7d,
    compute_whale_flow,
    FLOW_SIGNALS,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def btc_trades():
    return generate_whale_trades("BTCUSDT")


@pytest.fixture(scope="module")
def eth_trades():
    return generate_whale_trades("ETHUSDT")


@pytest.fixture(scope="module")
def sol_trades():
    return generate_whale_trades("SOLUSDT")


@pytest.fixture(scope="module")
def btc_result():
    return compute_whale_flow("BTCUSDT")


@pytest.fixture(scope="module")
def eth_result():
    return compute_whale_flow("ETHUSDT")


@pytest.fixture(scope="module")
def default_result():
    return compute_whale_flow()


# ── generate_whale_trades ─────────────────────────────────────────────────────

def test_generate_returns_list(btc_trades):
    assert isinstance(btc_trades, list)


def test_generate_nonempty(btc_trades):
    assert len(btc_trades) > 0


def test_generate_trade_has_ts(btc_trades):
    for t in btc_trades:
        assert "ts" in t


def test_generate_trade_has_direction(btc_trades):
    for t in btc_trades:
        assert t["direction"] in ("inflow", "outflow")


def test_generate_trade_has_amount_usd(btc_trades):
    for t in btc_trades:
        assert "amount_usd" in t
        assert t["amount_usd"] > 0


def test_generate_all_trades_above_threshold(btc_trades):
    """All simulated trades must be whale-sized (>100 BTC equiv ~$4M+)."""
    for t in btc_trades:
        assert t["amount_usd"] >= 1_000_000  # $1M minimum per spec


def test_generate_deterministic_same_symbol(btc_trades):
    """Same symbol => same data every call."""
    second = generate_whale_trades("BTCUSDT")
    assert len(btc_trades) == len(second)
    assert btc_trades[0]["amount_usd"] == second[0]["amount_usd"]


def test_generate_different_symbols_give_different_data(btc_trades, eth_trades):
    """Different symbols => different distributions."""
    btc_total = sum(t["amount_usd"] for t in btc_trades)
    eth_total = sum(t["amount_usd"] for t in eth_trades)
    assert btc_total != eth_total


def test_generate_covers_7_days(btc_trades):
    """Trades must span at least 6 days of the 7-day window."""
    import time
    now = time.time()
    days = set()
    for t in btc_trades:
        age_days = int((now - t["ts"]) / 86400)
        days.add(age_days)
    assert len(days) >= 6


def test_generate_has_both_directions(btc_trades):
    directions = {t["direction"] for t in btc_trades}
    assert "inflow" in directions
    assert "outflow" in directions


# ── compute_inflow_outflow ────────────────────────────────────────────────────

def test_inflow_outflow_returns_dict(btc_trades):
    result = compute_inflow_outflow(btc_trades)
    assert isinstance(result, dict)


def test_inflow_outflow_has_whale_inflow_7d(btc_trades):
    result = compute_inflow_outflow(btc_trades)
    assert "whale_inflow_7d" in result


def test_inflow_outflow_has_whale_outflow_7d(btc_trades):
    result = compute_inflow_outflow(btc_trades)
    assert "whale_outflow_7d" in result


def test_inflow_outflow_has_daily_buckets(btc_trades):
    result = compute_inflow_outflow(btc_trades)
    assert "daily_buckets" in result
    assert len(result["daily_buckets"]) == 7


def test_inflow_outflow_nonnegative(btc_trades):
    result = compute_inflow_outflow(btc_trades)
    assert result["whale_inflow_7d"] >= 0
    assert result["whale_outflow_7d"] >= 0


def test_inflow_outflow_empty_trades():
    result = compute_inflow_outflow([])
    assert result["whale_inflow_7d"] == 0
    assert result["whale_outflow_7d"] == 0


def test_inflow_outflow_daily_buckets_structure(btc_trades):
    result = compute_inflow_outflow(btc_trades)
    for bucket in result["daily_buckets"]:
        assert "day" in bucket
        assert "inflow" in bucket
        assert "outflow" in bucket
        assert "net" in bucket


# ── compute_accumulation_score ────────────────────────────────────────────────

def test_accumulation_score_range():
    score = compute_accumulation_score(inflow=500_000, outflow=100_000)
    assert 0 <= score <= 100


def test_accumulation_score_high_inflow():
    """Much more inflow than outflow => high score."""
    score = compute_accumulation_score(inflow=1_000_000, outflow=100_000)
    assert score > 60


def test_accumulation_score_high_outflow():
    """Much more outflow than inflow => low score."""
    score = compute_accumulation_score(inflow=100_000, outflow=1_000_000)
    assert score < 40


def test_accumulation_score_zero_volume():
    """Zero total volume => neutral 50."""
    score = compute_accumulation_score(inflow=0, outflow=0)
    assert score == 50


def test_accumulation_score_equal_flows():
    """Equal inflow and outflow => neutral ~50."""
    score = compute_accumulation_score(inflow=500_000, outflow=500_000)
    assert 45 <= score <= 55


def test_accumulation_score_all_inflow():
    """All inflow, no outflow => score clamped at 100."""
    score = compute_accumulation_score(inflow=1_000_000, outflow=0)
    assert score == 100


def test_accumulation_score_all_outflow():
    """All outflow, no inflow => score clamped at 0."""
    score = compute_accumulation_score(inflow=0, outflow=1_000_000)
    assert score == 0


# ── compute_flow_signal ───────────────────────────────────────────────────────

def test_flow_signal_accumulating():
    signal = compute_flow_signal(accumulation_score=75)
    assert signal == "accumulating"


def test_flow_signal_distributing():
    signal = compute_flow_signal(accumulation_score=25)
    assert signal == "distributing"


def test_flow_signal_neutral():
    signal = compute_flow_signal(accumulation_score=50)
    assert signal == "neutral"


def test_flow_signal_boundary_70():
    signal = compute_flow_signal(accumulation_score=70)
    assert signal == "accumulating"


def test_flow_signal_boundary_30():
    signal = compute_flow_signal(accumulation_score=30)
    assert signal == "distributing"


def test_flow_signal_valid_enum():
    for score in [0, 10, 30, 50, 70, 90, 100]:
        sig = compute_flow_signal(score)
        assert sig in FLOW_SIGNALS


# ── compute_trend_7d ──────────────────────────────────────────────────────────

def test_trend_7d_returns_float(btc_trades):
    io = compute_inflow_outflow(btc_trades)
    trend = compute_trend_7d(io["daily_buckets"])
    assert isinstance(trend, float)


def test_trend_7d_positive_when_net_increasing():
    # bucket[0]=today (highest net), bucket[6]=oldest (lowest net)
    # net flow is increasing toward today => positive trend (accumulation growing)
    buckets = [
        {"day": i, "inflow": 100_000 * (7 - i), "outflow": 50_000, "net": 50_000 * (7 - i)}
        for i in range(7)
    ]
    trend = compute_trend_7d(buckets)
    assert trend > 0


def test_trend_7d_negative_when_net_decreasing():
    # bucket[0]=today (lowest/most negative net), bucket[6]=oldest (highest)
    # net flow is decreasing toward today => negative trend (distribution growing)
    buckets = [
        {"day": i, "inflow": 50_000, "outflow": 100_000 * (7 - i), "net": -50_000 * (7 - i)}
        for i in range(7)
    ]
    trend = compute_trend_7d(buckets)
    assert trend < 0


def test_trend_7d_zero_for_flat():
    buckets = [
        {"day": i, "inflow": 100_000, "outflow": 100_000, "net": 0}
        for i in range(7)
    ]
    trend = compute_trend_7d(buckets)
    assert abs(trend) < 1.0  # near zero


# ── compute_whale_flow (full result) ──────────────────────────────────────────

def test_full_result_is_dict(btc_result):
    assert isinstance(btc_result, dict)


def test_full_result_has_symbol(btc_result):
    assert btc_result["symbol"] == "BTCUSDT"


def test_full_result_has_whale_inflow_7d(btc_result):
    assert "whale_inflow_7d" in btc_result
    assert btc_result["whale_inflow_7d"] >= 0


def test_full_result_has_whale_outflow_7d(btc_result):
    assert "whale_outflow_7d" in btc_result
    assert btc_result["whale_outflow_7d"] >= 0


def test_full_result_has_net_flow_bps(btc_result):
    assert "net_flow_bps" in btc_result
    assert isinstance(btc_result["net_flow_bps"], (int, float))


def test_full_result_has_accumulation_score(btc_result):
    score = btc_result["accumulation_score"]
    assert 0 <= score <= 100


def test_full_result_has_flow_signal(btc_result):
    assert btc_result["flow_signal"] in FLOW_SIGNALS


def test_full_result_has_trend_7d(btc_result):
    assert "trend_7d" in btc_result
    assert isinstance(btc_result["trend_7d"], float)


def test_full_result_has_daily_buckets(btc_result):
    buckets = btc_result["daily_buckets"]
    assert isinstance(buckets, list)
    assert len(buckets) == 7


def test_full_result_default_symbol(default_result):
    assert default_result["symbol"] == "BTCUSDT"


def test_full_result_different_per_symbol(btc_result, eth_result):
    assert btc_result["whale_inflow_7d"] != eth_result["whale_inflow_7d"]


def test_full_result_net_flow_bps_formula(btc_result):
    """net_flow_bps = (inflow - outflow) / (inflow + outflow) * 10000, or 0 if zero vol."""
    inflow = btc_result["whale_inflow_7d"]
    outflow = btc_result["whale_outflow_7d"]
    total = inflow + outflow
    if total > 0:
        expected = round((inflow - outflow) / total * 10000, 2)
    else:
        expected = 0.0
    assert abs(btc_result["net_flow_bps"] - expected) < 0.1
