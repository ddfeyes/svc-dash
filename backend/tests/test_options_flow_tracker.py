"""50+ tests for the options flow tracker feature."""
import asyncio
import math
import os
import sys
import tempfile

import pytest

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test_oft.db")
os.environ["SYMBOL_BINANCE"] = "BANANAS31USDT"
os.environ["SYMBOL_BYBIT"] = "BANANAS31USDT"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from metrics import (
    # New analytical helpers
    _oft_skew_label,
    _oft_is_unusual,
    _oft_strike_bucket,
    _oft_expiry_weight,
    _oft_call_put_ratio,
    _oft_iv_skew_label,
    _oft_flow_severity,
    _oft_aggregate_by_strike,
    _oft_net_gamma,
    # Pre-existing simulation helpers
    _oft_skew_signal,
    _oft_skew_ratio,
    _oft_unusual_threshold,
    _oft_net_flow,
    _oft_dominant_expiry,
    _oft_skew_percentile,
    _oft_make_instrument,
    _oft_simulate_large_trades,
    _oft_compute_skew_by_expiry,
    _oft_detect_unusual_flow,
    _oft_build_strike_heatmap,
    compute_options_flow_tracker,
)


# ── frontend file helpers ──────────────────────────────────────────────────────

def _root():
    return os.path.join(os.path.dirname(__file__), "..", "..")


def _read_html():
    with open(os.path.join(_root(), "frontend", "index.html")) as f:
        return f.read()


def _read_js():
    with open(os.path.join(_root(), "frontend", "app.js")) as f:
        return f.read()


# ── _oft_skew_signal ──────────────────────────────────────────────────────────

def test_skew_signal_bullish_ratio_above_1_25():
    assert _oft_skew_signal(1260, 1000) == "bullish"


def test_skew_signal_exactly_1_25_is_neutral():
    assert _oft_skew_signal(1250, 1000) == "neutral"


def test_skew_signal_strong_bullish():
    assert _oft_skew_signal(5000, 1000) == "bullish"


def test_skew_signal_bearish():
    assert _oft_skew_signal(700, 1000) == "bearish"


def test_skew_signal_bearish_zero_calls():
    assert _oft_skew_signal(0, 1000) == "bearish"


def test_skew_signal_neutral_equal():
    assert _oft_skew_signal(1000, 1000) == "neutral"


def test_skew_signal_neutral_near_boundary_high():
    assert _oft_skew_signal(1240, 1000) == "neutral"


def test_skew_signal_neutral_near_boundary_low():
    assert _oft_skew_signal(810, 1000) == "neutral"


def test_skew_signal_zero_put_bullish():
    assert _oft_skew_signal(100000, 0) == "bullish"


# ── _oft_skew_ratio ────────────────────────────────────────────────────────────

def test_skew_ratio_basic():
    assert _oft_skew_ratio(2000, 1000) == pytest.approx(2.0, abs=0.01)


def test_skew_ratio_zero_put_returns_10():
    assert _oft_skew_ratio(500, 0) == 10.0


def test_skew_ratio_capped_at_10():
    assert _oft_skew_ratio(1_000_000, 1) == 10.0


def test_skew_ratio_below_one():
    assert _oft_skew_ratio(800, 1000) == pytest.approx(0.8, abs=0.01)


def test_skew_ratio_equal():
    assert _oft_skew_ratio(1000, 1000) == pytest.approx(1.0, abs=0.01)


# ── _oft_unusual_threshold ─────────────────────────────────────────────────────

def test_unusual_threshold_above_3x():
    assert _oft_unusual_threshold(300001, 100000) is True


def test_unusual_threshold_exactly_3x_not_unusual():
    assert _oft_unusual_threshold(300000, 100000) is False


def test_unusual_threshold_below_3x():
    assert _oft_unusual_threshold(200000, 100000) is False


def test_unusual_threshold_large_trade():
    assert _oft_unusual_threshold(5_000_000, 500_000) is True


# ── _oft_net_flow ──────────────────────────────────────────────────────────────

def test_net_flow_positive():
    assert _oft_net_flow(2_000_000, 800_000) == pytest.approx(1_200_000, abs=1)


def test_net_flow_negative():
    assert _oft_net_flow(500_000, 1_500_000) == pytest.approx(-1_000_000, abs=1)


def test_net_flow_zero():
    assert _oft_net_flow(1_000_000, 1_000_000) == pytest.approx(0, abs=1)


# ── _oft_dominant_expiry ───────────────────────────────────────────────────────

def test_dominant_expiry_picks_highest_volume():
    skew = {
        "28MAR26": {"call_volume_usd": 100_000, "put_volume_usd": 50_000},
        "25APR26": {"call_volume_usd": 800_000, "put_volume_usd": 200_000},
        "27JUN26": {"call_volume_usd": 200_000, "put_volume_usd": 100_000},
    }
    assert _oft_dominant_expiry(skew) == "25APR26"


def test_dominant_expiry_empty():
    assert _oft_dominant_expiry({}) == ""


def test_dominant_expiry_single():
    skew = {"28MAR26": {"call_volume_usd": 500_000, "put_volume_usd": 300_000}}
    assert _oft_dominant_expiry(skew) == "28MAR26"


# ── _oft_skew_percentile ───────────────────────────────────────────────────────

def test_skew_percentile_in_range():
    for ratio in [0.1, 0.5, 1.0, 1.5, 2.0, 5.0]:
        pct = _oft_skew_percentile(ratio)
        assert 0.0 <= pct <= 100.0, f"Out of range for ratio={ratio}"


def test_skew_percentile_at_1_is_near_50():
    pct = _oft_skew_percentile(1.0)
    assert 45.0 <= pct <= 55.0


def test_skew_percentile_higher_ratio_higher_pct():
    assert _oft_skew_percentile(2.0) > _oft_skew_percentile(1.0)


def test_skew_percentile_low_ratio_low_pct():
    assert _oft_skew_percentile(0.3) < _oft_skew_percentile(1.0)


# ── _oft_make_instrument ───────────────────────────────────────────────────────

def test_make_instrument_call():
    assert _oft_make_instrument(70000, "28MAR26", "call") == "BTC-28MAR26-70000-C"


def test_make_instrument_put():
    assert _oft_make_instrument(60000, "25APR26", "put") == "BTC-25APR26-60000-P"


def test_make_instrument_format_has_four_parts():
    parts = _oft_make_instrument(80000, "27JUN26", "call").split("-")
    assert len(parts) == 4
    assert parts[0] == "BTC"
    assert parts[3] == "C"


# ── _oft_simulate_large_trades ─────────────────────────────────────────────────

def test_simulate_trades_count():
    trades = _oft_simulate_large_trades()
    assert len(trades) == 40


def test_simulate_trades_all_above_100k():
    for t in _oft_simulate_large_trades():
        assert t["notional_usd"] >= 100_000


def test_simulate_trades_required_keys():
    required = {"ts", "exchange", "instrument", "type", "strike", "expiry",
                "side", "contracts", "btc_price", "premium_per_contract",
                "notional_usd", "iv", "delta"}
    for t in _oft_simulate_large_trades():
        assert required.issubset(t.keys())


def test_simulate_trades_types_valid():
    for t in _oft_simulate_large_trades():
        assert t["type"] in ("call", "put")


def test_simulate_trades_sides_valid():
    for t in _oft_simulate_large_trades():
        assert t["side"] in ("buy", "sell")


def test_simulate_trades_exchanges_valid():
    for t in _oft_simulate_large_trades():
        assert t["exchange"] in ("deribit", "lyra")


def test_simulate_trades_sorted_newest_first():
    trades = _oft_simulate_large_trades()
    for i in range(len(trades) - 1):
        assert trades[i]["ts"] >= trades[i + 1]["ts"]


def test_simulate_trades_deterministic():
    assert _oft_simulate_large_trades() == _oft_simulate_large_trades()


def test_simulate_trades_delta_in_range():
    for t in _oft_simulate_large_trades():
        assert 0.0 <= t["delta"] <= 1.0


def test_simulate_trades_contracts_positive():
    for t in _oft_simulate_large_trades():
        assert t["contracts"] > 0


# ── _oft_compute_skew_by_expiry ────────────────────────────────────────────────

def test_skew_by_expiry_nonempty():
    skew = _oft_compute_skew_by_expiry(_oft_simulate_large_trades())
    assert len(skew) > 0


def test_skew_by_expiry_required_keys():
    for exp, v in _oft_compute_skew_by_expiry(_oft_simulate_large_trades()).items():
        assert "call_volume_usd" in v
        assert "put_volume_usd" in v
        assert "skew_ratio" in v
        assert "skew_signal" in v
        assert "net_flow_usd" in v


def test_skew_by_expiry_volumes_non_negative():
    for v in _oft_compute_skew_by_expiry(_oft_simulate_large_trades()).values():
        assert v["call_volume_usd"] >= 0
        assert v["put_volume_usd"] >= 0


def test_skew_by_expiry_signal_valid():
    for v in _oft_compute_skew_by_expiry(_oft_simulate_large_trades()).values():
        assert v["skew_signal"] in ("bullish", "bearish", "neutral")


def test_skew_by_expiry_empty_returns_empty():
    assert _oft_compute_skew_by_expiry([]) == {}


def test_skew_by_expiry_single_call():
    skew = _oft_compute_skew_by_expiry([{"expiry": "28MAR26", "type": "call", "notional_usd": 500_000}])
    assert skew["28MAR26"]["call_volume_usd"] == 500_000
    assert skew["28MAR26"]["skew_signal"] == "bullish"


# ── _oft_detect_unusual_flow ───────────────────────────────────────────────────

def test_detect_unusual_flow_returns_list():
    assert isinstance(_oft_detect_unusual_flow(_oft_simulate_large_trades()), list)


def test_detect_unusual_flow_severity_values():
    for a in _oft_detect_unusual_flow(_oft_simulate_large_trades()):
        assert a["severity"] in ("high", "critical")


def test_detect_unusual_flow_required_keys():
    required = {"ts", "instrument", "exchange", "notional_usd", "side", "type", "severity", "reason"}
    for a in _oft_detect_unusual_flow(_oft_simulate_large_trades()):
        assert required.issubset(a.keys())


def test_detect_unusual_flow_empty_input():
    assert _oft_detect_unusual_flow([]) == []


def test_detect_unusual_flow_sorted_by_notional():
    alerts = _oft_detect_unusual_flow(_oft_simulate_large_trades())
    for i in range(len(alerts) - 1):
        assert alerts[i]["notional_usd"] >= alerts[i + 1]["notional_usd"]


# ── _oft_build_strike_heatmap ──────────────────────────────────────────────────

def test_strike_heatmap_nonempty():
    assert len(_oft_build_strike_heatmap(_oft_simulate_large_trades())) > 0


def test_strike_heatmap_required_keys():
    for v in _oft_build_strike_heatmap(_oft_simulate_large_trades()).values():
        assert "call_notional_usd" in v
        assert "put_notional_usd" in v
        assert "net_flow_usd" in v
        assert "dominant" in v


def test_strike_heatmap_dominant_values():
    for v in _oft_build_strike_heatmap(_oft_simulate_large_trades()).values():
        assert v["dominant"] in ("call", "put")


def test_strike_heatmap_empty_input():
    assert _oft_build_strike_heatmap([]) == {}


def test_strike_heatmap_net_flow_buy_adds():
    hm = _oft_build_strike_heatmap([{"strike": 75000, "type": "call", "side": "buy", "notional_usd": 200_000}])
    assert hm["75000"]["net_flow_usd"] == pytest.approx(200_000, abs=1)


def test_strike_heatmap_net_flow_sell_subtracts():
    hm = _oft_build_strike_heatmap([{"strike": 75000, "type": "put", "side": "sell", "notional_usd": 150_000}])
    assert hm["75000"]["net_flow_usd"] == pytest.approx(-150_000, abs=1)


# ── _oft_skew_label (new analytical helper) ───────────────────────────────────

def test_oft_skew_label_bullish():
    assert _oft_skew_label(0.60) == "bullish"


def test_oft_skew_label_bearish():
    assert _oft_skew_label(0.40) == "bearish"


def test_oft_skew_label_neutral():
    assert _oft_skew_label(0.50) == "neutral"


# ── _oft_strike_bucket (new analytical helper) ────────────────────────────────

def test_oft_strike_bucket_atm():
    assert _oft_strike_bucket(95000.0, 95000.0) == "ATM"


def test_oft_strike_bucket_dotm():
    assert _oft_strike_bucket(110000.0, 95000.0) == "DOTM"


def test_oft_strike_bucket_ditm():
    assert _oft_strike_bucket(80000.0, 95000.0) == "DITM"


# ── compute_options_flow_tracker integration ───────────────────────────────────

@pytest.fixture(scope="module")
def oft_result():
    return asyncio.run(compute_options_flow_tracker())


def test_returns_dict(oft_result):
    assert isinstance(oft_result, dict)


def test_has_large_trades(oft_result):
    assert "large_trades" in oft_result
    assert isinstance(oft_result["large_trades"], list)


def test_has_skew_by_expiry(oft_result):
    assert "skew_by_expiry" in oft_result
    assert isinstance(oft_result["skew_by_expiry"], dict)


def test_has_unusual_flow_alerts(oft_result):
    assert "unusual_flow_alerts" in oft_result
    assert isinstance(oft_result["unusual_flow_alerts"], list)


def test_has_strike_heatmap(oft_result):
    assert "strike_heatmap" in oft_result
    assert isinstance(oft_result["strike_heatmap"], dict)


def test_has_summary(oft_result):
    assert "summary" in oft_result


def test_has_description(oft_result):
    assert "description" in oft_result
    assert len(oft_result["description"]) > 10


def test_large_trades_all_above_100k(oft_result):
    for t in oft_result["large_trades"]:
        assert t["notional_usd"] >= 100_000


def test_large_trades_at_most_20(oft_result):
    assert len(oft_result["large_trades"]) <= 20


def test_summary_required_keys(oft_result):
    required = {"total_call_volume_usd", "total_put_volume_usd", "overall_skew_ratio",
                "net_flow_direction", "dominant_expiry", "skew_percentile",
                "unusual_activity_count", "total_trades_analyzed", "exchanges"}
    assert required.issubset(oft_result["summary"].keys())


def test_summary_call_vol_positive(oft_result):
    assert oft_result["summary"]["total_call_volume_usd"] > 0


def test_summary_put_vol_positive(oft_result):
    assert oft_result["summary"]["total_put_volume_usd"] > 0


def test_summary_net_flow_direction_valid(oft_result):
    assert oft_result["summary"]["net_flow_direction"] in ("bullish", "bearish", "neutral")


def test_summary_skew_percentile_in_range(oft_result):
    pct = oft_result["summary"]["skew_percentile"]
    assert 0.0 <= pct <= 100.0


def test_summary_total_trades_analyzed_is_40(oft_result):
    assert oft_result["summary"]["total_trades_analyzed"] == 40


def test_summary_exchanges_contain_known_venues(oft_result):
    known = {"deribit", "lyra"}
    assert len(known & set(oft_result["summary"]["exchanges"])) > 0


def test_summary_dominant_expiry_nonempty(oft_result):
    assert oft_result["summary"]["dominant_expiry"] != ""


def test_unusual_alerts_limited_to_10(oft_result):
    assert len(oft_result["unusual_flow_alerts"]) <= 10


def test_skew_by_expiry_nonempty_integration(oft_result):
    assert len(oft_result["skew_by_expiry"]) > 0


def test_strike_heatmap_nonempty_integration(oft_result):
    assert len(oft_result["strike_heatmap"]) > 0


def test_deterministic(oft_result):
    r2 = asyncio.run(compute_options_flow_tracker())
    assert oft_result["summary"]["total_call_volume_usd"] == r2["summary"]["total_call_volume_usd"]
    assert oft_result["summary"]["total_put_volume_usd"] == r2["summary"]["total_put_volume_usd"]


# ── HTML / JS integration ──────────────────────────────────────────────────────

def test_html_card_exists():
    assert 'id="card-options-flow"' in _read_html()


def test_html_card_title():
    assert "Options Flow Tracker" in _read_html()


def test_html_content_div():
    assert 'id="options-flow-content"' in _read_html()


def test_html_badge():
    assert 'id="options-flow-badge"' in _read_html()


def test_html_meta_mentions_notional():
    assert "100k" in _read_html()


def test_js_function_defined():
    assert "refreshOptionsFlowTracker" in _read_js()


def test_js_called_in_refresh():
    assert "safe(refreshOptionsFlowTracker)" in _read_js()


def test_js_api_path():
    assert "/options-flow-tracker" in _read_js()


def test_js_skew_by_expiry_referenced():
    assert "skew_by_expiry" in _read_js()


def test_js_unusual_flow_alerts_referenced():
    assert "unusual_flow_alerts" in _read_js()


def test_js_strike_heatmap_referenced():
    assert "strike_heatmap" in _read_js()
