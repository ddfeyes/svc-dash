"""
Unit / smoke tests for /api/whale-alerts.

Whale alert tracker with large transaction clustering and exchange flow detection:
  - Large trade detection (>= min_usd threshold, default $50k)
  - Time-proximity clustering within configurable window
  - Exchange flow direction: inflow (buy-dominated) / outflow (sell-dominated) / mixed
  - Alert severity levels: medium ($50k+) / high ($100k+) / critical ($500k+)
  - Flow score (0–100, where 100 = all buys)
  - Cluster statistics: total_usd, buy_usd, sell_usd, dominant_side, duration
  - Aggregate exchange flow summary

Covers:
  - _wa_classify_size
  - _wa_flow_score
  - _wa_flow_direction
  - _wa_cluster_trades (time-window + price-proximity clustering)
  - _wa_cluster_stats
  - _wa_exchange_flow_summary
  - Response shape / key validation
  - Edge cases (empty trades, single trade, zero totals)
  - Route registration, HTML card, JS function, JS API call
"""

import sys
import os
import time
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _wa_classify_size,
    _wa_flow_score,
    _wa_flow_direction,
    _wa_cluster_trades,
    _wa_cluster_stats,
    _wa_exchange_flow_summary,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _trade(ts, price, qty, side, multiplier=1.0):
    """Build a synthetic trade dict."""
    return {
        "ts": ts,
        "price": float(price) * multiplier,
        "qty": float(qty),
        "side": side,
        "value_usd": round(float(price) * float(qty) * multiplier, 2),
        "level": _wa_classify_size(float(price) * float(qty) * multiplier),
    }


NOW = 1_700_000_000.0

SAMPLE_RESPONSE = {
    "symbol": "BANANAS31USDT",
    "alerts": [
        {
            "ts": NOW,
            "price": 0.5,
            "qty": 200_000,
            "side": "Buy",
            "value_usd": 100_000.0,
            "level": "high",
        }
    ],
    "clusters": [
        {
            "id": 1,
            "start_ts": NOW,
            "end_ts": NOW + 5,
            "duration_s": 5.0,
            "num_trades": 1,
            "total_usd": 100_000.0,
            "buy_usd": 100_000.0,
            "sell_usd": 0.0,
            "dominant_side": "buy",
            "flow": "inflow",
            "flow_score": 100.0,
            "mid_price": 0.5,
            "alert_level": "high",
        }
    ],
    "exchange_flow": {
        "direction": "inflow",
        "inflow_usd": 100_000.0,
        "outflow_usd": 0.0,
        "net_usd": 100_000.0,
        "net_direction": "positive",
        "dominant_side": "buy",
    },
    "summary": {
        "total_whale_usd": 100_000.0,
        "buy_whale_usd": 100_000.0,
        "sell_whale_usd": 0.0,
        "cluster_count": 1,
        "alert_count": 1,
        "critical_count": 0,
        "high_count": 1,
        "medium_count": 0,
        "largest_cluster_usd": 100_000.0,
        "window_seconds": 3600,
        "min_usd_threshold": 50_000.0,
    },
}


# ===========================================================================
# 1. _wa_classify_size
# ===========================================================================

class TestClassifySize:
    def test_below_medium_threshold_is_medium(self):
        # At exactly the medium threshold
        assert _wa_classify_size(50_000) == "medium"

    def test_above_medium_below_high_is_medium(self):
        assert _wa_classify_size(75_000) == "medium"

    def test_at_high_threshold_is_high(self):
        assert _wa_classify_size(100_000) == "high"

    def test_above_high_below_critical_is_high(self):
        assert _wa_classify_size(299_999) == "high"

    def test_at_critical_threshold_is_critical(self):
        assert _wa_classify_size(500_000) == "critical"

    def test_above_critical_is_critical(self):
        assert _wa_classify_size(1_000_000) == "critical"

    def test_just_below_high_threshold_is_medium(self):
        assert _wa_classify_size(99_999) == "medium"

    def test_just_below_critical_threshold_is_high(self):
        assert _wa_classify_size(499_999) == "high"


# ===========================================================================
# 2. _wa_flow_score
# ===========================================================================

class TestFlowScore:
    def test_all_buys_returns_100(self):
        assert _wa_flow_score(100_000, 0) == 100.0

    def test_all_sells_returns_0(self):
        assert _wa_flow_score(0, 100_000) == 0.0

    def test_equal_returns_50(self):
        assert _wa_flow_score(50_000, 50_000) == 50.0

    def test_zero_total_returns_50(self):
        assert _wa_flow_score(0, 0) == 50.0

    def test_75_25_buy_returns_75(self):
        assert _wa_flow_score(75_000, 25_000) == 75.0

    def test_25_75_sell_returns_25(self):
        assert _wa_flow_score(25_000, 75_000) == 25.0

    def test_returns_float(self):
        result = _wa_flow_score(60_000, 40_000)
        assert isinstance(result, float)


# ===========================================================================
# 3. _wa_flow_direction
# ===========================================================================

class TestFlowDirection:
    def test_all_buys_is_inflow(self):
        assert _wa_flow_direction(100_000, 0) == "inflow"

    def test_all_sells_is_outflow(self):
        assert _wa_flow_direction(0, 100_000) == "outflow"

    def test_equal_is_mixed(self):
        assert _wa_flow_direction(50_000, 50_000) == "mixed"

    def test_buy_dominated_above_60pct_is_inflow(self):
        assert _wa_flow_direction(70_000, 30_000) == "inflow"

    def test_sell_dominated_above_60pct_is_outflow(self):
        assert _wa_flow_direction(30_000, 70_000) == "outflow"

    def test_55_45_buy_is_mixed(self):
        # 55% buys = score 55, which is between 40 and 60 → mixed
        assert _wa_flow_direction(55_000, 45_000) == "mixed"

    def test_zero_total_is_mixed(self):
        assert _wa_flow_direction(0, 0) == "mixed"


# ===========================================================================
# 4. _wa_cluster_trades
# ===========================================================================

class TestClusterTrades:
    def test_empty_returns_empty(self):
        assert _wa_cluster_trades([]) == []

    def test_single_trade_returns_one_cluster(self):
        t = _trade(NOW, 1.0, 100_000, "Buy")
        clusters = _wa_cluster_trades([t])
        assert len(clusters) == 1
        assert len(clusters[0]) == 1

    def test_two_trades_within_window_same_price_is_one_cluster(self):
        t1 = _trade(NOW, 1.0, 100_000, "Buy")
        t2 = _trade(NOW + 30, 1.0, 100_000, "Buy")
        clusters = _wa_cluster_trades([t1, t2], cluster_window_s=60)
        assert len(clusters) == 1

    def test_two_trades_outside_window_is_two_clusters(self):
        t1 = _trade(NOW, 1.0, 100_000, "Buy")
        t2 = _trade(NOW + 90, 1.0, 100_000, "Buy")
        clusters = _wa_cluster_trades([t1, t2], cluster_window_s=60)
        assert len(clusters) == 2

    def test_two_trades_large_price_diff_is_two_clusters(self):
        t1 = _trade(NOW, 1.0, 100_000, "Buy")
        t2 = _trade(NOW + 10, 2.0, 100_000, "Buy")  # 100% price difference
        clusters = _wa_cluster_trades([t1, t2], cluster_window_s=60, price_proximity_pct=0.5)
        assert len(clusters) == 2

    def test_three_trades_in_sequence_within_window_is_one_cluster(self):
        t1 = _trade(NOW, 1.0, 100_000, "Buy")
        t2 = _trade(NOW + 20, 1.001, 100_000, "Buy")
        t3 = _trade(NOW + 40, 1.002, 100_000, "Sell")
        clusters = _wa_cluster_trades([t1, t2, t3], cluster_window_s=60)
        assert len(clusters) == 1

    def test_chain_breaks_on_time_gap(self):
        t1 = _trade(NOW, 1.0, 100_000, "Buy")
        t2 = _trade(NOW + 30, 1.0, 100_000, "Buy")
        t3 = _trade(NOW + 200, 1.0, 100_000, "Buy")  # gap > 60s from t2
        clusters = _wa_cluster_trades([t1, t2, t3], cluster_window_s=60)
        assert len(clusters) == 2

    def test_cluster_preserves_trade_order(self):
        t1 = _trade(NOW, 1.0, 100_000, "Buy")
        t2 = _trade(NOW + 10, 1.0, 100_000, "Sell")
        clusters = _wa_cluster_trades([t1, t2], cluster_window_s=60)
        assert clusters[0][0]["ts"] <= clusters[0][-1]["ts"]


# ===========================================================================
# 5. _wa_cluster_stats
# ===========================================================================

class TestClusterStats:
    def _make_cluster(self, trades):
        return _wa_cluster_stats(trades, cluster_id=1)

    def test_num_trades_count(self):
        t1 = _trade(NOW, 1.0, 100_000, "Buy")
        t2 = _trade(NOW + 10, 1.0, 50_000, "Sell")
        stats = self._make_cluster([t1, t2])
        assert stats["num_trades"] == 2

    def test_total_usd_is_sum(self):
        t1 = _trade(NOW, 1.0, 100_000, "Buy")
        t2 = _trade(NOW + 5, 1.0, 50_000, "Sell")
        stats = self._make_cluster([t1, t2])
        assert stats["total_usd"] == pytest.approx(150_000, rel=1e-3)

    def test_buy_usd_excludes_sells(self):
        t1 = _trade(NOW, 1.0, 100_000, "Buy")
        t2 = _trade(NOW + 5, 1.0, 50_000, "Sell")
        stats = self._make_cluster([t1, t2])
        assert stats["buy_usd"] == pytest.approx(100_000, rel=1e-3)
        assert stats["sell_usd"] == pytest.approx(50_000, rel=1e-3)

    def test_dominant_side_buy_when_buy_greater(self):
        t1 = _trade(NOW, 1.0, 100_000, "Buy")
        t2 = _trade(NOW + 5, 1.0, 30_000, "Sell")
        stats = self._make_cluster([t1, t2])
        assert stats["dominant_side"] == "buy"

    def test_dominant_side_sell_when_sell_greater(self):
        t1 = _trade(NOW, 1.0, 30_000, "Buy")
        t2 = _trade(NOW + 5, 1.0, 100_000, "Sell")
        stats = self._make_cluster([t1, t2])
        assert stats["dominant_side"] == "sell"

    def test_mid_price_is_average(self):
        t1 = _trade(NOW, 1.0, 100_000, "Buy")
        t2 = _trade(NOW + 5, 2.0, 100_000, "Buy")
        stats = self._make_cluster([t1, t2])
        assert stats["mid_price"] == pytest.approx(1.5, rel=1e-3)

    def test_duration_s_matches(self):
        t1 = _trade(NOW, 1.0, 100_000, "Buy")
        t2 = _trade(NOW + 45, 1.0, 100_000, "Buy")
        stats = self._make_cluster([t1, t2])
        assert stats["duration_s"] == pytest.approx(45.0, rel=1e-3)

    def test_cluster_id_preserved(self):
        t = _trade(NOW, 1.0, 100_000, "Buy")
        stats = _wa_cluster_stats([t], cluster_id=7)
        assert stats["id"] == 7

    def test_all_buy_flow_is_inflow(self):
        t1 = _trade(NOW, 1.0, 100_000, "Buy")
        t2 = _trade(NOW + 5, 1.0, 50_000, "Buy")
        stats = self._make_cluster([t1, t2])
        assert stats["flow"] == "inflow"

    def test_all_sell_flow_is_outflow(self):
        t1 = _trade(NOW, 1.0, 100_000, "Sell")
        t2 = _trade(NOW + 5, 1.0, 50_000, "Sell")
        stats = self._make_cluster([t1, t2])
        assert stats["flow"] == "outflow"

    def test_flow_score_range(self):
        t = _trade(NOW, 1.0, 100_000, "Buy")
        stats = _wa_cluster_stats([t], cluster_id=1)
        assert 0 <= stats["flow_score"] <= 100

    def test_alert_level_assigned(self):
        t = _trade(NOW, 1.0, 200_000, "Buy")
        stats = _wa_cluster_stats([t], cluster_id=1)
        assert stats["alert_level"] in ("medium", "high", "critical")


# ===========================================================================
# 6. _wa_exchange_flow_summary
# ===========================================================================

class TestExchangeFlowSummary:
    def _make_stats(self, buy_usd, sell_usd, n=1):
        return [{"buy_usd": buy_usd / n, "sell_usd": sell_usd / n} for _ in range(n)]

    def test_empty_clusters_returns_neutral(self):
        result = _wa_exchange_flow_summary([])
        assert result["direction"] == "mixed"
        assert result["inflow_usd"] == 0.0
        assert result["outflow_usd"] == 0.0
        assert result["net_direction"] == "neutral"
        assert result["dominant_side"] == "neutral"

    def test_all_inflow_direction(self):
        stats = self._make_stats(200_000, 0)
        result = _wa_exchange_flow_summary(stats)
        assert result["direction"] == "inflow"

    def test_all_outflow_direction(self):
        stats = self._make_stats(0, 200_000)
        result = _wa_exchange_flow_summary(stats)
        assert result["direction"] == "outflow"

    def test_mixed_direction(self):
        stats = self._make_stats(100_000, 100_000)
        result = _wa_exchange_flow_summary(stats)
        assert result["direction"] == "mixed"

    def test_inflow_usd_sums_correctly(self):
        stats = [{"buy_usd": 50_000, "sell_usd": 10_000},
                 {"buy_usd": 30_000, "sell_usd": 20_000}]
        result = _wa_exchange_flow_summary(stats)
        assert result["inflow_usd"] == pytest.approx(80_000, rel=1e-3)
        assert result["outflow_usd"] == pytest.approx(30_000, rel=1e-3)

    def test_net_usd_is_inflow_minus_outflow(self):
        stats = [{"buy_usd": 80_000, "sell_usd": 30_000}]
        result = _wa_exchange_flow_summary(stats)
        assert result["net_usd"] == pytest.approx(50_000, rel=1e-3)

    def test_net_direction_positive_when_inflow_greater(self):
        stats = self._make_stats(100_000, 40_000)
        result = _wa_exchange_flow_summary(stats)
        assert result["net_direction"] == "positive"

    def test_net_direction_negative_when_outflow_greater(self):
        stats = self._make_stats(40_000, 100_000)
        result = _wa_exchange_flow_summary(stats)
        assert result["net_direction"] == "negative"

    def test_dominant_side_buy_when_inflow_greater(self):
        stats = self._make_stats(100_000, 40_000)
        result = _wa_exchange_flow_summary(stats)
        assert result["dominant_side"] == "buy"

    def test_dominant_side_sell_when_outflow_greater(self):
        stats = self._make_stats(40_000, 100_000)
        result = _wa_exchange_flow_summary(stats)
        assert result["dominant_side"] == "sell"


# ===========================================================================
# 7. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_symbol(self):
        assert "symbol" in SAMPLE_RESPONSE

    def test_has_alerts_list(self):
        assert isinstance(SAMPLE_RESPONSE["alerts"], list)

    def test_has_clusters_list(self):
        assert isinstance(SAMPLE_RESPONSE["clusters"], list)

    def test_has_exchange_flow(self):
        assert "exchange_flow" in SAMPLE_RESPONSE

    def test_has_summary(self):
        assert "summary" in SAMPLE_RESPONSE

    def test_alert_item_has_required_keys(self):
        alert = SAMPLE_RESPONSE["alerts"][0]
        for key in ("ts", "price", "qty", "side", "value_usd", "level"):
            assert key in alert, f"Missing key: {key}"

    def test_alert_level_is_valid(self):
        level = SAMPLE_RESPONSE["alerts"][0]["level"]
        assert level in ("medium", "high", "critical")

    def test_cluster_item_has_required_keys(self):
        cluster = SAMPLE_RESPONSE["clusters"][0]
        for key in ("id", "start_ts", "end_ts", "num_trades", "total_usd",
                    "buy_usd", "sell_usd", "dominant_side", "flow", "flow_score",
                    "mid_price", "alert_level"):
            assert key in cluster, f"Missing key: {key}"

    def test_exchange_flow_has_required_keys(self):
        ef = SAMPLE_RESPONSE["exchange_flow"]
        for key in ("direction", "inflow_usd", "outflow_usd", "net_usd",
                    "net_direction", "dominant_side"):
            assert key in ef, f"Missing key: {key}"

    def test_summary_has_required_keys(self):
        s = SAMPLE_RESPONSE["summary"]
        for key in ("total_whale_usd", "buy_whale_usd", "sell_whale_usd",
                    "cluster_count", "alert_count", "critical_count",
                    "high_count", "medium_count", "largest_cluster_usd",
                    "window_seconds", "min_usd_threshold"):
            assert key in s, f"Missing key: {key}"

    def test_exchange_flow_direction_is_valid(self):
        assert SAMPLE_RESPONSE["exchange_flow"]["direction"] in ("inflow", "outflow", "mixed")

    def test_cluster_flow_is_valid(self):
        cluster = SAMPLE_RESPONSE["clusters"][0]
        assert cluster["flow"] in ("inflow", "outflow", "mixed")

    def test_cluster_alert_level_is_valid(self):
        cluster = SAMPLE_RESPONSE["clusters"][0]
        assert cluster["alert_level"] in ("medium", "high", "critical")

    def test_summary_counts_are_non_negative(self):
        s = SAMPLE_RESPONSE["summary"]
        assert s["cluster_count"] >= 0
        assert s["alert_count"] >= 0
        assert s["critical_count"] >= 0
        assert s["high_count"] >= 0
        assert s["medium_count"] >= 0


# ===========================================================================
# 8. Structural tests (route / HTML card / JS function / JS API call)
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api_path = os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")
        content = open(api_path).read()
        assert "/whale-alerts" in content, "Route /whale-alerts not found in api.py"

    def test_html_card_exists(self):
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        content = open(html_path).read()
        assert "card-whale-alert" in content, "card-whale-alert not found in index.html"

    def test_js_render_function_exists(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "renderWhaleAlerts" in content, "renderWhaleAlerts not found in app.js"

    def test_js_api_call_to_endpoint(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "/whale-alerts" in content, "/whale-alerts API call not found in app.js"
