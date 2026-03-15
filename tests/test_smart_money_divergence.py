"""
TDD tests for smart money divergence detector.

Spec: docs/superpowers/specs/2026-03-15-smart-money-divergence.md

Pure function: compute_smart_money_divergence(trades, threshold_usd, bucket_seconds)

Retail CVD  = cumulative delta of trades where price*qty < threshold_usd
Smart CVD   = cumulative delta of trades where price*qty >= threshold_usd
delta uses is_buyer_aggressor when present, falls back to side field

divergence_score = (smart_cvd - retail_cvd) / (abs(smart_cvd) + abs(retail_cvd) + 1e-8)

Signals:
  |score| < 0.15 and same direction  → "aligned"
  |score| < 0.15 and diff/no dir     → "neutral"
  score >= 0.15                       → "accumulation"
  score <= -0.15                      → "distribution"
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import compute_smart_money_divergence  # noqa: E402


# ── helpers ───────────────────────────────────────────────────────────────────

def _trade(ts, price, qty, side="buy", is_buyer_aggressor=None):
    d = {"ts": float(ts), "price": float(price), "qty": float(qty), "side": side}
    if is_buyer_aggressor is not None:
        d["is_buyer_aggressor"] = int(is_buyer_aggressor)
    return d


def _large_buy(ts=0, usd=50000):
    """Large buy: price*qty >= threshold."""
    return _trade(ts, price=usd, qty=1.0, side="buy")

def _large_sell(ts=0, usd=50000):
    return _trade(ts, price=usd, qty=1.0, side="sell")

def _small_buy(ts=0, usd=500):
    return _trade(ts, price=usd, qty=1.0, side="buy")

def _small_sell(ts=0, usd=500):
    return _trade(ts, price=usd, qty=1.0, side="sell")


# ═══════════════════════════════════════════════════════════════════════════════
# Structure
# ═══════════════════════════════════════════════════════════════════════════════

class TestStructure:
    def test_empty_trades_returns_valid_dict(self):
        r = compute_smart_money_divergence([], threshold_usd=10000, bucket_seconds=300)
        assert isinstance(r, dict)

    def test_result_has_required_fields(self):
        r = compute_smart_money_divergence([_large_buy()], threshold_usd=10000)
        for f in ("smart_cvd", "retail_cvd", "smart_trade_count", "retail_trade_count",
                  "divergence_score", "signal", "smart_pct", "divergence_detected", "buckets"):
            assert f in r, f"missing: {f}"

    def test_bucket_has_required_fields(self):
        r = compute_smart_money_divergence([_large_buy(ts=0), _small_buy(ts=1)],
                                           threshold_usd=10000, bucket_seconds=300)
        b = r["buckets"][0]
        for f in ("ts", "smart_cvd", "retail_cvd"):
            assert f in b, f"missing bucket field: {f}"

    def test_empty_gives_neutral_signal(self):
        r = compute_smart_money_divergence([])
        assert r["signal"] == "neutral"
        assert r["divergence_detected"] is False

    def test_all_values_zero_on_empty(self):
        r = compute_smart_money_divergence([])
        assert r["smart_cvd"] == pytest.approx(0.0)
        assert r["retail_cvd"] == pytest.approx(0.0)
        assert r["smart_trade_count"] == 0
        assert r["retail_trade_count"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# Trade classification
# ═══════════════════════════════════════════════════════════════════════════════

class TestTradeClassification:
    def test_large_buy_goes_to_smart_cvd(self):
        r = compute_smart_money_divergence([_large_buy(usd=50000)], threshold_usd=10000)
        assert r["smart_cvd"] == pytest.approx(50000.0)
        assert r["retail_cvd"] == pytest.approx(0.0)

    def test_large_sell_goes_to_smart_cvd_negative(self):
        r = compute_smart_money_divergence([_large_sell(usd=50000)], threshold_usd=10000)
        assert r["smart_cvd"] == pytest.approx(-50000.0)

    def test_small_buy_goes_to_retail_cvd(self):
        r = compute_smart_money_divergence([_small_buy(usd=500)], threshold_usd=10000)
        assert r["retail_cvd"] == pytest.approx(500.0)
        assert r["smart_cvd"] == pytest.approx(0.0)

    def test_small_sell_goes_to_retail_cvd_negative(self):
        r = compute_smart_money_divergence([_small_sell(usd=500)], threshold_usd=10000)
        assert r["retail_cvd"] == pytest.approx(-500.0)

    def test_exactly_at_threshold_is_smart(self):
        """price*qty == threshold_usd → smart (>=)."""
        t = _trade(0, price=10000.0, qty=1.0, side="buy")
        r = compute_smart_money_divergence([t], threshold_usd=10000)
        assert r["smart_cvd"] == pytest.approx(10000.0)
        assert r["smart_trade_count"] == 1
        assert r["retail_trade_count"] == 0

    def test_just_below_threshold_is_retail(self):
        t = _trade(0, price=9999.99, qty=1.0, side="buy")
        r = compute_smart_money_divergence([t], threshold_usd=10000)
        assert r["retail_cvd"] == pytest.approx(9999.99)
        assert r["retail_trade_count"] == 1
        assert r["smart_trade_count"] == 0

    def test_trade_counts_correct(self):
        trades = [_large_buy(ts=i) for i in range(3)] + [_small_buy(ts=i+10) for i in range(7)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000)
        assert r["smart_trade_count"] == 3
        assert r["retail_trade_count"] == 7

    def test_value_usd_uses_price_times_qty(self):
        """value = price * qty, not just price."""
        t = _trade(0, price=200.0, qty=60.0, side="buy")   # 200*60=12000 → smart
        r = compute_smart_money_divergence([t], threshold_usd=10000)
        assert r["smart_cvd"] == pytest.approx(12000.0)
        assert r["smart_trade_count"] == 1

    def test_is_buyer_aggressor_true_overrides_side(self):
        """is_buyer_aggressor=True → buy regardless of side field."""
        t = _trade(0, price=50000.0, qty=1.0, side="sell", is_buyer_aggressor=True)
        r = compute_smart_money_divergence([t], threshold_usd=10000)
        assert r["smart_cvd"] == pytest.approx(50000.0)   # treated as buy

    def test_is_buyer_aggressor_false_overrides_side(self):
        t = _trade(0, price=50000.0, qty=1.0, side="buy", is_buyer_aggressor=False)
        r = compute_smart_money_divergence([t], threshold_usd=10000)
        assert r["smart_cvd"] == pytest.approx(-50000.0)  # treated as sell


# ═══════════════════════════════════════════════════════════════════════════════
# Divergence score
# ═══════════════════════════════════════════════════════════════════════════════

class TestDivergenceScore:
    def test_score_formula(self):
        """divergence_score = (smart_cvd - retail_cvd) / (|smart_cvd| + |retail_cvd| + 1e-8)."""
        # Use threshold=50 so price=100 is smart and price=30 is retail
        trades = [_trade(0, price=100.0, qty=1.0, side="buy"),   # smart: 100 >= 50
                  _trade(1, price=30.0,  qty=1.0, side="sell")]  # retail: 30 < 50
        r = compute_smart_money_divergence(trades, threshold_usd=50)
        # smart_cvd=+100, retail_cvd=-30
        expected = (100 - (-30)) / (100 + 30 + 1e-8)
        assert r["divergence_score"] == pytest.approx(expected, rel=1e-4)

    def test_score_range_minus_one_to_one(self):
        import random; random.seed(42)
        trades = [
            _trade(i, price=random.uniform(100, 50000), qty=random.uniform(0.1, 10),
                   side=random.choice(["buy","sell"]))
            for i in range(50)
        ]
        r = compute_smart_money_divergence(trades, threshold_usd=10000)
        assert -1.0 <= r["divergence_score"] <= 1.0

    def test_perfect_accumulation_score_near_one(self):
        """Smart buys only, retail sells only → score ≈ +1."""
        trades = [_large_buy(usd=50000, ts=i) for i in range(5)] + \
                 [_small_sell(usd=100, ts=i+10) for i in range(5)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000)
        # smart=+250000, retail=-500 → score near +1
        assert r["divergence_score"] > 0.9

    def test_perfect_distribution_score_near_minus_one(self):
        """Smart sells only, retail buys only → score ≈ -1."""
        trades = [_large_sell(usd=50000, ts=i) for i in range(5)] + \
                 [_small_buy(usd=100, ts=i+10) for i in range(5)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000)
        assert r["divergence_score"] < -0.9

    def test_empty_score_is_zero(self):
        r = compute_smart_money_divergence([])
        assert r["divergence_score"] == pytest.approx(0.0)


# ═══════════════════════════════════════════════════════════════════════════════
# Signals
# ═══════════════════════════════════════════════════════════════════════════════

class TestSignals:
    def test_smart_buy_retail_sell_is_accumulation(self):
        """Classic accumulation: smart buying, retail selling."""
        trades = [_large_buy(usd=50000, ts=i) for i in range(5)] + \
                 [_small_sell(usd=500, ts=i+10) for i in range(5)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000)
        assert r["signal"] == "accumulation"
        assert r["divergence_detected"] is True

    def test_smart_sell_retail_buy_is_distribution(self):
        """Classic distribution: smart selling, retail buying."""
        trades = [_large_sell(usd=50000, ts=i) for i in range(5)] + \
                 [_small_buy(usd=500, ts=i+10) for i in range(5)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000)
        assert r["signal"] == "distribution"
        assert r["divergence_detected"] is True

    def test_both_buy_similar_magnitude_is_aligned(self):
        """Smart and retail both buying proportionally → aligned (low divergence score)."""
        # threshold=950: price=1000 is smart, price=900 is retail
        trades = [_trade(i, price=1000.0, qty=1.0, side="buy") for i in range(5)] + \
                 [_trade(i+10, price=900.0, qty=1.0, side="buy") for i in range(5)]
        r = compute_smart_money_divergence(trades, threshold_usd=950)
        # smart_cvd=+5000, retail_cvd=+4500 → score=(5000-4500)/(5000+4500+ε)=500/9500≈0.053 → aligned
        assert r["signal"] == "aligned"

    def test_both_sell_similar_magnitude_is_aligned(self):
        # threshold=950: price=1000 is smart, price=900 is retail
        trades = [_trade(i, price=1000.0, qty=1.0, side="sell") for i in range(5)] + \
                 [_trade(i+10, price=900.0, qty=1.0, side="sell") for i in range(5)]
        r = compute_smart_money_divergence(trades, threshold_usd=950)
        # smart_cvd=-5000, retail_cvd=-4500 → score=(-5000+4500)/(5000+4500+ε)=-500/9500≈-0.053 → aligned
        assert r["signal"] == "aligned"

    def test_balanced_is_neutral(self):
        """Equal smart buy and sell, equal retail → neutral."""
        trades = [_large_buy(usd=1000), _large_sell(usd=1000),
                  _small_buy(usd=100), _small_sell(usd=100)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000)
        assert r["signal"] == "neutral"
        assert r["divergence_detected"] is False

    def test_divergence_detected_false_when_neutral(self):
        r = compute_smart_money_divergence([])
        assert r["divergence_detected"] is False

    def test_divergence_detected_false_when_aligned(self):
        # threshold=950: price=1000 is smart, price=900 is retail — both buying → aligned
        trades = [_trade(i, price=1000.0, qty=1.0, side="buy") for i in range(5)] + \
                 [_trade(i+10, price=900.0, qty=1.0, side="buy") for i in range(5)]
        r = compute_smart_money_divergence(trades, threshold_usd=950)
        assert r["divergence_detected"] is False

    def test_divergence_detected_true_when_accumulation(self):
        trades = [_large_buy(usd=50000, ts=i) for i in range(5)] + \
                 [_small_sell(usd=500, ts=i+10) for i in range(5)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000)
        assert r["divergence_detected"] is True

    def test_custom_threshold_changes_classification(self):
        """With threshold=100k, a 50k trade is retail not smart."""
        t = _large_buy(usd=50000)
        r = compute_smart_money_divergence([t], threshold_usd=100000)
        assert r["retail_cvd"] == pytest.approx(50000.0)
        assert r["smart_cvd"] == pytest.approx(0.0)


# ═══════════════════════════════════════════════════════════════════════════════
# smart_pct
# ═══════════════════════════════════════════════════════════════════════════════

class TestSmartPct:
    def test_smart_pct_all_smart(self):
        trades = [_large_buy(usd=50000, ts=i) for i in range(5)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000)
        assert r["smart_pct"] == pytest.approx(1.0)

    def test_smart_pct_all_retail(self):
        trades = [_small_buy(usd=100, ts=i) for i in range(5)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000)
        assert r["smart_pct"] == pytest.approx(0.0)

    def test_smart_pct_mixed(self):
        """smart vol=3*50000=150000, retail vol=7*500=3500 → smart_pct=150000/153500."""
        trades = [_large_buy(usd=50000, ts=i) for i in range(3)] + \
                 [_small_buy(usd=500, ts=i+10) for i in range(7)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000)
        expected = 150000.0 / (150000.0 + 3500.0)
        assert r["smart_pct"] == pytest.approx(expected, rel=1e-4)

    def test_smart_pct_zero_on_empty(self):
        r = compute_smart_money_divergence([])
        assert r["smart_pct"] == pytest.approx(0.0)


# ═══════════════════════════════════════════════════════════════════════════════
# Bucket aggregation
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuckets:
    def test_trades_in_same_bucket_aggregated(self):
        trades = [_large_buy(ts=i*10, usd=50000) for i in range(5)]   # all in bucket 0
        r = compute_smart_money_divergence(trades, threshold_usd=10000, bucket_seconds=300)
        assert len(r["buckets"]) == 1

    def test_trades_in_different_buckets_separate(self):
        trades = [_large_buy(ts=0), _large_buy(ts=300), _large_buy(ts=600)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000, bucket_seconds=300)
        assert len(r["buckets"]) == 3

    def test_bucket_ts_aligns_to_floor(self):
        t = _large_buy(ts=350)
        r = compute_smart_money_divergence([t], threshold_usd=10000, bucket_seconds=300)
        assert r["buckets"][0]["ts"] == 300.0

    def test_bucket_smart_cvd_correct(self):
        trades = [_large_buy(ts=0, usd=30000), _large_sell(ts=60, usd=10000)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000, bucket_seconds=300)
        assert r["buckets"][0]["smart_cvd"] == pytest.approx(20000.0)

    def test_bucket_retail_cvd_correct(self):
        trades = [_small_buy(ts=0, usd=300), _small_sell(ts=60, usd=200)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000, bucket_seconds=300)
        assert r["buckets"][0]["retail_cvd"] == pytest.approx(100.0)

    def test_buckets_sorted_ascending(self):
        trades = [_large_buy(ts=600), _large_buy(ts=0), _large_buy(ts=300)]
        r = compute_smart_money_divergence(trades, threshold_usd=10000, bucket_seconds=300)
        ts_vals = [b["ts"] for b in r["buckets"]]
        assert ts_vals == sorted(ts_vals)

    def test_bucket_smart_and_retail_independent(self):
        """Each bucket tracks smart and retail separately."""
        trades = [
            _large_buy(ts=0, usd=1000),    # smart buy bucket 0 (1000 >= threshold 500)
            _small_sell(ts=60, usd=200),   # retail sell bucket 0 (200 < threshold 500)
        ]
        r = compute_smart_money_divergence(trades, threshold_usd=500, bucket_seconds=300)
        b = r["buckets"][0]
        assert b["smart_cvd"] == pytest.approx(1000.0)
        assert b["retail_cvd"] == pytest.approx(-200.0)

    def test_total_matches_sum_of_buckets(self):
        import random; random.seed(17)
        trades = [
            _trade(i*20, price=random.uniform(100, 60000),
                   qty=random.uniform(0.1, 5.0),
                   side=random.choice(["buy","sell"]))
            for i in range(30)
        ]
        r = compute_smart_money_divergence(trades, threshold_usd=10000, bucket_seconds=300)
        sum_smart  = sum(b["smart_cvd"]  for b in r["buckets"])
        sum_retail = sum(b["retail_cvd"] for b in r["buckets"])
        assert r["smart_cvd"]  == pytest.approx(sum_smart,  rel=1e-5)
        assert r["retail_cvd"] == pytest.approx(sum_retail, rel=1e-5)
