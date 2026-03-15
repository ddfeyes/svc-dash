"""Tests for compute_whale_clustering."""
import math
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from metrics import compute_whale_clustering

# ── helpers ───────────────────────────────────────────────────────────────────

def _trade(price, value_usd, side="buy", qty=None):
    if qty is None:
        qty = value_usd / price
    return {"price": float(price), "qty": float(qty), "side": side, "value_usd": float(value_usd)}


def _run(trades, **kwargs):
    return compute_whale_clustering(trades, **kwargs)


# Pre-built scenario with known math (bin_size=10, zone_sigma=0 → threshold=mean)
#
#   price 100: buy 10k + sell 5k  → total 15k, bin 0
#   price 110: buy 30k + sell 10k → total 40k, bin 1   ← DOMINANT
#   price 120: buy  8k             → total  8k, bin 2
#
# all_vols = [15k, 40k, 8k], mean = 21k, zone_threshold (sigma=0) = 21k
# 40k > 21k → bin1 is zone; 15k, 8k are not

TRADES_3BIN = [
    _trade(100, 10_000, "buy"),
    _trade(100,  5_000, "sell"),
    _trade(110, 30_000, "buy"),
    _trade(110, 10_000, "sell"),
    _trade(120,  8_000, "buy"),
]


# ── TestStructure ─────────────────────────────────────────────────────────────

class TestStructure:
    def test_returns_dict(self):
        assert isinstance(_run(TRADES_3BIN, bin_size=10), dict)

    def test_has_bins_key(self):
        assert "bins" in _run(TRADES_3BIN, bin_size=10)

    def test_has_zones_key(self):
        assert "zones" in _run(TRADES_3BIN, bin_size=10)

    def test_has_top_zone_price_key(self):
        assert "top_zone_price" in _run(TRADES_3BIN, bin_size=10)

    def test_has_bin_size_key(self):
        assert "bin_size" in _run(TRADES_3BIN, bin_size=10)

    def test_has_trade_count_key(self):
        assert "trade_count" in _run(TRADES_3BIN, bin_size=10)

    def test_has_total_usd_key(self):
        assert "total_usd" in _run(TRADES_3BIN, bin_size=10)

    def test_has_price_min_key(self):
        assert "price_min" in _run(TRADES_3BIN, bin_size=10)

    def test_has_price_max_key(self):
        assert "price_max" in _run(TRADES_3BIN, bin_size=10)

    def test_has_zone_threshold_usd_key(self):
        assert "zone_threshold_usd" in _run(TRADES_3BIN, bin_size=10)

    def test_has_non_empty_bins_key(self):
        assert "non_empty_bins" in _run(TRADES_3BIN, bin_size=10)

    def test_bins_is_list(self):
        assert isinstance(_run(TRADES_3BIN, bin_size=10)["bins"], list)

    def test_zones_is_list(self):
        assert isinstance(_run(TRADES_3BIN, bin_size=10)["zones"], list)

    def test_bin_has_required_fields(self):
        r = _run(TRADES_3BIN, bin_size=10)
        required = {"price_low", "price_high", "price_mid", "total_usd",
                    "buy_usd", "sell_usd", "count", "buy_count", "sell_count",
                    "is_zone", "dominance"}
        for b in r["bins"]:
            assert required.issubset(b.keys()), f"bin missing keys: {required - b.keys()}"

    def test_bin_size_reflected_in_result(self):
        r = _run(TRADES_3BIN, bin_size=10)
        assert r["bin_size"] == 10.0


# ── TestBinning ───────────────────────────────────────────────────────────────

class TestBinning:
    def test_same_price_same_bin(self):
        trades = [_trade(100, 10_000), _trade(100, 20_000)]
        r = _run(trades, bin_size=10)
        assert len(r["bins"]) == 1
        assert r["bins"][0]["count"] == 2

    def test_different_prices_different_bins(self):
        trades = [_trade(100, 10_000), _trade(150, 20_000)]
        r = _run(trades, bin_size=10)
        assert len(r["bins"]) == 2

    def test_trades_within_same_bin_aggregated(self):
        # 100 and 109 both fall in [100, 110) with bin_size=10
        trades = [_trade(100, 10_000), _trade(109, 5_000)]
        r = _run(trades, bin_size=10)
        assert len(r["bins"]) == 1
        assert abs(r["bins"][0]["total_usd"] - 15_000) < 1

    def test_trade_at_bin_boundary_goes_to_next(self):
        # price=110 is at the boundary: idx = (110-100)/10 = 1 (next bin)
        trades = [_trade(100, 10_000), _trade(110, 20_000)]
        r = _run(trades, bin_size=10)
        assert len(r["bins"]) == 2

    def test_bins_sorted_ascending_by_price(self):
        r = _run(TRADES_3BIN, bin_size=10)
        lows = [b["price_low"] for b in r["bins"]]
        assert lows == sorted(lows)

    def test_price_mid_is_bin_center(self):
        r = _run(TRADES_3BIN, bin_size=10)
        for b in r["bins"]:
            expected_mid = b["price_low"] + 10 / 2
            assert abs(b["price_mid"] - expected_mid) < 1e-9

    def test_price_high_equals_low_plus_bin_size(self):
        r = _run(TRADES_3BIN, bin_size=10)
        for b in r["bins"]:
            assert abs(b["price_high"] - (b["price_low"] + 10)) < 1e-9

    def test_price_min_max_from_trades(self):
        r = _run(TRADES_3BIN, bin_size=10)
        assert r["price_min"] == 100.0
        assert r["price_max"] == 120.0

    def test_auto_bin_size_computed_from_range(self):
        trades = [_trade(100, 10_000), _trade(200, 20_000)]
        r = _run(trades, n_bins=10)
        # range=100, n_bins=10 → bin_size=10
        assert abs(r["bin_size"] - 10.0) < 1e-9

    def test_only_non_empty_bins_returned(self):
        # Trades at 100 and 200 with bin_size=10 → bins 0 and 10, no in-between
        trades = [_trade(100, 10_000), _trade(200, 20_000)]
        r = _run(trades, bin_size=10)
        assert r["non_empty_bins"] == 2
        assert len(r["bins"]) == 2


# ── TestAggregation ───────────────────────────────────────────────────────────

class TestAggregation:
    def test_total_usd_is_buy_plus_sell(self):
        r = _run(TRADES_3BIN, bin_size=10)
        for b in r["bins"]:
            assert abs(b["total_usd"] - (b["buy_usd"] + b["sell_usd"])) < 1

    def test_buy_usd_only_buy_side(self):
        r = _run(TRADES_3BIN, bin_size=10)
        bin0 = next(b for b in r["bins"] if b["price_low"] == 100.0)
        assert abs(bin0["buy_usd"] - 10_000) < 1

    def test_sell_usd_only_sell_side(self):
        r = _run(TRADES_3BIN, bin_size=10)
        bin0 = next(b for b in r["bins"] if b["price_low"] == 100.0)
        assert abs(bin0["sell_usd"] - 5_000) < 1

    def test_count_equals_buy_count_plus_sell_count(self):
        r = _run(TRADES_3BIN, bin_size=10)
        for b in r["bins"]:
            assert b["count"] == b["buy_count"] + b["sell_count"]

    def test_trade_count_total(self):
        r = _run(TRADES_3BIN, bin_size=10)
        assert r["trade_count"] == 5

    def test_total_usd_result_sum(self):
        r = _run(TRADES_3BIN, bin_size=10)
        expected = 10_000 + 5_000 + 30_000 + 10_000 + 8_000
        assert abs(r["total_usd"] - expected) < 1

    def test_bin1_has_correct_totals(self):
        r = _run(TRADES_3BIN, bin_size=10)
        bin1 = next(b for b in r["bins"] if b["price_low"] == 110.0)
        assert abs(bin1["total_usd"] - 40_000) < 1
        assert abs(bin1["buy_usd"]   - 30_000) < 1
        assert abs(bin1["sell_usd"]  - 10_000) < 1
        assert bin1["count"] == 2


# ── TestZoneDetection ─────────────────────────────────────────────────────────

class TestZoneDetection:
    def test_dominant_bin_is_zone(self):
        # zone_sigma=0 → threshold=mean; bin1(40k) > mean(21k) → zone
        r = _run(TRADES_3BIN, bin_size=10, zone_sigma=0)
        bin1 = next(b for b in r["bins"] if b["price_low"] == 110.0)
        assert bin1["is_zone"] is True

    def test_smaller_bins_not_zones(self):
        r = _run(TRADES_3BIN, bin_size=10, zone_sigma=0)
        bin0 = next(b for b in r["bins"] if b["price_low"] == 100.0)
        bin2 = next(b for b in r["bins"] if b["price_low"] == 120.0)
        assert bin0["is_zone"] is False
        assert bin2["is_zone"] is False

    def test_uniform_bins_no_zones(self):
        # All bins at same volume → std=0 → threshold=mean → strict > → no zones
        trades = [
            _trade(100, 10_000), _trade(110, 10_000), _trade(120, 10_000),
        ]
        r = _run(trades, bin_size=10)
        assert all(not b["is_zone"] for b in r["bins"])

    def test_top_zone_price_is_highest_usd_zone(self):
        r = _run(TRADES_3BIN, bin_size=10, zone_sigma=0)
        # bin1 at 110 is the only zone (mid = 115)
        assert abs(r["top_zone_price"] - 115.0) < 1e-6

    def test_top_zone_price_none_when_no_zones(self):
        trades = [_trade(100, 10_000), _trade(110, 10_000)]
        r = _run(trades, bin_size=10)
        assert r["top_zone_price"] is None

    def test_zones_list_contains_zone_mids(self):
        r = _run(TRADES_3BIN, bin_size=10, zone_sigma=0)
        assert 115.0 in r["zones"]  # bin1 mid = 110 + 10/2 = 115

    def test_zones_list_empty_when_no_zones(self):
        trades = [_trade(100, 10_000), _trade(110, 10_000)]
        r = _run(trades, bin_size=10)
        assert r["zones"] == []

    def test_zone_sigma_zero_uses_mean_as_threshold(self):
        r = _run(TRADES_3BIN, bin_size=10, zone_sigma=0)
        # threshold = mean of ALL bins (3 bins here, all non-empty)
        expected_mean = (15_000 + 40_000 + 8_000) / 3
        assert abs(r["zone_threshold_usd"] - expected_mean) < 1

    def test_high_zone_sigma_produces_no_zones(self):
        r = _run(TRADES_3BIN, bin_size=10, zone_sigma=100)
        assert r["zones"] == []
        assert all(not b["is_zone"] for b in r["bins"])

    def test_multiple_zones_returned(self):
        # Two dominant bins, one tiny
        trades = [
            _trade(100, 50_000), _trade(110, 50_000),
            _trade(120,    100),
        ]
        r = _run(trades, bin_size=10, zone_sigma=0)
        zone_bins = [b for b in r["bins"] if b["is_zone"]]
        assert len(zone_bins) == 2


# ── TestDominance ─────────────────────────────────────────────────────────────

class TestDominance:
    def test_buy_dominant(self):
        trades = [_trade(100, 30_000, "buy"), _trade(100, 10_000, "sell"),
                  _trade(110, 10_000)]
        r = _run(trades, bin_size=10)
        bin0 = next(b for b in r["bins"] if b["price_low"] == 100.0)
        assert bin0["dominance"] == "buy"

    def test_sell_dominant(self):
        trades = [_trade(100, 10_000, "buy"), _trade(100, 30_000, "sell"),
                  _trade(110, 10_000)]
        r = _run(trades, bin_size=10)
        bin0 = next(b for b in r["bins"] if b["price_low"] == 100.0)
        assert bin0["dominance"] == "sell"

    def test_neutral_when_equal(self):
        trades = [_trade(100, 10_000, "buy"), _trade(100, 10_000, "sell"),
                  _trade(110, 10_000)]
        r = _run(trades, bin_size=10)
        bin0 = next(b for b in r["bins"] if b["price_low"] == 100.0)
        assert bin0["dominance"] == "neutral"

    def test_buy_only_is_buy(self):
        trades = [_trade(100, 50_000, "buy"), _trade(110, 10_000)]
        r = _run(trades, bin_size=10)
        bin0 = next(b for b in r["bins"] if b["price_low"] == 100.0)
        assert bin0["dominance"] == "buy"

    def test_sell_only_is_sell(self):
        trades = [_trade(100, 50_000, "sell"), _trade(110, 10_000)]
        r = _run(trades, bin_size=10)
        bin0 = next(b for b in r["bins"] if b["price_low"] == 100.0)
        assert bin0["dominance"] == "sell"


# ── TestEdgeCases ─────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_empty_trades_safe_return(self):
        r = _run([])
        assert r["trade_count"] == 0
        assert r["bins"] == []
        assert r["zones"] == []
        assert r["top_zone_price"] is None

    def test_single_trade(self):
        r = _run([_trade(100, 50_000)])
        assert r["trade_count"] == 1
        assert len(r["bins"]) == 1
        assert r["non_empty_bins"] == 1

    def test_all_trades_same_price(self):
        trades = [_trade(100, 10_000), _trade(100, 20_000), _trade(100, 30_000)]
        r = _run(trades, bin_size=10)
        assert len(r["bins"]) == 1
        assert r["bins"][0]["count"] == 3
        assert r["top_zone_price"] is None  # single bin, std=0, no zone

    def test_two_bins_only(self):
        trades = [_trade(100, 10_000), _trade(200, 10_000)]
        r = _run(trades, bin_size=10)
        assert r["non_empty_bins"] == 2
        assert isinstance(r["bins"], list)

    def test_custom_bin_size_respected(self):
        trades = [_trade(100, 10_000), _trade(101, 20_000)]
        r_small = _run(trades, bin_size=1)   # 2 separate bins
        r_big   = _run(trades, bin_size=10)  # both in same bin
        assert r_small["non_empty_bins"] == 2
        assert r_big["non_empty_bins"] == 1

    def test_non_empty_bins_count_correct(self):
        r = _run(TRADES_3BIN, bin_size=10)
        assert r["non_empty_bins"] == 3
