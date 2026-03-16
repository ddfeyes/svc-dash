"""
Unit / smoke tests for /api/nft-market-pulse.

NFT Market Pulse — floor price trends for top 5 collections (7d),
wash-trade-adjusted volume, blue-chip index vs BTC correlation,
listing/sales ratio as liquidity signal.

Helpers covered:
  - _nft_floor_change_pct
  - _nft_wash_adjusted_volume
  - _nft_bluechip_index
  - _nft_btc_correlation
  - _nft_listing_sales_ratio
  - _nft_liquidity_label
  - _nft_trend_direction
  - _nft_volume_zscore
  - SAMPLE_RESPONSE shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys, os, pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _nft_floor_change_pct,
    _nft_wash_adjusted_volume,
    _nft_bluechip_index,
    _nft_btc_correlation,
    _nft_listing_sales_ratio,
    _nft_liquidity_label,
    _nft_trend_direction,
    _nft_volume_zscore,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "collections": {
        "Bored Ape Yacht Club": {
            "floor_eth": 12.5,
            "floor_change_24h_pct": -2.3,
            "floor_change_7d_pct": 5.1,
            "volume_24h_eth": 450.0,
            "volume_adjusted_eth": 380.0,
            "wash_rate": 0.156,
            "listings": 1200,
            "sales_24h": 45,
            "listing_sales_ratio": 26.7,
            "liquidity": "warm",
            "trend": "rising",
            "history_7d": [
                {"date": "2024-11-14", "floor_eth": 11.8},
                {"date": "2024-11-15", "floor_eth": 12.0},
                {"date": "2024-11-16", "floor_eth": 11.9},
                {"date": "2024-11-17", "floor_eth": 12.1},
                {"date": "2024-11-18", "floor_eth": 12.3},
                {"date": "2024-11-19", "floor_eth": 12.4},
                {"date": "2024-11-20", "floor_eth": 12.5},
            ],
        },
        "CryptoPunks": {
            "floor_eth": 42.0,
            "floor_change_24h_pct": 0.5,
            "floor_change_7d_pct": 2.4,
            "volume_24h_eth": 1260.0,
            "volume_adjusted_eth": 1050.0,
            "wash_rate": 0.167,
            "listings": 900,
            "sales_24h": 30,
            "listing_sales_ratio": 30.0,
            "liquidity": "cool",
            "trend": "rising",
            "history_7d": [
                {"date": "2024-11-14", "floor_eth": 41.0},
                {"date": "2024-11-20", "floor_eth": 42.0},
            ],
        },
        "Azuki": {
            "floor_eth": 5.8,
            "floor_change_24h_pct": -1.7,
            "floor_change_7d_pct": -3.2,
            "volume_24h_eth": 290.0,
            "volume_adjusted_eth": 245.0,
            "wash_rate": 0.155,
            "listings": 1800,
            "sales_24h": 32,
            "listing_sales_ratio": 56.25,
            "liquidity": "cold",
            "trend": "falling",
            "history_7d": [
                {"date": "2024-11-14", "floor_eth": 6.0},
                {"date": "2024-11-20", "floor_eth": 5.8},
            ],
        },
        "Pudgy Penguins": {
            "floor_eth": 8.3,
            "floor_change_24h_pct": 3.1,
            "floor_change_7d_pct": 7.5,
            "volume_24h_eth": 415.0,
            "volume_adjusted_eth": 350.0,
            "wash_rate": 0.157,
            "listings": 600,
            "sales_24h": 50,
            "listing_sales_ratio": 12.0,
            "liquidity": "warm",
            "trend": "rising",
            "history_7d": [
                {"date": "2024-11-14", "floor_eth": 7.7},
                {"date": "2024-11-20", "floor_eth": 8.3},
            ],
        },
        "Doodles": {
            "floor_eth": 2.1,
            "floor_change_24h_pct": -0.9,
            "floor_change_7d_pct": -1.4,
            "volume_24h_eth": 84.0,
            "volume_adjusted_eth": 70.0,
            "wash_rate": 0.167,
            "listings": 2200,
            "sales_24h": 22,
            "listing_sales_ratio": 100.0,
            "liquidity": "cold",
            "trend": "falling",
            "history_7d": [
                {"date": "2024-11-14", "floor_eth": 2.13},
                {"date": "2024-11-20", "floor_eth": 2.1},
            ],
        },
    },
    "bluechip_index": {
        "value": 58.3,
        "change_24h_pct": 0.4,
        "change_7d_pct": 2.1,
        "btc_correlation": 0.72,
        "trend": "rising",
    },
    "market": {
        "total_volume_24h_eth": 2499.0,
        "adjusted_volume_24h_eth": 2095.0,
        "wash_trade_pct": 16.2,
        "volume_zscore": 0.8,
        "avg_listing_sales_ratio": 45.0,
        "market_liquidity": "cool",
    },
    "history_7d": [
        {"date": "2024-11-14", "index_value": 55.0, "total_volume_eth": 2200.0},
        {"date": "2024-11-20", "index_value": 58.3, "total_volume_eth": 2499.0},
    ],
    "description": "NFT market: blue-chip index rising — moderate liquidity",
}


# ===========================================================================
# 1. _nft_floor_change_pct
# ===========================================================================

class TestNftFloorChangePct:
    def test_positive_change(self):
        assert _nft_floor_change_pct(11.0, 10.0) > 0

    def test_negative_change(self):
        assert _nft_floor_change_pct(9.0, 10.0) < 0

    def test_no_change_is_zero(self):
        assert _nft_floor_change_pct(10.0, 10.0) == pytest.approx(0.0)

    def test_zero_previous_returns_zero(self):
        assert _nft_floor_change_pct(10.0, 0.0) == pytest.approx(0.0)

    def test_correct_magnitude(self):
        assert _nft_floor_change_pct(11.0, 10.0) == pytest.approx(10.0, abs=0.01)

    def test_returns_float(self):
        assert isinstance(_nft_floor_change_pct(11.0, 10.0), float)


# ===========================================================================
# 2. _nft_wash_adjusted_volume
# ===========================================================================

class TestNftWashAdjustedVolume:
    def test_zero_wash_returns_full_volume(self):
        assert _nft_wash_adjusted_volume(1000.0, 0.0) == pytest.approx(1000.0)

    def test_full_wash_returns_zero(self):
        assert _nft_wash_adjusted_volume(1000.0, 1.0) == pytest.approx(0.0)

    def test_typical_wash_rate(self):
        result = _nft_wash_adjusted_volume(1000.0, 0.2)
        assert result == pytest.approx(800.0, abs=0.01)

    def test_result_less_than_raw(self):
        assert _nft_wash_adjusted_volume(500.0, 0.15) < 500.0

    def test_wash_rate_clamped_above_1(self):
        assert _nft_wash_adjusted_volume(1000.0, 1.5) == pytest.approx(0.0, abs=0.01)

    def test_returns_float(self):
        assert isinstance(_nft_wash_adjusted_volume(1000.0, 0.15), float)


# ===========================================================================
# 3. _nft_bluechip_index
# ===========================================================================

class TestNftBluechipIndex:
    def test_empty_returns_zero(self):
        assert _nft_bluechip_index({}) == pytest.approx(0.0)

    def test_all_zero_returns_zero(self):
        assert _nft_bluechip_index({"A": 0.0, "B": 0.0}) == pytest.approx(0.0, abs=0.01)

    def test_result_in_0_100_range(self):
        result = _nft_bluechip_index({"BAYC": 12.5, "Punks": 42.0, "Azuki": 5.8})
        assert 0.0 <= result <= 100.0

    def test_higher_floors_higher_index(self):
        low  = _nft_bluechip_index({"A": 1.0, "B": 2.0, "C": 3.0})
        high = _nft_bluechip_index({"A": 10.0, "B": 20.0, "C": 30.0})
        assert high > low

    def test_returns_float(self):
        assert isinstance(_nft_bluechip_index({"A": 10.0}), float)

    def test_reference_sum_returns_100(self):
        # All floors sum to reference → 100
        assert _nft_bluechip_index({"A": 500.0}, reference=500.0) == pytest.approx(100.0, abs=0.01)


# ===========================================================================
# 4. _nft_btc_correlation
# ===========================================================================

class TestNftBtcCorrelation:
    def test_empty_returns_zero(self):
        assert _nft_btc_correlation([], []) == pytest.approx(0.0)

    def test_short_series_returns_zero(self):
        assert _nft_btc_correlation([1.0], [1.0]) == pytest.approx(0.0)

    def test_identical_series_returns_one(self):
        s = [10.0, 20.0, 30.0, 40.0, 50.0]
        assert _nft_btc_correlation(s, s) == pytest.approx(1.0, abs=0.01)

    def test_opposite_series_returns_negative(self):
        idx = [10.0, 20.0, 30.0, 40.0, 50.0]
        btc = [50.0, 40.0, 30.0, 20.0, 10.0]
        assert _nft_btc_correlation(idx, btc) < 0

    def test_result_in_minus1_to_1(self):
        idx = [10.0, 12.0, 11.0, 14.0, 13.0, 15.0, 16.0]
        btc = [30000, 31000, 29000, 32000, 31500, 33000, 34000]
        r = _nft_btc_correlation(idx, btc)
        assert -1.0 <= r <= 1.0

    def test_mismatched_lengths_returns_zero(self):
        assert _nft_btc_correlation([1.0, 2.0], [1.0, 2.0, 3.0]) == pytest.approx(0.0)


# ===========================================================================
# 5. _nft_listing_sales_ratio
# ===========================================================================

class TestNftListingSalesRatio:
    def test_basic_ratio(self):
        assert _nft_listing_sales_ratio(100, 10) == pytest.approx(10.0, abs=0.01)

    def test_zero_sales_returns_large_value(self):
        assert _nft_listing_sales_ratio(1000, 0) >= 999.0

    def test_more_listings_higher_ratio(self):
        assert _nft_listing_sales_ratio(200, 10) > _nft_listing_sales_ratio(100, 10)

    def test_more_sales_lower_ratio(self):
        assert _nft_listing_sales_ratio(100, 20) < _nft_listing_sales_ratio(100, 10)

    def test_returns_float(self):
        assert isinstance(_nft_listing_sales_ratio(100, 10), float)

    def test_equal_listings_sales_is_one(self):
        assert _nft_listing_sales_ratio(50, 50) == pytest.approx(1.0, abs=0.01)


# ===========================================================================
# 6. _nft_liquidity_label
# ===========================================================================

class TestNftLiquidityLabel:
    def test_very_low_ratio_is_hot(self):
        assert _nft_liquidity_label(5.0) == "hot"

    def test_low_ratio_is_warm(self):
        assert _nft_liquidity_label(15.0) == "warm"

    def test_mid_ratio_is_cool(self):
        assert _nft_liquidity_label(30.0) == "cool"

    def test_high_ratio_is_cold(self):
        assert _nft_liquidity_label(50.0) == "cold"

    def test_returns_valid_string(self):
        assert _nft_liquidity_label(20.0) in ("hot", "warm", "cool", "cold")


# ===========================================================================
# 7. _nft_trend_direction
# ===========================================================================

class TestNftTrendDirection:
    def test_empty_returns_stable(self):
        assert _nft_trend_direction([]) == "stable"

    def test_single_returns_stable(self):
        assert _nft_trend_direction([10.0]) == "stable"

    def test_rising_prices(self):
        assert _nft_trend_direction([10.0, 11.0, 12.0, 13.0, 14.0]) == "rising"

    def test_falling_prices(self):
        assert _nft_trend_direction([14.0, 13.0, 12.0, 11.0, 10.0]) == "falling"

    def test_flat_prices_is_stable(self):
        assert _nft_trend_direction([10.0] * 7) == "stable"

    def test_returns_valid_string(self):
        assert _nft_trend_direction([1.0, 2.0, 3.0]) in ("rising", "falling", "stable")


# ===========================================================================
# 8. _nft_volume_zscore
# ===========================================================================

class TestNftVolumeZscore:
    def test_empty_history_returns_zero(self):
        assert _nft_volume_zscore(500.0, []) == pytest.approx(0.0)

    def test_single_history_returns_zero(self):
        assert _nft_volume_zscore(500.0, [500.0]) == pytest.approx(0.0)

    def test_current_at_mean_returns_near_zero(self):
        history = [400.0, 500.0, 600.0, 500.0, 400.0]
        mean = sum(history) / len(history)
        assert abs(_nft_volume_zscore(mean, history)) < 0.01

    def test_above_mean_returns_positive(self):
        history = [100.0, 200.0, 150.0, 180.0]
        assert _nft_volume_zscore(10000.0, history) > 0

    def test_below_mean_returns_negative(self):
        history = [800.0, 900.0, 850.0, 950.0]
        assert _nft_volume_zscore(0.0, history) < 0

    def test_uniform_history_returns_zero(self):
        history = [500.0] * 10
        assert _nft_volume_zscore(500.0, history) == pytest.approx(0.0, abs=0.01)


# ===========================================================================
# 9. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    COLLECTIONS = (
        "Bored Ape Yacht Club", "CryptoPunks", "Azuki", "Pudgy Penguins", "Doodles"
    )
    COLLECTION_KEYS = (
        "floor_eth", "floor_change_24h_pct", "floor_change_7d_pct",
        "volume_24h_eth", "volume_adjusted_eth", "wash_rate",
        "listings", "sales_24h", "listing_sales_ratio",
        "liquidity", "trend", "history_7d",
    )

    def test_has_collections_dict(self):
        assert isinstance(SAMPLE_RESPONSE["collections"], dict)

    def test_has_all_five_collections(self):
        for c in self.COLLECTIONS:
            assert c in SAMPLE_RESPONSE["collections"], f"Missing collection: {c}"

    def test_each_collection_has_required_keys(self):
        for name, coll in SAMPLE_RESPONSE["collections"].items():
            for k in self.COLLECTION_KEYS:
                assert k in coll, f"{name} missing key '{k}'"

    def test_liquidity_values_valid(self):
        for name, coll in SAMPLE_RESPONSE["collections"].items():
            assert coll["liquidity"] in ("hot", "warm", "cool", "cold"), \
                f"{name} invalid liquidity '{coll['liquidity']}'"

    def test_trend_values_valid(self):
        for name, coll in SAMPLE_RESPONSE["collections"].items():
            assert coll["trend"] in ("rising", "falling", "stable"), \
                f"{name} invalid trend '{coll['trend']}'"

    def test_wash_rate_in_range(self):
        for name, coll in SAMPLE_RESPONSE["collections"].items():
            assert 0.0 <= coll["wash_rate"] <= 1.0, f"{name} wash_rate out of range"

    def test_history_7d_is_list(self):
        for name, coll in SAMPLE_RESPONSE["collections"].items():
            assert isinstance(coll["history_7d"], list), f"{name} history_7d not a list"

    def test_has_bluechip_index_dict(self):
        assert isinstance(SAMPLE_RESPONSE["bluechip_index"], dict)

    def test_bluechip_index_has_required_keys(self):
        for k in ("value", "change_24h_pct", "change_7d_pct", "btc_correlation", "trend"):
            assert k in SAMPLE_RESPONSE["bluechip_index"], f"bluechip_index missing '{k}'"

    def test_btc_correlation_in_range(self):
        r = SAMPLE_RESPONSE["bluechip_index"]["btc_correlation"]
        assert -1.0 <= r <= 1.0

    def test_has_market_dict(self):
        assert isinstance(SAMPLE_RESPONSE["market"], dict)

    def test_market_has_required_keys(self):
        for k in ("total_volume_24h_eth", "adjusted_volume_24h_eth",
                  "wash_trade_pct", "volume_zscore",
                  "avg_listing_sales_ratio", "market_liquidity"):
            assert k in SAMPLE_RESPONSE["market"], f"market missing '{k}'"

    def test_market_liquidity_valid(self):
        assert SAMPLE_RESPONSE["market"]["market_liquidity"] in ("hot", "warm", "cool", "cold")

    def test_has_history_7d_list(self):
        assert isinstance(SAMPLE_RESPONSE["history_7d"], list)

    def test_history_items_have_keys(self):
        for item in SAMPLE_RESPONSE["history_7d"]:
            for k in ("date", "index_value", "total_volume_eth"):
                assert k in item, f"history_7d item missing '{k}'"

    def test_has_description_string(self):
        assert isinstance(SAMPLE_RESPONSE["description"], str)


# ===========================================================================
# 10. Structural
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api = open(os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")).read()
        assert "/nft-market-pulse" in api, "/nft-market-pulse route missing"

    def test_html_card_exists(self):
        html = open(os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")).read()
        assert "card-nft-market-pulse" in html, "card-nft-market-pulse missing"

    def test_js_render_function_exists(self):
        js = open(os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")).read()
        assert "renderNftMarketPulse" in js, "renderNftMarketPulse missing"

    def test_js_api_call_to_endpoint(self):
        js = open(os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")).read()
        assert "/nft-market-pulse" in js, "/nft-market-pulse call missing"
