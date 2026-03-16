"""Unit tests for the cross-chain arbitrage monitor card (50+ tests).

Tests cover:
- compute_cross_chain_arb_monitor() output structure and field types
- Per-asset chain data (prices, liquidity, gas costs)
- Best-spread computation and fee-adjusted profit math
- Spread calculation helpers (pure logic)
- Fee-adjusted profit helpers
- Signal classification thresholds
- Top-opportunities ordering and fields
- Bridge route list completeness
- Arb-frequency heatmap dimensions
- API endpoint availability
- HTML card presence in index.html
- JS renderer presence in app.js
"""

import asyncio
import os
import sys
import tempfile

import pytest

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test_cca.db")
os.environ["SYMBOL_BINANCE"] = "BANANAS31USDT"
os.environ["SYMBOL_BYBIT"] = "BANANAS31USDT"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage import init_db
from metrics import compute_cross_chain_arb_monitor

# ---------------------------------------------------------------------------
# Helpers (mirror the logic from metrics.py for pure-logic tests)
# ---------------------------------------------------------------------------

def calc_spread_bps(buy_price: float, sell_price: float) -> float:
    """Spread in basis points: ((sell - buy) / buy) * 10000."""
    if buy_price == 0:
        return 0.0
    return ((sell_price - buy_price) / buy_price) * 10_000


def calc_fee_adjusted_profit(
    spread_bps: float,
    trade_size_usd: float,
    bridge_fee_bps: float,
    gas_buy: float,
    gas_sell: float,
) -> float:
    gross = (spread_bps / 10_000) * trade_size_usd
    bridge_cost = (bridge_fee_bps / 10_000) * trade_size_usd
    return gross - bridge_cost - gas_buy - gas_sell


def classify_signal(fee_adj_bps: float) -> str:
    if fee_adj_bps > 10.0:
        return "high_opportunity"
    if fee_adj_bps > 3.0:
        return "moderate"
    return "low"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def arb_data(event_loop):
    event_loop.run_until_complete(init_db())
    return event_loop.run_until_complete(compute_cross_chain_arb_monitor())


# ===========================================================================
# Group 1: Top-level output structure (10 tests)
# ===========================================================================

class TestOutputStructure:
    def test_returns_dict(self, arb_data):
        assert isinstance(arb_data, dict)

    def test_has_status_ok(self, arb_data):
        assert arb_data.get("status") == "ok"

    def test_has_ts_field(self, arb_data):
        assert "ts" in arb_data
        assert isinstance(arb_data["ts"], float)
        assert arb_data["ts"] > 0

    def test_has_assets_field(self, arb_data):
        assert "assets" in arb_data
        assert isinstance(arb_data["assets"], dict)

    def test_has_top_opportunities(self, arb_data):
        assert "top_opportunities" in arb_data
        assert isinstance(arb_data["top_opportunities"], list)

    def test_has_bridge_routes(self, arb_data):
        assert "bridge_routes" in arb_data
        assert isinstance(arb_data["bridge_routes"], list)

    def test_has_arb_frequency_heatmap(self, arb_data):
        assert "arb_frequency_heatmap" in arb_data
        assert isinstance(arb_data["arb_frequency_heatmap"], dict)

    def test_has_signal(self, arb_data):
        assert "signal" in arb_data
        assert isinstance(arb_data["signal"], str)

    def test_has_best_opportunity(self, arb_data):
        assert "best_opportunity" in arb_data

    def test_assets_contain_btc_eth_usdc(self, arb_data):
        assets = arb_data["assets"]
        assert "BTC" in assets
        assert "ETH" in assets
        assert "USDC" in assets


# ===========================================================================
# Group 2: Per-asset chain data (10 tests)
# ===========================================================================

EXPECTED_CHAINS = {"ETH", "BSC", "ARB", "OP", "BASE"}


class TestChainData:
    def test_each_asset_has_five_chains(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            assert len(aData["chains"]) == 5, f"{asset} should have 5 chains"

    def test_chain_names_correct(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            assert set(aData["chains"].keys()) == EXPECTED_CHAINS

    def test_btc_price_realistic(self, arb_data):
        chains = arb_data["assets"]["BTC"]["chains"]
        for c, v in chains.items():
            assert 50_000 < v["price"] < 80_000, f"BTC price on {c} out of range"

    def test_eth_price_realistic(self, arb_data):
        chains = arb_data["assets"]["ETH"]["chains"]
        for c, v in chains.items():
            assert 2_000 < v["price"] < 5_000, f"ETH price on {c} out of range"

    def test_usdc_price_near_one(self, arb_data):
        chains = arb_data["assets"]["USDC"]["chains"]
        for c, v in chains.items():
            assert 0.99 < v["price"] < 1.01, f"USDC price on {c} not near 1"

    def test_chains_have_price_field(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            for chain, cData in aData["chains"].items():
                assert "price" in cData, f"{asset}/{chain} missing price"

    def test_chains_have_liquidity_usd(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            for chain, cData in aData["chains"].items():
                assert "liquidity_usd" in cData

    def test_chains_have_gas_cost_usd(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            for chain, cData in aData["chains"].items():
                assert "gas_cost_usd" in cData

    def test_prices_are_positive(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            for chain, cData in aData["chains"].items():
                assert cData["price"] > 0

    def test_liquidity_is_positive(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            for chain, cData in aData["chains"].items():
                assert cData["liquidity_usd"] > 0


# ===========================================================================
# Group 3: Best-spread structure per asset (10 tests)
# ===========================================================================

class TestBestSpread:
    def test_each_asset_has_best_spread(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            assert "best_spread" in aData, f"{asset} missing best_spread"

    def test_best_spread_has_buy_chain(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            assert "buy_chain" in aData["best_spread"]

    def test_best_spread_has_sell_chain(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            assert "sell_chain" in aData["best_spread"]

    def test_best_spread_bps_is_float(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            val = aData["best_spread"]["spread_bps"]
            assert isinstance(val, float), f"{asset} spread_bps not float"

    def test_best_spread_has_gross_profit(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            assert "gross_profit_usd" in aData["best_spread"]

    def test_best_spread_has_fee_adjusted_profit(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            assert "fee_adjusted_profit_usd" in aData["best_spread"]
            assert "fee_adjusted_profit_bps" in aData["best_spread"]

    def test_best_spread_has_is_profitable(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            assert "is_profitable" in aData["best_spread"]
            assert isinstance(aData["best_spread"]["is_profitable"], bool)

    def test_best_spread_bps_non_negative(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            assert aData["best_spread"]["spread_bps"] >= 0

    def test_buy_and_sell_chains_differ(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            bs = aData["best_spread"]
            assert bs["buy_chain"] != bs["sell_chain"]

    def test_bridge_protocol_present(self, arb_data):
        for asset, aData in arb_data["assets"].items():
            assert "bridge_protocol" in aData["best_spread"]
            assert aData["best_spread"]["bridge_protocol"] != ""


# ===========================================================================
# Group 4: Spread and profit calculation helpers (8 tests)
# ===========================================================================

class TestSpreadCalculation:
    def test_spread_bps_formula_basic(self):
        bps = calc_spread_bps(100.0, 101.0)
        assert abs(bps - 100.0) < 0.01

    def test_spread_zero_when_prices_equal(self):
        assert calc_spread_bps(100.0, 100.0) == 0.0

    def test_spread_positive_when_sell_higher(self):
        assert calc_spread_bps(100.0, 102.0) > 0

    def test_spread_negative_when_sell_lower(self):
        assert calc_spread_bps(100.0, 98.0) < 0

    def test_fee_adjusted_profit_formula(self):
        # 50 bps gross on 10k = $50, bridge 3 bps = $3, gas $1+$0.5
        profit = calc_fee_adjusted_profit(50.0, 10_000.0, 3.0, 1.0, 0.5)
        assert abs(profit - (50.0 - 3.0 - 1.5)) < 0.01

    def test_profit_positive_when_spread_exceeds_fees(self):
        profit = calc_fee_adjusted_profit(100.0, 10_000.0, 3.0, 0.5, 0.5)
        assert profit > 0

    def test_profit_negative_when_fees_exceed_spread(self):
        profit = calc_fee_adjusted_profit(1.0, 10_000.0, 10.0, 12.0, 0.5)
        assert profit < 0

    def test_spread_handles_zero_buy_price(self):
        # Should return 0 without exception
        bps = calc_spread_bps(0.0, 100.0)
        assert bps == 0.0


# ===========================================================================
# Group 5: Signal classification (5 tests)
# ===========================================================================

class TestSignalClassification:
    def test_high_opportunity_above_10_bps(self):
        assert classify_signal(15.0) == "high_opportunity"

    def test_moderate_between_3_and_10_bps(self):
        assert classify_signal(5.0) == "moderate"

    def test_low_at_or_below_3_bps(self):
        assert classify_signal(2.9) == "low"

    def test_signal_is_string(self, arb_data):
        assert isinstance(arb_data["signal"], str)

    def test_signal_valid_values(self, arb_data):
        assert arb_data["signal"] in {"high_opportunity", "moderate", "low"}


# ===========================================================================
# Group 6: Top opportunities (5 tests)
# ===========================================================================

class TestTopOpportunities:
    def test_top_opportunities_is_list(self, arb_data):
        assert isinstance(arb_data["top_opportunities"], list)

    def test_top_opportunities_not_empty(self, arb_data):
        assert len(arb_data["top_opportunities"]) > 0

    def test_opportunity_has_required_fields(self, arb_data):
        required = {
            "asset", "buy_chain", "sell_chain", "spread_bps",
            "fee_adjusted_profit_bps", "bridge_route", "bridge_time_sec",
            "is_profitable", "trade_size_usd", "fee_adjusted_profit_usd",
        }
        for opp in arb_data["top_opportunities"]:
            assert required <= opp.keys(), f"Missing fields: {required - opp.keys()}"

    def test_sorted_by_fee_adjusted_profit_descending(self, arb_data):
        opps = arb_data["top_opportunities"]
        profits = [o["fee_adjusted_profit_bps"] for o in opps]
        assert profits == sorted(profits, reverse=True)

    def test_opportunity_has_bridge_route_string(self, arb_data):
        for opp in arb_data["top_opportunities"]:
            assert isinstance(opp["bridge_route"], str)
            assert "via" in opp["bridge_route"]


# ===========================================================================
# Group 7: Bridge routes (5 tests)
# ===========================================================================

EXPECTED_PROTOCOLS = {"Stargate", "Across", "Hop", "Celer", "Synapse"}


class TestBridgeRoutes:
    def test_bridge_routes_is_list(self, arb_data):
        assert isinstance(arb_data["bridge_routes"], list)

    def test_bridge_routes_have_five_protocols(self, arb_data):
        protocols = {r["protocol"] for r in arb_data["bridge_routes"]}
        assert protocols == EXPECTED_PROTOCOLS

    def test_bridge_routes_have_fee_bps(self, arb_data):
        for r in arb_data["bridge_routes"]:
            assert "fee_bps" in r
            assert r["fee_bps"] > 0

    def test_bridge_routes_have_time_sec(self, arb_data):
        for r in arb_data["bridge_routes"]:
            assert "time_sec" in r
            assert r["time_sec"] > 0

    def test_best_bridge_is_minimum_fee(self, arb_data):
        routes = arb_data["bridge_routes"]
        fees = [r["fee_bps"] for r in routes]
        assert min(fees) == pytest.approx(3.0)  # Celer


# ===========================================================================
# Group 8: Arb frequency heatmap (5 tests)
# ===========================================================================

class TestArbFrequencyHeatmap:
    def test_heatmap_has_hours_field(self, arb_data):
        assert "hours" in arb_data["arb_frequency_heatmap"]

    def test_heatmap_has_24_hours(self, arb_data):
        hours = arb_data["arb_frequency_heatmap"]["hours"]
        assert len(hours) == 24
        assert hours == list(range(24))

    def test_heatmap_has_five_chain_pairs(self, arb_data):
        pairs = arb_data["arb_frequency_heatmap"]["chain_pairs"]
        assert len(pairs) == 5

    def test_heatmap_counts_is_24x5(self, arb_data):
        counts = arb_data["arb_frequency_heatmap"]["counts"]
        assert len(counts) == 24
        for row in counts:
            assert len(row) == 5

    def test_heatmap_counts_are_non_negative_ints(self, arb_data):
        counts = arb_data["arb_frequency_heatmap"]["counts"]
        for row in counts:
            for val in row:
                assert isinstance(val, int)
                assert val >= 0


# ===========================================================================
# Group 9: API endpoint registration (5 tests)
# ===========================================================================

class TestApiEndpoint:
    @pytest.mark.asyncio
    async def test_api_module_imports(self):
        await init_db()
        from api import router
        assert router is not None

    @pytest.mark.asyncio
    async def test_cross_chain_arb_route_registered(self):
        await init_db()
        from api import router
        paths = [r.path for r in router.routes]
        assert any("cross-chain-arb" in p for p in paths)

    @pytest.mark.asyncio
    async def test_endpoint_returns_dict(self):
        await init_db()
        from metrics import compute_cross_chain_arb_monitor
        result = await compute_cross_chain_arb_monitor()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_endpoint_has_signal_field(self):
        await init_db()
        from metrics import compute_cross_chain_arb_monitor
        result = await compute_cross_chain_arb_monitor()
        assert "signal" in result

    @pytest.mark.asyncio
    async def test_endpoint_has_top_opportunities(self):
        await init_db()
        from metrics import compute_cross_chain_arb_monitor
        result = await compute_cross_chain_arb_monitor()
        assert len(result["top_opportunities"]) > 0


# ===========================================================================
# Group 10: HTML and JS frontend (5 tests)
# ===========================================================================

def _read_frontend(filename: str) -> str:
    base = os.path.join(os.path.dirname(__file__), "..", "..", "frontend")
    path = os.path.join(base, filename)
    with open(path, encoding="utf-8") as f:
        return f.read()


class TestFrontend:
    def test_html_has_card_cross_chain_arb(self):
        html = _read_frontend("index.html")
        assert 'id="card-cross-chain-arb"' in html

    def test_html_has_cross_chain_arb_content_div(self):
        html = _read_frontend("index.html")
        assert 'id="cross-chain-arb-content"' in html

    def test_html_has_cross_chain_arb_badge(self):
        html = _read_frontend("index.html")
        assert 'id="cross-chain-arb-badge"' in html

    def test_js_has_refresh_cross_chain_arb(self):
        js = _read_frontend("app.js")
        assert "refreshCrossChainArb" in js

    def test_js_refresh_called_in_refresh_loop(self):
        js = _read_frontend("app.js")
        # Should appear in the refresh() batches
        assert "safe(refreshCrossChainArb)" in js


# ===========================================================================
# Group 11: Best opportunity field (2 extra tests to reach 60+)
# ===========================================================================

class TestBestOpportunity:
    def test_best_opportunity_not_none(self, arb_data):
        assert arb_data["best_opportunity"] is not None

    def test_best_opportunity_has_all_fields(self, arb_data):
        bo = arb_data["best_opportunity"]
        assert "asset" in bo
        assert "route" in bo
        assert "spread_bps" in bo
        assert "fee_adjusted_profit_bps" in bo
        assert "is_profitable" in bo

    def test_best_opportunity_asset_known(self, arb_data):
        bo = arb_data["best_opportunity"]
        assert bo["asset"] in {"BTC", "ETH", "USDC"}

    def test_best_opportunity_route_contains_via(self, arb_data):
        bo = arb_data["best_opportunity"]
        assert "via" in bo["route"]

    def test_best_opportunity_spread_non_negative(self, arb_data):
        bo = arb_data["best_opportunity"]
        assert bo["spread_bps"] >= 0.0

    def test_best_opportunity_matches_top_of_sorted_list(self, arb_data):
        opps = arb_data["top_opportunities"]
        bo   = arb_data["best_opportunity"]
        if opps:
            assert opps[0]["asset"] == bo["asset"]
