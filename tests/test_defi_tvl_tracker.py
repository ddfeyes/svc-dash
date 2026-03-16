"""
Unit / smoke tests for /api/defi-tvl-tracker.

DeFi TVL tracker — top protocols by TVL, chain dominance breakdown,
momentum signal, and 30-day sparkline history.

Data source: DeFi Llama public API (free, no auth required).
Mock data layer provides realistic fallback values.

Key metrics:
  total_tvl       — sum of all tracked protocol TVLs
  protocol rank   — top 10 by TVL, with 24h/7d change %
  chain dominance — ETH/BSC/SOL/ARB share of total TVL
  momentum        — TVL acceleration: accelerating/stable/declining
  sparkline       — 30-day daily TVL series

Momentum classification:
  accelerating — 7d change rate > 5%
  declining    — 7d change rate < -5%
  stable       — otherwise

Chain dominance: collapse chains < 3% TVL share into "Others"

Covers:
  - _dt_tvl_change_pct
  - _dt_chain_dominance
  - _dt_momentum_signal
  - _dt_rank_protocols
  - _dt_format_tvl
  - _dt_category_breakdown
  - _dt_dominance_others
  - Response shape / key validation
  - Structural: route, HTML card, JS function, JS API call
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from metrics import (
    _dt_tvl_change_pct,
    _dt_chain_dominance,
    _dt_momentum_signal,
    _dt_rank_protocols,
    _dt_format_tvl,
    _dt_category_breakdown,
    _dt_dominance_others,
)

# ---------------------------------------------------------------------------
# Sample response fixture
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE = {
    "total_tvl_usd":      95_000_000_000.0,
    "tvl_change_24h_pct":  2.3,
    "tvl_change_7d_pct":  -1.8,
    "momentum":            "stable",
    "protocols": [
        {"name": "Lido",        "tvl_usd": 28_000_000_000.0, "chain": "Ethereum",  "category": "Liquid Staking", "change_24h_pct":  1.2, "change_7d_pct":  3.5},
        {"name": "AAVE",        "tvl_usd": 12_000_000_000.0, "chain": "Multi",     "category": "Lending",        "change_24h_pct":  0.5, "change_7d_pct": -2.1},
        {"name": "Uniswap",     "tvl_usd":  6_500_000_000.0, "chain": "Ethereum",  "category": "DEX",            "change_24h_pct":  3.1, "change_7d_pct":  1.0},
        {"name": "Curve",       "tvl_usd":  5_800_000_000.0, "chain": "Multi",     "category": "DEX",            "change_24h_pct": -0.3, "change_7d_pct": -4.2},
        {"name": "MakerDAO",    "tvl_usd":  5_200_000_000.0, "chain": "Ethereum",  "category": "CDP",            "change_24h_pct":  0.8, "change_7d_pct":  0.5},
        {"name": "JustLend",    "tvl_usd":  4_700_000_000.0, "chain": "Tron",      "category": "Lending",        "change_24h_pct":  1.5, "change_7d_pct":  2.8},
        {"name": "Kamino",      "tvl_usd":  3_200_000_000.0, "chain": "Solana",    "category": "Lending",        "change_24h_pct":  4.2, "change_7d_pct":  8.1},
        {"name": "Eigen Layer", "tvl_usd":  3_000_000_000.0, "chain": "Ethereum",  "category": "Restaking",      "change_24h_pct":  0.0, "change_7d_pct": -1.2},
        {"name": "PancakeSwap", "tvl_usd":  2_100_000_000.0, "chain": "BSC",       "category": "DEX",            "change_24h_pct":  1.9, "change_7d_pct":  3.3},
        {"name": "GMX",         "tvl_usd":  1_800_000_000.0, "chain": "Arbitrum",  "category": "Derivatives",    "change_24h_pct":  2.7, "change_7d_pct":  5.9},
    ],
    "chain_dominance": {
        "Ethereum": 58.2,
        "BSC":       8.1,
        "Solana":    6.4,
        "Arbitrum":  5.2,
        "Others":   22.1,
    },
    "history": [
        {"date": "2024-11-14", "tvl_usd": 88_000_000_000.0},
        {"date": "2024-11-15", "tvl_usd": 89_500_000_000.0},
        {"date": "2024-11-16", "tvl_usd": 87_200_000_000.0},
        {"date": "2024-11-17", "tvl_usd": 91_000_000_000.0},
        {"date": "2024-11-18", "tvl_usd": 93_400_000_000.0},
        {"date": "2024-11-19", "tvl_usd": 94_800_000_000.0},
        {"date": "2024-11-20", "tvl_usd": 95_000_000_000.0},
    ],
    "description": "DeFi TVL $95B — stable momentum, ETH dominance 58%",
}


# ===========================================================================
# 1. _dt_tvl_change_pct
# ===========================================================================

class TestDtTvlChangePct:
    def test_increase_returns_positive(self):
        assert _dt_tvl_change_pct(110.0, 100.0) == pytest.approx(10.0, rel=1e-4)

    def test_decrease_returns_negative(self):
        assert _dt_tvl_change_pct(90.0, 100.0) == pytest.approx(-10.0, rel=1e-4)

    def test_no_change_returns_zero(self):
        assert _dt_tvl_change_pct(100.0, 100.0) == pytest.approx(0.0, abs=1e-6)

    def test_zero_previous_returns_zero(self):
        assert _dt_tvl_change_pct(100.0, 0.0) == pytest.approx(0.0, abs=1e-6)

    def test_zero_current_returns_minus_100(self):
        assert _dt_tvl_change_pct(0.0, 100.0) == pytest.approx(-100.0, rel=1e-4)

    def test_returns_float(self):
        assert isinstance(_dt_tvl_change_pct(110.0, 100.0), float)

    def test_large_increase(self):
        pct = _dt_tvl_change_pct(200.0, 100.0)
        assert pct == pytest.approx(100.0, rel=1e-4)


# ===========================================================================
# 2. _dt_chain_dominance
# ===========================================================================

class TestDtChainDominance:
    def test_single_chain_is_100_pct(self):
        result = _dt_chain_dominance({"Ethereum": 1_000_000.0})
        assert result["Ethereum"] == pytest.approx(100.0, rel=1e-4)

    def test_two_equal_chains_are_50_50(self):
        result = _dt_chain_dominance({"Ethereum": 500.0, "BSC": 500.0})
        assert result["Ethereum"] == pytest.approx(50.0, rel=1e-4)
        assert result["BSC"]      == pytest.approx(50.0, rel=1e-4)

    def test_percentages_sum_to_100(self):
        chains = {"Ethereum": 600.0, "BSC": 200.0, "Solana": 150.0, "Arbitrum": 50.0}
        result = _dt_chain_dominance(chains)
        assert sum(result.values()) == pytest.approx(100.0, rel=1e-4)

    def test_empty_returns_empty(self):
        assert _dt_chain_dominance({}) == {}

    def test_zero_total_returns_empty(self):
        assert _dt_chain_dominance({"Ethereum": 0.0, "BSC": 0.0}) == {}

    def test_values_in_0_100(self):
        chains = {"A": 300.0, "B": 500.0, "C": 200.0}
        for pct in _dt_chain_dominance(chains).values():
            assert 0.0 <= pct <= 100.0

    def test_proportional_output(self):
        result = _dt_chain_dominance({"X": 750.0, "Y": 250.0})
        assert result["X"] == pytest.approx(75.0, rel=1e-4)
        assert result["Y"] == pytest.approx(25.0, rel=1e-4)


# ===========================================================================
# 3. _dt_momentum_signal
# ===========================================================================

class TestDtMomentumSignal:
    def test_empty_returns_stable(self):
        assert _dt_momentum_signal([]) == "stable"

    def test_single_returns_stable(self):
        assert _dt_momentum_signal([1e11]) == "stable"

    def test_strongly_rising_series_is_accelerating(self):
        # Start 50B, end 80B → +60%
        series = [50e9, 55e9, 60e9, 65e9, 70e9, 75e9, 80e9]
        assert _dt_momentum_signal(series) == "accelerating"

    def test_strongly_falling_series_is_declining(self):
        series = [80e9, 75e9, 70e9, 65e9, 60e9, 55e9, 50e9]
        assert _dt_momentum_signal(series) == "declining"

    def test_flat_series_is_stable(self):
        series = [90e9] * 7
        assert _dt_momentum_signal(series) == "stable"

    def test_returns_valid_string(self):
        result = _dt_momentum_signal([80e9, 82e9, 84e9])
        assert result in ("accelerating", "stable", "declining")


# ===========================================================================
# 4. _dt_rank_protocols
# ===========================================================================

class TestDtRankProtocols:
    PROTOCOLS = [
        {"name": "A", "tvl_usd": 1_000.0},
        {"name": "B", "tvl_usd": 5_000.0},
        {"name": "C", "tvl_usd": 3_000.0},
        {"name": "D", "tvl_usd": 2_000.0},
        {"name": "E", "tvl_usd":   800.0},
    ]

    def test_returns_top_n(self):
        result = _dt_rank_protocols(self.PROTOCOLS, 3)
        assert len(result) == 3

    def test_sorted_descending_by_tvl(self):
        result = _dt_rank_protocols(self.PROTOCOLS, 5)
        tvls = [p["tvl_usd"] for p in result]
        assert tvls == sorted(tvls, reverse=True)

    def test_first_is_highest_tvl(self):
        result = _dt_rank_protocols(self.PROTOCOLS, 1)
        assert result[0]["name"] == "B"

    def test_empty_list_returns_empty(self):
        assert _dt_rank_protocols([], 5) == []

    def test_n_larger_than_list_returns_all(self):
        result = _dt_rank_protocols(self.PROTOCOLS, 100)
        assert len(result) == len(self.PROTOCOLS)

    def test_preserves_protocol_fields(self):
        result = _dt_rank_protocols(self.PROTOCOLS, 1)
        assert "name" in result[0]
        assert "tvl_usd" in result[0]


# ===========================================================================
# 5. _dt_format_tvl
# ===========================================================================

class TestDtFormatTvl:
    def test_billions_formatted(self):
        assert "B" in _dt_format_tvl(95_000_000_000.0)

    def test_millions_formatted(self):
        assert "M" in _dt_format_tvl(250_000_000.0)

    def test_below_million_formatted(self):
        result = _dt_format_tvl(500_000.0)
        assert "K" in result or "$" in result

    def test_zero_returns_string(self):
        result = _dt_format_tvl(0.0)
        assert isinstance(result, str)

    def test_exact_billion(self):
        result = _dt_format_tvl(1_000_000_000.0)
        assert "1" in result and "B" in result

    def test_returns_string(self):
        assert isinstance(_dt_format_tvl(1_000_000_000.0), str)

    def test_starts_with_dollar(self):
        assert _dt_format_tvl(1_000_000_000.0).startswith("$")


# ===========================================================================
# 6. _dt_category_breakdown
# ===========================================================================

class TestDtCategoryBreakdown:
    PROTOCOLS = [
        {"name": "Lido",    "category": "Liquid Staking", "tvl_usd": 28e9},
        {"name": "AAVE",    "category": "Lending",        "tvl_usd": 12e9},
        {"name": "Uniswap", "category": "DEX",            "tvl_usd":  6.5e9},
        {"name": "Curve",   "category": "DEX",            "tvl_usd":  5.8e9},
        {"name": "Maker",   "category": "CDP",            "tvl_usd":  5.2e9},
    ]

    def test_returns_dict(self):
        assert isinstance(_dt_category_breakdown(self.PROTOCOLS), dict)

    def test_dex_sums_both_protocols(self):
        result = _dt_category_breakdown(self.PROTOCOLS)
        assert result["DEX"] == pytest.approx(6.5e9 + 5.8e9, rel=1e-4)

    def test_all_categories_present(self):
        result = _dt_category_breakdown(self.PROTOCOLS)
        for cat in ("Liquid Staking", "Lending", "DEX", "CDP"):
            assert cat in result

    def test_empty_returns_empty(self):
        assert _dt_category_breakdown([]) == {}

    def test_missing_category_field_skipped(self):
        protocols = [{"name": "X", "tvl_usd": 100.0}]  # no 'category'
        result = _dt_category_breakdown(protocols)
        assert isinstance(result, dict)

    def test_values_are_floats(self):
        result = _dt_category_breakdown(self.PROTOCOLS)
        for v in result.values():
            assert isinstance(v, float)


# ===========================================================================
# 7. _dt_dominance_others
# ===========================================================================

class TestDtDominanceOthers:
    def test_small_chains_collapsed(self):
        pcts = {"Ethereum": 60.0, "BSC": 10.0, "TinyChain": 2.0, "TinyChain2": 1.5}
        result = _dt_dominance_others(pcts, threshold=3.0)
        assert "Others" in result
        assert "TinyChain" not in result

    def test_large_chains_preserved(self):
        pcts = {"Ethereum": 60.0, "BSC": 10.0, "Solana": 8.0}
        result = _dt_dominance_others(pcts, threshold=3.0)
        assert "Ethereum" in result
        assert "BSC" in result
        assert "Solana" in result

    def test_others_sum_correct(self):
        pcts = {"A": 60.0, "B": 25.0, "C": 10.0, "D": 5.0}
        result = _dt_dominance_others(pcts, threshold=8.0)
        # C (10%) survives, D (5%) collapses
        assert result.get("Others", 0) == pytest.approx(5.0, rel=1e-4)

    def test_empty_returns_empty(self):
        assert _dt_dominance_others({}, threshold=3.0) == {}

    def test_no_small_chains_no_others_key(self):
        pcts = {"Ethereum": 60.0, "BSC": 40.0}
        result = _dt_dominance_others(pcts, threshold=3.0)
        assert "Others" not in result


# ===========================================================================
# 8. SAMPLE_RESPONSE structure
# ===========================================================================

class TestSampleResponseShape:
    def test_has_total_tvl(self):
        assert SAMPLE_RESPONSE["total_tvl_usd"] > 0

    def test_has_24h_change(self):
        assert "tvl_change_24h_pct" in SAMPLE_RESPONSE

    def test_has_7d_change(self):
        assert "tvl_change_7d_pct" in SAMPLE_RESPONSE

    def test_has_momentum(self):
        assert SAMPLE_RESPONSE["momentum"] in ("accelerating", "stable", "declining")

    def test_has_protocols_list(self):
        assert isinstance(SAMPLE_RESPONSE["protocols"], list)

    def test_protocols_count_is_ten(self):
        assert len(SAMPLE_RESPONSE["protocols"]) == 10

    def test_each_protocol_has_required_keys(self):
        for p in SAMPLE_RESPONSE["protocols"]:
            for key in ("name", "tvl_usd", "chain", "category", "change_24h_pct", "change_7d_pct"):
                assert key in p, f"protocol '{p.get('name')}' missing '{key}'"

    def test_has_chain_dominance_dict(self):
        assert isinstance(SAMPLE_RESPONSE["chain_dominance"], dict)

    def test_chain_dominance_has_others(self):
        assert "Others" in SAMPLE_RESPONSE["chain_dominance"]

    def test_chain_dominance_sums_to_100(self):
        total = sum(SAMPLE_RESPONSE["chain_dominance"].values())
        assert total == pytest.approx(100.0, abs=0.5)

    def test_has_history_list(self):
        assert isinstance(SAMPLE_RESPONSE["history"], list)

    def test_history_items_have_required_keys(self):
        for item in SAMPLE_RESPONSE["history"]:
            for key in ("date", "tvl_usd"):
                assert key in item, f"history item missing '{key}'"

    def test_has_description(self):
        assert isinstance(SAMPLE_RESPONSE["description"], str)


# ===========================================================================
# 9. Structural tests
# ===========================================================================

class TestStructural:
    def test_route_registered_in_api_py(self):
        api_path = os.path.join(os.path.dirname(__file__), "..", "backend", "api.py")
        content = open(api_path).read()
        assert "/defi-tvl-tracker" in content, "/defi-tvl-tracker route missing"

    def test_html_card_exists(self):
        html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")
        content = open(html_path).read()
        assert "card-defi-tvl-tracker" in content, "card-defi-tvl-tracker missing"

    def test_js_render_function_exists(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "renderDefiTvlTracker" in content, "renderDefiTvlTracker missing"

    def test_js_api_call_to_endpoint(self):
        js_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "app.js")
        content = open(js_path).read()
        assert "/defi-tvl-tracker" in content, "/defi-tvl-tracker call missing"
