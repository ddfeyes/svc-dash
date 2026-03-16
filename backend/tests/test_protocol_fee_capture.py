"""Tests for protocol fee capture metric — TDD first."""
import os
import sys
import pytest

os.environ.setdefault("DB_PATH", "/tmp/test_protocol_fee_capture.db")
os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
os.environ.setdefault("SYMBOL_BYBIT", "BANANAS31USDT")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from metrics import compute_protocol_fee_capture

PROTOCOLS = [
    "Uniswap", "Aave", "Compound", "Curve", "dYdX", "GMX", "Lido",
    "MakerDAO", "Balancer", "Synthetix", "1inch", "Sushiswap", "Yearn",
    "Convex", "Frax", "Pendle", "Velodrome", "Camelot", "Radiant", "Gains",
]


@pytest.mark.asyncio
async def test_returns_dict():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_top_level_keys():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    assert "protocols" in result
    assert "top_protocol" in result
    assert "total_defi_fees_24h" in result
    assert "fee_leader_signal" in result


@pytest.mark.asyncio
async def test_protocols_is_list():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    assert isinstance(result["protocols"], list)


@pytest.mark.asyncio
async def test_protocols_count():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    assert len(result["protocols"]) == 20


@pytest.mark.asyncio
async def test_protocol_entry_keys():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    p = result["protocols"][0]
    assert "name" in p
    assert "fee_24h" in p
    assert "fee_7d" in p
    assert "fee_30d" in p
    assert "ps_ratio" in p
    assert "growth_rate_7d" in p


@pytest.mark.asyncio
async def test_protocol_names_are_strings():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert isinstance(p["name"], str)
        assert len(p["name"]) > 0


@pytest.mark.asyncio
async def test_all_expected_protocols_present():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    names = {p["name"] for p in result["protocols"]}
    for proto in PROTOCOLS:
        assert proto in names, f"{proto} missing from protocols"


@pytest.mark.asyncio
async def test_fee_24h_positive():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert p["fee_24h"] > 0, f"{p['name']} fee_24h should be positive"


@pytest.mark.asyncio
async def test_fee_7d_positive():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert p["fee_7d"] > 0, f"{p['name']} fee_7d should be positive"


@pytest.mark.asyncio
async def test_fee_30d_positive():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert p["fee_30d"] > 0, f"{p['name']} fee_30d should be positive"


@pytest.mark.asyncio
async def test_fee_7d_geq_fee_24h():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert p["fee_7d"] >= p["fee_24h"], f"{p['name']} fee_7d < fee_24h"


@pytest.mark.asyncio
async def test_fee_30d_geq_fee_7d():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert p["fee_30d"] >= p["fee_7d"], f"{p['name']} fee_30d < fee_7d"


@pytest.mark.asyncio
async def test_ps_ratio_positive():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert p["ps_ratio"] > 0, f"{p['name']} ps_ratio should be positive"


@pytest.mark.asyncio
async def test_growth_rate_7d_is_float():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert isinstance(p["growth_rate_7d"], float), f"{p['name']} growth_rate_7d not float"


@pytest.mark.asyncio
async def test_top_protocol_is_string():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    assert isinstance(result["top_protocol"], str)
    assert len(result["top_protocol"]) > 0


@pytest.mark.asyncio
async def test_top_protocol_is_in_list():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    names = {p["name"] for p in result["protocols"]}
    assert result["top_protocol"] in names


@pytest.mark.asyncio
async def test_top_protocol_has_highest_fee_24h():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    top = result["top_protocol"]
    top_fee = next(p["fee_24h"] for p in result["protocols"] if p["name"] == top)
    max_fee = max(p["fee_24h"] for p in result["protocols"])
    assert top_fee == max_fee


@pytest.mark.asyncio
async def test_total_defi_fees_24h_positive():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    assert result["total_defi_fees_24h"] > 0


@pytest.mark.asyncio
async def test_total_defi_fees_24h_is_sum():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    expected = round(sum(p["fee_24h"] for p in result["protocols"]), 2)
    assert abs(result["total_defi_fees_24h"] - expected) < 0.1


@pytest.mark.asyncio
async def test_fee_leader_signal_valid_values():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    assert result["fee_leader_signal"] in ("dominant", "fragmented", "neutral")


@pytest.mark.asyncio
async def test_dominant_signal_when_top_protocol_large_share():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    top = result["top_protocol"]
    top_fee = next(p["fee_24h"] for p in result["protocols"] if p["name"] == top)
    total = result["total_defi_fees_24h"]
    share = top_fee / total if total > 0 else 0
    if share > 0.35:
        assert result["fee_leader_signal"] == "dominant"


@pytest.mark.asyncio
async def test_protocols_sorted_by_fee_24h_desc():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    fees = [p["fee_24h"] for p in result["protocols"]]
    assert fees == sorted(fees, reverse=True)


@pytest.mark.asyncio
async def test_uniswap_in_top5():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    top5_names = [p["name"] for p in result["protocols"][:5]]
    assert "Uniswap" in top5_names


@pytest.mark.asyncio
async def test_fee_24h_uniswap_gt_1m():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    uni = next(p for p in result["protocols"] if p["name"] == "Uniswap")
    assert uni["fee_24h"] > 1_000_000


@pytest.mark.asyncio
async def test_fee_7d_approx_7x_24h():
    """7d fees should be roughly 5-10x the 24h fees."""
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        ratio = p["fee_7d"] / p["fee_24h"]
        assert 4 <= ratio <= 12, f"{p['name']} fee_7d/fee_24h ratio {ratio:.2f} out of range"


@pytest.mark.asyncio
async def test_fee_30d_approx_30x_24h():
    """30d fees should be roughly 20-40x the 24h fees."""
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        ratio = p["fee_30d"] / p["fee_24h"]
        assert 18 <= ratio <= 45, f"{p['name']} fee_30d/fee_24h ratio {ratio:.2f} out of range"


@pytest.mark.asyncio
async def test_ps_ratio_reasonable_range():
    """P/S ratios for DeFi protocols should be between 1 and 1000."""
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert 1 <= p["ps_ratio"] <= 1000, f"{p['name']} ps_ratio {p['ps_ratio']} out of range"


@pytest.mark.asyncio
async def test_growth_rate_7d_range():
    """Growth rate should be between -100% and +500%."""
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert -100 <= p["growth_rate_7d"] <= 500, (
            f"{p['name']} growth_rate_7d {p['growth_rate_7d']} out of range"
        )


@pytest.mark.asyncio
async def test_idempotent_same_symbol():
    """Two calls with same symbol should return same top_protocol."""
    r1 = await compute_protocol_fee_capture("BANANAS31USDT")
    r2 = await compute_protocol_fee_capture("BANANAS31USDT")
    assert r1["top_protocol"] == r2["top_protocol"]


@pytest.mark.asyncio
async def test_different_symbol_returns_same_structure():
    """Different symbol arg should still return valid structure."""
    result = await compute_protocol_fee_capture("COSUSDT")
    assert "protocols" in result
    assert len(result["protocols"]) == 20


@pytest.mark.asyncio
async def test_total_fees_24h_is_float():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    assert isinstance(result["total_defi_fees_24h"], float)


@pytest.mark.asyncio
async def test_fee_values_are_floats():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert isinstance(p["fee_24h"], float)
        assert isinstance(p["fee_7d"], float)
        assert isinstance(p["fee_30d"], float)
        assert isinstance(p["ps_ratio"], float)


@pytest.mark.asyncio
async def test_no_none_values_in_protocol():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        for key, val in p.items():
            assert val is not None, f"{p['name']}.{key} is None"


@pytest.mark.asyncio
async def test_top_protocol_not_empty_string():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    assert result["top_protocol"] != ""


@pytest.mark.asyncio
async def test_total_fees_24h_gt_1m():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    assert result["total_defi_fees_24h"] > 1_000_000


@pytest.mark.asyncio
async def test_aave_present():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    names = [p["name"] for p in result["protocols"]]
    assert "Aave" in names


@pytest.mark.asyncio
async def test_lido_present():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    names = [p["name"] for p in result["protocols"]]
    assert "Lido" in names


@pytest.mark.asyncio
async def test_gmx_present():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    names = [p["name"] for p in result["protocols"]]
    assert "GMX" in names


@pytest.mark.asyncio
async def test_growth_rate_7d_rounded():
    """Growth rate should be rounded to 2 decimal places."""
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert p["growth_rate_7d"] == round(p["growth_rate_7d"], 2)


@pytest.mark.asyncio
async def test_ps_ratio_rounded():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert p["ps_ratio"] == round(p["ps_ratio"], 2)


@pytest.mark.asyncio
async def test_fee_24h_rounded():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert p["fee_24h"] == round(p["fee_24h"], 2)


@pytest.mark.asyncio
async def test_fee_7d_rounded():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert p["fee_7d"] == round(p["fee_7d"], 2)


@pytest.mark.asyncio
async def test_fee_30d_rounded():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert p["fee_30d"] == round(p["fee_30d"], 2)


@pytest.mark.asyncio
async def test_fragmented_signal_when_no_dominant():
    """If no protocol has >35% share and top share <20%, signal should be fragmented."""
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    top = result["top_protocol"]
    top_fee = next(p["fee_24h"] for p in result["protocols"] if p["name"] == top)
    total = result["total_defi_fees_24h"]
    share = top_fee / total if total > 0 else 0
    if share < 0.20:
        assert result["fee_leader_signal"] == "fragmented"


@pytest.mark.asyncio
async def test_protocol_names_unique():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    names = [p["name"] for p in result["protocols"]]
    assert len(names) == len(set(names))


@pytest.mark.asyncio
async def test_uniswap_fee_24h_deterministic():
    """Seeded simulation: Uniswap fee_24h should be the same across calls."""
    r1 = await compute_protocol_fee_capture("BANANAS31USDT")
    r2 = await compute_protocol_fee_capture("BANANAS31USDT")
    uni1 = next(p["fee_24h"] for p in r1["protocols"] if p["name"] == "Uniswap")
    uni2 = next(p["fee_24h"] for p in r2["protocols"] if p["name"] == "Uniswap")
    assert uni1 == uni2


@pytest.mark.asyncio
async def test_gmx_fee_greater_than_gains():
    """GMX (derivatives DEX) should have higher fee than Gains by seed."""
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    gmx = next(p["fee_24h"] for p in result["protocols"] if p["name"] == "GMX")
    gains = next(p["fee_24h"] for p in result["protocols"] if p["name"] == "Gains")
    assert gmx > gains


@pytest.mark.asyncio
async def test_fee_leader_signal_type():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    assert isinstance(result["fee_leader_signal"], str)


@pytest.mark.asyncio
async def test_protocols_entry_count_exactly_6_fields():
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        assert len(p) == 6, f"{p['name']} has {len(p)} fields, expected 6"


@pytest.mark.asyncio
async def test_annualized_fees_implied_by_ps():
    """ps_ratio = market_cap / annualized_fees; annualized ~ fee_30d * 12."""
    result = await compute_protocol_fee_capture("BANANAS31USDT")
    for p in result["protocols"]:
        annualized = p["fee_30d"] * 12
        implied_mcap = p["ps_ratio"] * annualized
        # Market cap should be > 0
        assert implied_mcap > 0
