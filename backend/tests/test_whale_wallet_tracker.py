"""Tests for compute_whale_wallet_tracker metric."""
import os
import sys
import tempfile

import pytest

os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "test.db"))
os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
os.environ.setdefault("SYMBOL_BYBIT", "BANANAS31USDT")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from metrics import compute_whale_wallet_tracker  # noqa: E402


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def result_bananas():
    import asyncio
    return asyncio.run(compute_whale_wallet_tracker("BANANAS31USDT"))


@pytest.fixture(scope="module")
def result_cos():
    import asyncio
    return asyncio.run(compute_whale_wallet_tracker("COSUSDT"))


@pytest.fixture(scope="module")
def result_default():
    import asyncio
    return asyncio.run(compute_whale_wallet_tracker())


# ── Top-level structure ───────────────────────────────────────────────────────

def test_returns_dict(result_bananas):
    assert isinstance(result_bananas, dict)


def test_has_top_wallets_key(result_bananas):
    assert "top_wallets" in result_bananas


def test_has_large_moves_24h_key(result_bananas):
    assert "large_moves_24h" in result_bananas


def test_has_age_distribution_key(result_bananas):
    assert "age_distribution" in result_bananas


def test_has_pct_exchange_key(result_bananas):
    assert "pct_exchange" in result_bananas


def test_has_pct_cold_key(result_bananas):
    assert "pct_cold" in result_bananas


def test_has_net_whale_flow_7d_key(result_bananas):
    assert "net_whale_flow_7d" in result_bananas


def test_has_whale_signal_key(result_bananas):
    assert "whale_signal" in result_bananas


# ── top_wallets list ──────────────────────────────────────────────────────────

def test_top_wallets_is_list(result_bananas):
    assert isinstance(result_bananas["top_wallets"], list)


def test_top_wallets_count_50(result_bananas):
    assert len(result_bananas["top_wallets"]) == 50


def test_top_wallets_have_wallet_id(result_bananas):
    for w in result_bananas["top_wallets"]:
        assert "wallet_id" in w


def test_top_wallets_have_address(result_bananas):
    for w in result_bananas["top_wallets"]:
        assert "address" in w
        assert isinstance(w["address"], str)
        assert len(w["address"]) > 0


def test_top_wallets_have_balance(result_bananas):
    for w in result_bananas["top_wallets"]:
        assert "balance" in w
        assert isinstance(w["balance"], (int, float))
        assert w["balance"] >= 0


def test_top_wallets_have_balance_usd(result_bananas):
    for w in result_bananas["top_wallets"]:
        assert "balance_usd" in w
        assert isinstance(w["balance_usd"], (int, float))
        assert w["balance_usd"] >= 0


def test_top_wallets_have_wallet_age_days(result_bananas):
    for w in result_bananas["top_wallets"]:
        assert "wallet_age_days" in w
        assert isinstance(w["wallet_age_days"], (int, float))
        assert w["wallet_age_days"] >= 0


def test_top_wallets_have_age_class(result_bananas):
    valid = {"whale", "shark", "fish"}
    for w in result_bananas["top_wallets"]:
        assert "age_class" in w
        assert w["age_class"] in valid


def test_top_wallets_have_is_exchange(result_bananas):
    for w in result_bananas["top_wallets"]:
        assert "is_exchange" in w
        assert isinstance(w["is_exchange"], bool)


def test_top_wallets_sorted_by_balance_desc(result_bananas):
    wallets = result_bananas["top_wallets"]
    for i in range(len(wallets) - 1):
        assert wallets[i]["balance_usd"] >= wallets[i + 1]["balance_usd"]


# ── age_class correctness ─────────────────────────────────────────────────────

def test_age_class_whale_requires_730_days(result_bananas):
    for w in result_bananas["top_wallets"]:
        if w["age_class"] == "whale":
            assert w["wallet_age_days"] >= 730


def test_age_class_shark_range(result_bananas):
    for w in result_bananas["top_wallets"]:
        if w["age_class"] == "shark":
            assert 180 <= w["wallet_age_days"] < 730


def test_age_class_fish_range(result_bananas):
    for w in result_bananas["top_wallets"]:
        if w["age_class"] == "fish":
            assert w["wallet_age_days"] < 180


# ── large_moves_24h list ──────────────────────────────────────────────────────

def test_large_moves_is_list(result_bananas):
    assert isinstance(result_bananas["large_moves_24h"], list)


def test_large_moves_amount_above_1m(result_bananas):
    for m in result_bananas["large_moves_24h"]:
        assert m["amount_usd"] >= 1_000_000


def test_large_moves_have_direction(result_bananas):
    valid_dirs = {"in", "out"}
    for m in result_bananas["large_moves_24h"]:
        assert "direction" in m
        assert m["direction"] in valid_dirs


def test_large_moves_have_wallet_id(result_bananas):
    for m in result_bananas["large_moves_24h"]:
        assert "wallet_id" in m


def test_large_moves_have_ts(result_bananas):
    for m in result_bananas["large_moves_24h"]:
        assert "ts" in m
        assert isinstance(m["ts"], (int, float))
        assert m["ts"] > 0


def test_large_moves_have_amount_usd(result_bananas):
    for m in result_bananas["large_moves_24h"]:
        assert "amount_usd" in m
        assert isinstance(m["amount_usd"], (int, float))


# ── age_distribution dict ─────────────────────────────────────────────────────

def test_age_distribution_is_dict(result_bananas):
    assert isinstance(result_bananas["age_distribution"], dict)


def test_age_distribution_has_whale(result_bananas):
    assert "whale" in result_bananas["age_distribution"]


def test_age_distribution_has_shark(result_bananas):
    assert "shark" in result_bananas["age_distribution"]


def test_age_distribution_has_fish(result_bananas):
    assert "fish" in result_bananas["age_distribution"]


def test_age_distribution_counts_sum_to_50(result_bananas):
    ad = result_bananas["age_distribution"]
    total = ad["whale"]["count"] + ad["shark"]["count"] + ad["fish"]["count"]
    assert total == 50


def test_age_distribution_pct_sum_approx_100(result_bananas):
    ad = result_bananas["age_distribution"]
    total = ad["whale"]["pct"] + ad["shark"]["pct"] + ad["fish"]["pct"]
    assert abs(total - 100.0) < 0.1


def test_age_distribution_pct_non_negative(result_bananas):
    ad = result_bananas["age_distribution"]
    for cls in ("whale", "shark", "fish"):
        assert ad[cls]["pct"] >= 0


# ── pct_exchange / pct_cold ───────────────────────────────────────────────────

def test_pct_exchange_is_float(result_bananas):
    assert isinstance(result_bananas["pct_exchange"], float)


def test_pct_cold_is_float(result_bananas):
    assert isinstance(result_bananas["pct_cold"], float)


def test_pct_exchange_range(result_bananas):
    assert 0.0 <= result_bananas["pct_exchange"] <= 100.0


def test_pct_cold_range(result_bananas):
    assert 0.0 <= result_bananas["pct_cold"] <= 100.0


def test_pct_exchange_plus_cold_lte_100(result_bananas):
    total = result_bananas["pct_exchange"] + result_bananas["pct_cold"]
    assert total <= 100.01  # small float tolerance


# ── net_whale_flow_7d ─────────────────────────────────────────────────────────

def test_net_whale_flow_7d_is_number(result_bananas):
    assert isinstance(result_bananas["net_whale_flow_7d"], (int, float))


# ── whale_signal ──────────────────────────────────────────────────────────────

def test_whale_signal_valid_values(result_bananas):
    assert result_bananas["whale_signal"] in {"accumulating", "distributing", "neutral"}


def test_whale_signal_accumulating_when_positive_flow(result_bananas):
    if result_bananas["net_whale_flow_7d"] > 0:
        assert result_bananas["whale_signal"] == "accumulating"


def test_whale_signal_distributing_when_negative_flow(result_bananas):
    if result_bananas["net_whale_flow_7d"] < 0:
        assert result_bananas["whale_signal"] == "distributing"


def test_whale_signal_neutral_when_zero_flow(result_bananas):
    if result_bananas["net_whale_flow_7d"] == 0:
        assert result_bananas["whale_signal"] == "neutral"


# ── Symbol isolation (seeded random) ─────────────────────────────────────────

def test_different_symbols_can_produce_different_results(result_bananas, result_cos):
    # Seeds differ by symbol → flows should differ
    b_flow = result_bananas["net_whale_flow_7d"]
    c_flow = result_cos["net_whale_flow_7d"]
    # They may or may not be equal, but the function must return without error
    assert isinstance(b_flow, (int, float))
    assert isinstance(c_flow, (int, float))


def test_default_symbol_returns_valid_result(result_default):
    assert "whale_signal" in result_default
    assert "top_wallets" in result_default
    assert len(result_default["top_wallets"]) == 50


# ── Determinism (same symbol → same result) ───────────────────────────────────

def test_deterministic_for_same_symbol():
    import asyncio
    r1 = asyncio.run(compute_whale_wallet_tracker("DEXEUSDT"))
    r2 = asyncio.run(compute_whale_wallet_tracker("DEXEUSDT"))
    assert r1["whale_signal"] == r2["whale_signal"]
    assert r1["net_whale_flow_7d"] == r2["net_whale_flow_7d"]
    assert r1["pct_exchange"] == r2["pct_exchange"]


# ── Additional coverage ───────────────────────────────────────────────────────

def test_top_wallets_wallet_id_non_empty(result_bananas):
    for w in result_bananas["top_wallets"]:
        assert len(w["wallet_id"]) > 0


def test_large_moves_at_least_one(result_bananas):
    """Seeded data always includes at least one large move."""
    assert len(result_bananas["large_moves_24h"]) >= 1


def test_age_distribution_count_types(result_bananas):
    ad = result_bananas["age_distribution"]
    for cls in ("whale", "shark", "fish"):
        assert isinstance(ad[cls]["count"], int)
        assert isinstance(ad[cls]["pct"], float)
