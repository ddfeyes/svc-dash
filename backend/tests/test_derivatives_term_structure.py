"""Tests for compute_derivatives_term_structure() — Wave 25.

55 tests covering all required keys, value ranges, structural invariants,
tenor ordering, determinism, and business logic.
"""
import asyncio
import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from metrics import compute_derivatives_term_structure

REQUIRED_KEYS = {
    "oi_distribution",
    "tenors",
    "contango_backwardation",
    "roll_cost_bps",
    "symbol",
    "timestamp",
    "structure_health",
}

OI_DIST_KEYS = {"perpetual", "quarterly", "bi_quarterly"}
TENOR_KEYS = {"tenor", "oi_usd", "oi_pct", "basis_annualized", "days_to_expiry"}
VALID_TENORS = {"perpetual", "quarterly", "bi_quarterly"}
VALID_CB = {"contango", "backwardation", "flat"}
VALID_HEALTH = {"healthy", "inverted", "normal"}


def run(coro):
    return asyncio.run(coro)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def result():
    return run(compute_derivatives_term_structure())


@pytest.fixture(scope="module")
def result2():
    return run(compute_derivatives_term_structure())


@pytest.fixture(scope="module")
def oi_dist(result):
    return result["oi_distribution"]


@pytest.fixture(scope="module")
def tenors(result):
    return result["tenors"]


@pytest.fixture(scope="module")
def perp_tenor(tenors):
    return next(t for t in tenors if t["tenor"] == "perpetual")


@pytest.fixture(scope="module")
def qtr_tenor(tenors):
    return next(t for t in tenors if t["tenor"] == "quarterly")


@pytest.fixture(scope="module")
def biqtr_tenor(tenors):
    return next(t for t in tenors if t["tenor"] == "bi_quarterly")


# ── Return type ───────────────────────────────────────────────────────────────


def test_returns_dict(result):
    assert isinstance(result, dict)


def test_is_async():
    import inspect
    assert inspect.iscoroutinefunction(compute_derivatives_term_structure)


# ── All required top-level keys ───────────────────────────────────────────────


def test_all_required_keys(result):
    assert REQUIRED_KEYS.issubset(result.keys())


def test_has_oi_distribution(result):
    assert "oi_distribution" in result


def test_has_tenors(result):
    assert "tenors" in result


def test_has_contango_backwardation(result):
    assert "contango_backwardation" in result


def test_has_roll_cost_bps(result):
    assert "roll_cost_bps" in result


def test_has_symbol(result):
    assert "symbol" in result


def test_has_timestamp(result):
    assert "timestamp" in result


def test_has_structure_health(result):
    assert "structure_health" in result


# ── oi_distribution ───────────────────────────────────────────────────────────


def test_oi_distribution_is_dict(oi_dist):
    assert isinstance(oi_dist, dict)


def test_oi_distribution_has_perpetual(oi_dist):
    assert "perpetual" in oi_dist


def test_oi_distribution_has_quarterly(oi_dist):
    assert "quarterly" in oi_dist


def test_oi_distribution_has_bi_quarterly(oi_dist):
    assert "bi_quarterly" in oi_dist


def test_oi_distribution_keys(oi_dist):
    assert OI_DIST_KEYS == set(oi_dist.keys())


def test_oi_distribution_sum_to_100(oi_dist):
    total = sum(oi_dist.values())
    assert abs(total - 100.0) < 0.01


def test_oi_dist_perpetual_in_range(oi_dist):
    assert 0.0 <= oi_dist["perpetual"] <= 100.0


def test_oi_dist_quarterly_in_range(oi_dist):
    assert 0.0 <= oi_dist["quarterly"] <= 100.0


def test_oi_dist_bi_quarterly_in_range(oi_dist):
    assert 0.0 <= oi_dist["bi_quarterly"] <= 100.0


def test_oi_dist_perpetual_is_float(oi_dist):
    assert isinstance(oi_dist["perpetual"], float)


def test_oi_dist_quarterly_is_float(oi_dist):
    assert isinstance(oi_dist["quarterly"], float)


def test_oi_dist_bi_quarterly_is_float(oi_dist):
    assert isinstance(oi_dist["bi_quarterly"], float)


# ── tenors list ───────────────────────────────────────────────────────────────


def test_tenors_is_list(tenors):
    assert isinstance(tenors, list)


def test_tenors_length_is_3(tenors):
    assert len(tenors) == 3


def test_tenor_names_unique(tenors):
    names = [t["tenor"] for t in tenors]
    assert len(names) == len(set(names))


def test_tenor_names_valid(tenors):
    for t in tenors:
        assert t["tenor"] in VALID_TENORS


def test_each_tenor_has_required_keys(tenors):
    for t in tenors:
        assert TENOR_KEYS.issubset(t.keys())


def test_oi_usd_positive(tenors):
    for t in tenors:
        assert t["oi_usd"] > 0


def test_oi_pct_in_range(tenors):
    for t in tenors:
        assert 0.0 <= t["oi_pct"] <= 100.0


def test_tenor_oi_pct_sum_to_100(tenors):
    total = sum(t["oi_pct"] for t in tenors)
    assert abs(total - 100.0) < 0.01


def test_basis_annualized_is_float(tenors):
    for t in tenors:
        assert isinstance(t["basis_annualized"], float)


def test_perp_days_to_expiry_is_zero(perp_tenor):
    assert perp_tenor["days_to_expiry"] == 0


def test_quarterly_days_to_expiry_positive(qtr_tenor):
    assert qtr_tenor["days_to_expiry"] > 0


def test_bi_quarterly_days_to_expiry_positive(biqtr_tenor):
    assert biqtr_tenor["days_to_expiry"] > 0


def test_quarterly_dte_less_than_bi_quarterly_dte(qtr_tenor, biqtr_tenor):
    assert qtr_tenor["days_to_expiry"] < biqtr_tenor["days_to_expiry"]


def test_perp_tenor_oi_usd_is_float(perp_tenor):
    assert isinstance(perp_tenor["oi_usd"], float)


def test_qtr_tenor_oi_usd_is_float(qtr_tenor):
    assert isinstance(qtr_tenor["oi_usd"], float)


def test_biqtr_tenor_oi_usd_is_float(biqtr_tenor):
    assert isinstance(biqtr_tenor["oi_usd"], float)


def test_all_oi_usd_positive(perp_tenor, qtr_tenor, biqtr_tenor):
    assert perp_tenor["oi_usd"] > 0
    assert qtr_tenor["oi_usd"] > 0
    assert biqtr_tenor["oi_usd"] > 0


# ── contango_backwardation ────────────────────────────────────────────────────


def test_contango_backwardation_is_str(result):
    assert isinstance(result["contango_backwardation"], str)


def test_contango_backwardation_valid(result):
    assert result["contango_backwardation"] in VALID_CB


# ── roll_cost_bps ─────────────────────────────────────────────────────────────


def test_roll_cost_bps_is_float(result):
    assert isinstance(result["roll_cost_bps"], float)


# ── symbol ────────────────────────────────────────────────────────────────────


def test_symbol_is_str(result):
    assert isinstance(result["symbol"], str)


def test_symbol_non_empty(result):
    assert len(result["symbol"]) > 0


# ── timestamp ─────────────────────────────────────────────────────────────────


def test_timestamp_is_str(result):
    assert isinstance(result["timestamp"], str)


def test_timestamp_non_empty(result):
    assert len(result["timestamp"]) > 0


# ── structure_health ──────────────────────────────────────────────────────────


def test_structure_health_is_str(result):
    assert isinstance(result["structure_health"], str)


def test_structure_health_valid(result):
    assert result["structure_health"] in VALID_HEALTH


# ── Determinism ───────────────────────────────────────────────────────────────


def test_deterministic_same_keys(result, result2):
    assert result.keys() == result2.keys()


def test_deterministic_oi_distribution(result, result2):
    assert result["oi_distribution"] == result2["oi_distribution"]


def test_deterministic_contango_backwardation(result, result2):
    assert result["contango_backwardation"] == result2["contango_backwardation"]


def test_deterministic_roll_cost_bps(result, result2):
    assert result["roll_cost_bps"] == result2["roll_cost_bps"]


def test_deterministic_structure_health(result, result2):
    assert result["structure_health"] == result2["structure_health"]


def test_deterministic_tenors_length(result, result2):
    assert len(result["tenors"]) == len(result2["tenors"])


def test_deterministic_tenor_names(result, result2):
    names1 = [t["tenor"] for t in result["tenors"]]
    names2 = [t["tenor"] for t in result2["tenors"]]
    assert names1 == names2


def test_deterministic_oi_usd_values(result, result2):
    for t1, t2 in zip(result["tenors"], result2["tenors"]):
        assert t1["oi_usd"] == t2["oi_usd"]


def test_deterministic_basis_values(result, result2):
    for t1, t2 in zip(result["tenors"], result2["tenors"]):
        assert t1["basis_annualized"] == t2["basis_annualized"]


def test_deterministic_symbol(result, result2):
    assert result["symbol"] == result2["symbol"]
