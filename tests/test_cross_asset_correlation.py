"""
Unit / smoke tests for /api/cross-asset-corr.

Cross-asset correlation: tracked alts (BANANAS31/COS/DEXE/LYN) vs
major crypto benchmarks (BTC/ETH/SOL/BNB) using rolling price-return
Pearson correlation.

Distinct from existing /correlations (symbol-to-symbol) and
/correlations/heatmap (20-period rolling between our 4 symbols):
  - Uses major crypto benchmarks as the reference axis
  - Configurable rolling window
  - Returns per-symbol benchmark correlation matrix + rolling history
  - Identifies strongest / weakest cross-asset pairs

Covers:
  - pearson_corr helper
  - log_returns helper
  - rolling_corr helper
  - corr_strength_label
  - corr_direction
  - build_cross_matrix
  - Response shape validation
  - Edge cases (constant series, short series, NaN-safe)
  - Route registration
  - HTML card / JS smoke tests
"""
import os
import sys
import math
import pytest

_ROOT = os.path.join(os.path.dirname(__file__), "..")


def _html() -> str:
    with open(os.path.join(_ROOT, "frontend", "index.html"), encoding="utf-8") as f:
        return f.read()


def _js() -> str:
    with open(os.path.join(_ROOT, "frontend", "app.js"), encoding="utf-8") as f:
        return f.read()


# ── Python mirrors of backend helpers ─────────────────────────────────────────

SYMBOLS    = ("BANANAS31USDT", "COSUSDT", "DEXEUSDT", "LYNUSDT")
BENCHMARKS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT")


def log_returns(prices: list[float]) -> list[float]:
    """Compute log-return series from a price sequence."""
    rets = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0 and prices[i] > 0:
            rets.append(math.log(prices[i] / prices[i - 1]))
        else:
            rets.append(0.0)
    return rets


def pearson_corr(x: list[float], y: list[float]) -> float | None:
    """
    Pearson correlation coefficient for two equal-length sequences.
    Returns None if computation is not possible (constant series, <2 pts).
    """
    n = min(len(x), len(y))
    if n < 2:
        return None
    x = x[:n]
    y = y[:n]
    mx = sum(x) / n
    my = sum(y) / n
    num   = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    dx    = math.sqrt(sum((xi - mx) ** 2 for xi in x))
    dy    = math.sqrt(sum((yi - my) ** 2 for yi in y))
    if dx == 0 or dy == 0:
        return None
    return round(max(-1.0, min(1.0, num / (dx * dy))), 6)


def rolling_corr(
    x: list[float],
    y: list[float],
    window: int,
) -> list[float | None]:
    """
    Compute rolling Pearson correlation with the given window size.
    Returns a list the same length as x; first (window-1) entries are None.
    """
    n = min(len(x), len(y))
    result: list[float | None] = []
    for i in range(n):
        if i < window - 1:
            result.append(None)
        else:
            xi = x[i - window + 1 : i + 1]
            yi = y[i - window + 1 : i + 1]
            result.append(pearson_corr(xi, yi))
    return result


def corr_strength_label(r: float | None) -> str:
    """Classify |r| into a human label."""
    if r is None:
        return "n/a"
    a = abs(r)
    if a >= 0.7:
        return "strong"
    if a >= 0.4:
        return "moderate"
    if a >= 0.2:
        return "weak"
    return "none"


def corr_direction(r: float | None) -> str:
    """Return 'positive', 'negative', or 'neutral'."""
    if r is None:
        return "neutral"
    if r > 0.1:
        return "positive"
    if r < -0.1:
        return "negative"
    return "neutral"


def build_cross_matrix(
    sym_returns: dict[str, list[float]],
    bench_returns: dict[str, list[float]],
) -> dict[str, dict[str, float | None]]:
    """
    Build a correlation matrix: {symbol: {benchmark: corr}}.
    Aligns on minimum length.
    """
    matrix: dict = {}
    for sym, sx in sym_returns.items():
        matrix[sym] = {}
        for bench, bx in bench_returns.items():
            n = min(len(sx), len(bx))
            matrix[sym][bench] = pearson_corr(sx[:n], bx[:n])
    return matrix


# ── Sample data for tests ─────────────────────────────────────────────────────

import random as _random
_random.seed(1337)

# Simulate correlated price series: alt = BTC * (1 + noise)
_btc  = [30000.0 + _random.gauss(0, 100) * i ** 0.1 for i in range(1, 61)]
_eth  = [1800.0  + _random.gauss(0, 20)  * i ** 0.1 for i in range(1, 61)]
_sol  = [20.0    + _random.gauss(0, 0.5) * i ** 0.1 for i in range(1, 61)]
_bnb  = [300.0   + _random.gauss(0, 5)   * i ** 0.1 for i in range(1, 61)]
_alt1 = [p * (1 + _random.gauss(0, 0.002)) for p in _btc]
_alt2 = [p * (1 + _random.gauss(0, 0.003)) for p in _eth]

BTC_RETS  = log_returns([max(0.001, p) for p in _btc])
ETH_RETS  = log_returns([max(0.001, p) for p in _eth])
SOL_RETS  = log_returns([max(0.001, p) for p in _sol])
BNB_RETS  = log_returns([max(0.001, p) for p in _bnb])
ALT1_RETS = log_returns([max(0.001, p) for p in _alt1])
ALT2_RETS = log_returns([max(0.001, p) for p in _alt2])

CONSTANT_PRICES = [1.0] * 30
CONSTANT_RETS   = log_returns(CONSTANT_PRICES)  # all 0.0


SAMPLE_RESPONSE = {
    "status": "ok",
    "symbol": "BANANAS31USDT",
    "window_seconds": 3600,
    "bucket_seconds": 60,
    "symbols": ["BANANAS31USDT", "COSUSDT", "DEXEUSDT", "LYNUSDT"],
    "benchmarks": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"],
    "matrix": {
        "BANANAS31USDT": {
            "BTCUSDT": 0.72, "ETHUSDT": 0.68, "SOLUSDT": 0.55, "BNBUSDT": 0.61,
        },
        "COSUSDT": {
            "BTCUSDT": 0.45, "ETHUSDT": 0.50, "SOLUSDT": 0.38, "BNBUSDT": 0.42,
        },
        "DEXEUSDT": {
            "BTCUSDT": 0.30, "ETHUSDT": 0.35, "SOLUSDT": 0.28, "BNBUSDT": 0.32,
        },
        "LYNUSDT": {
            "BTCUSDT": -0.10, "ETHUSDT": -0.05, "SOLUSDT": 0.08, "BNBUSDT": 0.02,
        },
    },
    "rolling": {
        "BANANAS31USDT": [
            {"ts": 1700000000.0, "BTCUSDT": 0.65, "ETHUSDT": 0.60},
            {"ts": 1700000060.0, "BTCUSDT": 0.70, "ETHUSDT": 0.65},
        ],
    },
    "strongest_pair": {"symbol": "BANANAS31USDT", "benchmark": "BTCUSDT", "corr": 0.72},
    "weakest_pair":   {"symbol": "LYNUSDT",        "benchmark": "BTCUSDT", "corr": -0.10},
    "description": "BANANAS31USDT shows strongest correlation with BTCUSDT (0.72)",
}


# ── log_returns tests ─────────────────────────────────────────────────────────

def test_log_returns_empty():
    assert log_returns([]) == []


def test_log_returns_single():
    assert log_returns([1.0]) == []


def test_log_returns_length():
    prices = [1.0, 1.1, 0.9, 1.05, 1.2]
    rets = log_returns(prices)
    assert len(rets) == len(prices) - 1


def test_log_returns_positive_price_increase():
    rets = log_returns([1.0, 2.0])
    assert rets[0] == pytest.approx(math.log(2.0))


def test_log_returns_flat_prices():
    rets = log_returns([1.0] * 5)
    for r in rets:
        assert r == pytest.approx(0.0)


def test_log_returns_nonneg_boundary():
    # zero price → 0.0 return (safe fallback)
    rets = log_returns([0.0, 1.0])
    assert rets[0] == 0.0


# ── pearson_corr tests ────────────────────────────────────────────────────────

def test_pearson_perfect_positive():
    x = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert pearson_corr(x, x) == pytest.approx(1.0)


def test_pearson_perfect_negative():
    x = [1.0, 2.0, 3.0, 4.0, 5.0]
    y = [-1.0, -2.0, -3.0, -4.0, -5.0]
    assert pearson_corr(x, y) == pytest.approx(-1.0)


def test_pearson_uncorrelated_approx():
    x = [1.0, 2.0, 3.0, 4.0, 5.0]
    y = [3.0, 1.0, 4.0, 1.0, 5.0]
    r = pearson_corr(x, y)
    assert r is not None
    assert -1.0 <= r <= 1.0


def test_pearson_constant_series():
    x = [1.0, 1.0, 1.0, 1.0]
    y = [1.0, 2.0, 3.0, 4.0]
    assert pearson_corr(x, y) is None


def test_pearson_both_constant():
    x = [1.0] * 5
    assert pearson_corr(x, x) is None


def test_pearson_single_point():
    assert pearson_corr([1.0], [2.0]) is None


def test_pearson_empty():
    assert pearson_corr([], []) is None


def test_pearson_clamped_to_bounds():
    x = [1.0, 2.0, 3.0]
    r = pearson_corr(x, x)
    assert r is not None
    assert -1.0 <= r <= 1.0


def test_pearson_correlated_alts():
    r = pearson_corr(ALT1_RETS, BTC_RETS)
    # alt1 was constructed to be correlated with BTC
    assert r is not None
    assert r > 0.5


# ── rolling_corr tests ────────────────────────────────────────────────────────

def test_rolling_corr_length():
    x = [float(i) for i in range(1, 21)]
    y = [float(i) * 1.1 for i in range(1, 21)]
    rc = rolling_corr(x, y, window=5)
    assert len(rc) == len(x)


def test_rolling_corr_leading_nones():
    x = [1.0, 2.0, 3.0, 4.0, 5.0]
    y = [1.0, 2.0, 3.0, 4.0, 5.0]
    rc = rolling_corr(x, y, window=3)
    assert rc[0] is None
    assert rc[1] is None
    assert rc[2] is not None


def test_rolling_corr_window_1():
    x = [1.0, 2.0, 3.0]
    y = [1.0, 2.0, 3.0]
    rc = rolling_corr(x, y, window=1)
    # window=1 → all None (can't compute corr with 1 point)
    assert all(v is None for v in rc)


def test_rolling_corr_perfect_positive():
    x = [float(i) for i in range(1, 11)]
    rc = rolling_corr(x, x, window=5)
    for v in rc[4:]:
        assert v == pytest.approx(1.0)


def test_rolling_corr_no_negative_out_of_bounds():
    x = ALT1_RETS[:20]
    y = BTC_RETS[:20]
    for v in rolling_corr(x, y, window=5):
        if v is not None:
            assert -1.0 <= v <= 1.0


# ── corr_strength_label tests ─────────────────────────────────────────────────

def test_strength_strong():
    assert corr_strength_label(0.8) == "strong"
    assert corr_strength_label(-0.75) == "strong"


def test_strength_moderate():
    assert corr_strength_label(0.5) == "moderate"
    assert corr_strength_label(-0.45) == "moderate"


def test_strength_weak():
    assert corr_strength_label(0.25) == "weak"
    assert corr_strength_label(-0.3) == "weak"


def test_strength_none():
    assert corr_strength_label(0.1) == "none"
    assert corr_strength_label(0.0) == "none"


def test_strength_none_input():
    assert corr_strength_label(None) == "n/a"


def test_strength_boundary_07():
    assert corr_strength_label(0.7) == "strong"


def test_strength_boundary_04():
    assert corr_strength_label(0.4) == "moderate"


def test_strength_boundary_02():
    assert corr_strength_label(0.2) == "weak"


# ── corr_direction tests ──────────────────────────────────────────────────────

def test_direction_positive():
    assert corr_direction(0.5) == "positive"


def test_direction_negative():
    assert corr_direction(-0.5) == "negative"


def test_direction_neutral_zero():
    assert corr_direction(0.0) == "neutral"


def test_direction_neutral_small():
    assert corr_direction(0.05) == "neutral"
    assert corr_direction(-0.05) == "neutral"


def test_direction_none_input():
    assert corr_direction(None) == "neutral"


def test_direction_boundary_pos():
    assert corr_direction(0.11) == "positive"


def test_direction_boundary_neg():
    assert corr_direction(-0.11) == "negative"


# ── build_cross_matrix tests ──────────────────────────────────────────────────

def test_cross_matrix_shape():
    sym_r   = {"A": ALT1_RETS[:20], "B": ALT2_RETS[:20]}
    bench_r = {"BTC": BTC_RETS[:20], "ETH": ETH_RETS[:20]}
    m = build_cross_matrix(sym_r, bench_r)
    assert set(m.keys()) == {"A", "B"}
    for row in m.values():
        assert set(row.keys()) == {"BTC", "ETH"}


def test_cross_matrix_diagonal_not_required():
    # symbols and benchmarks are different axes — no self-correlation needed
    sym_r   = {"A": ALT1_RETS[:20]}
    bench_r = {"BTC": BTC_RETS[:20]}
    m = build_cross_matrix(sym_r, bench_r)
    assert "A" in m
    assert "BTC" in m["A"]


def test_cross_matrix_values_in_range():
    sym_r   = {"A": ALT1_RETS[:30], "B": ALT2_RETS[:30]}
    bench_r = {"BTC": BTC_RETS[:30], "ETH": ETH_RETS[:30]}
    m = build_cross_matrix(sym_r, bench_r)
    for row in m.values():
        for v in row.values():
            if v is not None:
                assert -1.0 <= v <= 1.0


def test_cross_matrix_constant_series_is_none():
    sym_r   = {"A": CONSTANT_RETS[:20]}
    bench_r = {"BTC": BTC_RETS[:20]}
    m = build_cross_matrix(sym_r, bench_r)
    # constant returns → None correlation
    assert m["A"]["BTC"] is None


def test_cross_matrix_correlated_pair():
    sym_r   = {"ALT1": ALT1_RETS}
    bench_r = {"BTC":  BTC_RETS}
    m = build_cross_matrix(sym_r, bench_r)
    r = m["ALT1"]["BTC"]
    assert r is not None
    assert r > 0.5  # alt1 was constructed correlated with btc


# ── Response shape tests ──────────────────────────────────────────────────────

def test_response_status():
    assert SAMPLE_RESPONSE["status"] == "ok"


def test_response_required_keys():
    for key in (
        "symbol", "window_seconds", "bucket_seconds",
        "symbols", "benchmarks", "matrix",
        "rolling", "strongest_pair", "weakest_pair", "description",
    ):
        assert key in SAMPLE_RESPONSE


def test_response_symbols_list():
    assert isinstance(SAMPLE_RESPONSE["symbols"], list)
    assert len(SAMPLE_RESPONSE["symbols"]) > 0


def test_response_benchmarks_list():
    assert isinstance(SAMPLE_RESPONSE["benchmarks"], list)
    assert len(SAMPLE_RESPONSE["benchmarks"]) > 0


def test_response_matrix_shape():
    m = SAMPLE_RESPONSE["matrix"]
    for sym in SAMPLE_RESPONSE["symbols"]:
        assert sym in m
        for bench in SAMPLE_RESPONSE["benchmarks"]:
            assert bench in m[sym]


def test_response_matrix_values_in_range():
    for row in SAMPLE_RESPONSE["matrix"].values():
        for v in row.values():
            if v is not None:
                assert -1.0 <= v <= 1.0


def test_response_rolling_is_dict():
    assert isinstance(SAMPLE_RESPONSE["rolling"], dict)


def test_response_rolling_history_keys():
    for hist in SAMPLE_RESPONSE["rolling"].values():
        for pt in hist:
            assert "ts" in pt


def test_response_strongest_pair_keys():
    sp = SAMPLE_RESPONSE["strongest_pair"]
    for key in ("symbol", "benchmark", "corr"):
        assert key in sp


def test_response_weakest_pair_keys():
    wp = SAMPLE_RESPONSE["weakest_pair"]
    for key in ("symbol", "benchmark", "corr"):
        assert key in wp


def test_response_description_nonempty():
    assert isinstance(SAMPLE_RESPONSE["description"], str)
    assert len(SAMPLE_RESPONSE["description"]) > 0


def test_response_window_positive():
    assert SAMPLE_RESPONSE["window_seconds"] > 0


def test_response_bucket_positive():
    assert SAMPLE_RESPONSE["bucket_seconds"] > 0


# ── Route registration ────────────────────────────────────────────────────────

def test_cross_asset_corr_route_registered():
    import tempfile
    os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "t.db"))
    os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from api import router
    paths = [r.path for r in router.routes]
    assert any("cross-asset-corr" in p for p in paths)


# ── HTML / JS smoke tests ─────────────────────────────────────────────────────

def test_html_has_cross_asset_corr_card():
    assert "card-cross-asset-corr" in _html()


def test_js_has_render_cross_asset_corr():
    assert "renderCrossAssetCorr" in _js()


def test_js_calls_cross_asset_corr_api():
    assert "cross-asset-corr" in _js()
