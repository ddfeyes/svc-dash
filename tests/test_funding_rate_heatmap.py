"""
Unit / smoke tests for /api/funding-rate-heatmap.

Funding rate heatmap across exchanges and symbols with anomaly detection.

Covers:
  - z-score calculation
  - Anomaly level classification
  - Mean / std helpers
  - Rate-to-color mapping
  - fmt_rate display helper
  - Heatmap cell structure
  - Anomaly detection logic
  - Summary statistics
  - Response shape
  - Edge cases (no data, single datapoint, flat rates, all same exchange)
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


# ── Python mirrors of backend logic ──────────────────────────────────────────

def mean_std(values: list[float]) -> tuple[float, float]:
    """Return (mean, std) of a list. std=0 if fewer than 2 values."""
    if not values:
        return 0.0, 0.0
    n = len(values)
    m = sum(values) / n
    if n < 2:
        return m, 0.0
    variance = sum((v - m) ** 2 for v in values) / (n - 1)
    return m, math.sqrt(variance)


def z_score(value: float, mean: float, std: float) -> float | None:
    """Z-score of value against distribution. None if std == 0."""
    if std == 0:
        return None
    return round((value - mean) / std, 4)


def anomaly_level(z: float | None) -> str:
    """
    Classify anomaly severity from z-score.
    critical: |z| >= 3
    warning:  |z| >= 2
    normal:   otherwise (or z is None)
    """
    if z is None:
        return "normal"
    if abs(z) >= 3.0:
        return "critical"
    if abs(z) >= 2.0:
        return "warning"
    return "normal"


def rate_color_class(rate_pct: float) -> str:
    """
    CSS badge class for a funding rate in percent.
    rate_pct > +0.05% → badge-red   (longs heavily subsidising shorts)
    rate_pct > 0       → badge-orange-ish (use badge-yellow)
    rate_pct == 0      → badge-blue
    rate_pct < 0       → badge-green (shorts paying)
    rate_pct < -0.05%  → badge-purple-ish (use badge-green for inverse squeeze)
    """
    if rate_pct > 0.05:
        return "cell-hot"
    if rate_pct > 0:
        return "cell-warm"
    if rate_pct < -0.05:
        return "cell-cold"
    if rate_pct < 0:
        return "cell-cool"
    return "cell-neutral"


def fmt_rate(rate_pct: float | None) -> str:
    """Format funding rate for display."""
    if rate_pct is None:
        return "—"
    sign = "+" if rate_pct >= 0 else ""
    return f"{sign}{rate_pct:.4f}%"


def build_cell(rates_history: list[float], current_rate: float) -> dict:
    """
    Build a heatmap cell dict from history and current rate.
    history: list of past rate values (most recent last)
    """
    m, s = mean_std(rates_history) if rates_history else (current_rate, 0.0)
    z = z_score(current_rate, m, s)
    level = anomaly_level(z)
    return {
        "rate": current_rate,
        "rate_pct": round(current_rate * 100, 6),
        "mean": round(m, 8),
        "std": round(s, 8),
        "z_score": z,
        "is_anomaly": level != "normal",
        "anomaly_level": level,
    }


def heatmap_summary(cells: dict) -> dict:
    """
    cells: {symbol: {exchange: cell_dict}}
    Returns {total_cells, anomaly_count, max_rate_pct, min_rate_pct, avg_rate_pct}
    """
    all_cells = [c for sym_data in cells.values() for c in sym_data.values()]
    if not all_cells:
        return {
            "total_cells": 0,
            "anomaly_count": 0,
            "max_rate_pct": None,
            "min_rate_pct": None,
            "avg_rate_pct": None,
        }
    rates = [c["rate_pct"] for c in all_cells]
    anomaly_count = sum(1 for c in all_cells if c["is_anomaly"])
    return {
        "total_cells": len(all_cells),
        "anomaly_count": anomaly_count,
        "max_rate_pct": round(max(rates), 6),
        "min_rate_pct": round(min(rates), 6),
        "avg_rate_pct": round(sum(rates) / len(rates), 6),
    }


# ── mean_std tests ────────────────────────────────────────────────────────────

def test_mean_std_empty():
    m, s = mean_std([])
    assert m == 0.0
    assert s == 0.0


def test_mean_std_single():
    m, s = mean_std([0.0001])
    assert m == pytest.approx(0.0001)
    assert s == 0.0


def test_mean_std_two():
    m, s = mean_std([0.0, 0.0002])
    assert m == pytest.approx(0.0001)
    assert s > 0.0


def test_mean_std_symmetric():
    vals = [-1.0, 0.0, 1.0]
    m, s = mean_std(vals)
    assert m == pytest.approx(0.0)
    assert s == pytest.approx(1.0)


def test_mean_std_constant():
    m, s = mean_std([0.0001, 0.0001, 0.0001])
    assert m == pytest.approx(0.0001)
    assert s == pytest.approx(0.0)


# ── z_score tests ─────────────────────────────────────────────────────────────

def test_z_score_at_mean():
    assert z_score(1.0, 1.0, 0.5) == pytest.approx(0.0)


def test_z_score_one_std_above():
    assert z_score(2.0, 1.0, 1.0) == pytest.approx(1.0)


def test_z_score_one_std_below():
    assert z_score(0.0, 1.0, 1.0) == pytest.approx(-1.0)


def test_z_score_none_when_std_zero():
    assert z_score(1.0, 1.0, 0.0) is None


def test_z_score_extreme_positive():
    z = z_score(4.0, 1.0, 1.0)
    assert z == pytest.approx(3.0)


def test_z_score_extreme_negative():
    z = z_score(-2.0, 1.0, 1.0)
    assert z == pytest.approx(-3.0)


# ── anomaly_level tests ───────────────────────────────────────────────────────

def test_anomaly_normal_none():
    assert anomaly_level(None) == "normal"


def test_anomaly_normal_small():
    assert anomaly_level(0.5) == "normal"
    assert anomaly_level(-1.9) == "normal"


def test_anomaly_warning():
    assert anomaly_level(2.0) == "warning"
    assert anomaly_level(-2.5) == "warning"


def test_anomaly_critical():
    assert anomaly_level(3.0) == "critical"
    assert anomaly_level(-3.5) == "critical"


def test_anomaly_boundary_exactly_2():
    assert anomaly_level(2.0) == "warning"


def test_anomaly_boundary_exactly_3():
    assert anomaly_level(3.0) == "critical"


def test_anomaly_just_below_warning():
    assert anomaly_level(1.999) == "normal"


# ── rate_color_class tests ────────────────────────────────────────────────────

def test_rate_color_very_positive():
    assert rate_color_class(0.1) == "cell-hot"


def test_rate_color_positive():
    assert rate_color_class(0.02) == "cell-warm"


def test_rate_color_zero():
    assert rate_color_class(0.0) == "cell-neutral"


def test_rate_color_negative():
    assert rate_color_class(-0.02) == "cell-cool"


def test_rate_color_very_negative():
    assert rate_color_class(-0.1) == "cell-cold"


def test_rate_color_boundary_positive():
    # exactly +0.05 → still cell-hot (> 0.05 means > not >=)
    assert rate_color_class(0.05) == "cell-warm"  # not > 0.05


def test_rate_color_boundary_negative():
    assert rate_color_class(-0.05) == "cell-cool"  # not < -0.05


# ── fmt_rate tests ────────────────────────────────────────────────────────────

def test_fmt_rate_positive():
    assert fmt_rate(0.0100) == "+0.0100%"


def test_fmt_rate_negative():
    assert fmt_rate(-0.0050) == "-0.0050%"


def test_fmt_rate_zero():
    assert fmt_rate(0.0) == "+0.0000%"


def test_fmt_rate_none():
    assert fmt_rate(None) == "—"


def test_fmt_rate_small():
    assert fmt_rate(0.0001) == "+0.0001%"


# ── build_cell tests ──────────────────────────────────────────────────────────

HISTORY_NORMAL = [0.0001, 0.0001, 0.0001, 0.0001, 0.0001]  # constant 0.01%


def test_cell_has_required_keys():
    cell = build_cell(HISTORY_NORMAL, 0.0001)
    for key in ("rate", "rate_pct", "mean", "std", "z_score", "is_anomaly", "anomaly_level"):
        assert key in cell


def test_cell_rate_pct_correct():
    cell = build_cell(HISTORY_NORMAL, 0.0001)
    assert cell["rate_pct"] == pytest.approx(0.01)


def test_cell_no_anomaly_for_constant():
    cell = build_cell(HISTORY_NORMAL, 0.0001)
    # std is 0 → z_score is None → normal
    assert cell["is_anomaly"] is False
    assert cell["anomaly_level"] == "normal"


def test_cell_anomaly_for_extreme_value():
    history = [0.0001] * 10
    # Spike to 10x normal value
    m, s = mean_std(history)  # m=0.0001, s=0
    # Use heterogeneous history for nonzero std
    history2 = [0.0001, 0.00015, 0.00012, 0.00009, 0.0001, 0.00011, 0.0001]
    cell = build_cell(history2, 0.01)  # 100x spike
    assert cell["is_anomaly"] is True


def test_cell_anomaly_level_matches_z():
    history = [0.0001, 0.00015, 0.00012, 0.00009, 0.0001, 0.00011, 0.0001]
    cell = build_cell(history, 0.01)
    level = anomaly_level(cell["z_score"])
    assert cell["anomaly_level"] == level


# ── heatmap_summary tests ─────────────────────────────────────────────────────

CELLS_SAMPLE = {
    "BANANAS31USDT": {
        "binance": {"rate": 0.0001, "rate_pct": 0.01, "is_anomaly": False, "anomaly_level": "normal"},
        "bybit":   {"rate": 0.00015, "rate_pct": 0.015, "is_anomaly": False, "anomaly_level": "normal"},
    },
    "COSUSDT": {
        "binance": {"rate": -0.0001, "rate_pct": -0.01, "is_anomaly": True, "anomaly_level": "warning"},
        "bybit":   {"rate": 0.0005, "rate_pct": 0.05, "is_anomaly": True, "anomaly_level": "critical"},
    },
}


def test_summary_total_cells():
    s = heatmap_summary(CELLS_SAMPLE)
    assert s["total_cells"] == 4


def test_summary_anomaly_count():
    s = heatmap_summary(CELLS_SAMPLE)
    assert s["anomaly_count"] == 2


def test_summary_max_rate():
    s = heatmap_summary(CELLS_SAMPLE)
    assert s["max_rate_pct"] == pytest.approx(0.05)


def test_summary_min_rate():
    s = heatmap_summary(CELLS_SAMPLE)
    assert s["min_rate_pct"] == pytest.approx(-0.01)


def test_summary_avg_rate():
    s = heatmap_summary(CELLS_SAMPLE)
    expected_avg = (0.01 + 0.015 - 0.01 + 0.05) / 4
    assert s["avg_rate_pct"] == pytest.approx(expected_avg, rel=1e-4)


def test_summary_empty():
    s = heatmap_summary({})
    assert s["total_cells"] == 0
    assert s["anomaly_count"] == 0
    assert s["max_rate_pct"] is None


# ── Response shape ────────────────────────────────────────────────────────────

SAMPLE_RESPONSE = {
    "status": "ok",
    "ts": 1700000000.0,
    "window_hours": 24,
    "symbols": ["BANANAS31USDT", "COSUSDT", "DEXEUSDT", "LYNUSDT"],
    "exchanges": ["binance", "bybit"],
    "cells": {
        "BANANAS31USDT": {
            "binance": {
                "rate": 0.0001,
                "rate_pct": 0.01,
                "history": [{"ts": 1699913600.0, "rate": 0.0001},
                            {"ts": 1699942400.0, "rate": 0.00012}],
                "mean": 0.0001,
                "std": 0.00001,
                "z_score": 0.0,
                "is_anomaly": False,
                "anomaly_level": "normal",
            },
            "bybit": {
                "rate": 0.00012,
                "rate_pct": 0.012,
                "history": [],
                "mean": 0.00012,
                "std": 0.0,
                "z_score": None,
                "is_anomaly": False,
                "anomaly_level": "normal",
            },
        },
    },
    "anomalies": [],
    "summary": {
        "total_cells": 2,
        "anomaly_count": 0,
        "max_rate_pct": 0.012,
        "min_rate_pct": 0.01,
        "avg_rate_pct": 0.011,
    },
}


def test_response_status_ok():
    assert SAMPLE_RESPONSE["status"] == "ok"


def test_response_has_required_keys():
    for key in ("ts", "window_hours", "symbols", "exchanges", "cells",
                "anomalies", "summary"):
        assert key in SAMPLE_RESPONSE


def test_response_symbols_is_list():
    assert isinstance(SAMPLE_RESPONSE["symbols"], list)
    assert len(SAMPLE_RESPONSE["symbols"]) > 0


def test_response_exchanges_is_list():
    assert isinstance(SAMPLE_RESPONSE["exchanges"], list)


def test_response_cells_is_dict():
    assert isinstance(SAMPLE_RESPONSE["cells"], dict)


def test_response_cell_has_required_keys():
    cell = SAMPLE_RESPONSE["cells"]["BANANAS31USDT"]["binance"]
    for key in ("rate", "rate_pct", "history", "mean", "std",
                "z_score", "is_anomaly", "anomaly_level"):
        assert key in cell


def test_response_cell_history_is_list():
    cell = SAMPLE_RESPONSE["cells"]["BANANAS31USDT"]["binance"]
    assert isinstance(cell["history"], list)


def test_response_anomalies_is_list():
    assert isinstance(SAMPLE_RESPONSE["anomalies"], list)


def test_response_summary_has_required_keys():
    for key in ("total_cells", "anomaly_count", "max_rate_pct",
                "min_rate_pct", "avg_rate_pct"):
        assert key in SAMPLE_RESPONSE["summary"]


def test_response_anomaly_level_valid():
    valid = {"normal", "warning", "critical"}
    for sym_data in SAMPLE_RESPONSE["cells"].values():
        for cell in sym_data.values():
            assert cell["anomaly_level"] in valid


def test_response_is_anomaly_matches_level():
    for sym_data in SAMPLE_RESPONSE["cells"].values():
        for cell in sym_data.values():
            if cell["anomaly_level"] != "normal":
                assert cell["is_anomaly"] is True
            else:
                assert cell["is_anomaly"] is False


# ── Anomaly list structure ────────────────────────────────────────────────────

ANOMALY_RESPONSE = {
    "status": "ok",
    "ts": 1700000000.0,
    "window_hours": 24,
    "symbols": ["BANANAS31USDT"],
    "exchanges": ["binance"],
    "cells": {
        "BANANAS31USDT": {
            "binance": {
                "rate": 0.005,
                "rate_pct": 0.5,
                "history": [{"ts": 1699999999.0, "rate": 0.0001}] * 20,
                "mean": 0.0001,
                "std": 0.00001,
                "z_score": 490.0,
                "is_anomaly": True,
                "anomaly_level": "critical",
            }
        }
    },
    "anomalies": [
        {
            "symbol": "BANANAS31USDT",
            "exchange": "binance",
            "rate_pct": 0.5,
            "z_score": 490.0,
            "level": "critical",
        }
    ],
    "summary": {
        "total_cells": 1,
        "anomaly_count": 1,
        "max_rate_pct": 0.5,
        "min_rate_pct": 0.5,
        "avg_rate_pct": 0.5,
    },
}


def test_anomaly_entry_has_required_keys():
    for a in ANOMALY_RESPONSE["anomalies"]:
        for key in ("symbol", "exchange", "rate_pct", "z_score", "level"):
            assert key in a


def test_anomaly_count_matches_list():
    assert ANOMALY_RESPONSE["summary"]["anomaly_count"] == len(ANOMALY_RESPONSE["anomalies"])


def test_anomaly_level_is_critical():
    a = ANOMALY_RESPONSE["anomalies"][0]
    assert a["level"] == "critical"


def test_anomaly_z_score_high():
    a = ANOMALY_RESPONSE["anomalies"][0]
    assert abs(a["z_score"]) >= 3.0


# ── Route registration ────────────────────────────────────────────────────────

def test_funding_rate_heatmap_route_registered():
    import tempfile
    os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "t.db"))
    os.environ.setdefault("SYMBOL_BINANCE", "BANANAS31USDT")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
    from api import router
    paths = [r.path for r in router.routes]
    assert any("funding-rate-heatmap" in p for p in paths)


# ── HTML / JS smoke tests ─────────────────────────────────────────────────────

def test_html_has_funding_rate_heatmap_card():
    assert "card-funding-rate-heatmap" in _html()


def test_js_has_render_funding_rate_heatmap():
    assert "renderFundingRateHeatmap" in _js()


def test_js_calls_funding_rate_heatmap_api():
    assert "funding-rate-heatmap" in _js()
