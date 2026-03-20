"""
Tests for chart initialization fixes (issue #166):
  - Volume Profile chart: initVolumeProfileChart must exist and target correct canvas
  - Adaptive VP chart: initAdaptiveVpChart must exist and target correct canvas
  - Aggressor chart: initAggressorChart must use vertical bars (not indexAxis:'y')
  - Correlations: renderCorrelations must guard against empty matrix objects
"""
import os
import re

_HERE = os.path.dirname(__file__)
_ROOT = os.path.join(_HERE, "..")
_JS   = os.path.join(_ROOT, "frontend", "app.js")
_HTML = os.path.join(_ROOT, "frontend", "index.html")


def _read(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


class TestVolumeProfileChartInit:
    def test_init_function_defined(self):
        js = _read(_JS)
        assert "function initVolumeProfileChart(" in js

    def test_targets_volume_profile_canvas(self):
        js = _read(_JS)
        # Extract function body
        m = re.search(
            r'function initVolumeProfileChart\(\)(.*?)^}',
            js, re.DOTALL | re.MULTILINE
        )
        assert m, "initVolumeProfileChart not found"
        body = m.group(1)
        assert "volume-profile-canvas" in body

    def test_assigns_to_volume_profile_chart_var(self):
        js = _read(_JS)
        m = re.search(
            r'function initVolumeProfileChart\(\)(.*?)^}',
            js, re.DOTALL | re.MULTILINE
        )
        assert m
        body = m.group(1)
        assert "volumeProfileChart" in body

    def test_uses_horizontal_bars(self):
        js = _read(_JS)
        m = re.search(
            r'function initVolumeProfileChart\(\)(.*?)^}',
            js, re.DOTALL | re.MULTILINE
        )
        assert m
        body = m.group(1)
        assert "indexAxis" in body and "'y'" in body

    def test_canvas_exists_in_html(self):
        html = _read(_HTML)
        assert 'id="volume-profile-canvas"' in html

    def test_init_called_in_init_function(self):
        js = _read(_JS)
        assert "safeInit(initVolumeProfileChart)" in js


class TestAdaptiveVpChartInit:
    def test_init_function_defined(self):
        js = _read(_JS)
        assert "function initAdaptiveVpChart(" in js

    def test_targets_adaptive_vp_canvas(self):
        js = _read(_JS)
        m = re.search(
            r'function initAdaptiveVpChart\(\)(.*?)^}',
            js, re.DOTALL | re.MULTILINE
        )
        assert m, "initAdaptiveVpChart not found"
        body = m.group(1)
        assert "adaptive-vp-canvas" in body

    def test_assigns_to_adaptive_vp_chart_var(self):
        js = _read(_JS)
        m = re.search(
            r'function initAdaptiveVpChart\(\)(.*?)^}',
            js, re.DOTALL | re.MULTILINE
        )
        assert m
        body = m.group(1)
        assert "adaptiveVpChart" in body


class TestAggressorChartInit:
    def test_init_function_defined(self):
        js = _read(_JS)
        assert "function initAggressorChart(" in js

    def test_targets_aggressor_ratio_canvas(self):
        js = _read(_JS)
        m = re.search(
            r'function initAggressorChart\(\)(.*?)^}',
            js, re.DOTALL | re.MULTILINE
        )
        assert m, "initAggressorChart not found"
        body = m.group(1)
        assert "aggressor-ratio-canvas" in body

    def test_does_not_use_horizontal_index_axis(self):
        """Aggressor chart is a time-series (vertical bars), not horizontal."""
        js = _read(_JS)
        m = re.search(
            r'function initAggressorChart\(\)(.*?)^}',
            js, re.DOTALL | re.MULTILINE
        )
        assert m
        body = m.group(1)
        # indexAxis:'y' would make it horizontal — wrong for time series
        assert "indexAxis" not in body or "'y'" not in body

    def test_buy_dataset_has_green_color(self):
        js = _read(_JS)
        m = re.search(
            r'function initAggressorChart\(\)(.*?)^}',
            js, re.DOTALL | re.MULTILINE
        )
        assert m
        body = m.group(1)
        # Should have a non-empty green background for Buy
        assert "0,224,130" in body  # green rgba

    def test_sell_dataset_has_red_color(self):
        js = _read(_JS)
        m = re.search(
            r'function initAggressorChart\(\)(.*?)^}',
            js, re.DOTALL | re.MULTILINE
        )
        assert m
        body = m.group(1)
        # Should have a non-empty red background for Sell
        assert "255,77,79" in body  # red rgba

    def test_uses_opts_from_chart_defaults(self):
        js = _read(_JS)
        m = re.search(
            r'function initAggressorChart\(\)(.*?)^}',
            js, re.DOTALL | re.MULTILINE
        )
        assert m
        body = m.group(1)
        # Should use the shared opts object
        assert "_chartDefaults" in body
        assert "options: opts" in body


class TestCorrelationsEmptyMatrix:
    def test_empty_matrix_guard_present(self):
        """renderCorrelations must check for empty matrix object, not just falsy."""
        js = _read(_JS)
        # Should check Object.keys length for empty matrix
        assert "Object.keys(data.matrix).length === 0" in js

    def test_guard_includes_falsy_check(self):
        """Should also guard against null/undefined matrix."""
        js = _read(_JS)
        # Check the combined guard pattern
        assert "!data?.matrix || Object.keys(data.matrix).length === 0" in js
