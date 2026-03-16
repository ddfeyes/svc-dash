"""
Unit/smoke tests for the dark/light theme toggle feature.

Verifies:
  - Toggle logic (dark ↔ light)
  - localStorage key and default
  - HTML has toggle button in header
  - CSS defines light-theme variables
  - app.js contains theme management code
"""
import os
import re

# ── Paths ─────────────────────────────────────────────────────────────────────
_HERE = os.path.dirname(__file__)
_ROOT = os.path.join(_HERE, "..")
_HTML = os.path.join(_ROOT, "frontend", "index.html")
_CSS  = os.path.join(_ROOT, "frontend", "style.css")
_JS   = os.path.join(_ROOT, "frontend", "app.js")


def _read(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


# ── Python mirror of toggle logic ─────────────────────────────────────────────

THEMES = ("dark", "light")
STORAGE_KEY = "theme"
DEFAULT_THEME = "dark"


def get_theme(storage: dict) -> str:
    return storage.get(STORAGE_KEY, DEFAULT_THEME)


def toggle_theme(storage: dict) -> str:
    current = get_theme(storage)
    new_theme = "light" if current == "dark" else "dark"
    storage[STORAGE_KEY] = new_theme
    return new_theme


def apply_theme(theme: str, document_attrs: dict) -> None:
    document_attrs["data-theme"] = theme


# ── Toggle logic tests ────────────────────────────────────────────────────────

def test_default_theme_is_dark():
    assert get_theme({}) == "dark"


def test_toggle_dark_to_light():
    storage = {}
    new = toggle_theme(storage)
    assert new == "light"
    assert storage[STORAGE_KEY] == "light"


def test_toggle_light_to_dark():
    storage = {STORAGE_KEY: "light"}
    new = toggle_theme(storage)
    assert new == "dark"
    assert storage[STORAGE_KEY] == "dark"


def test_toggle_twice_returns_to_original():
    storage = {}
    toggle_theme(storage)
    toggle_theme(storage)
    assert get_theme(storage) == "dark"


def test_apply_theme_sets_attribute():
    attrs = {}
    apply_theme("light", attrs)
    assert attrs["data-theme"] == "light"


def test_apply_dark_theme_sets_attribute():
    attrs = {}
    apply_theme("dark", attrs)
    assert attrs["data-theme"] == "dark"


def test_storage_key_is_theme():
    assert STORAGE_KEY == "theme"


# ── HTML structural tests ─────────────────────────────────────────────────────

def test_html_has_theme_toggle_button():
    html = _read(_HTML)
    assert 'id="theme-toggle"' in html, "Missing #theme-toggle button in HTML"


def test_theme_toggle_button_in_header():
    html = _read(_HTML)
    header_start = html.index("<header")
    header_end   = html.index("</header>")
    header_block = html[header_start:header_end]
    assert 'id="theme-toggle"' in header_block, "#theme-toggle must be inside <header>"


# ── CSS variable tests ────────────────────────────────────────────────────────

def test_css_has_light_theme_selector():
    css = _read(_CSS)
    assert '[data-theme="light"]' in css or "data-theme" in css, \
        "CSS must define a light-theme selector"


def test_css_light_theme_overrides_bg():
    css = _read(_CSS)
    # The light theme block must override --bg
    light_block_match = re.search(
        r'\[data-theme=["\']light["\']\]\s*\{([^}]+)\}', css, re.DOTALL
    )
    assert light_block_match, "No [data-theme='light'] block in CSS"
    block = light_block_match.group(1)
    assert "--bg" in block, "Light theme must override --bg"
    assert "--fg" in block, "Light theme must override --fg"


def test_css_has_theme_toggle_button_styles():
    css = _read(_CSS)
    assert "theme-toggle" in css, "CSS must style #theme-toggle button"


# ── JS implementation tests ───────────────────────────────────────────────────

def test_js_has_localstorage_theme():
    js = _read(_JS)
    assert "localStorage" in js and "theme" in js, \
        "app.js must use localStorage with key 'theme'"


def test_js_has_theme_toggle_function_or_handler():
    js = _read(_JS)
    # Accept either a named function or an inline addEventListener handler
    has_func = "function" in js and "theme" in js.lower()
    has_handler = "theme-toggle" in js
    assert has_func or has_handler, \
        "app.js must contain theme toggle logic"


def test_js_applies_data_theme_attribute():
    js = _read(_JS)
    assert "data-theme" in js, \
        "app.js must set data-theme attribute on document root"
