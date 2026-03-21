"""
Static (no-server) checks for issue #189 dashboard audit.

These tests read app.js and index.html directly — no live server needed.
  - No double /api/api prefix in apiFetch() calls
  - Every setErr() content ID exists as an HTML element in index.html
"""

import os
import re

ROOT = os.path.join(os.path.dirname(__file__), "..")
_JS_PATH = os.path.join(ROOT, "frontend", "app.js")
_HTML_PATH = os.path.join(ROOT, "frontend", "index.html")


def _js() -> str:
    with open(_JS_PATH, encoding="utf-8") as f:
        return f.read()


def _html() -> str:
    with open(_HTML_PATH, encoding="utf-8") as f:
        return f.read()


class TestFrontendJsIntegrity:
    """Static checks on app.js / index.html — no server required."""

    def test_no_double_api_prefix_in_apifetch(self):
        """apiFetch already prepends /api — callers must not include /api."""
        matches = re.findall(r'apiFetch\(["\']\/api\/', _js())
        assert not matches, (
            f"Found {len(matches)} apiFetch() call(s) with double /api/ prefix. "
            "Remove the extra /api — apiFetch() adds it automatically."
        )

    def test_all_seterr_cards_have_content_divs_in_html(self):
        """Every content ID used in setErr() must exist as an HTML element."""
        html = _html()
        js = _js()
        ids = re.findall(r"setErr\(['\"]([^'\"]+)['\"]", js)
        missing = [
            cid
            for cid in ids
            if f'id="{cid}"' not in html and f"id='{cid}'" not in html
        ]
        assert (
            not missing
        ), f"These content IDs used in setErr() are missing from index.html: {missing}"
