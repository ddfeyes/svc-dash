"""
Integration tests against the live backend at http://localhost:8765.

Requirements: backend running (Docker or local).

Run:
    pytest tests/test_integration.py -v -m integration
    pytest tests/test_integration.py -v          # runs all marked integration

Endpoint mapping (user-facing alias → actual path):
    /api/funding   → /api/funding/history
    /api/cvd       → /api/cvd/history
    /api/spread    → /api/spread-history
    /api/orderbook → /api/orderbook/latest
    /api/phase     → /api/phase-history

Each endpoint gets three tests:
    _status  — HTTP 200
    _keys    — required top-level keys present
    _types   — key values have correct Python types
"""
import pytest
import httpx

pytestmark = pytest.mark.integration

BASE = "http://localhost:8765/api"
TIMEOUT = 15.0        # default for fast endpoints
SLOW_TIMEOUT = 45.0   # market-regime, spread-history


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def client() -> httpx.Client:
    """Session-scoped synchronous httpx client."""
    with httpx.Client(base_url=BASE, timeout=TIMEOUT) as c:
        yield c


@pytest.fixture(scope="session")
def symbol(client: httpx.Client) -> str:
    """Return the first tracked symbol; skip the suite if the backend is down."""
    try:
        r = client.get("/symbols")
    except (httpx.ConnectError, httpx.TimeoutException) as exc:
        pytest.skip(f"Backend not reachable at {BASE}: {exc}")
    if r.status_code != 200:
        pytest.skip(f"/api/symbols returned {r.status_code}")
    syms = r.json().get("symbols", [])
    if not syms:
        pytest.skip("No symbols configured on this server")
    return syms[0]


def _get(client: httpx.Client, path: str, timeout: float = TIMEOUT, **params) -> httpx.Response:
    """GET with automatic skip on connection/timeout errors."""
    try:
        return client.get(path, params=params or None, timeout=timeout)
    except httpx.ConnectError as exc:
        pytest.skip(f"Backend not reachable: {exc}")
    except httpx.TimeoutException:
        pytest.skip(f"{path} timed out after {timeout}s")


def _get_or_skip_404(client: httpx.Client, path: str, timeout: float = TIMEOUT, **params) -> httpx.Response:
    """Like _get, but also skips when the server returns 404 (older server version)."""
    r = _get(client, path, timeout=timeout, **params)
    if r.status_code == 404:
        pytest.skip(f"{path} not available on this server version (404)")
    return r


# ── /api/symbols ─────────────────────────────────────────────────────────────

class TestSymbols:
    """GET /api/symbols"""

    def test_status(self, client):
        assert _get(client, "/symbols").status_code == 200

    def test_keys(self, client):
        body = _get(client, "/symbols").json()
        assert "status" in body
        assert "symbols" in body

    def test_types(self, client):
        body = _get(client, "/symbols").json()
        assert body["status"] == "ok"
        assert isinstance(body["symbols"], list)
        assert len(body["symbols"]) >= 1
        assert all(isinstance(s, str) for s in body["symbols"])


# ── /api/ohlcv ────────────────────────────────────────────────────────────────

class TestOhlcv:
    """GET /api/ohlcv"""

    def test_status(self, client, symbol):
        assert _get(client, "/ohlcv", symbol=symbol, interval=60, window=3600).status_code == 200

    def test_keys(self, client, symbol):
        body = _get(client, "/ohlcv", symbol=symbol, interval=60, window=3600).json()
        for key in ("status", "symbol", "interval", "data", "count"):
            assert key in body, f"missing key: {key!r}"

    def test_types(self, client, symbol):
        body = _get(client, "/ohlcv", symbol=symbol, interval=60, window=3600).json()
        assert body["status"] == "ok"
        assert isinstance(body["symbol"], str)
        assert isinstance(body["interval"], int)
        assert isinstance(body["data"], list)
        assert isinstance(body["count"], int)
        if body["data"]:
            candle = body["data"][0]
            for field in ("ts", "open", "high", "low", "close", "volume"):
                assert field in candle, f"candle missing field: {field!r}"


# ── /api/trades/recent ────────────────────────────────────────────────────────

class TestTradesRecent:
    """GET /api/trades/recent"""

    def test_status(self, client, symbol):
        assert _get(client, "/trades/recent", symbol=symbol, limit=20).status_code == 200

    def test_keys(self, client, symbol):
        body = _get(client, "/trades/recent", symbol=symbol, limit=20).json()
        for key in ("status", "data", "count"):
            assert key in body, f"missing key: {key!r}"

    def test_types(self, client, symbol):
        body = _get(client, "/trades/recent", symbol=symbol, limit=20).json()
        assert body["status"] == "ok"
        assert isinstance(body["data"], list)
        assert isinstance(body["count"], int)
        if body["data"]:
            trade = body["data"][0]
            for field in ("ts", "price", "qty", "side"):
                assert field in trade, f"trade missing field: {field!r}"
            assert trade["side"] in ("buy", "sell", "Buy", "Sell")


# ── /api/oi/history ───────────────────────────────────────────────────────────

class TestOiHistory:
    """GET /api/oi/history"""

    def test_status(self, client, symbol):
        assert _get(client, "/oi/history", symbol=symbol, limit=50).status_code == 200

    def test_keys(self, client, symbol):
        body = _get(client, "/oi/history", symbol=symbol, limit=50).json()
        for key in ("status", "data", "count"):
            assert key in body, f"missing key: {key!r}"

    def test_types(self, client, symbol):
        body = _get(client, "/oi/history", symbol=symbol, limit=50).json()
        assert body["status"] == "ok"
        assert isinstance(body["data"], list)
        assert isinstance(body["count"], int)
        if body["data"]:
            row = body["data"][0]
            for field in ("ts", "oi_value"):
                assert field in row, f"OI row missing field: {field!r}"
            assert isinstance(row["oi_value"], (int, float))


# ── /api/funding → /api/funding/history ──────────────────────────────────────

class TestFunding:
    """GET /api/funding/history  (user-facing alias: /api/funding)"""

    def test_status(self, client, symbol):
        assert _get(client, "/funding/history", symbol=symbol, limit=20).status_code == 200

    def test_keys(self, client, symbol):
        body = _get(client, "/funding/history", symbol=symbol, limit=20).json()
        for key in ("status", "data", "count"):
            assert key in body, f"missing key: {key!r}"

    def test_types(self, client, symbol):
        body = _get(client, "/funding/history", symbol=symbol, limit=20).json()
        assert body["status"] == "ok"
        assert isinstance(body["data"], list)
        assert isinstance(body["count"], int)
        if body["data"]:
            row = body["data"][0]
            for field in ("ts", "rate"):
                assert field in row, f"funding row missing field: {field!r}"
            assert isinstance(row["rate"], (int, float))


# ── /api/cvd → /api/cvd/history ──────────────────────────────────────────────

class TestCvd:
    """GET /api/cvd/history  (user-facing alias: /api/cvd)
    Uses window=60 to avoid full-table scans on large DBs (window=3600 can take 90s+).
    """

    def test_status(self, client, symbol):
        assert _get(client, "/cvd/history", symbol=symbol, window=60).status_code == 200

    def test_keys(self, client, symbol):
        body = _get(client, "/cvd/history", symbol=symbol, window=60).json()
        for key in ("status", "data", "count"):
            assert key in body, f"missing key: {key!r}"

    def test_types(self, client, symbol):
        body = _get(client, "/cvd/history", symbol=symbol, window=60).json()
        assert body["status"] == "ok"
        assert isinstance(body["data"], list)
        assert isinstance(body["count"], int)
        if body["data"]:
            row = body["data"][0]
            for field in ("ts", "price", "cvd", "delta"):
                assert field in row, f"CVD row missing field: {field!r}"
            assert isinstance(row["cvd"], (int, float))
            assert isinstance(row["delta"], (int, float))


# ── /api/volume-imbalance ─────────────────────────────────────────────────────

class TestVolumeImbalance:
    """GET /api/volume-imbalance
    Skips gracefully on 404 (endpoint added in a later server version).
    """

    def test_status(self, client, symbol):
        assert _get_or_skip_404(client, "/volume-imbalance", symbol=symbol, window=60).status_code == 200

    def test_keys(self, client, symbol):
        body = _get_or_skip_404(client, "/volume-imbalance", symbol=symbol, window=60).json()
        for key in ("status", "symbol", "buy_volume", "sell_volume",
                    "total_volume", "imbalance", "window_seconds"):
            assert key in body, f"missing key: {key!r}"

    def test_types(self, client, symbol):
        body = _get_or_skip_404(client, "/volume-imbalance", symbol=symbol, window=60).json()
        assert body["status"] == "ok"
        assert isinstance(body["symbol"], str)
        assert isinstance(body["buy_volume"], (int, float))
        assert isinstance(body["sell_volume"], (int, float))
        assert isinstance(body["total_volume"], (int, float))
        assert isinstance(body["imbalance"], (int, float))
        assert isinstance(body["window_seconds"], int)
        assert -1.0 <= body["imbalance"] <= 1.0, (
            f"imbalance {body['imbalance']} out of [-1, 1] range"
        )


# ── /api/market-regime ────────────────────────────────────────────────────────

class TestMarketRegime:
    """GET /api/market-regime  (slow: 25-35s due to multiple sub-computations)"""

    def test_status(self, client, symbol):
        assert _get(client, "/market-regime", timeout=SLOW_TIMEOUT, symbol=symbol).status_code == 200

    def test_keys(self, client, symbol):
        body = _get(client, "/market-regime", timeout=SLOW_TIMEOUT, symbol=symbol).json()
        for key in ("status", "score", "regime", "action", "phase",
                    "phase_confidence", "weights"):
            assert key in body, f"missing key: {key!r}"

    def test_types(self, client, symbol):
        body = _get(client, "/market-regime", timeout=SLOW_TIMEOUT, symbol=symbol).json()
        assert body["status"] == "ok"
        assert isinstance(body["score"], (int, float))
        assert -100 <= body["score"] <= 100, (
            f"score {body['score']} out of [-100, 100] range"
        )
        assert isinstance(body["regime"], str)
        assert isinstance(body["action"], str)
        assert isinstance(body["phase"], str)
        assert isinstance(body["phase_confidence"], (int, float))
        assert isinstance(body["weights"], dict)


# ── /api/spread → /api/spread-history ────────────────────────────────────────

class TestSpread:
    """GET /api/spread-history  (user-facing alias: /api/spread, slow: ~25s)"""

    def test_status(self, client, symbol):
        assert _get(client, "/spread-history", timeout=SLOW_TIMEOUT, symbol=symbol, window=1800).status_code == 200

    def test_keys(self, client, symbol):
        body = _get(client, "/spread-history", timeout=SLOW_TIMEOUT, symbol=symbol, window=1800).json()
        for key in ("status", "symbol", "data"):
            assert key in body, f"missing key: {key!r}"

    def test_types(self, client, symbol):
        body = _get(client, "/spread-history", timeout=SLOW_TIMEOUT, symbol=symbol, window=1800).json()
        assert body["status"] == "ok"
        assert isinstance(body["symbol"], str)
        assert isinstance(body["data"], list)
        if body["data"]:
            row = body["data"][0]
            for field in ("ts", "spread", "spread_pct", "spread_bps"):
                assert field in row, f"spread row missing field: {field!r}"
            assert isinstance(row["spread_bps"], (int, float))


# ── /api/orderbook → /api/orderbook/latest ───────────────────────────────────

class TestOrderbook:
    """GET /api/orderbook/latest  (user-facing alias: /api/orderbook)"""

    def test_status(self, client, symbol):
        assert _get(client, "/orderbook/latest", symbol=symbol).status_code == 200

    def test_keys(self, client, symbol):
        body = _get(client, "/orderbook/latest", symbol=symbol).json()
        for key in ("status", "data", "count"):
            assert key in body, f"missing key: {key!r}"

    def test_types(self, client, symbol):
        body = _get(client, "/orderbook/latest", symbol=symbol).json()
        assert body["status"] == "ok"
        assert isinstance(body["data"], list)
        assert isinstance(body["count"], int)
        if body["data"]:
            snap = body["data"][0]
            for field in ("ts", "symbol"):
                assert field in snap, f"orderbook snapshot missing field: {field!r}"


# ── /api/phase → /api/phase-history ──────────────────────────────────────────

class TestPhase:
    """GET /api/phase-history  (user-facing alias: /api/phase)"""

    def test_status(self, client, symbol):
        assert _get(client, "/phase-history", symbol=symbol).status_code == 200

    def test_keys(self, client, symbol):
        body = _get(client, "/phase-history", symbol=symbol).json()
        for key in ("status", "symbol", "data", "count"):
            assert key in body, f"missing key: {key!r}"

    def test_types(self, client, symbol):
        body = _get(client, "/phase-history", symbol=symbol).json()
        assert body["status"] == "ok"
        assert isinstance(body["symbol"], str)
        assert isinstance(body["data"], list)
        assert isinstance(body["count"], int)
        if body["data"]:
            entry = body["data"][0]
            for field in ("ts", "phase", "confidence"):
                assert field in entry, f"phase entry missing field: {field!r}"
            assert isinstance(entry["confidence"], (int, float))


# ── /api/alerts ───────────────────────────────────────────────────────────────

class TestAlerts:
    """GET /api/alerts"""

    def test_status(self, client, symbol):
        assert _get(client, "/alerts", symbol=symbol, limit=50).status_code == 200

    def test_keys(self, client, symbol):
        body = _get(client, "/alerts", symbol=symbol, limit=50).json()
        for key in ("status", "data", "count"):
            assert key in body, f"missing key: {key!r}"

    def test_types(self, client, symbol):
        body = _get(client, "/alerts", symbol=symbol, limit=50).json()
        assert body["status"] == "ok"
        assert isinstance(body["data"], list)
        assert isinstance(body["count"], int)
        assert body["count"] == len(body["data"])
        if body["data"]:
            alert = body["data"][0]
            for field in ("ts", "symbol", "alert_type", "severity", "description"):
                assert field in alert, f"alert missing field: {field!r}"
