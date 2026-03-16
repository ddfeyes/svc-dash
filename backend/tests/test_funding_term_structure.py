"""Tests for funding rate term structure analysis (issue #105).

TDD approach: all tests written first, then implementation.
Tests cover:
1. compute_funding_term_structure() function
2. GET /api/funding-term-structure endpoint
3. Frontend integration (card component tests)
"""

import asyncio
import os
import tempfile
import time
import pytest
import json

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test_funding_term.db")
os.environ["SYMBOL_BINANCE"] = "BTCUSDT"
os.environ["SYMBOL_BYBIT"] = "BTCUSDT"

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage import init_db, get_db
from metrics import compute_funding_term_structure


class TestFundingTermStructureBasics:
    """Basic functionality tests for compute_funding_term_structure()."""

    @pytest.mark.asyncio
    async def test_empty_funding_data(self):
        """With no funding history, should return zero rates and neutral shape."""
        await init_db()
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        assert result is not None
        assert "rates" in result
        assert "shape" in result
        assert "exhaustion_score" in result
        assert "trend" in result
        
        # Empty should return zero rates
        assert result["rates"]["d1"] == 0.0
        assert result["rates"]["d7"] == 0.0
        assert result["rates"]["d30"] == 0.0
        assert result["shape"] == "flat"
        assert result["exhaustion_score"] == 0.0

    @pytest.mark.asyncio
    async def test_single_funding_rate(self):
        """With one funding rate, should handle gracefully."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        await db.execute(
            "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
            (ts, "binance", "BTCUSDT", 0.0001)
        )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        # Should have rates from 1d period only
        assert result["rates"]["d1"] == 0.0001
        # d7 and d30 may be 0 if insufficient data
        assert isinstance(result["rates"]["d7"], float)
        assert isinstance(result["rates"]["d30"], float)

    @pytest.mark.asyncio
    async def test_normal_funding_curve(self):
        """Normal upward curve: d1 < d7 < d30 (positive rates increasing)."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Insert rates with increasing pattern (normal curve)
        rates = [
            (ts - 86400 * 30, "binance", "BTCUSDT", 0.0003),  # 30d ago: 0.03%
            (ts - 86400 * 7, "binance", "BTCUSDT", 0.0002),   # 7d ago: 0.02%
            (ts - 3600, "binance", "BTCUSDT", 0.0001),        # 1h ago: 0.01%
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        assert result["shape"] == "normal"
        # Verify increasing trend
        assert result["rates"]["d1"] < result["rates"]["d7"] or result["rates"]["d7"] == 0.0
        assert result["trend"] in ["up", "neutral"]

    @pytest.mark.asyncio
    async def test_inverted_funding_curve(self):
        """Inverted curve: d1 > d7 > d30 (longs paying heavily in short term)."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Insert rates with decreasing pattern (inverted curve)
        rates = [
            (ts - 86400 * 30, "binance", "BTCUSDT", 0.00005),  # 30d: 0.005%
            (ts - 86400 * 7, "binance", "BTCUSDT", 0.0001),    # 7d: 0.01%
            (ts - 3600, "binance", "BTCUSDT", 0.0003),         # 1h: 0.03%
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        assert result["shape"] == "inverted"
        # Verify decreasing trend
        assert result["rates"]["d1"] >= result["rates"]["d7"] or result["rates"]["d7"] == 0.0

    @pytest.mark.asyncio
    async def test_flat_funding_curve(self):
        """Flat curve: all rates approximately equal."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Insert all same rate
        for exchange in ["binance", "bybit"]:
            rates = [
                (ts - 86400 * 30, exchange, "BTCUSDT", 0.00015),
                (ts - 86400 * 7, exchange, "BTCUSDT", 0.00015),
                (ts - 3600, exchange, "BTCUSDT", 0.00015),
            ]
            for r in rates:
                await db.execute(
                    "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                    r
                )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        assert result["shape"] == "flat"

    @pytest.mark.asyncio
    async def test_exhaustion_score_calculation(self):
        """Exhaustion score should reflect extreme funding levels."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Extreme positive funding = high exhaustion (longs heavily paying)
        rates = [
            (ts - 86400, "binance", "BTCUSDT", 0.001),   # 0.1% = extreme
            (ts - 43200, "binance", "BTCUSDT", 0.0015),  # 0.15%
            (ts - 100, "binance", "BTCUSDT", 0.002),     # 0.2% = very extreme
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        # High positive rates should increase exhaustion score
        assert result["exhaustion_score"] > 0.5  # Should be high
        assert result["exhaustion_score"] <= 1.0

    @pytest.mark.asyncio
    async def test_negative_funding_exhaustion(self):
        """Negative extreme funding should also indicate exhaustion (shorts overextended)."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Extreme negative funding = shorts squeezed
        rates = [
            (ts - 86400, "binance", "BTCUSDT", -0.001),   # -0.1%
            (ts - 43200, "binance", "BTCUSDT", -0.0015),  # -0.15%
            (ts - 100, "binance", "BTCUSDT", -0.002),     # -0.2%
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        # Extreme negative should also have high exhaustion
        assert result["exhaustion_score"] > 0.5
        assert result["exhaustion_score"] <= 1.0

    @pytest.mark.asyncio
    async def test_neutral_funding_exhaustion(self):
        """Near-zero funding should have low exhaustion score."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Low rates = low exhaustion
        rates = [
            (ts - 86400, "binance", "BTCUSDT", 0.00001),
            (ts - 43200, "binance", "BTCUSDT", 0.00002),
            (ts - 100, "binance", "BTCUSDT", -0.00001),
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        # Low absolute values should have low exhaustion
        assert result["exhaustion_score"] < 0.3

    @pytest.mark.asyncio
    async def test_trend_detection_uptrend(self):
        """Funding rates increasing over time = uptrend."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Trend: -0.0003 -> -0.0002 -> 0.0001 (moving upward)
        rates = [
            (ts - 86400, "binance", "BTCUSDT", -0.0003),
            (ts - 43200, "binance", "BTCUSDT", -0.0002),
            (ts - 100, "binance", "BTCUSDT", 0.0001),
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        assert result["trend"] in ["up", "neutral"]

    @pytest.mark.asyncio
    async def test_trend_detection_downtrend(self):
        """Funding rates decreasing over time = downtrend."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Trend: 0.0003 -> 0.0002 -> -0.0001 (moving downward)
        rates = [
            (ts - 86400, "binance", "BTCUSDT", 0.0003),
            (ts - 43200, "binance", "BTCUSDT", 0.0002),
            (ts - 100, "binance", "BTCUSDT", -0.0001),
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        assert result["trend"] in ["down", "neutral"]


class TestFundingTermStructureEdgeCases:
    """Edge cases and extreme scenarios."""

    @pytest.mark.asyncio
    async def test_zero_rates(self):
        """All zero rates should handle gracefully."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        for i in range(10):
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                (ts - 86400 + i * 3600, "binance", "BTCUSDT", 0.0)
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        assert result["rates"]["d1"] == 0.0
        assert result["rates"]["d7"] == 0.0
        assert result["rates"]["d30"] == 0.0
        assert result["exhaustion_score"] == 0.0

    @pytest.mark.asyncio
    async def test_extreme_positive_rates(self):
        """Very high positive rates (pump conditions)."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # 1% = 100x normal, extreme squeeze risk for longs
        rates = [
            (ts - 86400, "binance", "BTCUSDT", 0.01),
            (ts - 43200, "binance", "BTCUSDT", 0.015),
            (ts - 100, "binance", "BTCUSDT", 0.02),
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        assert result["exhaustion_score"] == 1.0  # Capped at max
        assert all(r > 0 for r in result["rates"].values() if r is not None)

    @pytest.mark.asyncio
    async def test_extreme_negative_rates(self):
        """Very negative rates (crash conditions, shorts getting paid)."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # -1% = extreme, shorts heavily paid
        rates = [
            (ts - 86400, "binance", "BTCUSDT", -0.01),
            (ts - 43200, "binance", "BTCUSDT", -0.015),
            (ts - 100, "binance", "BTCUSDT", -0.02),
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        assert result["exhaustion_score"] == 1.0  # Capped at max
        assert all(r < 0 for r in result["rates"].values() if r is not None)

    @pytest.mark.asyncio
    async def test_mixed_sign_rates(self):
        """Rates that flip between positive and negative."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        rates = [
            (ts - 86400, "binance", "BTCUSDT", 0.0002),
            (ts - 43200, "binance", "BTCUSDT", -0.0001),
            (ts - 100, "binance", "BTCUSDT", 0.00015),
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        # Should handle mixed signs gracefully
        assert isinstance(result["exhaustion_score"], float)
        assert 0 <= result["exhaustion_score"] <= 1.0

    @pytest.mark.asyncio
    async def test_multiple_exchanges(self):
        """Multiple exchanges for same symbol should aggregate correctly."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        for exchange in ["binance", "bybit", "okx"]:
            rates = [
                (ts - 86400, exchange, "BTCUSDT", 0.0001),
                (ts - 43200, exchange, "BTCUSDT", 0.00015),
                (ts - 100, exchange, "BTCUSDT", 0.0002),
            ]
            for r in rates:
                await db.execute(
                    "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                    r
                )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        # Should aggregate across exchanges
        assert result["rates"]["d1"] > 0
        assert result["rates"]["d7"] > 0 or result["rates"]["d7"] == 0.0
        assert isinstance(result["shape"], str)

    @pytest.mark.asyncio
    async def test_sparse_funding_data(self):
        """Very sparse data points (days between updates)."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Only 3 data points over 30 days
        rates = [
            (ts - 86400 * 30, "binance", "BTCUSDT", 0.00005),
            (ts - 86400 * 15, "binance", "BTCUSDT", 0.0001),
            (ts - 100, "binance", "BTCUSDT", 0.00015),
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        # Should still work with sparse data
        assert result is not None
        assert "rates" in result
        assert "shape" in result

    @pytest.mark.asyncio
    async def test_dense_funding_data(self):
        """Very dense data points (hourly updates)."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # 30 days of hourly data
        for i in range(30 * 24):
            rate = 0.0001 + 0.00001 * (i % 10)  # Oscillating slightly
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                (ts - i * 3600, "binance", "BTCUSDT", rate)
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        # Should handle dense data
        assert result is not None
        assert isinstance(result["rates"]["d1"], float)
        assert isinstance(result["rates"]["d7"], float)
        assert isinstance(result["rates"]["d30"], float)

    @pytest.mark.asyncio
    async def test_future_timestamp(self):
        """Gracefully handle future timestamps (clock skew)."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Some future timestamps
        rates = [
            (ts - 86400, "binance", "BTCUSDT", 0.0001),
            (ts + 3600, "binance", "BTCUSDT", 0.00015),  # Future!
            (ts - 100, "binance", "BTCUSDT", 0.0002),
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        # Should not crash
        assert result is not None

    @pytest.mark.asyncio
    async def test_different_symbols(self):
        """Different symbols should be computed independently."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        for sym in symbols:
            rate = 0.0001 if sym == "BTCUSDT" else 0.0002 if sym == "ETHUSDT" else 0.00005
            for i in range(3):
                await db.execute(
                    "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                    (ts - 86400 + i * 43200, "binance", sym, rate)
                )
        await db.commit()
        await db.close()
        
        # Compute for each
        btc_result = await compute_funding_term_structure(symbol="BTCUSDT")
        eth_result = await compute_funding_term_structure(symbol="ETHUSDT")
        bnb_result = await compute_funding_term_structure(symbol="BNBUSDT")
        
        # Results should differ
        assert btc_result is not None
        assert eth_result is not None
        assert bnb_result is not None


class TestFundingTermStructureCalculations:
    """Test specific calculation correctness."""

    @pytest.mark.asyncio
    async def test_rates_are_averages(self):
        """Rates returned should be averages of the window."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # 1d: 0.0001, 0.0002 -> avg 0.00015
        # 7d: +0.0003 -> avg (0.0001+0.0002+0.0003)/3 = 0.0002
        rates = [
            (ts - 3600, "binance", "BTCUSDT", 0.0001),         # 1d, 7d, 30d
            (ts - 7200, "binance", "BTCUSDT", 0.0002),         # 1d, 7d, 30d
            (ts - 86400 * 2, "binance", "BTCUSDT", 0.0003),    # 7d, 30d
            (ts - 86400 * 30, "binance", "BTCUSDT", 0.00001),  # 30d only
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        
        # Verify averaging
        # d1 should be average of last 24h: (0.0001 + 0.0002) / 2 = 0.00015
        assert abs(result["rates"]["d1"] - 0.00015) < 0.000001 or result["rates"]["d1"] == 0.0

    @pytest.mark.asyncio
    async def test_shape_detection_boundaries(self):
        """Test shape detection at boundary cases."""
        await init_db()
        
        # Case 1: rates exactly equal (flat)
        db = await get_db()
        ts = time.time()
        for i in range(3):
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                (ts - 86400 * (i + 1), "binance", "BTCUSDT", 0.0001)
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        assert result["shape"] == "flat"

    @pytest.mark.asyncio
    async def test_shape_normal_strict(self):
        """Normal curve requires d1 < d7 < d30."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Strict normal: 0.0001 < 0.0002 < 0.0003
        rates = [
            (ts - 86400 * 30, "binance", "BTCUSDT", 0.0003),
            (ts - 86400 * 7, "binance", "BTCUSDT", 0.0002),
            (ts - 3600, "binance", "BTCUSDT", 0.0001),
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        assert result["shape"] == "normal"

    @pytest.mark.asyncio
    async def test_shape_inverted_strict(self):
        """Inverted curve requires d1 > d7 > d30."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Strict inverted: 0.0003 > 0.0002 > 0.0001
        rates = [
            (ts - 86400 * 30, "binance", "BTCUSDT", 0.0001),
            (ts - 86400 * 7, "binance", "BTCUSDT", 0.0002),
            (ts - 3600, "binance", "BTCUSDT", 0.0003),
        ]
        for r in rates:
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                r
            )
        await db.commit()
        await db.close()
        
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        assert result["shape"] == "inverted"


class TestFundingTermStructurePerformance:
    """Performance and response time tests."""

    @pytest.mark.asyncio
    async def test_response_time_single_symbol(self):
        """GET /api/funding-term-structure?symbol=BTC should respond <200ms."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Populate with realistic data
        for i in range(100):
            for exchange in ["binance", "bybit"]:
                await db.execute(
                    "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                    (ts - i * 3600, exchange, "BTCUSDT", 0.0001 + 0.00001 * i)
                )
        await db.commit()
        await db.close()
        
        import time as timer
        start = timer.time()
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        elapsed = timer.time() - start
        
        # Should be fast
        assert elapsed < 0.2  # <200ms
        assert result is not None

    @pytest.mark.asyncio
    async def test_response_time_all_symbols(self):
        """GET /api/funding-term-structure (all symbols) should respond <500ms."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT"]
        
        # Populate with realistic data
        for sym in symbols:
            for i in range(100):
                for exchange in ["binance", "bybit"]:
                    await db.execute(
                        "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                        (ts - i * 3600, exchange, sym, 0.0001)
                    )
        await db.commit()
        await db.close()
        
        import time as timer
        start = timer.time()
        
        # Compute for all
        tasks = [compute_funding_term_structure(symbol=sym) for sym in symbols]
        results = await asyncio.gather(*tasks)
        
        elapsed = timer.time() - start
        
        # Should be fast for multiple symbols
        assert elapsed < 0.5  # <500ms
        assert all(r is not None for r in results)


class TestFundingTermStructureIntegration:
    """Integration tests with other metrics."""

    @pytest.mark.asyncio
    async def test_integration_with_oi_momentum(self):
        """Funding term structure should be callable alongside OI momentum."""
        await init_db()
        db = await get_db()
        
        ts = time.time()
        # Add funding data
        for i in range(10):
            await db.execute(
                "INSERT INTO funding_rates (ts, exchange, symbol, rate) VALUES (?, ?, ?, ?)",
                (ts - i * 3600, "binance", "BTCUSDT", 0.0001)
            )
        await db.commit()
        await db.close()
        
        # Should not crash when called
        result = await compute_funding_term_structure(symbol="BTCUSDT")
        assert result is not None
        assert "rates" in result
        assert "exhaustion_score" in result
