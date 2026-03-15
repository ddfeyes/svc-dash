"""REST pollers: OI and funding rate from Binance and Bybit — multi-symbol."""
import asyncio
import logging
import os
import time
from typing import List

import httpx

from storage import insert_oi, insert_funding

logger = logging.getLogger(__name__)

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1"))

BINANCE_OI_URL = "https://fapi.binance.com/fapi/v1/openInterest"
BINANCE_FUNDING_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
BYBIT_OI_URL = "https://api.bybit.com/v5/market/open-interest"
BYBIT_FUNDING_URL = "https://api.bybit.com/v5/market/tickers"


def get_symbols() -> List[str]:
    symbols_env = os.getenv("SYMBOLS", "")
    if symbols_env:
        return [s.strip().upper() for s in symbols_env.split(",") if s.strip()]
    return [os.getenv("SYMBOL_BINANCE", "BANANAS31USDT")]


async def poll_binance_oi(client: httpx.AsyncClient, symbol: str):
    try:
        r = await client.get(BINANCE_OI_URL, params={"symbol": symbol}, timeout=5)
        r.raise_for_status()
        data = r.json()
        oi_value = float(data.get("openInterest", 0))
        await insert_oi("binance", symbol, oi_value)
    except Exception as e:
        logger.warning(f"[Binance OI/{symbol}] {e}")


async def poll_binance_funding(client: httpx.AsyncClient, symbol: str):
    try:
        r = await client.get(BINANCE_FUNDING_URL, params={"symbol": symbol}, timeout=5)
        r.raise_for_status()
        data = r.json()
        rate = float(data.get("lastFundingRate", 0))
        next_ts = float(data.get("nextFundingTime", 0)) / 1000
        await insert_funding("binance", symbol, rate, next_ts)
    except Exception as e:
        logger.warning(f"[Binance funding/{symbol}] {e}")


async def poll_bybit_oi(client: httpx.AsyncClient, symbol: str):
    try:
        r = await client.get(
            BYBIT_OI_URL,
            params={"category": "linear", "symbol": symbol, "intervalTime": "5min", "limit": 1},
            timeout=5
        )
        r.raise_for_status()
        data = r.json()
        result = data.get("result", {}).get("list", [])
        if result:
            oi_value = float(result[0].get("openInterest", 0))
            await insert_oi("bybit", symbol, oi_value)
    except Exception as e:
        logger.warning(f"[Bybit OI/{symbol}] {e}")


async def poll_bybit_funding(client: httpx.AsyncClient, symbol: str):
    try:
        r = await client.get(
            BYBIT_FUNDING_URL,
            params={"category": "linear", "symbol": symbol},
            timeout=5
        )
        r.raise_for_status()
        data = r.json()
        result = data.get("result", {}).get("list", [])
        if result:
            item = result[0]
            rate = float(item.get("fundingRate", 0))
            next_ts_str = item.get("nextFundingTime", "0")
            next_ts = float(next_ts_str) / 1000 if next_ts_str else 0
            await insert_funding("bybit", symbol, rate, next_ts)
    except Exception as e:
        logger.warning(f"[Bybit funding/{symbol}] {e}")


async def poller_loop():
    """Poll OI and funding for all symbols every POLL_INTERVAL seconds."""
    symbols = get_symbols()
    logger.info(f"Starting REST pollers for {len(symbols)} symbols: {symbols}")
    async with httpx.AsyncClient() as client:
        while True:
            t0 = time.time()
            tasks = []
            for sym in symbols:
                tasks += [
                    poll_binance_oi(client, sym),
                    poll_binance_funding(client, sym),
                    poll_bybit_oi(client, sym),
                    poll_bybit_funding(client, sym),
                ]
            await asyncio.gather(*tasks, return_exceptions=True)
            elapsed = time.time() - t0
            await asyncio.sleep(max(0, POLL_INTERVAL - elapsed))
