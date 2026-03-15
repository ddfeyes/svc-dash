"""WebSocket collectors: Binance + Bybit — multi-symbol."""
import asyncio
import json
import logging
import os
import time
from typing import List

import websockets

from storage import (
    insert_orderbook, insert_trade, insert_liquidation
)

logger = logging.getLogger(__name__)

BINANCE_WS = "wss://fstream.binance.com/stream"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"

RECONNECT_DELAY = 5  # seconds


def get_symbols() -> List[str]:
    """Return list of symbols from env. SYMBOLS overrides SYMBOL_BINANCE."""
    symbols_env = os.getenv("SYMBOLS", "")
    if symbols_env:
        return [s.strip().upper() for s in symbols_env.split(",") if s.strip()]
    # Fallback to single symbol
    return [os.getenv("SYMBOL_BINANCE", "BANANAS31USDT")]


# ─── Binance ──────────────────────────────────────────────────────────────────

async def binance_collector(symbol: str):
    sym_lower = symbol.lower()
    streams = [
        f"{sym_lower}@depth20@100ms",
        f"{sym_lower}@aggTrade",
        f"{sym_lower}@forceOrder",
    ]
    url = f"{BINANCE_WS}?streams=" + "/".join(streams)

    while True:
        try:
            logger.info(f"[Binance/{symbol}] Connecting: {url}")
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        stream = msg.get("stream", "")
                        data = msg.get("data", {})

                        if "depth20" in stream:
                            await _handle_binance_orderbook(data, symbol)
                        elif "aggTrade" in stream:
                            await _handle_binance_trade(data, symbol)
                        elif "forceOrder" in stream:
                            await _handle_binance_liquidation(data, symbol)
                    except Exception as e:
                        logger.warning(f"[Binance/{symbol}] msg error: {e}")

        except Exception as e:
            logger.error(f"[Binance/{symbol}] WS error: {e}. Reconnecting in {RECONNECT_DELAY}s")
            await asyncio.sleep(RECONNECT_DELAY)


async def _handle_binance_orderbook(data: dict, symbol: str):
    bids = data.get("b", [])
    asks = data.get("a", [])
    await insert_orderbook("binance", symbol, bids, asks)


async def _handle_binance_trade(data: dict, symbol: str):
    price = float(data.get("p", 0))
    qty = float(data.get("q", 0))
    is_buyer_maker = data.get("m", False)
    side = "sell" if is_buyer_maker else "buy"
    trade_id = str(data.get("a", ""))
    await insert_trade("binance", symbol, price, qty, side, trade_id)


async def _handle_binance_liquidation(data: dict, symbol: str):
    order = data.get("o", {})
    side = order.get("S", "").lower()
    price = float(order.get("ap", 0) or order.get("p", 0))
    qty = float(order.get("q", 0))
    if price and qty:
        await insert_liquidation("binance", symbol, side, price, qty)


# ─── Bybit ────────────────────────────────────────────────────────────────────

# Per-symbol orderbook state for Bybit
_bybit_ob: dict = {}  # symbol -> {"bids": {}, "asks": {}}


async def bybit_collector(symbol: str):
    _bybit_ob[symbol] = {"bids": {}, "asks": {}}

    while True:
        try:
            logger.info(f"[Bybit/{symbol}] Connecting: {BYBIT_WS}")
            async with websockets.connect(BYBIT_WS, ping_interval=20, ping_timeout=10) as ws:
                sub_msg = json.dumps({
                    "op": "subscribe",
                    "args": [
                        f"orderbook.20.{symbol}",
                        f"publicTrade.{symbol}",
                        f"liquidation.{symbol}",
                    ]
                })
                await ws.send(sub_msg)

                async def heartbeat():
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send(json.dumps({"op": "ping"}))
                        except Exception:
                            break

                hb_task = asyncio.create_task(heartbeat())

                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                            topic = msg.get("topic", "")
                            data = msg.get("data", {})

                            if topic.startswith("orderbook"):
                                await _handle_bybit_orderbook(data, msg.get("type", ""), symbol)
                            elif topic.startswith("publicTrade"):
                                await _handle_bybit_trades(data, symbol)
                            elif topic.startswith("liquidation"):
                                await _handle_bybit_liquidation(data, symbol)
                        except Exception as e:
                            logger.warning(f"[Bybit/{symbol}] msg error: {e}")
                finally:
                    hb_task.cancel()

        except Exception as e:
            logger.error(f"[Bybit/{symbol}] WS error: {e}. Reconnecting in {RECONNECT_DELAY}s")
            await asyncio.sleep(RECONNECT_DELAY)


async def _handle_bybit_orderbook(data: dict, msg_type: str, symbol: str):
    ob = _bybit_ob.setdefault(symbol, {"bids": {}, "asks": {}})

    if msg_type == "snapshot":
        ob["bids"] = {p: q for p, q in data.get("b", [])}
        ob["asks"] = {p: q for p, q in data.get("a", [])}
    else:  # delta
        for p, q in data.get("b", []):
            if float(q) == 0:
                ob["bids"].pop(p, None)
            else:
                ob["bids"][p] = q
        for p, q in data.get("a", []):
            if float(q) == 0:
                ob["asks"].pop(p, None)
            else:
                ob["asks"][p] = q

    bids = sorted([[p, q] for p, q in ob["bids"].items()],
                  key=lambda x: float(x[0]), reverse=True)[:20]
    asks = sorted([[p, q] for p, q in ob["asks"].items()],
                  key=lambda x: float(x[0]))[:20]

    if bids and asks:
        await insert_orderbook("bybit", symbol, bids, asks)


async def _handle_bybit_trades(data: list, symbol: str):
    for t in data if isinstance(data, list) else [data]:
        price = float(t.get("p", 0))
        qty = float(t.get("v", 0))
        side = t.get("S", "").lower()
        trade_id = str(t.get("i", ""))
        if price and qty:
            await insert_trade("bybit", symbol, price, qty, side, trade_id)


async def _handle_bybit_liquidation(data: dict, symbol: str):
    if isinstance(data, list):
        for item in data:
            await _process_bybit_liq(item, symbol)
    else:
        await _process_bybit_liq(data, symbol)


async def _process_bybit_liq(data: dict, symbol: str):
    side = data.get("side", "").lower()
    price = float(data.get("price", 0))
    qty = float(data.get("size", 0))
    if price and qty:
        await insert_liquidation("bybit", symbol, side, price, qty)


async def run_all_collectors():
    symbols = get_symbols()
    logger.info(f"Starting collectors for symbols: {symbols}")
    tasks = []
    for sym in symbols:
        tasks.append(asyncio.create_task(binance_collector(sym), name=f"binance-{sym}"))
        tasks.append(asyncio.create_task(bybit_collector(sym), name=f"bybit-{sym}"))
    await asyncio.gather(*tasks)
