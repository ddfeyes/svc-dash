"""Entry point: FastAPI app + background collectors + pollers."""
import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

# Load .env — supports DOTENV_PATH env var (for Docker), fallback to ~/.lain-secrets/.env
from dotenv import load_dotenv
_dotenv_path = os.environ.get("DOTENV_PATH")
if _dotenv_path and Path(_dotenv_path).exists():
    load_dotenv(_dotenv_path)
else:
    _env_path = Path.home() / ".lain-secrets" / ".env"
    if _env_path.exists():
        load_dotenv(_env_path)
    else:
        load_dotenv(".env")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from storage import init_db, cleanup_old_data, insert_pattern, insert_phase_snapshot
from collectors import run_all_collectors, get_symbols
from pollers import poller_loop
from api import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


async def cleanup_loop():
    while True:
        await asyncio.sleep(3600)
        try:
            await cleanup_old_data()
            logger.info("DB cleanup done")
        except Exception as e:
            logger.error(f"DB cleanup error: {e}")


async def pattern_detection_loop():
    """Periodically detect accumulation/distribution patterns and persist them."""
    from metrics import detect_accumulation_distribution_pattern
    await asyncio.sleep(60)  # warm-up: wait for data to arrive
    _last_pattern: dict = {}   # symbol -> last persisted pattern type+ts
    PERSIST_INTERVAL = 120     # only save if pattern changed or 2 min passed
    DETECT_INTERVAL  = 30      # run detection every 30s

    while True:
        try:
            syms = get_symbols()
            for sym in syms:
                try:
                    result = await detect_accumulation_distribution_pattern(symbol=sym)
                    pattern = result.get("pattern", "balanced")
                    confidence = result.get("confidence", 0)

                    # Only persist non-balanced patterns with reasonable confidence
                    if pattern != "balanced" and confidence >= 0.35:
                        last = _last_pattern.get(sym, {})
                        last_ts   = last.get("ts", 0)
                        last_type = last.get("pattern")
                        now = result["ts"]

                        # Persist if pattern changed or 2 min elapsed
                        if last_type != pattern or (now - last_ts) >= PERSIST_INTERVAL:
                            await insert_pattern(
                                symbol=sym,
                                pattern_type=pattern,
                                confidence=confidence,
                                signals=result.get("signals", {}),
                                description=result.get("description", ""),
                            )
                            _last_pattern[sym] = {"pattern": pattern, "ts": now}
                            logger.info(f"Pattern persisted: {sym} {pattern} conf={confidence:.2f}")
                except Exception as e:
                    logger.warning(f"Pattern detection failed for {sym}: {e}")

            await asyncio.sleep(DETECT_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Pattern loop error: {e}")
            await asyncio.sleep(60)


async def phase_snapshot_loop():
    """Periodically snapshot market phase for all symbols (historical replay data)."""
    from metrics import classify_market_phase, compute_market_regime
    await asyncio.sleep(45)  # wait for collectors to warm up
    SNAP_INTERVAL = 30  # snapshot every 30s

    while True:
        try:
            syms = get_symbols()
            for sym in syms:
                try:
                    phase_result = await classify_market_phase(symbol=sym)
                    regime_result = await compute_market_regime(symbol=sym)
                    phase = phase_result.get("phase", "unknown")
                    confidence = phase_result.get("confidence", 0.0)
                    signals = phase_result.get("signals", {})
                    composite = regime_result.get("composite_score") if isinstance(regime_result, dict) else None
                    await insert_phase_snapshot(
                        symbol=sym,
                        phase=phase,
                        confidence=confidence,
                        signals=signals,
                        composite_score=composite,
                    )
                except Exception as e:
                    logger.warning(f"Phase snapshot failed for {sym}: {e}")
            await asyncio.sleep(SNAP_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Phase snapshot loop error: {e}")
            await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing DB...")
    await init_db()
    logger.info("DB ready. Running startup cleanup...")
    try:
        await cleanup_old_data()
        logger.info("Startup cleanup done")
    except Exception as e:
        logger.warning(f"Startup cleanup skipped: {e}")
    logger.info("Starting background tasks...")

    # Start background tasks
    tasks = [
        asyncio.create_task(run_all_collectors(), name="collectors"),
        asyncio.create_task(poller_loop(), name="pollers"),
        asyncio.create_task(cleanup_loop(), name="cleanup"),
        asyncio.create_task(pattern_detection_loop(), name="pattern_detector"),
        asyncio.create_task(phase_snapshot_loop(), name="phase_snapshots"),
    ]

    logger.info("All background tasks started")
    yield

    # Shutdown
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Shutdown complete")


app = FastAPI(
    title="BANANAS31 Dashboard API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

# Serve frontend
frontend_dir = Path(__file__).parent.parent / "frontend"
if frontend_dir.exists():
    @app.get("/")
    async def serve_index():
        return FileResponse(str(frontend_dir / "index.html"))

    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")


@app.get("/health")
async def health():
    from collectors import get_symbols
    return {"status": "ok", "symbols": get_symbols()}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    host = os.getenv("HOST", "0.0.0.0")
    uvicorn.run("main:app", host=host, port=port, reload=False)
