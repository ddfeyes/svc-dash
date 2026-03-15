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

from storage import init_db, cleanup_old_data
from collectors import run_all_collectors
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing DB...")
    await init_db()
    logger.info("DB ready. Starting background tasks...")

    # Start background tasks
    tasks = [
        asyncio.create_task(run_all_collectors(), name="collectors"),
        asyncio.create_task(poller_loop(), name="pollers"),
        asyncio.create_task(cleanup_loop(), name="cleanup"),
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
