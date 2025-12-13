from fastapi import FastAPI, Query, HTTPException
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import os

from clients.binance_client import BinanceClient
from clients.hyblock_client import HyblockClient
from logic.candles_service import get_multi_timeframe_candles
from logic.hyblock_service import get_hyblock_raw
from logic.snapshot_tracker import tracker
from logic.snapshot_persistence import (
    get_snapshots_for_symbol,
    get_snapshot_by_id,
    get_latest_snapshot,
    get_stats as get_db_stats
)
from logic.snapshot_scheduler import (
    start_snapshot_scheduler,
    stop_snapshot_scheduler,
    get_scheduler_status
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

binance_client = BinanceClient()
hyblock_client = HyblockClient()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup: Start snapshot scheduler if enabled
    if os.environ.get("ENABLE_SNAPSHOT_SCHEDULER", "true").lower() == "true":
        interval = int(os.environ.get("SNAPSHOT_INTERVAL_MINUTES", "60"))
        start_snapshot_scheduler(binance_client, hyblock_client, interval_minutes=interval)
        logger.info(f"Snapshot scheduler started (interval: {interval}min)")
    yield
    # Shutdown: Stop scheduler
    stop_snapshot_scheduler()


app = FastAPI(
    title="Klaken V2",
    description="Raw candles and Hyblock data for LLM analysis",
    version="2.0.0",
    lifespan=lifespan
)


def get_base_symbol(symbol: str) -> str:
    symbol = symbol.upper().strip()
    for suffix in ["USDT", "USD", "PERP"]:
        if symbol.endswith(suffix):
            return symbol[:-len(suffix)]
    return symbol


@app.get("/")
async def root():
    return {
        "service": "Klaken V2",
        "description": "Raw candles and Hyblock data for LLM analysis",
        "endpoints": [
            "/status?symbol=SOL",
            "/health",
            "/snapshots",
            "/snapshots/latest",
            "/history/stats",
            "/history/{symbol}?hours=24&limit=50",
            "/history/{symbol}/latest",
            "/history/snapshot/{id}"
        ],
        "usage": "GET /status?symbol=SOL"
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/status")
async def get_status(
    symbol: str = Query(..., description="Symbol: SOL, BTC or ETH")
):
    base_symbol = get_base_symbol(symbol)

    if base_symbol not in ["SOL", "BTC", "ETH"]:
        raise HTTPException(
            status_code=400,
            detail=f"Symbol must be SOL, BTC or ETH, got: {base_symbol}"
        )

    logger.info(f"Processing /status for {base_symbol}")

    try:
        candles = await get_multi_timeframe_candles(base_symbol, binance_client)
        hyblock = await get_hyblock_raw(base_symbol, hyblock_client)

        # Guardar snapshot para tracking (fail-safe)
        try:
            if hyblock and "liquidation_levels" in hyblock:
                current_price = await binance_client.get_current_price(f"{base_symbol}USDT")
                tracker.save(hyblock["liquidation_levels"], current_price)
        except Exception:
            pass  # Silencioso - no afecta el flujo principal

        return {
            "symbol": base_symbol,
            "candles": candles,
            "hyblock": hyblock,
            "meta": {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "symbol_binance": candles.get("symbol_binance", f"{base_symbol}USDT")
            }
        }

    except Exception as e:
        logger.error(f"Error processing {base_symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/snapshots")
def list_snapshots():
    """Lista snapshots disponibles en memoria"""
    return {
        "count": len(tracker.snapshots),
        "snapshots": tracker.list_snapshots()
    }


@app.get("/snapshots/latest")
def get_latest_snapshot():
    """Obtiene el snapshot más reciente"""
    snapshot = tracker.get_latest()
    if not snapshot:
        raise HTTPException(status_code=404, detail="no_snapshots")
    return snapshot


@app.get("/snapshots/{timestamp}")
def get_snapshot(timestamp: str):
    """Obtiene un snapshot específico por timestamp"""
    snapshot = tracker.get_snapshot(timestamp)
    if not snapshot:
        raise HTTPException(status_code=404, detail="snapshot_not_found")
    return snapshot


# =============================================================================
# PERSISTENT SNAPSHOT ENDPOINTS (SQLite-backed)
# =============================================================================

@app.get("/history/stats")
def history_stats():
    """Get snapshot database statistics."""
    db_stats = get_db_stats()
    scheduler_status = get_scheduler_status()
    return {
        "scheduler": scheduler_status,
        "database": db_stats
    }


@app.get("/history/{symbol}")
def get_symbol_history(
    symbol: str,
    hours: int = Query(default=24, ge=1, le=720, description="Hours to look back (max 30 days)"),
    limit: int = Query(default=50, ge=1, le=500, description="Max snapshots to return")
):
    """
    Get historical snapshots for a symbol.

    Returns list of snapshots with metadata (no cluster data for efficiency).
    Use /history/snapshot/{id} to get full cluster data.
    """
    base_symbol = get_base_symbol(symbol)
    if base_symbol not in ["SOL", "BTC", "ETH"]:
        raise HTTPException(status_code=400, detail=f"Invalid symbol: {base_symbol}")

    snapshots = get_snapshots_for_symbol(base_symbol, hours=hours, limit=limit)
    return {
        "symbol": base_symbol,
        "hours": hours,
        "count": len(snapshots),
        "snapshots": snapshots
    }


@app.get("/history/{symbol}/latest")
def get_symbol_latest(symbol: str):
    """Get the most recent persistent snapshot for a symbol (includes full cluster data)."""
    base_symbol = get_base_symbol(symbol)
    if base_symbol not in ["SOL", "BTC", "ETH"]:
        raise HTTPException(status_code=400, detail=f"Invalid symbol: {base_symbol}")

    snapshot = get_latest_snapshot(base_symbol)
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"No snapshots found for {base_symbol}")

    return snapshot


@app.get("/history/snapshot/{snapshot_id}")
def get_snapshot_detail(snapshot_id: int):
    """Get full snapshot data by ID (includes all cluster data)."""
    snapshot = get_snapshot_by_id(snapshot_id)
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return snapshot
