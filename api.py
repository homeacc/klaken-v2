from fastapi import FastAPI, Query, HTTPException
from datetime import datetime, timezone
import logging

from clients.binance_client import BinanceClient
from clients.hyblock_client import HyblockClient
from logic.candles_service import get_multi_timeframe_candles
from logic.hyblock_service import get_hyblock_raw
from logic.snapshot_tracker import tracker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Klaken V2",
    description="Raw candles and Hyblock data for LLM analysis",
    version="2.0.0"
)

binance_client = BinanceClient()
hyblock_client = HyblockClient()


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
        "endpoints": ["/status", "/health", "/snapshots", "/snapshots/latest"],
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
