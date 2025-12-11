from fastapi import FastAPI, Query, HTTPException
from datetime import datetime, timezone
import logging

from clients.binance_client import BinanceClient
from clients.hyblock_client import HyblockClient
from logic.candles_service import get_multi_timeframe_candles
from logic.hyblock_service import get_hyblock_raw

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
        "endpoints": ["/status", "/health"],
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
    symbol: str = Query(..., description="Symbol: SOL or BTC")
):
    base_symbol = get_base_symbol(symbol)

    if base_symbol not in ["SOL", "BTC"]:
        raise HTTPException(
            status_code=400,
            detail=f"Symbol must be SOL or BTC, got: {base_symbol}"
        )

    logger.info(f"Processing /status for {base_symbol}")

    try:
        candles = await get_multi_timeframe_candles(base_symbol, binance_client)
        hyblock = await get_hyblock_raw(base_symbol, hyblock_client)

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
