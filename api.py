from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from datetime import datetime, timezone
import logging

from clients.binance_client import BinanceClient
from clients.hyblock_client import HyblockClient
from logic.candles_service import get_multi_timeframe_candles
from logic.hyblock_service import get_hyblock_raw
from logic.squeeze_service import get_squeeze_data, format_squeeze_html, format_squeeze_error_html, SqueezeError

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


SUPPORTED_SYMBOLS = ["SOL", "BTC"]


@app.get("/")
async def root():
    return {
        "service": "Klaken V2",
        "description": "Raw candles and Hyblock data for LLM analysis",
        "endpoints": ["/status", "/health", "/squeeze", "/squeeze/view"],
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


@app.get("/squeeze")
async def get_squeeze(
    symbol: str = Query(..., description="Symbol: SOL or BTC")
):
    base_symbol = get_base_symbol(symbol)

    if base_symbol not in SUPPORTED_SYMBOLS:
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_symbol", "detail": "Supported symbols: SOL, BTC"}
        )

    logger.info(f"Processing /squeeze for {base_symbol}")

    try:
        data = await get_squeeze_data(base_symbol, binance_client, hyblock_client)
        return data

    except SqueezeError as e:
        return JSONResponse(
            status_code=503,
            content={"error": e.error_code, "detail": e.detail}
        )
    except Exception as e:
        logger.error(f"Unexpected error in /squeeze for {base_symbol}: {e}")
        return JSONResponse(
            status_code=503,
            content={"error": "internal_error", "detail": str(e)}
        )


@app.get("/squeeze/view", response_class=HTMLResponse)
async def get_squeeze_view(
    symbol: str = Query(..., description="Symbol: SOL or BTC")
):
    base_symbol = get_base_symbol(symbol)

    if base_symbol not in SUPPORTED_SYMBOLS:
        return HTMLResponse(
            content=format_squeeze_error_html("invalid_symbol", "Supported symbols: SOL, BTC"),
            status_code=400
        )

    logger.info(f"Processing /squeeze/view for {base_symbol}")

    try:
        data = await get_squeeze_data(base_symbol, binance_client, hyblock_client)
        html = format_squeeze_html(data)
        return HTMLResponse(content=html)

    except SqueezeError as e:
        return HTMLResponse(
            content=format_squeeze_error_html(e.error_code, e.detail),
            status_code=503
        )
    except Exception as e:
        logger.error(f"Unexpected error in /squeeze/view for {base_symbol}: {e}")
        return HTMLResponse(
            content=format_squeeze_error_html("internal_error", str(e)),
            status_code=503
        )
