from typing import Dict
import logging

from clients.binance_client import BinanceClient

logger = logging.getLogger(__name__)


def normalize_symbol(symbol: str) -> str:
    symbol = symbol.upper().strip()
    for suffix in ["USDT", "USD", "PERP"]:
        if symbol.endswith(suffix):
            symbol = symbol[:-len(suffix)]
            break
    return f"{symbol}USDT"


async def get_multi_timeframe_candles(symbol: str, binance_client: BinanceClient) -> Dict:
    symbol_binance = normalize_symbol(symbol)

    try:
        h1 = await binance_client.get_candles_1h(symbol_binance, limit=100)
        m30 = await binance_client.get_candles_30m(symbol_binance, limit=100)
        m15 = await binance_client.get_candles_15m(symbol_binance, limit=100)
        m5 = await binance_client.get_candles_5m(symbol_binance, limit=100)

        current_price = await binance_client.get_current_price(symbol_binance)

        return {
            "symbol_binance": symbol_binance,
            "current_price": current_price,
            "h1": h1,
            "m30": m30,
            "m15": m15,
            "m5": m5
        }
    except Exception as e:
        logger.error(f"Error fetching candles for {symbol}: {e}")
        return {
            "symbol_binance": symbol_binance,
            "current_price": 0,
            "error": str(e),
            "h1": [],
            "m30": [],
            "m15": [],
            "m5": []
        }
