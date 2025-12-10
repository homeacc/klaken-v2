import httpx
from datetime import datetime, timezone
from typing import List, Dict

from config.settings import settings


class BinanceClient:
    def __init__(self):
        self.base_url = settings.BINANCE_BASE_URL

    async def get_klines(self, symbol: str, interval: str, limit: int = 100) -> List[Dict]:
        url = f"{self.base_url}/api/v3/klines"
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit
        }

        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, timeout=10.0)
            response.raise_for_status()
            raw = response.json()

        candles = []
        for k in raw:
            candles.append({
                "time": datetime.utcfromtimestamp(k[0] / 1000).replace(tzinfo=timezone.utc).isoformat(),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5])
            })
        return candles

    async def get_candles_1h(self, symbol: str, limit: int = 100) -> List[Dict]:
        return await self.get_klines(symbol, "1h", limit)

    async def get_candles_30m(self, symbol: str, limit: int = 100) -> List[Dict]:
        return await self.get_klines(symbol, "30m", limit)

    async def get_candles_15m(self, symbol: str, limit: int = 100) -> List[Dict]:
        return await self.get_klines(symbol, "15m", limit)

    async def get_candles_5m(self, symbol: str, limit: int = 100) -> List[Dict]:
        return await self.get_klines(symbol, "5m", limit)

    async def get_current_price(self, symbol: str) -> float:
        url = f"{self.base_url}/api/v3/ticker/price"
        params = {"symbol": symbol.upper()}

        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, timeout=5.0)
            response.raise_for_status()
            return float(response.json()["price"])
