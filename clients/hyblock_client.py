import httpx
import time
from typing import Dict, Optional, Any
import logging

from config.settings import settings

logger = logging.getLogger(__name__)


class HyblockClient:
    AUTH_URL = "https://auth-api.hyblockcapital.com/oauth2/token"
    BASE_URL = "https://api.hyblockcapital.com/v2"

    def __init__(self):
        self.api_key = settings.HYBLOCK_API_KEY
        self.client_id = settings.HYBLOCK_CLIENT_ID
        self.client_secret = settings.HYBLOCK_CLIENT_SECRET
        self.exchange = settings.HYBLOCK_EXCHANGE

        self._token: Optional[str] = None
        self._token_expires_at: float = 0

    async def _ensure_token(self) -> str:
        if self._token and time.time() < self._token_expires_at - 60:
            return self._token

        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(self.AUTH_URL, headers=headers, data=data, timeout=10.0)

            if response.status_code != 200:
                logger.error(f"OAuth2 failed: {response.status_code} - {response.text}")
                raise Exception(f"Hyblock OAuth2 failed: {response.status_code}")

            token_data = response.json()
            self._token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)
            self._token_expires_at = time.time() + expires_in

            logger.info("Hyblock OAuth2 token obtained")
            return self._token

    async def get(self, path: str, params: Dict = None) -> Optional[Any]:
        token = await self._ensure_token()

        url = f"{self.BASE_URL}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "x-api-key": self.api_key,
            "Accept": "application/json"
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers, params=params, timeout=15.0)

                if response.status_code == 404:
                    logger.warning(f"Endpoint not found: {path}")
                    return None
                elif response.status_code == 401:
                    logger.error(f"Auth failed for {path} - check credentials")
                    self._token = None
                    return None
                elif response.status_code == 422:
                    logger.warning(f"Invalid params for {path}: {params}")
                    return None

                response.raise_for_status()
                return response.json()

            except httpx.TimeoutException:
                logger.error(f"Timeout calling {path}")
                return None
            except Exception as e:
                logger.error(f"Error calling {path}: {e}")
                return None

    async def get_liquidation_levels(self, symbol: str) -> Optional[Dict]:
        params = {
            "coin": symbol.lower(),
            "exchange": self.exchange,
            "leverage": "all",
            "position": "all"
        }
        return await self.get("/liquidationLevels", params)

    async def get_open_interest(self, symbol: str) -> Optional[Dict]:
        params = {
            "coin": symbol.lower(),
            "exchange": self.exchange
        }
        return await self.get("/openInterest", params)

    async def get_funding_rate(self, symbol: str) -> Optional[Dict]:
        params = {
            "coin": symbol.lower(),
            "exchange": self.exchange
        }
        return await self.get("/fundingRate", params)

    async def get_top_traders(self, symbol: str) -> Optional[Dict]:
        params = {
            "coin": symbol.lower(),
            "exchange": self.exchange
        }
        return await self.get("/topTraderPositions", params)
