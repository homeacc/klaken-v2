from typing import Dict, Optional, List
import logging

from clients.hyblock_client import HyblockClient

logger = logging.getLogger(__name__)


def normalize_symbol_base(symbol: str) -> str:
    symbol = symbol.upper().strip()
    for suffix in ["USDT", "USD", "PERP"]:
        if symbol.endswith(suffix):
            return symbol[:-len(suffix)]
    return symbol


def _extract_latest(data: Optional[Dict], fields: List[str]) -> Optional[Dict]:
    """Extrae el dato más reciente de un endpoint histórico"""
    if not data or "data" not in data:
        return None

    items = data["data"]
    if not isinstance(items, list) or len(items) == 0:
        return None

    latest = items[-1]  # Más reciente al final
    result = {"timestamp": latest.get("openDate")}  # Hyblock usa openDate, no timestamp
    for field in fields:
        if field in latest:
            result[field] = latest.get(field)
    return result


async def get_hyblock_raw(symbol: str, hyblock_client: HyblockClient) -> Dict:
    symbol_base = normalize_symbol_base(symbol)

    # liquidationLevels - snapshot (no cambiar)
    liquidation_levels = await hyblock_client.get_liquidation_levels(symbol_base)

    # Endpoints históricos (fail-safe individual)
    top_traders = None
    open_interest = None
    funding_rate = None
    whale_retail_delta = None

    try:
        raw = await hyblock_client.get_top_traders(symbol_base)
        top_traders = _extract_latest(raw, ["longPct", "shortPct", "lsRatio"])
    except Exception as e:
        logger.warning(f"Error top_traders: {e}")

    try:
        raw = await hyblock_client.get_open_interest(symbol_base)
        open_interest = _extract_latest(raw, ["open", "high", "low", "close"])
    except Exception as e:
        logger.warning(f"Error open_interest: {e}")

    try:
        raw = await hyblock_client.get_funding_rate(symbol_base)
        funding_rate = _extract_latest(raw, ["fundingRate", "indicativeFundingRate"])
    except Exception as e:
        logger.warning(f"Error funding_rate: {e}")

    try:
        raw = await hyblock_client.get_whale_retail_delta(symbol_base)
        whale_retail_delta = _extract_latest(raw, ["whaleRetailDelta"])
    except Exception as e:
        logger.warning(f"Error whale_retail_delta: {e}")

    return {
        "liquidation_levels": liquidation_levels,
        "top_traders": top_traders,
        "open_interest": open_interest,
        "funding_rate": funding_rate,
        "whale_retail_delta": whale_retail_delta
    }
