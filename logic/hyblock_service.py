from typing import Dict
import logging

from clients.hyblock_client import HyblockClient

logger = logging.getLogger(__name__)


def normalize_symbol_base(symbol: str) -> str:
    symbol = symbol.upper().strip()
    for suffix in ["USDT", "USD", "PERP"]:
        if symbol.endswith(suffix):
            return symbol[:-len(suffix)]
    return symbol


async def get_hyblock_raw(symbol: str, hyblock_client: HyblockClient) -> Dict:
    symbol_base = normalize_symbol_base(symbol)

    liquidation_levels = await hyblock_client.get_liquidation_levels(symbol_base)
    open_interest = await hyblock_client.get_open_interest(symbol_base)
    funding_rate = await hyblock_client.get_funding_rate(symbol_base)
    top_traders = await hyblock_client.get_top_traders(symbol_base)

    return {
        "liquidation_levels": liquidation_levels,
        "open_interest": open_interest,
        "funding_rate": funding_rate,
        "top_traders": top_traders
    }
