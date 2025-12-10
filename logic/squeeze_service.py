from typing import Dict, List, Optional
from datetime import datetime, timezone
import logging

from clients.hyblock_client import HyblockClient
from clients.binance_client import BinanceClient

logger = logging.getLogger(__name__)


class SqueezeError(Exception):
    def __init__(self, error_code: str, detail: str):
        self.error_code = error_code
        self.detail = detail
        super().__init__(detail)


async def get_squeeze_data(
    symbol: str,
    binance_client: BinanceClient,
    hyblock_client: HyblockClient
) -> Dict:
    binance_symbol = f"{symbol}USDT"

    try:
        current_price = await binance_client.get_current_price(binance_symbol)
    except Exception as e:
        logger.error(f"Binance price fetch failed: {e}")
        raise SqueezeError("binance_unavailable", "Could not fetch current price")

    liquidation_data = await hyblock_client.get_liquidation_levels(symbol)

    if liquidation_data is None:
        raise SqueezeError("hyblock_unavailable", "Could not fetch liquidation data")

    entries = liquidation_data.get("data", [])
    if not entries:
        entries = liquidation_data.get("entries", [])
    if not entries and isinstance(liquidation_data, list):
        entries = liquidation_data

    shorts = []
    longs = []

    for entry in entries:
        price = entry.get("price")
        side = entry.get("side", "").lower()
        size_usd = entry.get("size") or entry.get("sizeUsd") or entry.get("size_usd") or 0
        leverage = entry.get("leverage", "medium").lower()

        if price is None or side not in ["short", "long"]:
            continue

        price = float(price)
        size_usd = int(float(size_usd))

        if leverage not in ["low", "medium", "high"]:
            leverage = "medium"

        distance_pct = ((price - current_price) / current_price) * 100

        item = {
            "price": round(price, 2),
            "size_usd": size_usd,
            "leverage": leverage,
            "distance_pct": round(distance_pct, 2)
        }

        if side == "short" and price > current_price:
            shorts.append(item)
        elif side == "long" and price < current_price:
            longs.append(item)

    shorts.sort(key=lambda x: abs(x["distance_pct"]))
    longs.sort(key=lambda x: abs(x["distance_pct"]))

    shorts = shorts[:15]
    longs = longs[:15]

    return {
        "symbol": symbol,
        "price": round(current_price, 2),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "hyblock_v2_liquidationLevels",
        "shorts": shorts,
        "longs": longs
    }


def format_squeeze_html(data: Dict) -> str:
    symbol = data["symbol"]
    price = data["price"]
    timestamp = data["timestamp"].replace("T", " ").replace("Z", " UTC")
    shorts = data["shorts"]
    longs = data["longs"]

    def format_size(size_usd: int) -> str:
        if size_usd >= 1_000_000:
            return f"${size_usd / 1_000_000:.1f}M"
        elif size_usd >= 1_000:
            return f"${size_usd / 1_000:.1f}K"
        else:
            return f"${size_usd}"

    def format_distance(distance_pct: float) -> str:
        sign = "+" if distance_pct >= 0 else ""
        return f"{sign}{distance_pct:.2f}%"

    lines = [
        f"SQUEEZE DATA - {symbol}/USD",
        f"Price: ${price}",
        f"Updated: {timestamp}",
        "",
        "=== SHORTS (above price) ==="
    ]

    shorts_within_10 = [s for s in shorts if abs(s["distance_pct"]) <= 10]
    if shorts_within_10:
        for s in shorts_within_10:
            lines.append(
                f"${s['price']:.2f} | {format_distance(s['distance_pct'])} | {format_size(s['size_usd'])} | {s['leverage']}"
            )
    else:
        lines.append("(no shorts within 10%)")

    lines.append("")
    lines.append("=== LONGS (below price) ===")

    longs_within_10 = [l for l in longs if abs(l["distance_pct"]) <= 10]
    if longs_within_10:
        for l in longs_within_10:
            lines.append(
                f"${l['price']:.2f} | {format_distance(l['distance_pct'])} | {format_size(l['size_usd'])} | {l['leverage']}"
            )
    else:
        lines.append("(no longs within 10%)")

    content = "\n".join(lines)

    return f"""<html>
<head><title>Squeeze Data - {symbol}</title></head>
<body>
<pre>
{content}
</pre>
</body>
</html>"""


def format_squeeze_error_html(error_code: str, detail: str) -> str:
    return f"""<html><body><pre>
ERROR: {error_code}
{detail}
</pre></body></html>"""
