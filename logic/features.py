"""
Feature calculations for trading signals.

Liquidation Magnet Score:
- Measures the "pull" of liquidation clusters on price
- Positive score: Price pulled UP (short liquidations above)
- Negative score: Price pulled DOWN (long liquidations below)
"""

from typing import List, Dict, Optional
import math


def calculate_liquidation_magnet_score(
    current_price: float,
    clusters: List[Dict],
    range_pct: float = 0.15  # Default 15% range filter
) -> float:
    """
    Calculate the Liquidation Magnet Score.

    The score indicates directional pressure from liquidation clusters:
    - Positive (0 to 1): Bullish magnet - shorts above price pulling UP
    - Negative (-1 to 0): Bearish magnet - longs below price pulling DOWN
    - Zero: Neutral - balanced or no significant clusters

    Args:
        current_price: Current market price
        clusters: List of cluster dicts with keys:
            - price: Cluster price level
            - size: Volume/size of the cluster
            - side: "short" or "long"
        range_pct: Only consider clusters within this % of current price

    Returns:
        Score from -1.0 to 1.0
    """
    # Validate price
    if current_price is None or current_price <= 0:
        return 0.0

    # Handle empty clusters
    if not clusters:
        return 0.0

    # Calculate range bounds
    range_low = current_price * (1 - range_pct)
    range_high = current_price * (1 + range_pct)

    bullish_weighted_sum = 0.0  # Shorts above price (bullish magnet)
    bearish_weighted_sum = 0.0  # Longs below price (bearish magnet)

    for cluster in clusters:
        # Extract values with None handling
        price = cluster.get("price")
        size = cluster.get("size")
        side = cluster.get("side")

        # Skip invalid clusters
        if price is None or size is None or side is None:
            continue

        try:
            price = float(price)
            size = float(size)
        except (ValueError, TypeError):
            continue

        # Skip if size is zero or negative
        if size <= 0:
            continue

        # Apply range filter
        if price < range_low or price > range_high:
            continue

        # Calculate distance weight (closer = more weight)
        # Using inverse distance: weight = 1 / (1 + distance_pct)
        distance_pct = abs(price - current_price) / current_price
        distance_weight = 1.0 / (1.0 + distance_pct * 10)  # Scale factor of 10

        weighted_size = size * distance_weight

        # Categorize by position and side
        if price > current_price and side == "short":
            # Shorts above price = bullish magnet (squeeze potential)
            bullish_weighted_sum += weighted_size
        elif price < current_price and side == "long":
            # Longs below price = bearish magnet (dump potential)
            bearish_weighted_sum += weighted_size
        elif price >= current_price and side == "short":
            # Shorts at or above = bullish
            bullish_weighted_sum += weighted_size
        elif price <= current_price and side == "long":
            # Longs at or below = bearish
            bearish_weighted_sum += weighted_size

    # Calculate net score
    total = bullish_weighted_sum + bearish_weighted_sum

    if total == 0:
        return 0.0

    # Score from -1 to 1
    # Positive = bullish dominance, Negative = bearish dominance
    raw_score = (bullish_weighted_sum - bearish_weighted_sum) / total

    # Apply magnitude scaling based on actual weighted volume
    # This prevents small single clusters from getting extreme scores
    # Reference volume: 500 units = moderate signal, 1000+ = strong
    reference_volume = 500.0
    magnitude_factor = min(1.0, math.log1p(total / reference_volume) / math.log1p(5))

    # Final score combines direction and magnitude
    # A small cluster alone won't give a strong signal
    scaled_score = raw_score * magnitude_factor

    # Apply sigmoid-like normalization to handle extreme cases
    # This keeps the score in bounds while preserving direction
    normalized_score = math.tanh(scaled_score * 2)  # Scale factor for sensitivity

    return round(normalized_score, 4)


def interpret_magnet_score(score: float) -> str:
    """
    Interpret the magnet score for human readability.

    Args:
        score: Liquidation magnet score (-1 to 1)

    Returns:
        Human-readable interpretation
    """
    if score > 0.7:
        return "STRONG_BULLISH_MAGNET"
    elif score > 0.3:
        return "BULLISH_MAGNET"
    elif score > 0.1:
        return "WEAK_BULLISH"
    elif score < -0.7:
        return "STRONG_BEARISH_MAGNET"
    elif score < -0.3:
        return "BEARISH_MAGNET"
    elif score < -0.1:
        return "WEAK_BEARISH"
    else:
        return "NEUTRAL"


# =============================================================================
# INSTITUTIONAL SCORING FUNCTIONS
# =============================================================================

def calculate_whale_score(whale_delta: Optional[float]) -> float:
    """
    Calculate score based on Whale/Retail Delta.

    Whale Delta measures the difference between whale and retail positioning.
    - Positive: Whales are more long than retail (bullish)
    - Negative: Whales are more short than retail (bearish)

    Args:
        whale_delta: Whale retail delta value

    Returns:
        Score: +1 (bullish), -1 (bearish), or 0 (neutral)
    """
    if whale_delta is None:
        return 0.0

    try:
        whale_delta = float(whale_delta)
    except (ValueError, TypeError):
        return 0.0

    if whale_delta > 5.0:
        return 1.0  # Bullish: Whales accumulating
    elif whale_delta < -5.0:
        return -1.0  # Bearish: Whales distributing
    else:
        return 0.0  # Neutral


def calculate_funding_score(funding_rate: Optional[float]) -> float:
    """
    Calculate score based on Funding Rate.

    Funding rate is a contrarian indicator:
    - High positive: Market overheated, longs paying shorts (bearish signal)
    - Negative: Capitulation/fear, shorts paying longs (bullish signal)

    Note: funding_rate comes as decimal (0.01 = 1%, 0.0001 = 0.01%)

    Args:
        funding_rate: Funding rate as decimal (e.g., 0.0001 for 0.01%)

    Returns:
        Score: +1 (bullish), -1 (bearish), or 0 (neutral)
    """
    if funding_rate is None:
        return 0.0

    try:
        funding_rate = float(funding_rate)
    except (ValueError, TypeError):
        return 0.0

    # Thresholds: 0.01% = 0.0001 in decimal
    high_threshold = 0.0001  # 0.01%
    low_threshold = -0.0001  # -0.01%

    if funding_rate > high_threshold:
        return -1.0  # Bearish: Market overheated, contrarian sell signal
    elif funding_rate < low_threshold:
        return 1.0  # Bullish: Capitulation, contrarian buy signal
    else:
        return 0.0  # Neutral


def calculate_ls_score(ls_ratio: Optional[float]) -> float:
    """
    Calculate score based on Long/Short Ratio (Top Traders).

    L/S Ratio measures positioning of top traders:
    - > 2.0: Crowded longs, potential for squeeze DOWN (bearish)
    - < 2.0: Healthy positioning, room to go UP (bullish)

    Args:
        ls_ratio: Long/Short ratio from top traders

    Returns:
        Score: +1 (bullish), -1 (bearish), or 0 (neutral)
    """
    if ls_ratio is None:
        return 0.0

    try:
        ls_ratio = float(ls_ratio)
    except (ValueError, TypeError):
        return 0.0

    if ls_ratio <= 0:
        return 0.0  # Invalid data

    if ls_ratio > 2.0:
        return -1.0  # Bearish: Crowded longs
    else:
        return 1.0  # Bullish: Healthy positioning


def calculate_confluence_score(
    magnet_score: float,
    whale_score: float,
    funding_score: float,
    ls_score: float,
    weights: Optional[Dict[str, float]] = None
) -> Dict:
    """
    Calculate overall confluence score from all institutional signals.

    Combines multiple signals into a unified analysis:
    - Magnet Score: Liquidation cluster pressure (-1 to 1)
    - Whale Score: Institutional positioning (-1, 0, 1)
    - Funding Score: Contrarian sentiment (-1, 0, 1)
    - L/S Score: Top trader positioning (-1, 0, 1)

    Args:
        magnet_score: Liquidation magnet score
        whale_score: Whale delta score
        funding_score: Funding rate score
        ls_score: Long/short ratio score
        weights: Optional custom weights for each score

    Returns:
        Dict with total score, interpretation, and breakdown
    """
    # Default weights (can be customized)
    if weights is None:
        weights = {
            "magnet": 1.0,
            "whale": 1.5,    # Whale positioning is strong signal
            "funding": 1.0,
            "ls": 1.0
        }

    # Calculate weighted sum
    weighted_sum = (
        magnet_score * weights["magnet"] +
        whale_score * weights["whale"] +
        funding_score * weights["funding"] +
        ls_score * weights["ls"]
    )

    total_weight = sum(weights.values())
    normalized_score = weighted_sum / total_weight

    # Count bullish/bearish signals
    signals = [magnet_score, whale_score, funding_score, ls_score]
    bullish_count = sum(1 for s in signals if s > 0.3)
    bearish_count = sum(1 for s in signals if s < -0.3)

    # Determine overall bias
    if normalized_score > 0.5:
        bias = "STRONG_BULLISH"
    elif normalized_score > 0.2:
        bias = "BULLISH"
    elif normalized_score > 0.05:
        bias = "LEAN_BULLISH"
    elif normalized_score < -0.5:
        bias = "STRONG_BEARISH"
    elif normalized_score < -0.2:
        bias = "BEARISH"
    elif normalized_score < -0.05:
        bias = "LEAN_BEARISH"
    else:
        bias = "NEUTRAL"

    # Check for confluence (multiple signals aligned)
    if bullish_count >= 3:
        confluence = "HIGH_BULLISH_CONFLUENCE"
    elif bearish_count >= 3:
        confluence = "HIGH_BEARISH_CONFLUENCE"
    elif bullish_count >= 2 and bearish_count == 0:
        confluence = "MODERATE_BULLISH"
    elif bearish_count >= 2 and bullish_count == 0:
        confluence = "MODERATE_BEARISH"
    else:
        confluence = "MIXED_SIGNALS"

    return {
        "score": round(normalized_score, 4),
        "bias": bias,
        "confluence": confluence,
        "bullish_signals": bullish_count,
        "bearish_signals": bearish_count,
        "breakdown": {
            "magnet": round(magnet_score, 4),
            "whale": round(whale_score, 4),
            "funding": round(funding_score, 4),
            "ls": round(ls_score, 4)
        }
    }


def interpret_institutional_signal(confluence_result: Dict) -> str:
    """
    Generate human-readable interpretation of institutional signals.

    Args:
        confluence_result: Result from calculate_confluence_score

    Returns:
        Human-readable signal interpretation
    """
    score = confluence_result["score"]
    bias = confluence_result["bias"]
    confluence = confluence_result["confluence"]
    breakdown = confluence_result["breakdown"]

    # Build interpretation
    parts = []

    # Overall
    if "STRONG" in bias:
        parts.append(f"SIGNAL: {bias}")
    elif bias != "NEUTRAL":
        parts.append(f"BIAS: {bias}")
    else:
        parts.append("NO CLEAR SIGNAL")

    # Confluence
    if "HIGH" in confluence:
        parts.append(f"({confluence})")

    # Key drivers
    drivers = []
    if breakdown["whale"] != 0:
        direction = "LONG" if breakdown["whale"] > 0 else "SHORT"
        drivers.append(f"Whales {direction}")
    if breakdown["funding"] != 0:
        direction = "BULLISH" if breakdown["funding"] > 0 else "BEARISH"
        drivers.append(f"Funding {direction}")
    if breakdown["ls"] != 0:
        direction = "HEALTHY" if breakdown["ls"] > 0 else "CROWDED"
        drivers.append(f"L/S {direction}")

    if drivers:
        parts.append("| " + ", ".join(drivers))

    return " ".join(parts)
