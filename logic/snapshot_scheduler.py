"""
Background Scheduler for Snapshot Collection.

Runs every hour to save liquidation cluster snapshots for SOL, BTC, ETH.
Uses APScheduler for reliable background execution.
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from logic.snapshot_persistence import save_snapshot, get_stats, cleanup_old_snapshots

logger = logging.getLogger(__name__)

# Scheduler instance
scheduler: Optional[AsyncIOScheduler] = None

# Symbols to track
SYMBOLS = ["SOL", "BTC", "ETH"]


async def collect_snapshots(binance_client, hyblock_client):
    """
    Collect and save snapshots for all symbols.

    Args:
        binance_client: BinanceClient instance
        hyblock_client: HyblockClient instance
    """
    from logic.hyblock_service import get_hyblock_raw

    logger.info(f"[SNAPSHOT] Starting hourly collection - {datetime.utcnow().isoformat()}")

    saved = 0
    errors = 0

    for symbol in SYMBOLS:
        try:
            # Get current price
            price = await binance_client.get_current_price(f"{symbol}USDT")

            # Get Hyblock data
            hyblock = await get_hyblock_raw(symbol, hyblock_client)

            if hyblock and "liquidation_levels" in hyblock:
                clusters_data = hyblock["liquidation_levels"]
                snapshot_id = save_snapshot(symbol, price, clusters_data)

                clusters_count = len(clusters_data.get("data", []))
                logger.info(f"[SNAPSHOT] {symbol}: Saved #{snapshot_id} - "
                           f"${price:.2f}, {clusters_count} clusters")
                saved += 1
            else:
                logger.warning(f"[SNAPSHOT] {symbol}: No liquidation data available")
                errors += 1

        except Exception as e:
            logger.error(f"[SNAPSHOT] {symbol}: Error - {e}")
            errors += 1

    logger.info(f"[SNAPSHOT] Collection complete: {saved} saved, {errors} errors")

    # Cleanup old data monthly (on 1st hour of day)
    if datetime.utcnow().hour == 0:
        deleted = cleanup_old_snapshots(days=30)
        if deleted > 0:
            logger.info(f"[SNAPSHOT] Cleaned up {deleted} old snapshots")


def start_snapshot_scheduler(binance_client, hyblock_client, interval_minutes: int = 60):
    """
    Start the background snapshot scheduler.

    Args:
        binance_client: BinanceClient instance
        hyblock_client: HyblockClient instance
        interval_minutes: How often to collect (default 60 = hourly)
    """
    global scheduler

    if scheduler is not None:
        logger.warning("[SNAPSHOT] Scheduler already running")
        return

    scheduler = AsyncIOScheduler()

    # Wrapper to run async function
    def job_wrapper():
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(collect_snapshots(binance_client, hyblock_client))
        else:
            loop.run_until_complete(collect_snapshots(binance_client, hyblock_client))

    # Schedule hourly job
    scheduler.add_job(
        job_wrapper,
        IntervalTrigger(minutes=interval_minutes),
        id="snapshot_collector",
        name="Hourly Snapshot Collector",
        replace_existing=True
    )

    scheduler.start()
    logger.info(f"[SNAPSHOT] Scheduler started - collecting every {interval_minutes} minutes")

    # Run initial collection
    logger.info("[SNAPSHOT] Running initial collection...")
    asyncio.create_task(collect_snapshots(binance_client, hyblock_client))


def stop_snapshot_scheduler():
    """Stop the background scheduler."""
    global scheduler
    if scheduler:
        scheduler.shutdown()
        scheduler = None
        logger.info("[SNAPSHOT] Scheduler stopped")


def get_scheduler_status() -> dict:
    """Get scheduler status and stats."""
    stats = get_stats()

    return {
        "scheduler_running": scheduler is not None and scheduler.running if scheduler else False,
        "symbols": SYMBOLS,
        "stats": stats
    }
