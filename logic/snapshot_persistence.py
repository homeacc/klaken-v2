"""
Persistent Snapshot Storage using SQLite.

Stores liquidation cluster snapshots for historical analysis.
Each snapshot captures: timestamp, symbol, price, and all clusters.
"""

import sqlite3
import json
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from pathlib import Path


# Database file location - use /tmp on Railway for ephemeral storage
# For true persistence, would need external DB (Postgres, etc.)
DB_PATH = os.environ.get("SNAPSHOT_DB_PATH", "data/snapshots.db")


def get_db_connection():
    """Get database connection, creating tables if needed."""
    # Ensure directory exists
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        Path(db_dir).mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Create tables if not exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            clusters_count INTEGER NOT NULL,
            clusters_json TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_snapshots_symbol_timestamp
        ON snapshots(symbol, timestamp)
    """)

    conn.commit()
    return conn


def save_snapshot(symbol: str, price: float, clusters_data: dict) -> int:
    """
    Save a snapshot to the database.

    Args:
        symbol: SOL, BTC, or ETH
        price: Current price at snapshot time
        clusters_data: Hyblock liquidation_levels data

    Returns:
        ID of inserted record
    """
    conn = get_db_connection()
    try:
        timestamp = datetime.utcnow().isoformat() + "Z"
        clusters = clusters_data.get("data", [])
        clusters_count = len(clusters)

        cursor = conn.execute("""
            INSERT INTO snapshots (timestamp, symbol, price, clusters_count, clusters_json)
            VALUES (?, ?, ?, ?, ?)
        """, (timestamp, symbol.upper(), price, clusters_count, json.dumps(clusters)))

        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def get_snapshots_for_symbol(
    symbol: str,
    hours: int = 24,
    limit: int = 100
) -> List[Dict]:
    """
    Get recent snapshots for a symbol.

    Args:
        symbol: SOL, BTC, or ETH
        hours: How many hours back to look
        limit: Max number of snapshots to return

    Returns:
        List of snapshot dicts (without full cluster data)
    """
    conn = get_db_connection()
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat() + "Z"

        cursor = conn.execute("""
            SELECT id, timestamp, symbol, price, clusters_count
            FROM snapshots
            WHERE symbol = ? AND timestamp > ?
            ORDER BY timestamp DESC
            LIMIT ?
        """, (symbol.upper(), cutoff, limit))

        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def get_snapshot_by_id(snapshot_id: int) -> Optional[Dict]:
    """Get full snapshot including cluster data by ID."""
    conn = get_db_connection()
    try:
        cursor = conn.execute("""
            SELECT id, timestamp, symbol, price, clusters_count, clusters_json
            FROM snapshots
            WHERE id = ?
        """, (snapshot_id,))

        row = cursor.fetchone()
        if row:
            result = dict(row)
            result["clusters"] = json.loads(result.pop("clusters_json"))
            return result
        return None
    finally:
        conn.close()


def get_latest_snapshot(symbol: str) -> Optional[Dict]:
    """Get the most recent snapshot for a symbol."""
    conn = get_db_connection()
    try:
        cursor = conn.execute("""
            SELECT id, timestamp, symbol, price, clusters_count, clusters_json
            FROM snapshots
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol.upper(),))

        row = cursor.fetchone()
        if row:
            result = dict(row)
            result["clusters"] = json.loads(result.pop("clusters_json"))
            return result
        return None
    finally:
        conn.close()


def get_stats() -> Dict:
    """Get database statistics."""
    conn = get_db_connection()
    try:
        stats = {"symbols": {}}

        # Get counts and date ranges per symbol
        for symbol in ["SOL", "BTC", "ETH"]:
            cursor = conn.execute("""
                SELECT
                    COUNT(*) as count,
                    MIN(timestamp) as first_snapshot,
                    MAX(timestamp) as last_snapshot
                FROM snapshots
                WHERE symbol = ?
            """, (symbol,))

            row = cursor.fetchone()
            stats["symbols"][symbol] = {
                "count": row["count"],
                "first": row["first_snapshot"],
                "last": row["last_snapshot"]
            }

        # Total count
        cursor = conn.execute("SELECT COUNT(*) as total FROM snapshots")
        stats["total_snapshots"] = cursor.fetchone()["total"]

        # DB file size
        if os.path.exists(DB_PATH):
            stats["db_size_mb"] = round(os.path.getsize(DB_PATH) / (1024 * 1024), 2)
        else:
            stats["db_size_mb"] = 0

        return stats
    finally:
        conn.close()


def cleanup_old_snapshots(days: int = 30) -> int:
    """
    Delete snapshots older than specified days.

    Args:
        days: Delete snapshots older than this

    Returns:
        Number of deleted records
    """
    conn = get_db_connection()
    try:
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat() + "Z"

        cursor = conn.execute("""
            DELETE FROM snapshots WHERE timestamp < ?
        """, (cutoff,))

        deleted = cursor.rowcount
        conn.commit()

        # Vacuum to reclaim space
        if deleted > 0:
            conn.execute("VACUUM")

        return deleted
    finally:
        conn.close()
