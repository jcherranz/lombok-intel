"""Archive old calendar_snapshots to keep the main database lean.

Moves snapshots older than a configurable number of days (default: 180)
to a separate SQLite archive database in data/archive/.
"""

import fcntl
import sqlite3
from datetime import date, timedelta
from pathlib import Path

from src.db.init_db import DB_PATH, get_connection
from src.utils import setup_logger

logger = setup_logger("archive")

DEFAULT_RETENTION_DAYS = 180
CHUNK_SIZE = 1000


def archive_old_snapshots(
    db_path: Path | None = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
) -> int:
    """Move calendar_snapshots older than retention_days to archive DB.

    Processes in chunks of CHUNK_SIZE to avoid memory pressure on large tables.
    Uses a file lock to prevent concurrent archive operations.
    Returns the number of rows archived.
    """
    db_path = db_path or DB_PATH
    archive_dir = db_path.parent / "archive"
    cutoff = (date.today() - timedelta(days=retention_days)).isoformat()

    # Prevent concurrent archive operations
    archive_dir.mkdir(parents=True, exist_ok=True)
    lock_path = archive_dir / ".archive.lock"
    lock_file = open(lock_path, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        logger.warning("Another archive operation is running — skipping.")
        lock_file.close()
        return 0

    archive_conn = None
    conn = get_connection(db_path)
    try:
        # Count rows to archive
        count = conn.execute(
            "SELECT COUNT(*) FROM calendar_snapshots WHERE snapshot_date < ?",
            (cutoff,),
        ).fetchone()[0]

        if count == 0:
            logger.info("No snapshots older than %s to archive.", cutoff)
            return 0

        logger.info("Archiving %d snapshots older than %s...", count, cutoff)

        # Create archive DB
        archive_db = archive_dir / f"snapshots_before_{cutoff}.db"
        archive_conn = sqlite3.connect(str(archive_db))
        archive_conn.execute("PRAGMA busy_timeout = 30000")

        # Create archive table with same schema
        archive_conn.execute("""
            CREATE TABLE IF NOT EXISTS calendar_snapshots (
                snapshot_id INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                listing_id TEXT NOT NULL,
                run_id INTEGER,
                snapshot_date TEXT NOT NULL,
                scraped_at TEXT,
                is_available INTEGER DEFAULT 1,
                price REAL,
                currency TEXT DEFAULT 'USD',
                available_rooms INTEGER
            )
        """)

        # Process in chunks to limit memory usage
        total_archived = 0
        while True:
            rows = conn.execute(
                "SELECT * FROM calendar_snapshots WHERE snapshot_date < ? LIMIT ?",
                (cutoff, CHUNK_SIZE),
            ).fetchall()

            if not rows:
                break

            placeholders = ",".join("?" * len(rows[0]))
            archive_conn.executemany(
                f"INSERT OR IGNORE INTO calendar_snapshots VALUES ({placeholders})",
                rows,
            )
            archive_conn.commit()

            # Delete the chunk from main DB by snapshot_id
            ids = [row[0] for row in rows]  # snapshot_id is first column
            conn.execute(
                f"DELETE FROM calendar_snapshots WHERE snapshot_id IN ({','.join('?' * len(ids))})",
                ids,
            )
            conn.commit()

            total_archived += len(rows)
            logger.info("  Archived chunk: %d rows (%d total)", len(rows), total_archived)

        logger.info(
            "Archived %d snapshots to %s", total_archived, archive_db.name
        )
        return total_archived

    finally:
        if archive_conn is not None:
            archive_conn.close()
        conn.close()
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Archive old calendar snapshots")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Retain snapshots newer than N days (default: {DEFAULT_RETENTION_DAYS})",
    )
    args = parser.parse_args()

    archived = archive_old_snapshots(retention_days=args.days)
    print(f"Archived {archived} snapshots.")
