"""Initialize the SQLite database from schema.sql."""

import sqlite3
from pathlib import Path

DB_DIR = Path(__file__).resolve().parent.parent.parent / "data"
DB_PATH = DB_DIR / "lombok_intel.db"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

_migrations_applied: set[str] = set()  # Track which DB paths have been migrated this process


def init_database(db_path: Path | None = None) -> sqlite3.Connection:
    """Create (or open) the database and apply the schema idempotently."""
    db_path = db_path or DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA synchronous = NORMAL")

    schema_sql = SCHEMA_PATH.read_text()
    conn.executescript(schema_sql)

    # Migrations for existing databases
    _apply_migrations(conn)

    conn.commit()
    return conn


def _apply_migrations(conn: sqlite3.Connection):
    """Apply incremental schema migrations to existing databases."""
    # M1: occupancy_events idempotency constraint (2026-03-03)
    # First, remove any existing duplicates so index creation succeeds
    try:
        conn.execute("""
            DELETE FROM occupancy_events
            WHERE event_id NOT IN (
                SELECT MIN(event_id)
                FROM occupancy_events
                GROUP BY source, listing_id, event_date, prev_run_id, curr_run_id
            )
        """)
    except Exception:
        pass  # Table may not exist yet or be empty
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uix_occupancy_events_dedup
        ON occupancy_events (source, listing_id, event_date, prev_run_id, curr_run_id)
    """)


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Get a connection to an existing database."""
    db_path = db_path or DB_PATH
    if not db_path.exists():
        return init_database(db_path)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.row_factory = sqlite3.Row
    db_key = str(db_path)
    if db_key not in _migrations_applied:
        _apply_migrations(conn)
        conn.commit()
        _migrations_applied.add(db_key)
    return conn


if __name__ == "__main__":
    conn = init_database()
    # Verify zones were seeded
    cursor = conn.execute("SELECT zone_id, name FROM zones ORDER BY zone_priority")
    print("Database initialized. Zones:")
    for row in cursor:
        print(f"  {row[0]}: {row[1]}")
    conn.close()
    print(f"\nDatabase at: {DB_PATH}")
