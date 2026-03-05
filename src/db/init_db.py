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
    conn.row_factory = sqlite3.Row

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

    # M2: Recreate v_adr_simple and v_revpar_monthly with dedup CTEs (2026-03-03)
    # DROP + CREATE replaces old non-deduped versions. Schema.sql has the
    # updated definitions via CREATE VIEW IF NOT EXISTS, so after dropping
    # we re-run the schema to pick up the new version.
    for view in ("v_adr_simple", "v_revpar_monthly"):
        conn.execute(f"DROP VIEW IF EXISTS {view}")

    # M3: Remove broken v_adr_by_zone_month (uses non-existent PERCENTILE function)
    conn.execute("DROP VIEW IF EXISTS v_adr_by_zone_month")

    # M4: Add 'discovery' to scrape_runs.run_type CHECK constraint (2026-03-05)
    # SQLite doesn't support ALTER CHECK, so recreate the table.
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='scrape_runs'"
    ).fetchone()
    needs_table_rebuild = row and "discovery" not in row[0]

    if needs_table_rebuild:
        # Drop view that references scrape_runs to avoid errors during rebuild
        # executescript auto-commits, which is needed for PRAGMA foreign_keys
        conn.executescript("""
            DROP VIEW IF EXISTS v_scrape_health;
            PRAGMA foreign_keys = OFF;
        """)
        conn.execute("""
            CREATE TABLE scrape_runs_new (
                run_id          INTEGER     PRIMARY KEY AUTOINCREMENT,
                source          TEXT        NOT NULL CHECK (source IN ('airbnb', 'booking')),
                run_type        TEXT        NOT NULL CHECK (run_type IN ('full', 'incremental', 'calendar_only', 'discovery')),
                started_at      TEXT        NOT NULL DEFAULT (datetime('now')),
                finished_at     TEXT,
                status          TEXT        NOT NULL DEFAULT 'running'
                                            CHECK (status IN ('running', 'completed', 'partial', 'failed')),
                listings_seen   INTEGER     DEFAULT 0,
                listings_new    INTEGER     DEFAULT 0,
                snapshots_added INTEGER     DEFAULT 0,
                error_message   TEXT,
                notes           TEXT
            )
        """)
        conn.execute("INSERT INTO scrape_runs_new SELECT * FROM scrape_runs")
        conn.execute("DROP TABLE scrape_runs")
        conn.execute("ALTER TABLE scrape_runs_new RENAME TO scrape_runs")
        conn.commit()
        conn.executescript("PRAGMA foreign_keys = ON;")

    # Re-apply schema to recreate views with new definitions
    schema_sql = SCHEMA_PATH.read_text()
    conn.executescript(schema_sql)


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
