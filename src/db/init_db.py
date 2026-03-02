"""Initialize the SQLite database from schema.sql."""

import sqlite3
from pathlib import Path

DB_DIR = Path(__file__).resolve().parent.parent.parent / "data"
DB_PATH = DB_DIR / "lombok_intel.db"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


def init_database(db_path: Path | None = None) -> sqlite3.Connection:
    """Create (or open) the database and apply the schema idempotently."""
    db_path = db_path or DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")

    schema_sql = SCHEMA_PATH.read_text()
    conn.executescript(schema_sql)
    conn.commit()
    return conn


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Get a connection to an existing database."""
    db_path = db_path or DB_PATH
    if not db_path.exists():
        return init_database(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
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
