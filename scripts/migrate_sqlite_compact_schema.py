from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.compact_schema import ensure_compact_schema


DEFAULT_DATABASE = BASE_DIR / "data" / "new_server_workspace.sqlite3"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply the compact audit, payout, and treasury schema to local SQLite.",
    )
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    args = parser.parse_args()
    database_path = args.database.expanduser().resolve()
    if not database_path.exists():
        raise SystemExit(f"SQLite database not found: {database_path}")

    migrated = ensure_compact_schema(database_path)
    with sqlite3.connect(database_path) as connection:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        audit_count = connection.execute("SELECT COUNT(*) FROM audit_events").fetchone()[0]
    if str(integrity).lower() != "ok" or violations:
        raise SystemExit(
            f"Validation failed: integrity={integrity}, foreign_keys={len(violations)}"
        )
    print(f"SQLite: {database_path}")
    print(f"Legacy logs migrated: {migrated:,}")
    print(f"Compact audit events: {audit_count:,}")
    print("Integrity: ok")


if __name__ == "__main__":
    main()
