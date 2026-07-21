from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.compact_schema import ensure_compact_schema


DEFAULT_OUTPUT = BASE_DIR / "data" / "new_server_workspace.sqlite3"
KST = timezone(timedelta(hours=9))

FULL_COPY_TABLES = {
    "alliance_item_bid_statuses",
    "alliance_payout_fee_rules",
    "alliances",
    "bid_items",
    "guild_alliance_role_mappings",
    "guild_bookkeepers",
    "guild_settings",
    "guilds",
    "item_bid_rules",
    "item_categories",
    "item_price_rules",
    "items",
    "scheduled_report_settings",
    "users",
    "web_admins",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clone production schema and recent related data into branch-local SQLite.",
    )
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace an existing SQLite test database.",
    )
    return parser.parse_args()


def postgres_connection() -> psycopg2.extensions.connection:
    load_dotenv(BASE_DIR / ".env")
    required = ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing PostgreSQL settings: {', '.join(missing)}")
    return psycopg2.connect(
        host=os.environ["PGHOST"],
        port=os.getenv("PGPORT", "5432"),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        connect_timeout=10,
        cursor_factory=RealDictCursor,
    )


def fetch_metadata(cursor: RealDictCursor) -> dict[str, Any]:
    cursor.execute(
        """
        SELECT table_name, column_name, data_type, is_nullable, column_default,
               ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
        """
    )
    columns: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in cursor.fetchall():
        columns[str(row["table_name"])].append(dict(row))

    cursor.execute(
        """
        SELECT tc.table_name, kcu.column_name, kcu.ordinal_position
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = 'public' AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY tc.table_name, kcu.ordinal_position
        """
    )
    primary_keys: dict[str, list[str]] = defaultdict(list)
    for row in cursor.fetchall():
        primary_keys[str(row["table_name"])].append(str(row["column_name"]))

    cursor.execute(
        """
        SELECT tc.table_name, kcu.column_name, ccu.table_name AS foreign_table_name,
               ccu.column_name AS foreign_column_name,
               rc.delete_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        JOIN information_schema.referential_constraints rc
          ON rc.constraint_name = tc.constraint_name
         AND rc.constraint_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
        ORDER BY tc.table_name, kcu.ordinal_position
        """
    )
    foreign_keys: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in cursor.fetchall():
        foreign_keys[str(row["table_name"])].append(dict(row))

    cursor.execute(
        """
        SELECT tablename, indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'public'
        ORDER BY tablename, indexname
        """
    )
    indexes = [dict(row) for row in cursor.fetchall()]
    return {
        "columns": dict(columns),
        "primary_keys": dict(primary_keys),
        "foreign_keys": dict(foreign_keys),
        "indexes": indexes,
    }


def selected_ids(
    cursor: RealDictCursor,
    cutoff_text: str,
    cutoff_date: str,
) -> dict[str, set[int]]:
    cursor.execute(
        """
        SELECT db.distribution_id
        FROM distribution_batches db
        INNER JOIN loot_events le ON le.loot_event_id = db.loot_event_id
        WHERE le.event_date >= %s
        """,
        (cutoff_date,),
    )
    distribution_ids = {int(row["distribution_id"]) for row in cursor.fetchall()}

    cursor.execute(
        """
        SELECT loot_event_id
        FROM loot_events
        WHERE event_date >= %s
        """,
        (cutoff_date,),
    )
    loot_event_ids = {int(row["loot_event_id"]) for row in cursor.fetchall()}
    if distribution_ids:
        cursor.execute(
            "SELECT loot_event_id FROM distribution_batches WHERE distribution_id = ANY(%s)",
            (list(distribution_ids),),
        )
        loot_event_ids.update(
            int(row["loot_event_id"])
            for row in cursor.fetchall()
            if row["loot_event_id"] is not None
        )

    cursor.execute(
        "SELECT attendance_id FROM attendance_sessions WHERE started_at >= %s",
        (cutoff_text,),
    )
    attendance_ids = {int(row["attendance_id"]) for row in cursor.fetchall()}

    cursor.execute(
        "SELECT live_session_id FROM attendance_live_sessions WHERE started_at >= %s",
        (cutoff_text,),
    )
    live_session_ids = {int(row["live_session_id"]) for row in cursor.fetchall()}
    return {
        "distribution": distribution_ids,
        "loot_event": loot_event_ids,
        "attendance": attendance_ids,
        "live_session": live_session_ids,
    }


def selection_for_table(
    table_name: str,
    ids: dict[str, set[int]],
    cutoff_text: str,
    cutoff_utc: datetime,
) -> tuple[str, tuple[Any, ...]]:
    if table_name in FULL_COPY_TABLES:
        return "", ()
    id_filters = {
        "attendance_sessions": ("attendance_id", ids["attendance"]),
        "attendance_entries": ("attendance_id", ids["attendance"]),
        "attendance_live_sessions": ("live_session_id", ids["live_session"]),
        "attendance_live_participants": ("live_session_id", ids["live_session"]),
        "loot_events": ("loot_event_id", ids["loot_event"]),
        "loot_event_items": ("loot_event_id", ids["loot_event"]),
        "loot_event_alliance_counts": ("loot_event_id", ids["loot_event"]),
        "loot_event_participants": ("loot_event_id", ids["loot_event"]),
        "distribution_batches": ("distribution_id", ids["distribution"]),
        "distribution_alliance_payouts": ("distribution_id", ids["distribution"]),
        "member_payout_rule_snapshots": ("distribution_id", ids["distribution"]),
        "member_payout_statuses": ("distribution_id", ids["distribution"]),
        "member_forfeiture_settlements": ("distribution_id", ids["distribution"]),
        "loot_fee_settlements": ("distribution_id", ids["distribution"]),
    }
    if table_name in id_filters:
        column_name, values = id_filters[table_name]
        return (
            f'WHERE "{column_name}" = ANY(%s)' if values else "WHERE FALSE",
            (sorted(values),) if values else (),
        )
    if table_name == "bid_item_results":
        return 'WHERE "selected_at" >= %s', (cutoff_text,)
    timestamp_filters = {
        "bot_command_queue": ("created_at", cutoff_utc),
        "discord_message_links": ("created_at", cutoff_utc),
        "notifications": ("created_at", cutoff_utc),
        "websocket_events": ("created_at", cutoff_utc),
        "work_logs": ("created_at", cutoff_utc),
    }
    if table_name in timestamp_filters:
        column_name, cutoff = timestamp_filters[table_name]
        return f'WHERE "{column_name}" >= %s', (cutoff,)
    return "", ()


def sqlite_type(postgres_type: str) -> str:
    if postgres_type in {"bigint", "integer", "smallint", "boolean"}:
        return "INTEGER"
    if postgres_type in {"numeric", "real", "double precision"}:
        return "NUMERIC"
    if "timestamp" in postgres_type or postgres_type in {"date", "time without time zone"}:
        return "TEXT"
    if postgres_type in {"json", "jsonb"}:
        return "TEXT"
    return "TEXT"


def sqlite_default(value: Any) -> str | None:
    if value is None:
        return None
    default = str(value)
    if default.startswith("nextval("):
        return None
    default = re.sub(r"::[A-Za-z0-9_\[\]]+", "", default)
    default = re.sub(r"\btrue\b", "1", default, flags=re.IGNORECASE)
    default = re.sub(r"\bfalse\b", "0", default, flags=re.IGNORECASE)
    return default


def create_schema(connection: sqlite3.Connection, metadata: dict[str, Any]) -> None:
    primary_keys: dict[str, list[str]] = metadata["primary_keys"]
    foreign_keys: dict[str, list[dict[str, Any]]] = metadata["foreign_keys"]
    for table_name, columns in metadata["columns"].items():
        definitions: list[str] = []
        pk_columns = primary_keys.get(table_name, [])
        for column in columns:
            column_name = str(column["column_name"])
            definition = f'"{column_name}" {sqlite_type(str(column["data_type"]))}'
            if len(pk_columns) == 1 and column_name == pk_columns[0]:
                definition += " PRIMARY KEY"
            if str(column["is_nullable"]) == "NO" and column_name not in pk_columns:
                definition += " NOT NULL"
            default = sqlite_default(column["column_default"])
            if default is not None:
                definition += f" DEFAULT {default}"
            definitions.append(definition)
        if len(pk_columns) > 1:
            definitions.append(
                "PRIMARY KEY (" + ", ".join(f'"{name}"' for name in pk_columns) + ")"
            )
        for foreign_key in foreign_keys.get(table_name, []):
            definition = (
                f'FOREIGN KEY ("{foreign_key["column_name"]}") '
                f'REFERENCES "{foreign_key["foreign_table_name"]}" '
                f'("{foreign_key["foreign_column_name"]}")'
            )
            delete_rule = str(foreign_key.get("delete_rule") or "NO ACTION")
            if delete_rule in {"CASCADE", "SET NULL", "RESTRICT", "NO ACTION"}:
                definition += f" ON DELETE {delete_rule}"
            definitions.append(definition)
        connection.execute(
            f'CREATE TABLE "{table_name}" (\n  ' + ",\n  ".join(definitions) + "\n)"
        )


def create_indexes(connection: sqlite3.Connection, metadata: dict[str, Any]) -> None:
    for index in metadata["indexes"]:
        index_name = str(index["indexname"])
        if index_name.endswith("_pkey"):
            continue
        indexdef = str(index["indexdef"])
        match = re.match(
            r"CREATE\s+(UNIQUE\s+)?INDEX\s+\S+\s+ON\s+public\.(\S+)\s+"
            r"USING\s+btree\s+(\(.+\))(?:\s+WHERE\s+(.+))?$",
            indexdef,
            flags=re.IGNORECASE,
        )
        if not match:
            continue
        unique, table_name, columns, predicate = match.groups()
        sql = (
            f"CREATE {'UNIQUE ' if unique else ''}INDEX "
            f'"{index_name}" ON "{table_name}" {columns}'
        )
        if predicate:
            normalized = predicate.strip()
            if normalized.startswith("(") and normalized.endswith(")"):
                normalized = normalized[1:-1]
            normalized = re.sub(r"\btrue\b", "1", normalized, flags=re.IGNORECASE)
            normalized = re.sub(r"\bfalse\b", "0", normalized, flags=re.IGNORECASE)
            sql += f" WHERE {normalized}"
        connection.execute(sql)


def normalize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, bool):
        return int(value)
    return value


def copy_rows(
    pg_cursor: RealDictCursor,
    sqlite_connection: sqlite3.Connection,
    metadata: dict[str, Any],
    ids: dict[str, set[int]],
    cutoff_text: str,
    cutoff_utc: datetime,
) -> dict[str, int]:
    copied: dict[str, int] = {}
    sqlite_connection.execute("PRAGMA foreign_keys=OFF")
    for table_name in metadata["columns"]:
        where_sql, params = selection_for_table(
            table_name,
            ids,
            cutoff_text,
            cutoff_utc,
        )
        pg_cursor.execute(f'SELECT * FROM "{table_name}" {where_sql}', params)
        rows = pg_cursor.fetchall()
        if table_name == "loot_events":
            rows = [
                {
                    **dict(row),
                    "attendance_id": (
                        row["attendance_id"]
                        if row["attendance_id"] in ids["attendance"]
                        else None
                    ),
                }
                for row in rows
            ]
        copied[table_name] = len(rows)
        if not rows:
            continue
        column_names = [str(column["column_name"]) for column in metadata["columns"][table_name]]
        placeholders = ", ".join("?" for _ in column_names)
        insert_sql = (
            f'INSERT INTO "{table_name}" ('
            + ", ".join(f'"{name}"' for name in column_names)
            + f") VALUES ({placeholders})"
        )
        sqlite_connection.executemany(
            insert_sql,
            [
                tuple(normalize_value(row[name]) for name in column_names)
                for row in rows
            ],
        )
    sqlite_connection.commit()
    sqlite_connection.execute("PRAGMA foreign_keys=ON")
    return copied


def verify_clone(connection: sqlite3.Connection) -> None:
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        sample = ", ".join(str(tuple(row)) for row in violations[:5])
        raise RuntimeError(f"SQLite foreign key validation failed: {sample}")
    result = connection.execute("PRAGMA integrity_check").fetchone()
    if result is None or str(result[0]).lower() != "ok":
        raise RuntimeError(f"SQLite integrity check failed: {result}")


def main() -> None:
    args = parse_args()
    if args.days < 1:
        raise SystemExit("--days must be at least 1")
    output = args.output.expanduser().resolve()
    if output.exists() and not args.replace:
        raise SystemExit(f"Output already exists: {output} (use --replace)")
    if output.exists():
        output.unlink()
    output.parent.mkdir(parents=True, exist_ok=True)

    cutoff_kst = datetime.now(KST) - timedelta(days=args.days)
    cutoff_date = cutoff_kst.strftime("%Y-%m-%d")
    cutoff_kst = datetime.strptime(cutoff_date, "%Y-%m-%d").replace(tzinfo=KST)
    cutoff_text = f"{cutoff_date} 00:00:00"
    cutoff_utc = cutoff_kst.astimezone(timezone.utc)

    pg_connection = postgres_connection()
    sqlite_connection = sqlite3.connect(output)
    try:
        with pg_connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            metadata = fetch_metadata(cursor)
            ids = selected_ids(cursor, cutoff_text, cutoff_date)
            create_schema(sqlite_connection, metadata)
            copied = copy_rows(
                cursor,
                sqlite_connection,
                metadata,
                ids,
                cutoff_text,
                cutoff_utc,
            )
            create_indexes(sqlite_connection, metadata)
            sqlite_connection.commit()
        verify_clone(sqlite_connection)
    except Exception:
        sqlite_connection.close()
        pg_connection.close()
        output.unlink(missing_ok=True)
        raise
    else:
        sqlite_connection.close()
        pg_connection.close()

    try:
        migrated_logs = ensure_compact_schema(output)
        with sqlite3.connect(output) as verification_connection:
            verify_clone(verification_connection)
    except Exception:
        output.unlink(missing_ok=True)
        raise

    print(f"SQLite clone: {output}")
    print(f"Cutoff (KST): {cutoff_text}")
    print(f"Tables: {len(copied)} / Rows: {sum(copied.values()):,}")
    for table_name, count in copied.items():
        print(f"  {table_name}: {count:,}")
    print(f"Compact audit logs migrated: {migrated_logs:,}")


if __name__ == "__main__":
    main()
