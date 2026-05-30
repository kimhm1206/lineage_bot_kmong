from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from db import _database_url, init_db


DEFAULT_SQLITE_PATH = BASE_DIR / "data" / "lineage_bot.sqlite3"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate the existing SQLite lineage bot database into PostgreSQL."
    )
    parser.add_argument(
        "--sqlite-path",
        default=str(DEFAULT_SQLITE_PATH),
        help="Path to the existing SQLite database.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read source counts only; do not write PostgreSQL.",
    )
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite_path)
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite DB not found: {sqlite_path}")

    source = load_sqlite_data(sqlite_path)
    print_source_summary(source)
    if args.dry_run:
        return

    init_db()
    migrate_to_postgres(source)
    print("[done] SQLite data migrated to PostgreSQL.")


def load_sqlite_data(sqlite_path: Path) -> dict[str, list[dict[str, Any]]]:
    connection = sqlite3.connect(sqlite_path)
    connection.row_factory = sqlite3.Row
    try:
        return {
            "guilds": read_table(connection, "guilds"),
            "guild_settings": read_table(connection, "guild_settings"),
            "alliances": read_table(connection, "alliances"),
            "users": read_table(connection, "users"),
            "attendance_sessions": read_table(connection, "attendance_sessions"),
            "attendance_entries": read_table(connection, "attendance_entries"),
        }
    finally:
        connection.close()


def read_table(connection: sqlite3.Connection, table_name: str) -> list[dict[str, Any]]:
    exists = connection.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    if exists is None:
        return []
    rows = connection.execute(f"SELECT * FROM {table_name}").fetchall()
    return [dict(row) for row in rows]


def print_source_summary(source: dict[str, list[dict[str, Any]]]) -> None:
    print("[source]")
    for table_name, rows in source.items():
        print(f"- {table_name}: {len(rows)}")


def migrate_to_postgres(source: dict[str, list[dict[str, Any]]]) -> None:
    connection = psycopg2.connect(
        _database_url(),
        connect_timeout=10,
        cursor_factory=RealDictCursor,
    )
    try:
        with connection:
            with connection.cursor() as cursor:
                migrate_guilds(cursor, source["guilds"], source["guild_settings"], source["attendance_sessions"])
                alliance_id_map = migrate_alliances(cursor, source["alliances"])
                migrate_users(cursor, source["users"], alliance_id_map)
                migrate_attendance_sessions(cursor, source["attendance_sessions"])
                migrate_attendance_entries(cursor, source["attendance_entries"])
                reset_sequences(cursor)
    finally:
        connection.close()


def migrate_guilds(
    cursor: psycopg2.extensions.cursor,
    guilds: list[dict[str, Any]],
    guild_settings: list[dict[str, Any]],
    attendance_sessions: list[dict[str, Any]],
) -> None:
    guild_ids = {int(row["guild_id"]) for row in guilds if row.get("guild_id") is not None}
    guild_ids.update(
        int(row["guild_id"])
        for row in guild_settings
        if row.get("guild_id") is not None
    )
    guild_ids.update(
        int(row["guild_id"])
        for row in attendance_sessions
        if row.get("guild_id") is not None
    )
    for guild_id in sorted(guild_ids):
        cursor.execute(
            """
            INSERT INTO guilds (guild_id)
            VALUES (%s)
            ON CONFLICT (guild_id) DO NOTHING
            """,
            (guild_id,),
        )

    for row in guild_settings:
        cursor.execute(
            """
            INSERT INTO guild_settings (
                guild_id,
                admin_channel_id,
                attendance_voice_channel_id,
                log_channel_id,
                timer,
                attendance_available_timer
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (guild_id) DO UPDATE SET
                admin_channel_id = EXCLUDED.admin_channel_id,
                attendance_voice_channel_id = EXCLUDED.attendance_voice_channel_id,
                log_channel_id = EXCLUDED.log_channel_id,
                timer = EXCLUDED.timer,
                attendance_available_timer = EXCLUDED.attendance_available_timer,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                row.get("guild_id"),
                row.get("admin_channel_id"),
                row.get("attendance_voice_channel_id"),
                row.get("log_channel_id"),
                row.get("timer"),
                row.get("attendance_available_timer"),
            ),
        )


def migrate_alliances(
    cursor: psycopg2.extensions.cursor,
    rows: list[dict[str, Any]],
) -> dict[int, int]:
    alliance_id_map: dict[int, int] = {}
    for row in rows:
        alliance_name = str(row.get("alliance_name") or "").strip()
        if not alliance_name:
            continue
        cursor.execute(
            """
            INSERT INTO alliances (
                alliance_name,
                display_name,
                tag_name,
                is_active
            )
            VALUES (%s, %s, %s, TRUE)
            ON CONFLICT (alliance_name) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                tag_name = EXCLUDED.tag_name,
                is_active = TRUE,
                updated_at = CURRENT_TIMESTAMP
            RETURNING alliance_id
            """,
            (
                alliance_name,
                alliance_name,
                alliance_name,
            ),
        )
        target = cursor.fetchone()
        if row.get("alliance_id") is not None:
            alliance_id_map[int(row["alliance_id"])] = int(target["alliance_id"])
    return alliance_id_map


def migrate_users(
    cursor: psycopg2.extensions.cursor,
    rows: list[dict[str, Any]],
    alliance_id_map: dict[int, int],
) -> None:
    for row in rows:
        source_alliance_id = row.get("alliance_id")
        target_alliance_id = (
            alliance_id_map.get(int(source_alliance_id))
            if source_alliance_id is not None
            else None
        )
        cursor.execute(
            """
            INSERT INTO users (
                user_id,
                alliance_id,
                discord_id,
                discord_nickname,
                game_nickname,
                is_active
            )
            VALUES (%s, %s, %s, %s, %s, TRUE)
            ON CONFLICT (user_id) DO UPDATE SET
                alliance_id = EXCLUDED.alliance_id,
                discord_id = EXCLUDED.discord_id,
                discord_nickname = EXCLUDED.discord_nickname,
                game_nickname = EXCLUDED.game_nickname,
                is_active = TRUE,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                row.get("user_id"),
                target_alliance_id,
                row.get("discord_id"),
                row.get("discord_nickname"),
                row.get("discord_nickname"),
            ),
        )


def migrate_attendance_sessions(cursor: psycopg2.extensions.cursor, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        cursor.execute(
            """
            INSERT INTO attendance_sessions (
                attendance_id,
                guild_id,
                started_at,
                ended_at,
                started_by_discord_id
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (attendance_id) DO UPDATE SET
                guild_id = EXCLUDED.guild_id,
                started_at = EXCLUDED.started_at,
                ended_at = EXCLUDED.ended_at,
                started_by_discord_id = EXCLUDED.started_by_discord_id
            """,
            (
                row.get("attendance_id"),
                row.get("guild_id"),
                row.get("started_at"),
                row.get("ended_at"),
                row.get("started_by_discord_id"),
            ),
        )


def migrate_attendance_entries(cursor: psycopg2.extensions.cursor, rows: list[dict[str, Any]]) -> None:
    values = [
        (row.get("attendance_id"), row.get("user_id"))
        for row in rows
        if row.get("attendance_id") is not None and row.get("user_id") is not None
    ]
    if not values:
        return
    execute_values(
        cursor,
        """
        INSERT INTO attendance_entries (attendance_id, user_id)
        VALUES %s
        ON CONFLICT (attendance_id, user_id) DO NOTHING
        """,
        values,
        page_size=1000,
    )


def reset_sequences(cursor: psycopg2.extensions.cursor) -> None:
    sequence_tables = {
        "alliances_alliance_id_seq": ("alliances", "alliance_id"),
        "users_user_id_seq": ("users", "user_id"),
        "attendance_sessions_attendance_id_seq": ("attendance_sessions", "attendance_id"),
    }
    for sequence_name, (table_name, column_name) in sequence_tables.items():
        cursor.execute(
            f"""
            SELECT setval(
                %s,
                COALESCE((SELECT MAX({column_name}) FROM {table_name}), 1),
                true
            )
            """,
            (sequence_name,),
        )


if __name__ == "__main__":
    main()
