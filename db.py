from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


load_dotenv()

DEFAULT_ALLIANCE_NAMES = ("정지", "랭커", "삼국", "해적", "보스", "인연")
TEST_DB_FLAG = "--test"


def is_test_database_mode() -> bool:
    return TEST_DB_FLAG in sys.argv or os.getenv("LINEAGE_DB_TARGET", "").lower() in {
        "test",
        "remote",
    }


@dataclass(slots=True)
class GuildSettings:
    guild_id: int
    admin_channel_id: int | None = None
    attendance_voice_channel_id: int | None = None
    log_channel_id: int | None = None
    timer: int | None = None
    attendance_available_timer: int | None = None


@dataclass(slots=True)
class Alliance:
    alliance_id: int
    alliance_name: str


def _database_url() -> str:
    test_mode = is_test_database_mode()
    url = os.getenv("DATABASE_URL") if test_mode else os.getenv("LOCAL_DATABASE_URL")
    if url:
        return url

    host = os.getenv("PGHOST") if test_mode else os.getenv("PGLOCALHOST", "127.0.0.1")
    database = (
        os.getenv("PGDATABASE")
        if test_mode
        else os.getenv("PGLOCALDATABASE", os.getenv("PGDATABASE"))
    )
    user = os.getenv("PGUSER") if test_mode else os.getenv("PGLOCALUSER", os.getenv("PGUSER"))
    password = (
        os.getenv("PGPASSWORD")
        if test_mode
        else os.getenv("PGLOCALPASSWORD", os.getenv("PGPASSWORD"))
    )
    port = os.getenv("PGPORT", "5432") if test_mode else os.getenv("PGLOCALPORT", "5432")
    if all((host, database, user, password)):
        encoded_user = quote(str(user), safe="")
        encoded_password = quote(str(password), safe="")
        encoded_database = quote(str(database), safe="")
        return f"postgresql://{encoded_user}:{encoded_password}@{host}:{port}/{encoded_database}"

    raise RuntimeError(
        "PostgreSQL 접속 정보가 없습니다. 기본 실행은 로컬 DB(PGLOCAL* 또는 PG*)를 사용하고, --test 실행은 .env의 PGHOST/PGDATABASE/PGUSER/PGPASSWORD를 사용합니다."
    )


def _connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        _database_url(),
        connect_timeout=10,
        cursor_factory=RealDictCursor,
    )


def _fetchone(sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            row = cursor.fetchone()
    return dict(row) if row is not None else None


def _fetchall(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
    return [dict(row) for row in rows]


def init_db() -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(SCHEMA_SQL)
            _ensure_postgres_columns(cursor)
            _drop_redundant_timestamp_columns(cursor)
            _seed_default_alliances(cursor)
        connection.commit()


def ensure_guild(guild_id: int) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO guilds (guild_id)
                VALUES (%s)
                ON CONFLICT (guild_id) DO NOTHING
                """,
                (guild_id,),
            )
            cursor.execute(
                """
                INSERT INTO guild_settings (guild_id)
                VALUES (%s)
                ON CONFLICT (guild_id) DO NOTHING
                """,
                (guild_id,),
            )
        connection.commit()


def get_configured_guild_id() -> int | None:
    row = _fetchone("SELECT guild_id FROM guilds ORDER BY guild_id LIMIT 1")
    return int(row["guild_id"]) if row else None


def get_settings(guild_id: int) -> GuildSettings:
    ensure_guild(guild_id)
    row = _fetchone(
        """
        SELECT guild_id, admin_channel_id, attendance_voice_channel_id, log_channel_id,
               timer, attendance_available_timer
        FROM guild_settings
        WHERE guild_id = %s
        """,
        (guild_id,),
    )
    if row is None:
        return GuildSettings(guild_id=guild_id)
    return GuildSettings(
        guild_id=int(row["guild_id"]),
        admin_channel_id=_optional_int(row["admin_channel_id"]),
        attendance_voice_channel_id=_optional_int(row["attendance_voice_channel_id"]),
        log_channel_id=_optional_int(row["log_channel_id"]),
        timer=_optional_int(row["timer"]),
        attendance_available_timer=_optional_int(row["attendance_available_timer"]),
    )


def update_setting(guild_id: int, column: str, value: int | None) -> GuildSettings:
    allowed_columns = {
        "admin_channel_id",
        "attendance_voice_channel_id",
        "log_channel_id",
        "timer",
        "attendance_available_timer",
    }
    if column not in allowed_columns:
        raise ValueError(f"Unsupported settings column: {column}")

    ensure_guild(guild_id)
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"UPDATE guild_settings SET {column} = %s, updated_at = CURRENT_TIMESTAMP WHERE guild_id = %s",
                (value, guild_id),
            )
        connection.commit()
    return get_settings(guild_id)


def save_attendance_session(
    guild_id: int,
    started_at: str,
    ended_at: str,
    started_by_discord_id: int | None,
    participants: list[dict[str, Any]],
) -> int:
    ensure_guild(guild_id)
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO attendance_sessions (
                    guild_id,
                    started_at,
                    ended_at,
                    started_by_discord_id
                )
                VALUES (%s, %s, %s, %s)
                RETURNING attendance_id
                """,
                (guild_id, started_at, ended_at, started_by_discord_id),
            )
            attendance_id = int(cursor.fetchone()["attendance_id"])

            for participant in participants:
                discord_id = int(participant["discord_id"])
                nickname = str(participant["discord_nickname"])
                alliance_id = _optional_int(participant.get("alliance_id"))

                cursor.execute(
                    """
                    SELECT user_id, alliance_id, discord_nickname
                    FROM users
                    WHERE discord_id = %s
                    """,
                    (discord_id,),
                )
                existing_user = cursor.fetchone()

                if existing_user is None:
                    cursor.execute(
                        """
                        INSERT INTO users (alliance_id, discord_id, discord_nickname)
                        VALUES (%s, %s, %s)
                        RETURNING user_id
                        """,
                        (alliance_id, discord_id, nickname),
                    )
                    user_id = int(cursor.fetchone()["user_id"])
                else:
                    user_id = int(existing_user["user_id"])
                    resolved_alliance_id = (
                        alliance_id
                        if alliance_id is not None
                        else _optional_int(existing_user["alliance_id"])
                    )
                    if (
                        _optional_int(existing_user["alliance_id"]) != resolved_alliance_id
                        or str(existing_user["discord_nickname"]) != nickname
                    ):
                        cursor.execute(
                            """
                            UPDATE users
                            SET alliance_id = %s,
                                discord_nickname = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE user_id = %s
                            """,
                            (resolved_alliance_id, nickname, user_id),
                        )

                cursor.execute(
                    """
                    INSERT INTO attendance_entries (attendance_id, user_id)
                    VALUES (%s, %s)
                    ON CONFLICT (attendance_id, user_id) DO NOTHING
                    """,
                    (attendance_id, user_id),
                )
        connection.commit()
    return attendance_id


def create_alliance(alliance_name: str) -> Alliance:
    normalized_name = alliance_name.strip()
    if not normalized_name:
        raise ValueError("Alliance name must not be empty.")

    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO alliances (alliance_name, display_name, tag_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (alliance_name) DO UPDATE SET
                    display_name = COALESCE(alliances.display_name, EXCLUDED.display_name),
                    tag_name = COALESCE(alliances.tag_name, EXCLUDED.tag_name),
                    updated_at = CURRENT_TIMESTAMP
                RETURNING alliance_id, alliance_name
                """,
                (normalized_name, normalized_name, normalized_name),
            )
            row = cursor.fetchone()
        connection.commit()
    return Alliance(alliance_id=int(row["alliance_id"]), alliance_name=str(row["alliance_name"]))


def get_or_create_alliance(alliance_name: str) -> Alliance:
    return create_alliance(alliance_name)


def get_alliance_names() -> list[str]:
    rows = _fetchall(
        """
        SELECT alliance_name
        FROM alliances
        WHERE is_active = TRUE
        ORDER BY sort_order ASC NULLS LAST, alliance_name ASC
        """
    )
    return [str(row["alliance_name"]) for row in rows]


def get_alliance_counts_for_discord_ids(discord_ids: list[int]) -> dict[str, int]:
    if not discord_ids:
        return {}

    rows = _fetchall(
        """
        SELECT
            COALESCE(a.alliance_name, '미분류') AS alliance_name,
            COUNT(*) AS member_count
        FROM users u
        LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
        WHERE u.discord_id = ANY(%s)
        GROUP BY COALESCE(a.alliance_name, '미분류')
        ORDER BY member_count DESC, alliance_name
        """,
        (list({int(discord_id) for discord_id in discord_ids}),),
    )
    counts = {str(row["alliance_name"]): int(row["member_count"]) for row in rows}
    missing = len(set(discord_ids)) - sum(counts.values())
    if missing > 0:
        counts["미분류"] = counts.get("미분류", 0) + missing
    return counts


def get_attendance_overview(
    guild_id: int | None,
    start_at: str | None = None,
    end_at: str | None = None,
) -> dict[str, int | None]:
    where_clause, params = _build_attendance_filter(guild_id, start_at, end_at)
    row = _fetchone(
        f"""
        SELECT
            COUNT(DISTINCT s.attendance_id) AS session_count,
            COUNT(e.user_id) AS total_attendance_count,
            COUNT(DISTINCT e.user_id) AS unique_user_count
        FROM attendance_sessions s
        LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
        {where_clause}
        """,
        tuple(params),
    )
    average_row = _fetchone(
        f"""
        SELECT AVG(session_size) AS average_attendance_count
        FROM (
            SELECT COUNT(e.user_id) AS session_size
            FROM attendance_sessions s
            LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
            {where_clause}
            GROUP BY s.attendance_id
        ) grouped_sessions
        """,
        tuple(params),
    )
    return {
        "session_count": int(row["session_count"]) if row and row["session_count"] is not None else 0,
        "total_attendance_count": int(row["total_attendance_count"]) if row and row["total_attendance_count"] is not None else 0,
        "unique_user_count": int(row["unique_user_count"]) if row and row["unique_user_count"] is not None else 0,
        "average_attendance_count": (
            int(round(float(average_row["average_attendance_count"])))
            if average_row and average_row["average_attendance_count"] is not None
            else 0
        ),
    }


def get_daily_attendance_stats(
    guild_id: int | None,
    start_at: str | None = None,
    end_at: str | None = None,
) -> list[dict[str, Any]]:
    where_clause, params = _build_attendance_filter(guild_id, start_at, end_at)
    rows = _fetchall(
        f"""
        SELECT
            LEFT(s.started_at, 10) AS attendance_date,
            COUNT(DISTINCT s.attendance_id) AS session_count,
            COUNT(e.user_id) AS attendance_count,
            COUNT(DISTINCT e.user_id) AS unique_user_count
        FROM attendance_sessions s
        LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
        {where_clause}
        GROUP BY LEFT(s.started_at, 10)
        ORDER BY attendance_date DESC
        """,
        tuple(params),
    )
    return [
        {
            "attendance_date": str(row["attendance_date"]),
            "session_count": int(row["session_count"]),
            "attendance_count": int(row["attendance_count"]),
            "unique_user_count": int(row["unique_user_count"]),
        }
        for row in rows
    ]


def get_alliance_attendance_stats(
    guild_id: int | None,
    start_at: str | None = None,
    end_at: str | None = None,
    search: str | None = None,
) -> list[dict[str, Any]]:
    where_clause, params = _build_attendance_filter(guild_id, start_at, end_at)
    search_clause = ""
    if search:
        search_clause = "HAVING COALESCE(a.alliance_name, '미분류') ILIKE %s"
        params.append(f"%{search.strip()}%")
    rows = _fetchall(
        f"""
        SELECT
            COALESCE(a.alliance_name, '미분류') AS alliance_name,
            COUNT(e.user_id) AS attendance_count,
            COUNT(DISTINCT e.user_id) AS unique_user_count,
            COUNT(DISTINCT s.attendance_id) AS session_count
        FROM attendance_sessions s
        LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
        LEFT JOIN users u ON u.user_id = e.user_id
        LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
        {where_clause}
        GROUP BY COALESCE(a.alliance_name, '미분류')
        {search_clause}
        ORDER BY attendance_count DESC, alliance_name ASC
        """,
        tuple(params),
    )
    return [
        {
            "alliance_name": str(row["alliance_name"]),
            "attendance_count": int(row["attendance_count"]),
            "unique_user_count": int(row["unique_user_count"]),
            "session_count": int(row["session_count"]),
        }
        for row in rows
    ]


def get_user_attendance_stats(
    guild_id: int | None,
    start_at: str | None = None,
    end_at: str | None = None,
    search: str | None = None,
    alliance_name: str | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    where_clause, params = _build_attendance_filter(guild_id, start_at, end_at)
    search_clause = ""
    if search:
        search_clause = """
        AND (
            u.discord_nickname ILIKE %s
            OR CAST(u.discord_id AS TEXT) ILIKE %s
            OR COALESCE(a.alliance_name, '미분류') ILIKE %s
        )
        """
        wildcard = f"%{search.strip()}%"
        params.extend([wildcard, wildcard, wildcard])
    if alliance_name:
        search_clause += " AND COALESCE(a.alliance_name, '미분류') = %s"
        params.append(alliance_name)

    params.append(int(limit))
    rows = _fetchall(
        f"""
        SELECT
            u.user_id,
            u.discord_id,
            u.discord_nickname,
            COALESCE(a.alliance_name, '미분류') AS alliance_name,
            COUNT(e.attendance_id) AS attendance_count
        FROM attendance_sessions s
        INNER JOIN attendance_entries e ON e.attendance_id = s.attendance_id
        INNER JOIN users u ON u.user_id = e.user_id
        LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
        {where_clause}
        {search_clause}
        GROUP BY u.user_id, u.discord_id, u.discord_nickname, COALESCE(a.alliance_name, '미분류')
        ORDER BY attendance_count DESC, u.discord_nickname ASC
        LIMIT %s
        """,
        tuple(params),
    )
    return [
        {
            "user_id": int(row["user_id"]),
            "discord_id": int(row["discord_id"]),
            "discord_nickname": str(row["discord_nickname"]),
            "alliance_name": str(row["alliance_name"]),
            "attendance_count": int(row["attendance_count"]),
        }
        for row in rows
    ]


def get_attendance_export_rows(
    guild_id: int | None,
    start_at: str | None = None,
    end_at: str | None = None,
    search: str | None = None,
    alliance_name: str | None = None,
) -> list[dict[str, Any]]:
    where_clause, params = _build_attendance_filter(guild_id, start_at, end_at)
    search_clause = ""
    if search:
        search_clause = """
        AND (
            COALESCE(a.alliance_name, '미분류') ILIKE %s
            OR u.discord_nickname ILIKE %s
            OR CAST(u.discord_id AS TEXT) ILIKE %s
        )
        """
        wildcard = f"%{search.strip()}%"
        params.extend([wildcard, wildcard, wildcard])
    if alliance_name:
        search_clause += " AND COALESCE(a.alliance_name, '미분류') = %s"
        params.append(alliance_name)
    rows = _fetchall(
        f"""
        SELECT
            s.started_at,
            u.discord_id,
            u.discord_nickname,
            COALESCE(a.alliance_name, '미분류') AS alliance_name
        FROM attendance_sessions s
        LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
        LEFT JOIN users u ON u.user_id = e.user_id
        LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
        {where_clause}
        {search_clause}
        ORDER BY s.started_at DESC, u.discord_nickname ASC
        """,
        tuple(params),
    )
    return [
        {
            "started_at": str(row["started_at"]),
            "discord_id": _optional_int(row["discord_id"]),
            "discord_nickname": str(row["discord_nickname"]) if row["discord_nickname"] is not None else "",
            "alliance_name": str(row["alliance_name"]),
        }
        for row in rows
    ]


def _build_attendance_filter(
    guild_id: int | None,
    start_at: str | None,
    end_at: str | None,
) -> tuple[str, list[Any]]:
    clauses = ["WHERE 1 = 1"]
    params: list[Any] = []
    if guild_id is not None:
        clauses.append("AND s.guild_id = %s")
        params.append(guild_id)
    if start_at:
        clauses.append("AND s.started_at >= %s")
        params.append(start_at)
    if end_at:
        clauses.append("AND s.started_at <= %s")
        params.append(end_at)
    return " ".join(clauses), params


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _ensure_postgres_columns(cursor: psycopg2.extensions.cursor) -> None:
    column_sql = [
        "ALTER TABLE alliances ADD COLUMN IF NOT EXISTS display_name TEXT",
        "ALTER TABLE alliances ADD COLUMN IF NOT EXISTS tag_name TEXT",
        "ALTER TABLE alliances ADD COLUMN IF NOT EXISTS color TEXT",
        "ALTER TABLE alliances ADD COLUMN IF NOT EXISTS sort_order INTEGER",
        "ALTER TABLE alliances ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE",
        "ALTER TABLE alliances ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS game_nickname TEXT",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS class_name TEXT",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS attribute_name TEXT",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS position_name TEXT",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS phone TEXT",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS memo TEXT",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP",
    ]
    for sql in column_sql:
        cursor.execute(sql)


def _drop_redundant_timestamp_columns(cursor: psycopg2.extensions.cursor) -> None:
    redundant_columns = {
        "guilds": ("created_at",),
        "alliances": ("created_at",),
        "users": ("created_at",),
        "attendance_sessions": ("created_at",),
        "attendance_entries": ("created_at",),
        "attendance_live_sessions": ("created_at", "updated_at"),
        "attendance_live_participants": ("created_at",),
        "bosses": ("created_at",),
        "boss_spawn_schedules": ("created_at",),
        "boss_hunt_sessions": ("created_at",),
        "boss_hunt_participants": ("created_at",),
        "boss_attendance_snapshots": ("created_at",),
        "items": ("created_at",),
        "loot_events": ("created_at",),
        "member_payout_groups": ("created_at",),
    }
    for table_name, column_names in redundant_columns.items():
        for column_name in column_names:
            cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column_name}")


def _seed_default_alliances(cursor: psycopg2.extensions.cursor) -> None:
    for index, alliance_name in enumerate(DEFAULT_ALLIANCE_NAMES, start=1):
        cursor.execute(
            """
            INSERT INTO alliances (alliance_name, display_name, tag_name, sort_order, is_active)
            VALUES (%s, %s, %s, %s, TRUE)
            ON CONFLICT (alliance_name) DO UPDATE SET
                display_name = COALESCE(alliances.display_name, EXCLUDED.display_name),
                tag_name = COALESCE(alliances.tag_name, EXCLUDED.tag_name),
                sort_order = COALESCE(alliances.sort_order, EXCLUDED.sort_order),
                is_active = TRUE,
                updated_at = CURRENT_TIMESTAMP
            """,
            (alliance_name, alliance_name, alliance_name, index),
        )


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS guilds (
    guild_id BIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS guild_settings (
    guild_id BIGINT PRIMARY KEY REFERENCES guilds(guild_id) ON DELETE CASCADE,
    admin_channel_id BIGINT,
    attendance_voice_channel_id BIGINT,
    log_channel_id BIGINT,
    timer INTEGER,
    attendance_available_timer INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS alliances (
    alliance_id BIGSERIAL PRIMARY KEY,
    alliance_name TEXT NOT NULL UNIQUE,
    display_name TEXT,
    tag_name TEXT,
    color TEXT,
    sort_order INTEGER,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users (
    user_id BIGSERIAL PRIMARY KEY,
    alliance_id BIGINT REFERENCES alliances(alliance_id),
    discord_id BIGINT NOT NULL UNIQUE,
    discord_nickname TEXT NOT NULL,
    game_nickname TEXT,
    class_name TEXT,
    attribute_name TEXT,
    position_name TEXT,
    phone TEXT,
    memo TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS attendance_sessions (
    attendance_id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
    started_at TEXT NOT NULL,
    ended_at TEXT NOT NULL,
    started_by_discord_id BIGINT
);

CREATE TABLE IF NOT EXISTS attendance_entries (
    attendance_id BIGINT NOT NULL REFERENCES attendance_sessions(attendance_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    PRIMARY KEY (attendance_id, user_id)
);

CREATE TABLE IF NOT EXISTS web_admins (
    admin_id BIGSERIAL PRIMARY KEY,
    discord_id BIGINT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'admin',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bot_command_queue (
    command_id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT REFERENCES guilds(guild_id) ON DELETE CASCADE,
    command_type TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending',
    result_json JSONB,
    requested_by_discord_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS attendance_live_sessions (
    live_session_id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
    discord_channel_id BIGINT,
    discord_message_id BIGINT,
    started_by_discord_id BIGINT,
    started_at TEXT NOT NULL,
    expires_at TEXT,
    ended_at TEXT,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS attendance_live_participants (
    live_session_id BIGINT NOT NULL REFERENCES attendance_live_sessions(live_session_id) ON DELETE CASCADE,
    discord_id BIGINT NOT NULL,
    display_name TEXT NOT NULL,
    alliance_id BIGINT REFERENCES alliances(alliance_id),
    joined_voice_at TEXT,
    attended_at TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'discord',
    PRIMARY KEY (live_session_id, discord_id)
);

CREATE TABLE IF NOT EXISTS bosses (
    boss_id BIGSERIAL PRIMARY KEY,
    boss_name TEXT NOT NULL UNIQUE,
    alias TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS boss_spawn_schedules (
    schedule_id BIGSERIAL PRIMARY KEY,
    boss_id BIGINT NOT NULL REFERENCES bosses(boss_id) ON DELETE CASCADE,
    time_label TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    memo TEXT
);

CREATE TABLE IF NOT EXISTS boss_hunt_sessions (
    hunt_id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT REFERENCES guilds(guild_id) ON DELETE CASCADE,
    boss_id BIGINT REFERENCES bosses(boss_id),
    hunt_at TEXT NOT NULL,
    title TEXT,
    source TEXT NOT NULL DEFAULT 'web',
    memo TEXT
);

CREATE TABLE IF NOT EXISTS boss_hunt_participants (
    hunt_id BIGINT NOT NULL REFERENCES boss_hunt_sessions(hunt_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    discord_id BIGINT,
    alliance_id BIGINT REFERENCES alliances(alliance_id),
    attended_at TEXT,
    PRIMARY KEY (hunt_id, user_id)
);

CREATE TABLE IF NOT EXISTS boss_attendance_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    total_hunts INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS boss_attendance_snapshot_rows (
    snapshot_id BIGINT NOT NULL REFERENCES boss_attendance_snapshots(snapshot_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    class_name TEXT,
    attendance_count INTEGER NOT NULL DEFAULT 0,
    attendance_rate NUMERIC(10, 6) NOT NULL DEFAULT 0,
    rank_overall INTEGER,
    rank_by_class INTEGER,
    PRIMARY KEY (snapshot_id, user_id)
);

CREATE TABLE IF NOT EXISTS items (
    item_id BIGSERIAL PRIMARY KEY,
    item_name TEXT NOT NULL UNIQUE,
    category TEXT,
    default_price NUMERIC(18, 2),
    is_bid_item BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    sort_order INTEGER,
    memo TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS item_categories (
    category_id BIGSERIAL PRIMARY KEY,
    category_name TEXT NOT NULL UNIQUE,
    sort_order INTEGER,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS item_price_rules (
    rule_id BIGSERIAL PRIMARY KEY,
    item_id BIGINT NOT NULL REFERENCES items(item_id) ON DELETE CASCADE,
    price NUMERIC(18, 2) NOT NULL,
    starts_at TEXT,
    ends_at TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    memo TEXT
);

CREATE TABLE IF NOT EXISTS item_bid_rules (
    rule_id BIGSERIAL PRIMARY KEY,
    item_id BIGINT NOT NULL REFERENCES items(item_id) ON DELETE CASCADE,
    rule_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    memo TEXT
);

CREATE TABLE IF NOT EXISTS alliance_item_bid_statuses (
    alliance_id BIGINT NOT NULL REFERENCES alliances(alliance_id) ON DELETE CASCADE,
    item_id BIGINT NOT NULL REFERENCES items(item_id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'available',
    completed_at TEXT,
    memo TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (alliance_id, item_id)
);

CREATE TABLE IF NOT EXISTS loot_events (
    loot_event_id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT REFERENCES guilds(guild_id) ON DELETE CASCADE,
    event_date TEXT NOT NULL,
    event_time_label TEXT,
    title TEXT,
    memo TEXT,
    created_by_discord_id BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS loot_event_items (
    loot_item_id BIGSERIAL PRIMARY KEY,
    loot_event_id BIGINT NOT NULL REFERENCES loot_events(loot_event_id) ON DELETE CASCADE,
    item_id BIGINT REFERENCES items(item_id),
    item_name_snapshot TEXT NOT NULL,
    buyer_name TEXT,
    buyer_alliance_id BIGINT REFERENCES alliances(alliance_id),
    sale_price NUMERIC(18, 2) NOT NULL DEFAULT 0,
    fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0,
    net_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    memo TEXT
);

CREATE TABLE IF NOT EXISTS loot_event_alliance_counts (
    loot_event_id BIGINT NOT NULL REFERENCES loot_events(loot_event_id) ON DELETE CASCADE,
    alliance_id BIGINT NOT NULL REFERENCES alliances(alliance_id) ON DELETE CASCADE,
    participant_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (loot_event_id, alliance_id)
);

CREATE TABLE IF NOT EXISTS loot_event_participants (
    loot_event_id BIGINT NOT NULL REFERENCES loot_events(loot_event_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    alliance_id BIGINT REFERENCES alliances(alliance_id),
    attended_at TEXT,
    source TEXT NOT NULL DEFAULT 'web',
    PRIMARY KEY (loot_event_id, user_id)
);

CREATE TABLE IF NOT EXISTS distribution_batches (
    distribution_id BIGSERIAL PRIMARY KEY,
    loot_event_id BIGINT REFERENCES loot_events(loot_event_id) ON DELETE SET NULL,
    total_sale_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    total_net_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    total_participant_count INTEGER NOT NULL DEFAULT 0,
    fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0,
    fee_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'draft',
    closed_at TEXT
);

CREATE TABLE IF NOT EXISTS distribution_lines (
    line_id BIGSERIAL PRIMARY KEY,
    distribution_id BIGINT NOT NULL REFERENCES distribution_batches(distribution_id) ON DELETE CASCADE,
    loot_item_id BIGINT REFERENCES loot_event_items(loot_item_id),
    line_type TEXT NOT NULL DEFAULT 'item',
    amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    memo TEXT
);

CREATE TABLE IF NOT EXISTS distribution_alliance_payouts (
    distribution_id BIGINT NOT NULL REFERENCES distribution_batches(distribution_id) ON DELETE CASCADE,
    alliance_id BIGINT NOT NULL REFERENCES alliances(alliance_id),
    participant_count INTEGER NOT NULL DEFAULT 0,
    gross_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    net_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    payout_status TEXT NOT NULL DEFAULT 'unpaid',
    payout_method TEXT,
    memo TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (distribution_id, alliance_id)
);

CREATE TABLE IF NOT EXISTS member_payout_groups (
    payout_group_id BIGSERIAL PRIMARY KEY,
    distribution_id BIGINT REFERENCES distribution_batches(distribution_id) ON DELETE SET NULL,
    alliance_id BIGINT REFERENCES alliances(alliance_id),
    title TEXT NOT NULL,
    total_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0,
    fee_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    per_member_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'draft'
);

CREATE TABLE IF NOT EXISTS member_payout_items (
    payout_item_id BIGSERIAL PRIMARY KEY,
    payout_group_id BIGINT NOT NULL REFERENCES member_payout_groups(payout_group_id) ON DELETE CASCADE,
    item_id BIGINT REFERENCES items(item_id),
    item_name_snapshot TEXT,
    amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    memo TEXT
);

CREATE TABLE IF NOT EXISTS member_payout_recipients (
    payout_group_id BIGINT NOT NULL REFERENCES member_payout_groups(payout_group_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    display_name_snapshot TEXT NOT NULL,
    share_weight NUMERIC(10, 4) NOT NULL DEFAULT 1,
    payout_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    payout_status TEXT NOT NULL DEFAULT 'unpaid',
    paid_at TEXT,
    memo TEXT,
    PRIMARY KEY (payout_group_id, user_id)
);

CREATE TABLE IF NOT EXISTS payout_transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    payout_group_id BIGINT REFERENCES member_payout_groups(payout_group_id) ON DELETE SET NULL,
    user_id BIGINT REFERENCES users(user_id),
    alliance_id BIGINT REFERENCES alliances(alliance_id),
    amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    transaction_type TEXT NOT NULL DEFAULT 'payout',
    status TEXT NOT NULL DEFAULT 'pending',
    processed_at TEXT,
    memo TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS websocket_events (
    event_id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS notifications (
    notification_id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT REFERENCES guilds(guild_id) ON DELETE CASCADE,
    target_type TEXT,
    target_id BIGINT,
    channel TEXT NOT NULL DEFAULT 'discord',
    message TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at TEXT
);

CREATE TABLE IF NOT EXISTS discord_message_links (
    link_id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT REFERENCES guilds(guild_id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    channel_id BIGINT,
    message_id BIGINT,
    message_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_guild_settings_admin_channel ON guild_settings(admin_channel_id);
CREATE INDEX IF NOT EXISTS idx_users_discord_id ON users(discord_id);
CREATE INDEX IF NOT EXISTS idx_users_alliance_id ON users(alliance_id);
CREATE INDEX IF NOT EXISTS idx_attendance_sessions_guild_started ON attendance_sessions(guild_id, started_at);
CREATE INDEX IF NOT EXISTS idx_attendance_entries_user_id ON attendance_entries(user_id);
CREATE INDEX IF NOT EXISTS idx_attendance_entries_attendance_id ON attendance_entries(attendance_id);
CREATE INDEX IF NOT EXISTS idx_bot_command_queue_status ON bot_command_queue(status, created_at);
CREATE INDEX IF NOT EXISTS idx_attendance_live_sessions_guild_status ON attendance_live_sessions(guild_id, status);
CREATE INDEX IF NOT EXISTS idx_items_name ON items(item_name);
CREATE INDEX IF NOT EXISTS idx_loot_events_date ON loot_events(event_date);
"""
