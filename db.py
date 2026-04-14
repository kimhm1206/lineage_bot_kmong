from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "lineage_bot.sqlite3"
LEGACY_DB_PATHS = (
    DATA_DIR / "settings.sqlite3",
    BASE_DIR / "settings.db",
)


@dataclass(slots=True)
class GuildSettings:
    guild_id: int
    admin_channel_id: int | None = None
    attendance_voice_channel_id: int | None = None
    timer: int | None = None


@dataclass(slots=True)
class Alliance:
    alliance_id: int
    alliance_name: str


def _connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH, timeout=30.0)
    connection.row_factory = sqlite3.Row
    for pragma in (
        "PRAGMA busy_timeout=30000",
        "PRAGMA journal_mode=WAL",
        "PRAGMA synchronous=NORMAL",
        "PRAGMA foreign_keys=ON",
    ):
        try:
            connection.execute(pragma)
        except sqlite3.OperationalError:
            continue
    return connection


def init_db() -> None:
    with _connect() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS guilds (
                guild_id INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                admin_channel_id INTEGER,
                attendance_voice_channel_id INTEGER,
                timer INTEGER,
                FOREIGN KEY (guild_id) REFERENCES guilds(guild_id) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alliances (
                alliance_id INTEGER PRIMARY KEY AUTOINCREMENT,
                alliance_name TEXT NOT NULL UNIQUE
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                alliance_id INTEGER,
                discord_id INTEGER NOT NULL UNIQUE,
                discord_nickname TEXT NOT NULL,
                FOREIGN KEY (alliance_id) REFERENCES alliances(alliance_id)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS attendance_sessions (
                attendance_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT NOT NULL,
                started_by_discord_id INTEGER,
                FOREIGN KEY (guild_id) REFERENCES guilds(guild_id) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS attendance_entries (
                attendance_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                alliance_id INTEGER,
                PRIMARY KEY (attendance_id, user_id),
                FOREIGN KEY (attendance_id) REFERENCES attendance_sessions(attendance_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (alliance_id) REFERENCES alliances(alliance_id)
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_guild_settings_admin_channel
            ON guild_settings(admin_channel_id)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_users_discord_id
            ON users(discord_id)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_users_alliance_id
            ON users(alliance_id)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_attendance_sessions_guild_started
            ON attendance_sessions(guild_id, started_at)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_attendance_entries_user_id
            ON attendance_entries(user_id)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_attendance_entries_alliance_id
            ON attendance_entries(alliance_id)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_attendance_entries_attendance_id
            ON attendance_entries(attendance_id)
            """
        )
        _migrate_guild_settings_schema(connection)
        connection.commit()
    _migrate_legacy_settings()


def ensure_guild(guild_id: int) -> None:
    with _connect() as connection:
        connection.execute(
            """
            INSERT INTO guilds (guild_id)
            VALUES (?)
            ON CONFLICT(guild_id) DO NOTHING
            """,
            (guild_id,),
        )
        connection.execute(
            """
            INSERT INTO guild_settings (guild_id)
            VALUES (?)
            ON CONFLICT(guild_id) DO NOTHING
            """,
            (guild_id,),
        )
        connection.commit()


def get_configured_guild_id() -> int | None:
    with _connect() as connection:
        row = connection.execute(
            "SELECT guild_id FROM guilds ORDER BY guild_id LIMIT 1"
        ).fetchone()
    return int(row["guild_id"]) if row else None


def get_settings(guild_id: int) -> GuildSettings:
    ensure_guild(guild_id)
    with _connect() as connection:
        row = connection.execute(
            """
            SELECT guild_id, admin_channel_id, attendance_voice_channel_id, timer
            FROM guild_settings
            WHERE guild_id = ?
            """,
            (guild_id,),
        ).fetchone()

    if row is None:
        return GuildSettings(guild_id=guild_id)

    return GuildSettings(
        guild_id=int(row["guild_id"]),
        admin_channel_id=_optional_int(row["admin_channel_id"]),
        attendance_voice_channel_id=_optional_int(row["attendance_voice_channel_id"]),
        timer=_optional_int(row["timer"]),
    )


def update_setting(guild_id: int, column: str, value: int | None) -> GuildSettings:
    allowed_columns = {
        "admin_channel_id",
        "attendance_voice_channel_id",
        "timer",
    }
    if column not in allowed_columns:
        raise ValueError(f"Unsupported settings column: {column}")

    ensure_guild(guild_id)
    with _connect() as connection:
        connection.execute(
            f"UPDATE guild_settings SET {column} = ? WHERE guild_id = ?",
            (value, guild_id),
        )
        connection.commit()

    return get_settings(guild_id)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def save_attendance_session(
    guild_id: int,
    started_at: str,
    ended_at: str,
    started_by_discord_id: int | None,
    participants: list[dict[str, Any]],
) -> int:
    ensure_guild(guild_id)
    with _connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO attendance_sessions (
                guild_id,
                started_at,
                ended_at,
                started_by_discord_id
            )
            VALUES (?, ?, ?, ?)
            """,
            (guild_id, started_at, ended_at, started_by_discord_id),
        )
        attendance_id = int(cursor.lastrowid)

        for participant in participants:
            discord_id = int(participant["discord_id"])
            nickname = str(participant["discord_nickname"])
            alliance_id = _optional_int(participant.get("alliance_id"))

            existing_user = connection.execute(
                """
                SELECT user_id, alliance_id, discord_nickname
                FROM users
                WHERE discord_id = ?
                """,
                (discord_id,),
            ).fetchone()

            if existing_user is None:
                user_cursor = connection.execute(
                    """
                    INSERT INTO users (alliance_id, discord_id, discord_nickname)
                    VALUES (?, ?, ?)
                    """,
                    (alliance_id, discord_id, nickname),
                )
                user_id = int(user_cursor.lastrowid)
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
                    connection.execute(
                        """
                        UPDATE users
                        SET alliance_id = ?, discord_nickname = ?
                        WHERE user_id = ?
                        """,
                        (resolved_alliance_id, nickname, user_id),
                    )
                alliance_id = resolved_alliance_id

            connection.execute(
                """
                INSERT INTO attendance_entries (attendance_id, user_id, alliance_id)
                VALUES (?, ?, ?)
                """,
                (attendance_id, user_id, alliance_id),
            )

        connection.commit()

    return attendance_id


def create_alliance(alliance_name: str) -> Alliance:
    normalized_name = alliance_name.strip()
    if not normalized_name:
        raise ValueError("Alliance name must not be empty.")

    with _connect() as connection:
        try:
            cursor = connection.execute(
                """
                INSERT INTO alliances (alliance_name)
                VALUES (?)
                """,
                (normalized_name,),
            )
            connection.commit()
            return Alliance(
                alliance_id=int(cursor.lastrowid),
                alliance_name=normalized_name,
            )
        except sqlite3.IntegrityError:
            existing = connection.execute(
                """
                SELECT alliance_id, alliance_name
                FROM alliances
                WHERE alliance_name = ?
                """,
                (normalized_name,),
            ).fetchone()
            if existing is None:
                raise
            return Alliance(
                alliance_id=int(existing["alliance_id"]),
                alliance_name=str(existing["alliance_name"]),
            )


def get_or_create_alliance(alliance_name: str) -> Alliance:
    return create_alliance(alliance_name)


def get_alliance_names() -> list[str]:
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT alliance_name
            FROM alliances
            ORDER BY alliance_name ASC
            """
        ).fetchall()
    return [str(row["alliance_name"]) for row in rows]


def get_alliance_counts_for_discord_ids(discord_ids: list[int]) -> dict[str, int]:
    if not discord_ids:
        return {}

    placeholders = ", ".join("?" for _ in discord_ids)
    with _connect() as connection:
        rows = connection.execute(
            f"""
            SELECT
                COALESCE(a.alliance_name, '미분류') AS alliance_name,
                COUNT(*) AS member_count
            FROM users u
            LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
            WHERE u.discord_id IN ({placeholders})
            GROUP BY COALESCE(a.alliance_name, '미분류')
            ORDER BY member_count DESC, alliance_name
            """,
            tuple(discord_ids),
        ).fetchall()

    counts = {str(row["alliance_name"]): int(row["member_count"]) for row in rows}
    missing = len(set(discord_ids)) - sum(counts.values())
    if missing > 0:
        counts["미분류"] = counts.get("미분류", 0) + missing
    return counts


def get_attendance_overview(
    guild_id: int,
    start_at: str | None = None,
    end_at: str | None = None,
) -> dict[str, int | None]:
    where_clause, params = _build_attendance_filter(guild_id, start_at, end_at)

    with _connect() as connection:
        row = connection.execute(
            f"""
            SELECT
                COUNT(DISTINCT s.attendance_id) AS session_count,
                COUNT(e.user_id) AS total_attendance_count,
                COUNT(DISTINCT e.user_id) AS unique_user_count
            FROM attendance_sessions s
            LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
            {where_clause}
            """,
            params,
        ).fetchone()
        average_row = connection.execute(
            f"""
            SELECT AVG(session_size) AS average_attendance_count
            FROM (
                SELECT COUNT(e.user_id) AS session_size
                FROM attendance_sessions s
                LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
                {where_clause}
                GROUP BY s.attendance_id
            )
            """,
            params,
        ).fetchone()

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
    guild_id: int,
    start_at: str | None = None,
    end_at: str | None = None,
) -> list[dict[str, Any]]:
    where_clause, params = _build_attendance_filter(guild_id, start_at, end_at)
    with _connect() as connection:
        rows = connection.execute(
            f"""
            SELECT
                substr(s.started_at, 1, 10) AS attendance_date,
                COUNT(DISTINCT s.attendance_id) AS session_count,
                COUNT(e.user_id) AS attendance_count,
                COUNT(DISTINCT e.user_id) AS unique_user_count
            FROM attendance_sessions s
            LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
            {where_clause}
            GROUP BY substr(s.started_at, 1, 10)
            ORDER BY attendance_date DESC
            """,
            params,
        ).fetchall()

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
    guild_id: int,
    start_at: str | None = None,
    end_at: str | None = None,
    search: str | None = None,
) -> list[dict[str, Any]]:
    where_clause, params = _build_attendance_filter(guild_id, start_at, end_at)
    search_clause = ""
    if search:
        search_clause = """
        HAVING COALESCE(a.alliance_name, '미분류') LIKE ?
        """
        params = [*params, f"%{search.strip()}%"]
    with _connect() as connection:
        rows = connection.execute(
            f"""
            SELECT
                COALESCE(a.alliance_name, '미분류') AS alliance_name,
                COUNT(e.user_id) AS attendance_count,
                COUNT(DISTINCT e.user_id) AS unique_user_count,
                COUNT(DISTINCT s.attendance_id) AS session_count
            FROM attendance_sessions s
            LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
            LEFT JOIN alliances a ON a.alliance_id = e.alliance_id
            {where_clause}
            GROUP BY COALESCE(a.alliance_name, '미분류')
            {search_clause}
            ORDER BY attendance_count DESC, alliance_name ASC
            """,
            params,
        ).fetchall()

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
    guild_id: int,
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
            u.discord_nickname LIKE ?
            OR CAST(u.discord_id AS TEXT) LIKE ?
            OR COALESCE(a.alliance_name, '미분류') LIKE ?
        )
        """
        wildcard = f"%{search.strip()}%"
        params = [*params, wildcard, wildcard, wildcard]
    if alliance_name:
        search_clause += """
        AND COALESCE(a.alliance_name, '미분류') = ?
        """
        params = [*params, alliance_name]

    params = [*params, int(limit)]
    with _connect() as connection:
        rows = connection.execute(
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
            LEFT JOIN alliances a ON a.alliance_id = e.alliance_id
            {where_clause}
            {search_clause}
            GROUP BY u.user_id, u.discord_id, u.discord_nickname, COALESCE(a.alliance_name, '미분류')
            ORDER BY attendance_count DESC, u.discord_nickname ASC
            LIMIT ?
            """,
            params,
        ).fetchall()

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
    guild_id: int,
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
            COALESCE(a.alliance_name, '미분류') LIKE ?
            OR u.discord_nickname LIKE ?
            OR CAST(u.discord_id AS TEXT) LIKE ?
        )
        """
        wildcard = f"%{search.strip()}%"
        params = [*params, wildcard, wildcard, wildcard]
    if alliance_name:
        search_clause += """
        AND COALESCE(a.alliance_name, '미분류') = ?
        """
        params = [*params, alliance_name]
    with _connect() as connection:
        rows = connection.execute(
            f"""
            SELECT
                s.started_at,
                u.discord_id,
                u.discord_nickname,
                COALESCE(a.alliance_name, '미분류') AS alliance_name
            FROM attendance_sessions s
            LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
            LEFT JOIN users u ON u.user_id = e.user_id
            LEFT JOIN alliances a ON a.alliance_id = e.alliance_id
            {where_clause}
            {search_clause}
            ORDER BY s.started_at DESC, u.discord_nickname ASC
            """,
            params,
        ).fetchall()

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
    guild_id: int,
    start_at: str | None,
    end_at: str | None,
) -> tuple[str, list[Any]]:
    clauses = ["WHERE s.guild_id = ?"]
    params: list[Any] = [guild_id]
    if start_at:
        clauses.append("AND s.started_at >= ?")
        params.append(start_at)
    if end_at:
        clauses.append("AND s.started_at <= ?")
        params.append(end_at)
    return " ".join(clauses), params


def _migrate_legacy_settings() -> None:
    if DB_PATH.exists():
        with _connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM guild_settings"
            ).fetchone()
        if row is not None and int(row["count"]) > 0:
            return

    for legacy_path in LEGACY_DB_PATHS:
        if not legacy_path.exists() or legacy_path == DB_PATH:
            continue

        legacy_connection = sqlite3.connect(legacy_path)
        legacy_connection.row_factory = sqlite3.Row
        try:
            has_settings_table = legacy_connection.execute(
                """
                SELECT 1
                FROM sqlite_master
                WHERE type = 'table' AND name = 'settings'
                """
            ).fetchone()
            if has_settings_table is None:
                continue

            rows = legacy_connection.execute(
                """
                SELECT guild_id, admin_channel_id, attendance_voice_channel_id, timer
                FROM settings
                """
            ).fetchall()
        finally:
            legacy_connection.close()

        if not rows:
            continue

        with _connect() as connection:
            for row in rows:
                guild_id = int(row["guild_id"])
                connection.execute(
                    """
                    INSERT INTO guilds (guild_id)
                    VALUES (?)
                    ON CONFLICT(guild_id) DO NOTHING
                    """,
                    (guild_id,),
                )
                connection.execute(
                    """
                    INSERT INTO guild_settings (
                        guild_id,
                        admin_channel_id,
                        attendance_voice_channel_id,
                        timer
                    )
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(guild_id) DO UPDATE SET
                        admin_channel_id = excluded.admin_channel_id,
                        attendance_voice_channel_id = excluded.attendance_voice_channel_id,
                        timer = excluded.timer
                    """,
                    (
                        guild_id,
                        row["admin_channel_id"],
                        row["attendance_voice_channel_id"],
                        row["timer"],
                    ),
                )
            connection.commit()
        return


def _migrate_guild_settings_schema(connection: sqlite3.Connection) -> None:
    columns = connection.execute("PRAGMA table_info(guild_settings)").fetchall()
    column_names = [str(column["name"]) for column in columns]
    expected_columns = [
        "guild_id",
        "admin_channel_id",
        "attendance_voice_channel_id",
        "timer",
    ]

    if column_names == expected_columns:
        return

    connection.execute(
        """
        CREATE TABLE guild_settings_new (
            guild_id INTEGER PRIMARY KEY,
            admin_channel_id INTEGER,
            attendance_voice_channel_id INTEGER,
            timer INTEGER,
            FOREIGN KEY (guild_id) REFERENCES guilds(guild_id) ON DELETE CASCADE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO guild_settings_new (
            guild_id,
            admin_channel_id,
            attendance_voice_channel_id,
            timer
        )
        SELECT
            guild_id,
            admin_channel_id,
            attendance_voice_channel_id,
            timer
        FROM guild_settings
        """
    )
    connection.execute("DROP TABLE guild_settings")
    connection.execute("ALTER TABLE guild_settings_new RENAME TO guild_settings")
