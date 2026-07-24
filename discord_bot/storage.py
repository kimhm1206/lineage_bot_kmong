from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


load_dotenv()

REQUIRED_TABLES = {
    "alliances",
    "attendance_entries",
    "attendance_sessions",
    "guild_alliance_role_mappings",
    "guild_settings",
    "guilds",
    "scheduled_report_settings",
    "users",
}


class BotDatabaseError(RuntimeError):
    pass


class UnregisteredGuildError(BotDatabaseError):
    pass


@dataclass(slots=True)
class GuildSettings:
    guild_id: int
    admin_channel_id: int | None = None
    attendance_voice_channel_id: int | None = None
    attendance_voice_channel_ids: tuple[int, ...] = ()
    log_channel_id: int | None = None
    timer: int | None = None
    attendance_available_timer: int | None = None


@dataclass(slots=True)
class Alliance:
    alliance_id: int
    alliance_name: str


class BotDatabase:
    """PostgreSQL facade for the Discord bot's runtime responsibilities."""

    def __init__(self) -> None:
        self._dsn = _database_dsn()

    @contextmanager
    def connect(self) -> Iterator[Any]:
        connection = psycopg2.connect(self._dsn, cursor_factory=RealDictCursor)
        try:
            yield connection
        finally:
            connection.close()

    def validate_schema(self) -> None:
        rows = self._fetchall(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = ANY(%s)
            """,
            (sorted(REQUIRED_TABLES),),
        )
        existing = {str(row["table_name"]) for row in rows}
        missing = sorted(REQUIRED_TABLES - existing)
        if missing:
            raise BotDatabaseError(
                "봇 DB에 필요한 테이블이 없습니다: " + ", ".join(missing)
            )

    def enabled_guild_ids(self) -> set[int]:
        rows = self._fetchall(
            "SELECT guild_id FROM guilds WHERE is_enabled IS TRUE"
        )
        return {int(row["guild_id"]) for row in rows}

    def is_guild_enabled(self, guild_id: int) -> bool:
        row = self._fetchone(
            """
            SELECT 1 AS allowed
            FROM guilds
            WHERE guild_id = %s
              AND is_enabled IS TRUE
            """,
            (int(guild_id),),
        )
        return row is not None

    def get_settings(self, guild_id: int) -> GuildSettings:
        row = self._fetchone(
            """
            SELECT
                g.guild_id,
                gs.admin_channel_id,
                gs.attendance_voice_channel_id,
                gs.log_channel_id,
                gs.timer,
                gs.attendance_available_timer
            FROM guilds g
            LEFT JOIN guild_settings gs ON gs.guild_id = g.guild_id
            WHERE g.guild_id = %s
              AND g.is_enabled IS TRUE
            """,
            (int(guild_id),),
        )
        if row is None:
            raise UnregisteredGuildError(
                f"등록되지 않았거나 비활성화된 서버입니다: {guild_id}"
            )
        voice_channel_id = _optional_int(row["attendance_voice_channel_id"])
        voice_channel_ids = (
            (voice_channel_id,) if voice_channel_id is not None else ()
        )
        return GuildSettings(
            guild_id=int(row["guild_id"]),
            admin_channel_id=_optional_int(row["admin_channel_id"]),
            attendance_voice_channel_id=voice_channel_id,
            attendance_voice_channel_ids=voice_channel_ids,
            log_channel_id=_optional_int(row["log_channel_id"]),
            timer=_optional_int(row["timer"]),
            attendance_available_timer=_optional_int(
                row["attendance_available_timer"]
            ),
        )

    def save_attendance_session(
        self,
        guild_id: int,
        started_at: str,
        started_by_discord_id: int | None,
        participants: list[dict[str, Any]],
    ) -> int:
        with self.connect() as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT 1
                        FROM guilds
                        WHERE guild_id = %s
                          AND is_enabled IS TRUE
                        FOR SHARE
                        """,
                        (int(guild_id),),
                    )
                    if cursor.fetchone() is None:
                        raise UnregisteredGuildError(
                            f"등록되지 않았거나 비활성화된 서버입니다: {guild_id}"
                        )

                    cursor.execute(
                        """
                        INSERT INTO attendance_sessions (
                            guild_id,
                            started_at,
                            started_by_discord_id
                        )
                        VALUES (%s, %s, %s)
                        RETURNING attendance_id
                        """,
                        (
                            int(guild_id),
                            str(started_at),
                            _optional_int(started_by_discord_id),
                        ),
                    )
                    attendance_id = int(cursor.fetchone()["attendance_id"])

                    for participant in participants:
                        cursor.execute(
                            """
                            INSERT INTO users (
                                alliance_id,
                                discord_id,
                                discord_nickname,
                                is_active,
                                updated_at
                            )
                            VALUES (%s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                            ON CONFLICT (discord_id) DO UPDATE SET
                                alliance_id = COALESCE(
                                    EXCLUDED.alliance_id,
                                    users.alliance_id
                                ),
                                discord_nickname = EXCLUDED.discord_nickname,
                                is_active = TRUE,
                                updated_at = CURRENT_TIMESTAMP
                            RETURNING user_id
                            """,
                            (
                                _optional_int(participant.get("alliance_id")),
                                int(participant["discord_id"]),
                                str(participant["discord_nickname"]),
                            ),
                        )
                        user_id = int(cursor.fetchone()["user_id"])
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
            except Exception:
                connection.rollback()
                raise

    def get_or_create_alliance(self, alliance_name: str) -> Alliance:
        normalized_name = alliance_name.strip()
        if not normalized_name:
            raise ValueError("혈맹 이름은 비어 있을 수 없습니다.")
        with self.connect() as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO alliances (
                            alliance_name,
                            display_name,
                            tag_name,
                            color,
                            sort_order,
                            is_active,
                            updated_at
                        )
                        VALUES (%s, %s, %s, '#64748b', 0, TRUE, CURRENT_TIMESTAMP)
                        ON CONFLICT (alliance_name) DO UPDATE SET
                            is_active = TRUE,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING alliance_id, alliance_name
                        """,
                        (normalized_name, normalized_name, normalized_name),
                    )
                    row = cursor.fetchone()
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return Alliance(
            alliance_id=int(row["alliance_id"]),
            alliance_name=str(row["alliance_name"]),
        )

    def get_guild_alliance_role_mappings(
        self,
        guild_id: int,
    ) -> list[dict[str, Any]]:
        return self._fetchall(
            """
            SELECT
                m.mapping_id,
                m.guild_id,
                m.role_id,
                m.role_name,
                m.alliance_id,
                a.alliance_name,
                m.updated_at
            FROM guild_alliance_role_mappings m
            INNER JOIN alliances a ON a.alliance_id = m.alliance_id
            INNER JOIN guilds g ON g.guild_id = m.guild_id
            WHERE m.guild_id = %s
              AND g.is_enabled IS TRUE
            ORDER BY a.alliance_name ASC, m.role_name ASC
            """,
            (int(guild_id),),
        )

    def get_user_attendance_stats(
        self,
        guild_id: int,
        start_at: str,
        end_at: str,
        search: str | None = None,
        alliance_name: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        conditions = [
            "s.guild_id = %s",
            "s.started_at >= %s",
            "s.started_at <= %s",
        ]
        params: list[Any] = [int(guild_id), start_at, end_at]
        if search:
            conditions.append(
                """
                (
                    u.discord_nickname ILIKE %s
                    OR CAST(u.discord_id AS TEXT) ILIKE %s
                    OR COALESCE(a.alliance_name, '미분류') ILIKE %s
                )
                """
            )
            wildcard = f"%{search.strip()}%"
            params.extend([wildcard, wildcard, wildcard])
        if alliance_name:
            conditions.append("COALESCE(a.alliance_name, '미분류') = %s")
            params.append(alliance_name)
        params.append(max(1, min(int(limit), 5000)))

        rows = self._fetchall(
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
            WHERE {' AND '.join(conditions)}
            GROUP BY
                u.user_id,
                u.discord_id,
                u.discord_nickname,
                COALESCE(a.alliance_name, '미분류')
            ORDER BY attendance_count DESC, u.discord_nickname ASC
            LIMIT %s
            """,
            tuple(params),
        )
        return [
            {
                **dict(row),
                "user_id": int(row["user_id"]),
                "discord_id": int(row["discord_id"]),
                "attendance_count": int(row["attendance_count"]),
            }
            for row in rows
        ]

    def get_active_scheduled_reports(self) -> list[dict[str, Any]]:
        return self._fetchall(
            """
            SELECT
                report_setting_id,
                guild_id,
                report_name,
                frequency,
                period_type,
                subject_type,
                result_type,
                run_time,
                channel_id,
                channel_name,
                schedule_json,
                query_json,
                render_json,
                status,
                last_sent_at,
                next_run_at,
                updated_at
            FROM scheduled_report_settings
            WHERE status = 'on'
            ORDER BY
                next_run_at NULLS FIRST,
                updated_at ASC,
                report_setting_id ASC
            """
        )

    def get_active_scheduled_report(
        self,
        report_setting_id: int,
    ) -> dict[str, Any] | None:
        return self._fetchone(
            """
            SELECT
                report_setting_id,
                guild_id,
                report_name,
                frequency,
                period_type,
                subject_type,
                result_type,
                run_time,
                channel_id,
                channel_name,
                schedule_json,
                query_json,
                render_json,
                status,
                last_sent_at,
                next_run_at,
                updated_at
            FROM scheduled_report_settings
            WHERE report_setting_id = %s
              AND status = 'on'
            """,
            (int(report_setting_id),),
        )

    def get_report_attendance_ranking(
        self,
        guild_id: int,
        start_at: str,
        end_at: str,
        *,
        group_by: str,
        rank_target: str,
        metric: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 10), 50))
        if rank_target == "alliance":
            label_expr = "COALESCE(a.alliance_name, '미분류')"
            value_expr = (
                "COUNT(DISTINCT u.user_id)"
                if metric == "unique_user_count"
                else "COUNT(e.user_id)"
            )
            sql = f"""
                WITH ranked AS (
                    SELECT
                        '전체'::text AS group_name,
                        {label_expr} AS label,
                        {value_expr} AS value,
                        ROW_NUMBER() OVER (
                            ORDER BY {value_expr} DESC, {label_expr} ASC
                        ) AS rank
                    FROM attendance_sessions s
                    INNER JOIN attendance_entries e
                        ON e.attendance_id = s.attendance_id
                    INNER JOIN users u ON u.user_id = e.user_id
                    LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
                    WHERE s.guild_id = %s
                      AND s.started_at >= %s
                      AND s.started_at <= %s
                    GROUP BY {label_expr}
                )
                SELECT group_name, label, value, rank
                FROM ranked
                WHERE rank <= %s
                ORDER BY group_name ASC, rank ASC
            """
        else:
            group_select = (
                "COALESCE(a.alliance_name, '미분류')"
                if group_by == "alliance"
                else "'전체'::text"
            )
            partition_sql = (
                f"PARTITION BY {group_select}" if group_by == "alliance" else ""
            )
            group_by_sql = (
                f"{group_select}, u.user_id, u.discord_nickname"
                if group_by == "alliance"
                else "u.user_id, u.discord_nickname"
            )
            sql = f"""
                WITH ranked AS (
                    SELECT
                        {group_select} AS group_name,
                        u.discord_nickname AS label,
                        COUNT(e.user_id) AS value,
                        ROW_NUMBER() OVER (
                            {partition_sql}
                            ORDER BY
                                COUNT(e.user_id) DESC,
                                u.discord_nickname ASC
                        ) AS rank
                    FROM attendance_sessions s
                    INNER JOIN attendance_entries e
                        ON e.attendance_id = s.attendance_id
                    INNER JOIN users u ON u.user_id = e.user_id
                    LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
                    WHERE s.guild_id = %s
                      AND s.started_at >= %s
                      AND s.started_at <= %s
                    GROUP BY {group_by_sql}
                )
                SELECT group_name, label, value, rank
                FROM ranked
                WHERE rank <= %s
                ORDER BY group_name ASC, rank ASC
            """
        rows = self._fetchall(
            sql,
            (int(guild_id), start_at, end_at, safe_limit),
        )
        return [
            {
                "group_name": str(row["group_name"]),
                "label": str(row["label"]),
                "value": int(row["value"] or 0),
                "rank": int(row["rank"] or 0),
            }
            for row in rows
        ]

    def update_scheduled_report_next_run(
        self,
        report_setting_id: int,
        next_run_at: str,
    ) -> None:
        self._execute(
            """
            UPDATE scheduled_report_settings
            SET next_run_at = %s
            WHERE report_setting_id = %s
              AND status = 'on'
            """,
            (next_run_at, int(report_setting_id)),
        )

    def mark_scheduled_report_sent(
        self,
        report_setting_id: int,
        last_sent_at: str,
        next_run_at: str,
    ) -> None:
        self._execute(
            """
            UPDATE scheduled_report_settings
            SET last_sent_at = %s,
                next_run_at = %s
            WHERE report_setting_id = %s
              AND status = 'on'
            """,
            (last_sent_at, next_run_at, int(report_setting_id)),
        )

    def _fetchone(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
    ) -> dict[str, Any] | None:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()
        return dict(row) if row is not None else None

    def _fetchall(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
    ) -> list[dict[str, Any]]:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        with self.connect() as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql, params)
                connection.commit()
            except Exception:
                connection.rollback()
                raise


def _database_dsn() -> str:
    explicit_url = (
        os.getenv("BOT_DATABASE_URL", "").strip()
        or os.getenv("DATABASE_URL", "").strip()
    )
    if explicit_url:
        return explicit_url.replace(
            "postgresql+asyncpg://",
            "postgresql://",
            1,
        )

    database_name = os.getenv("PGDATABASE", "").strip()
    if not database_name:
        raise BotDatabaseError(
            "BOT_DATABASE_URL 또는 PGDATABASE가 설정되어 있지 않습니다."
        )
    return " ".join(
        (
            f"host={os.getenv('PGHOST', '127.0.0.1')}",
            f"port={os.getenv('PGPORT', '5432')}",
            f"dbname={database_name}",
            f"user={os.getenv('PGUSER', 'postgres')}",
            f"password={os.getenv('PGPASSWORD', '')}",
        )
    )


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


database = BotDatabase()
