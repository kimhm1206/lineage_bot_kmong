from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import quote

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json, RealDictCursor


load_dotenv()

DEFAULT_ALLIANCE_NAMES = ("정지", "랭커", "삼국", "해적", "보스", "인연")
DEFAULT_DISTRIBUTION_FEE_RATE = Decimal("0.10")
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


class Database:
    """Small shared database facade used by the bot and the web app."""

    def is_test_mode(self) -> bool:
        return is_test_database_mode()

    def url(self) -> str:
        return _database_url()

    def connect(self) -> psycopg2.extensions.connection:
        return _connect()

    def fetchone(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
    ) -> dict[str, Any] | None:
        return _fetchone(sql, params)

    def fetchall(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
    ) -> list[dict[str, Any]]:
        return _fetchall(sql, params)

    def init_schema(self) -> None:
        init_db()

    def ensure_guild(self, guild_id: int) -> None:
        ensure_guild(guild_id)

    def get_configured_guild_id(self) -> int | None:
        return get_configured_guild_id()

    def get_settings(self, guild_id: int) -> GuildSettings:
        return get_settings(guild_id)

    def update_setting(
        self,
        guild_id: int,
        column: str,
        value: int | None,
    ) -> GuildSettings:
        return update_setting(guild_id, column, value)

    def save_attendance_session(
        self,
        guild_id: int,
        started_at: str,
        ended_at: str,
        started_by_discord_id: int | None,
        participants: list[dict[str, Any]],
    ) -> int:
        return save_attendance_session(
            guild_id,
            started_at,
            ended_at,
            started_by_discord_id,
            participants,
        )

    def create_alliance(self, alliance_name: str) -> Alliance:
        return create_alliance(alliance_name)

    def get_or_create_alliance(self, alliance_name: str) -> Alliance:
        return get_or_create_alliance(alliance_name)

    def get_alliance_names(self) -> list[str]:
        return get_alliance_names()

    def get_alliance_counts_for_discord_ids(
        self,
        discord_ids: list[int],
    ) -> dict[str, int]:
        return get_alliance_counts_for_discord_ids(discord_ids)

    def get_guild_alliance_role_mappings(self, guild_id: int) -> list[dict[str, Any]]:
        return get_guild_alliance_role_mappings(guild_id)

    def upsert_guild_alliance_role_mapping(
        self,
        guild_id: int,
        role_id: int,
        role_name: str,
        alliance_name: str,
    ) -> None:
        upsert_guild_alliance_role_mapping(
            guild_id,
            role_id,
            role_name,
            alliance_name,
        )

    def delete_guild_alliance_role_mapping(
        self,
        guild_id: int,
        mapping_id: int,
    ) -> None:
        delete_guild_alliance_role_mapping(guild_id, mapping_id)

    def resolve_alliance_by_role_ids(
        self,
        guild_id: int,
        role_ids: list[int],
    ) -> Alliance | None:
        return resolve_alliance_by_role_ids(guild_id, role_ids)

    def get_attendance_overview(
        self,
        guild_id: int | None,
        start_at: str | None = None,
        end_at: str | None = None,
    ) -> dict[str, int | None]:
        return get_attendance_overview(guild_id, start_at, end_at)

    def get_daily_attendance_stats(
        self,
        guild_id: int | None,
        start_at: str | None = None,
        end_at: str | None = None,
    ) -> list[dict[str, Any]]:
        return get_daily_attendance_stats(guild_id, start_at, end_at)

    def get_alliance_attendance_stats(
        self,
        guild_id: int | None,
        start_at: str | None = None,
        end_at: str | None = None,
        search: str | None = None,
    ) -> list[dict[str, Any]]:
        return get_alliance_attendance_stats(guild_id, start_at, end_at, search)

    def get_user_attendance_stats(
        self,
        guild_id: int | None,
        start_at: str | None = None,
        end_at: str | None = None,
        search: str | None = None,
        alliance_name: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        return get_user_attendance_stats(
            guild_id,
            start_at,
            end_at,
            search,
            alliance_name,
            limit,
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
        return get_report_attendance_ranking(
            guild_id,
            start_at,
            end_at,
            group_by=group_by,
            rank_target=rank_target,
            metric=metric,
            limit=limit,
        )

    def get_attendance_export_rows(
        self,
        guild_id: int | None,
        start_at: str | None = None,
        end_at: str | None = None,
        search: str | None = None,
        alliance_name: str | None = None,
    ) -> list[dict[str, Any]]:
        return get_attendance_export_rows(
            guild_id,
            start_at,
            end_at,
            search,
            alliance_name,
        )

    def get_attendance_status_sessions(
        self,
        guild_id: int,
        limit: int = 10,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        return get_attendance_status_sessions(guild_id, limit, offset)

    def count_attendance_status_sessions(self, guild_id: int) -> int:
        return count_attendance_status_sessions(guild_id)

    def get_active_scheduled_reports(self) -> list[dict[str, Any]]:
        return get_active_scheduled_reports()

    def get_active_scheduled_report(
        self,
        report_setting_id: int,
    ) -> dict[str, Any] | None:
        return get_active_scheduled_report(report_setting_id)

    def update_scheduled_report_next_run(
        self,
        report_setting_id: int,
        next_run_at: str,
    ) -> None:
        update_scheduled_report_next_run(report_setting_id, next_run_at)

    def mark_scheduled_report_sent(
        self,
        report_setting_id: int,
        last_sent_at: str,
        next_run_at: str,
    ) -> None:
        mark_scheduled_report_sent(report_setting_id, last_sent_at, next_run_at)

    def claim_bot_commands(
        self,
        limit: int = 5,
        guild_ids: list[int] | tuple[int, ...] | None = None,
    ) -> list[dict[str, Any]]:
        return claim_bot_commands(limit, guild_ids)

    def complete_bot_command(
        self,
        command_id: int,
        result: dict[str, Any] | None = None,
    ) -> None:
        complete_bot_command(command_id, result)

    def fail_bot_command(self, command_id: int, error_message: str) -> None:
        fail_bot_command(command_id, error_message)

    def start_live_attendance(
        self,
        guild_id: int,
        *,
        discord_channel_id: int | None,
        discord_message_id: int | None,
        started_by_discord_id: int | None,
        started_at: str,
        expires_at: str | None,
    ) -> int:
        return start_live_attendance(
            guild_id,
            discord_channel_id=discord_channel_id,
            discord_message_id=discord_message_id,
            started_by_discord_id=started_by_discord_id,
            started_at=started_at,
            expires_at=expires_at,
        )

    def add_live_attendance_participant(
        self,
        live_session_id: int,
        *,
        discord_id: int,
        display_name: str,
        alliance_id: int | None,
        joined_voice_at: str | None,
        attended_at: str,
        source: str = "discord",
    ) -> None:
        add_live_attendance_participant(
            live_session_id,
            discord_id=discord_id,
            display_name=display_name,
            alliance_id=alliance_id,
            joined_voice_at=joined_voice_at,
            attended_at=attended_at,
            source=source,
        )

    def finish_live_attendance(
        self,
        live_session_id: int | None,
        *,
        guild_id: int,
        ended_at: str,
    ) -> None:
        finish_live_attendance(live_session_id, guild_id=guild_id, ended_at=ended_at)

    def get_live_attendance_state(self, guild_id: int) -> dict[str, Any]:
        return get_live_attendance_state(guild_id)

    def get_item_price_settings(self, guild_id: int) -> list[dict[str, Any]]:
        return get_item_price_settings(guild_id)

    def upsert_item_price(
        self,
        guild_id: int,
        *,
        item_name: str,
        default_price: Decimal,
        category: str | None = None,
        memo: str | None = None,
        is_bid_item: bool = True,
    ) -> int:
        return upsert_item_price(
            guild_id,
            item_name=item_name,
            default_price=default_price,
            category=category,
            memo=memo,
            is_bid_item=is_bid_item,
        )

    def update_item_price(
        self,
        guild_id: int,
        item_id: int,
        *,
        item_name: str,
        default_price: Decimal,
        category: str | None = None,
        memo: str | None = None,
        is_bid_item: bool = True,
    ) -> None:
        update_item_price(
            guild_id,
            item_id,
            item_name=item_name,
            default_price=default_price,
            category=category,
            memo=memo,
            is_bid_item=is_bid_item,
        )

    def deactivate_item_price(self, guild_id: int, item_id: int) -> None:
        deactivate_item_price(guild_id, item_id)

    def get_latest_adena_rate(self, guild_id: int) -> Decimal:
        return get_latest_adena_rate(guild_id)

    def create_loot_drop(
        self,
        guild_id: int,
        *,
        attendance_id: int,
        item_id: int | None,
        item_name: str,
        cash_price_krw: Decimal,
        sale_price: Decimal,
        adena_rate: Decimal,
        buyer_name: str | None,
        memo: str | None,
        created_by_discord_id: int | None,
    ) -> int:
        return create_loot_drop(
            guild_id,
            attendance_id=attendance_id,
            item_id=item_id,
            item_name=item_name,
            cash_price_krw=cash_price_krw,
            sale_price=sale_price,
            adena_rate=adena_rate,
            buyer_name=buyer_name,
            memo=memo,
            created_by_discord_id=created_by_discord_id,
        )

    def update_loot_drop(
        self,
        guild_id: int,
        loot_event_id: int,
        *,
        cash_price_krw: Decimal,
        sale_price: Decimal,
        adena_rate: Decimal,
        buyer_name: str | None,
        memo: str | None,
    ) -> None:
        update_loot_drop(
            guild_id,
            loot_event_id,
            cash_price_krw=cash_price_krw,
            sale_price=sale_price,
            adena_rate=adena_rate,
            buyer_name=buyer_name,
            memo=memo,
        )

    def delete_loot_drop(self, guild_id: int, loot_event_id: int) -> None:
        delete_loot_drop(guild_id, loot_event_id)

    def get_loot_drop_events(self, guild_id: int, limit: int = 30) -> list[dict[str, Any]]:
        return get_loot_drop_events(guild_id, limit)

    def update_distribution_alliance_payout_status(
        self,
        guild_id: int,
        distribution_id: int,
        alliance_id: int,
        payout_status: str,
    ) -> None:
        update_distribution_alliance_payout_status(
            guild_id,
            distribution_id,
            alliance_id,
            payout_status,
        )


database = Database()


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


def get_guild_alliance_role_mappings(guild_id: int) -> list[dict[str, Any]]:
    rows = _fetchall(
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
        WHERE m.guild_id = %s
        ORDER BY a.alliance_name ASC, m.role_name ASC
        """,
        (guild_id,),
    )
    return [
        {
            "mapping_id": int(row["mapping_id"]),
            "guild_id": int(row["guild_id"]),
            "role_id": int(row["role_id"]),
            "role_name": str(row["role_name"]),
            "alliance_id": int(row["alliance_id"]),
            "alliance_name": str(row["alliance_name"]),
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def upsert_guild_alliance_role_mapping(
    guild_id: int,
    role_id: int,
    role_name: str,
    alliance_name: str,
) -> None:
    alliance = get_or_create_alliance(alliance_name)
    normalized_role_name = role_name.strip() or str(role_id)
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO guild_alliance_role_mappings (
                    guild_id,
                    role_id,
                    role_name,
                    alliance_id
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (guild_id, role_id) DO UPDATE SET
                    role_name = EXCLUDED.role_name,
                    alliance_id = EXCLUDED.alliance_id,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (guild_id, role_id, normalized_role_name, alliance.alliance_id),
            )
        connection.commit()


def delete_guild_alliance_role_mapping(guild_id: int, mapping_id: int) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM guild_alliance_role_mappings
                WHERE guild_id = %s AND mapping_id = %s
                """,
                (guild_id, mapping_id),
            )
        connection.commit()


def resolve_alliance_by_role_ids(guild_id: int, role_ids: list[int]) -> Alliance | None:
    if not role_ids:
        return None
    row = _fetchone(
        """
        WITH role_priority AS (
            SELECT role_id, ordinality
            FROM unnest(%s::bigint[]) WITH ORDINALITY AS role_ids(role_id, ordinality)
        )
        SELECT a.alliance_id, a.alliance_name
        FROM role_priority rp
        INNER JOIN guild_alliance_role_mappings m
            ON m.guild_id = %s
           AND m.role_id = rp.role_id
        INNER JOIN alliances a ON a.alliance_id = m.alliance_id
        ORDER BY rp.ordinality ASC
        LIMIT 1
        """,
        (role_ids, guild_id),
    )
    if row is None:
        return None
    return Alliance(alliance_id=int(row["alliance_id"]), alliance_name=str(row["alliance_name"]))


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


def count_attendance_status_sessions(guild_id: int) -> int:
    row = _fetchone(
        """
        SELECT COUNT(*) AS session_count
        FROM attendance_sessions
        WHERE guild_id = %s
        """,
        (guild_id,),
    )
    return int(row["session_count"] or 0) if row else 0


def get_attendance_status_sessions(
    guild_id: int,
    limit: int = 10,
    offset: int = 0,
) -> list[dict[str, Any]]:
    session_rows = _fetchall(
        """
        SELECT
            s.attendance_id,
            s.started_at,
            s.ended_at,
            s.started_by_discord_id,
            COALESCE(
                MAX(starter.discord_nickname),
                s.started_by_discord_id::text
            ) AS started_by_name,
            COUNT(e.user_id) AS participant_count
        FROM attendance_sessions s
        LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
        LEFT JOIN users starter ON starter.discord_id = s.started_by_discord_id
        WHERE s.guild_id = %s
        GROUP BY
            s.attendance_id,
            s.started_at,
            s.ended_at,
            s.started_by_discord_id
        ORDER BY s.started_at DESC
        LIMIT %s
        OFFSET %s
        """,
        (guild_id, int(limit), int(offset)),
    )
    session_ids = [int(row["attendance_id"]) for row in session_rows]
    if not session_ids:
        return []

    participant_rows = _fetchall(
        """
        SELECT
            e.attendance_id,
            COALESCE(a.alliance_name, '미분류') AS alliance_name,
            u.discord_id,
            u.discord_nickname
        FROM attendance_entries e
        INNER JOIN users u ON u.user_id = e.user_id
        LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
        WHERE e.attendance_id = ANY(%s::bigint[])
        ORDER BY e.attendance_id DESC, alliance_name ASC, u.discord_nickname ASC
        """,
        (session_ids,),
    )
    grouped: dict[int, dict[str, list[dict[str, Any]]]] = {
        attendance_id: {} for attendance_id in session_ids
    }
    for row in participant_rows:
        attendance_id = int(row["attendance_id"])
        alliance_name = str(row["alliance_name"])
        grouped.setdefault(attendance_id, {}).setdefault(alliance_name, []).append(
            {
                "discord_id": int(row["discord_id"]),
                "discord_nickname": str(row["discord_nickname"]),
            }
        )

    sessions: list[dict[str, Any]] = []
    for row in session_rows:
        attendance_id = int(row["attendance_id"])
        alliances = [
            {
                "alliance_name": alliance_name,
                "count": len(members),
                "members": members,
            }
            for alliance_name, members in sorted(grouped.get(attendance_id, {}).items())
        ]
        sessions.append(
            {
                "attendance_id": attendance_id,
                "started_at": str(row["started_at"]),
                "ended_at": str(row["ended_at"]),
                "started_by_discord_id": _optional_int(row["started_by_discord_id"]),
                "started_by_name": row["started_by_name"] or "",
                "participant_count": int(row["participant_count"] or 0),
                "alliances": alliances,
            }
        )
    return sessions


def get_active_scheduled_reports() -> list[dict[str, Any]]:
    rows = _fetchall(
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
    return [dict(row) for row in rows]


def get_active_scheduled_report(report_setting_id: int) -> dict[str, Any] | None:
    row = _fetchone(
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
    return dict(row) if row else None


def get_report_attendance_ranking(
    guild_id: int,
    start_at: str,
    end_at: str,
    *,
    group_by: str,
    rank_target: str,
    metric: str,
    limit: int,
) -> list[dict[str, Any]]:
    group_expr = "COALESCE(a.alliance_name, '미분류')" if group_by == "alliance" else "'전체'"
    safe_limit = max(1, min(int(limit or 10), 50))

    if rank_target == "alliance":
        label_expr = "COALESCE(a.alliance_name, '미분류')"
        value_expr = (
            "COUNT(DISTINCT u.user_id)"
            if metric == "unique_user_count"
            else "COUNT(e.user_id)"
        )
        rows = _fetchall(
            f"""
            WITH ranked AS (
                SELECT
                    {group_expr} AS group_name,
                    {label_expr} AS label,
                    {value_expr} AS value,
                    ROW_NUMBER() OVER (
                        PARTITION BY {group_expr}
                        ORDER BY {value_expr} DESC, {label_expr} ASC
                    ) AS rank
                FROM attendance_sessions s
                INNER JOIN attendance_entries e ON e.attendance_id = s.attendance_id
                INNER JOIN users u ON u.user_id = e.user_id
                LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
                WHERE s.guild_id = %s
                  AND s.started_at >= %s
                  AND s.started_at <= %s
                GROUP BY {group_expr}, {label_expr}
            )
            SELECT group_name, label, value, rank
            FROM ranked
            WHERE rank <= %s
            ORDER BY group_name ASC, rank ASC
            """,
            (guild_id, start_at, end_at, safe_limit),
        )
    elif metric == "unique_user_count":
        rows = _fetchall(
            f"""
            WITH ranked AS (
                SELECT
                    {group_expr} AS group_name,
                    u.discord_nickname AS label,
                    1 AS value,
                    ROW_NUMBER() OVER (
                        PARTITION BY {group_expr}
                        ORDER BY u.discord_nickname ASC
                    ) AS rank
                FROM attendance_sessions s
                INNER JOIN attendance_entries e ON e.attendance_id = s.attendance_id
                INNER JOIN users u ON u.user_id = e.user_id
                LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
                WHERE s.guild_id = %s
                  AND s.started_at >= %s
                  AND s.started_at <= %s
                GROUP BY {group_expr}, u.user_id, u.discord_nickname
            )
            SELECT group_name, label, value, rank
            FROM ranked
            WHERE rank <= %s
            ORDER BY group_name ASC, rank ASC
            """,
            (guild_id, start_at, end_at, safe_limit),
        )
    else:
        rows = _fetchall(
            f"""
            WITH ranked AS (
                SELECT
                    {group_expr} AS group_name,
                    u.discord_nickname AS label,
                    COUNT(e.user_id) AS value,
                    ROW_NUMBER() OVER (
                        PARTITION BY {group_expr}
                        ORDER BY COUNT(e.user_id) DESC, u.discord_nickname ASC
                    ) AS rank
                FROM attendance_sessions s
                INNER JOIN attendance_entries e ON e.attendance_id = s.attendance_id
                INNER JOIN users u ON u.user_id = e.user_id
                LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
                WHERE s.guild_id = %s
                  AND s.started_at >= %s
                  AND s.started_at <= %s
                GROUP BY {group_expr}, u.user_id, u.discord_nickname
            )
            SELECT group_name, label, value, rank
            FROM ranked
            WHERE rank <= %s
            ORDER BY group_name ASC, rank ASC
            """,
            (guild_id, start_at, end_at, safe_limit),
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
    report_setting_id: int,
    next_run_at: str,
) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE scheduled_report_settings
                SET next_run_at = %s
                WHERE report_setting_id = %s
                  AND status = 'on'
                """,
                (next_run_at, int(report_setting_id)),
            )
        connection.commit()


def mark_scheduled_report_sent(
    report_setting_id: int,
    last_sent_at: str,
    next_run_at: str,
) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE scheduled_report_settings
                SET
                    last_sent_at = %s,
                    next_run_at = %s
                WHERE report_setting_id = %s
                  AND status = 'on'
                """,
                (last_sent_at, next_run_at, int(report_setting_id)),
            )
        connection.commit()


def claim_bot_commands(
    limit: int = 5,
    guild_ids: list[int] | tuple[int, ...] | None = None,
) -> list[dict[str, Any]]:
    normalized_guild_ids = (
        sorted({int(guild_id) for guild_id in guild_ids})
        if guild_ids is not None
        else None
    )
    if normalized_guild_ids == []:
        return []

    guild_filter = ""
    params: list[Any] = []
    if normalized_guild_ids is not None:
        guild_filter = "AND guild_id = ANY(%s::bigint[])"
        params.append(normalized_guild_ids)
    params.append(int(limit))

    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                WITH picked AS (
                    SELECT command_id
                    FROM bot_command_queue
                    WHERE status = 'pending'
                      {guild_filter}
                    ORDER BY created_at ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE bot_command_queue q
                SET status = 'processing'
                FROM picked
                WHERE q.command_id = picked.command_id
                RETURNING
                    q.command_id,
                    q.guild_id,
                    q.command_type,
                    q.payload_json,
                    q.requested_by_discord_id,
                    q.created_at
                """,
                tuple(params),
            )
            rows = cursor.fetchall()
        connection.commit()
    return [dict(row) for row in rows]


def complete_bot_command(
    command_id: int,
    result: dict[str, Any] | None = None,
) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bot_command_queue
                SET
                    status = 'completed',
                    result_json = %s,
                    processed_at = CURRENT_TIMESTAMP,
                    error_message = NULL
                WHERE command_id = %s
                """,
                (Json(result or {}), command_id),
            )
        connection.commit()


def fail_bot_command(command_id: int, error_message: str) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bot_command_queue
                SET
                    status = 'failed',
                    processed_at = CURRENT_TIMESTAMP,
                    error_message = %s
                WHERE command_id = %s
                """,
                (error_message[:1000], command_id),
            )
        connection.commit()


def start_live_attendance(
    guild_id: int,
    *,
    discord_channel_id: int | None,
    discord_message_id: int | None,
    started_by_discord_id: int | None,
    started_at: str,
    expires_at: str | None,
) -> int:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE attendance_live_sessions
                SET status = 'ended', ended_at = COALESCE(ended_at, %s)
                WHERE guild_id = %s AND status = 'active'
                """,
                (started_at, guild_id),
            )
            cursor.execute(
                """
                INSERT INTO attendance_live_sessions (
                    guild_id,
                    discord_channel_id,
                    discord_message_id,
                    started_by_discord_id,
                    started_at,
                    expires_at,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'active')
                RETURNING live_session_id
                """,
                (
                    guild_id,
                    discord_channel_id,
                    discord_message_id,
                    started_by_discord_id,
                    started_at,
                    expires_at,
                ),
            )
            row = cursor.fetchone()
        connection.commit()
    return int(row["live_session_id"])


def add_live_attendance_participant(
    live_session_id: int,
    *,
    discord_id: int,
    display_name: str,
    alliance_id: int | None,
    joined_voice_at: str | None,
    attended_at: str,
    source: str = "discord",
) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO attendance_live_participants (
                    live_session_id,
                    discord_id,
                    display_name,
                    alliance_id,
                    joined_voice_at,
                    attended_at,
                    source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (live_session_id, discord_id) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    alliance_id = EXCLUDED.alliance_id,
                    joined_voice_at = COALESCE(
                        attendance_live_participants.joined_voice_at,
                        EXCLUDED.joined_voice_at
                    ),
                    attended_at = EXCLUDED.attended_at,
                    source = EXCLUDED.source
                """,
                (
                    live_session_id,
                    discord_id,
                    display_name,
                    alliance_id,
                    joined_voice_at,
                    attended_at,
                    source,
                ),
            )
        connection.commit()


def finish_live_attendance(
    live_session_id: int | None,
    *,
    guild_id: int,
    ended_at: str,
) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            if live_session_id is not None:
                cursor.execute(
                    """
                    UPDATE attendance_live_sessions
                    SET status = 'ended', ended_at = %s
                    WHERE live_session_id = %s
                    """,
                    (ended_at, live_session_id),
                )
            else:
                cursor.execute(
                    """
                    UPDATE attendance_live_sessions
                    SET status = 'ended', ended_at = %s
                    WHERE guild_id = %s AND status = 'active'
                    """,
                    (ended_at, guild_id),
                )
        connection.commit()


def get_live_attendance_state(guild_id: int) -> dict[str, Any]:
    session = _fetchone(
        """
        SELECT
            live_session_id,
            guild_id,
            discord_channel_id,
            discord_message_id,
            started_by_discord_id,
            started_at,
            expires_at,
            ended_at,
            status
        FROM attendance_live_sessions
        WHERE guild_id = %s AND status = 'active'
        ORDER BY live_session_id DESC
        LIMIT 1
        """,
        (guild_id,),
    )
    if session is None:
        return {"active": False, "session": None, "participants": []}

    participants = _fetchall(
        """
        SELECT
            p.discord_id,
            p.display_name,
            COALESCE(a.alliance_name, '미분류') AS alliance_name,
            p.joined_voice_at,
            p.attended_at,
            p.source
        FROM attendance_live_participants p
        LEFT JOIN alliances a ON a.alliance_id = p.alliance_id
        WHERE p.live_session_id = %s
        ORDER BY p.attended_at ASC, p.display_name ASC
        """,
        (session["live_session_id"],),
    )
    return {
        "active": True,
        "session": dict(session),
        "participants": [
            {
                "discord_id": int(row["discord_id"]),
                "display_name": str(row["display_name"]),
                "alliance_name": str(row["alliance_name"]),
                "joined_voice_at": row["joined_voice_at"] or "",
                "attended_at": row["attended_at"] or "",
                "source": row["source"] or "discord",
            }
            for row in participants
        ],
    }


def get_item_price_settings(guild_id: int) -> list[dict[str, Any]]:
    rows = _fetchall(
        """
        SELECT *
        FROM (
            SELECT DISTINCT ON (LOWER(item_name))
                item_id,
                guild_id,
                item_name,
                category,
                default_price,
                is_bid_item,
                is_active,
                memo,
                sort_order,
                updated_at
            FROM items
            WHERE is_active = TRUE
            ORDER BY
                LOWER(item_name),
                CASE WHEN guild_id IS NULL THEN 0 ELSE 1 END,
                updated_at DESC,
                sort_order ASC NULLS LAST,
                item_name ASC
        ) picked
        ORDER BY sort_order ASC NULLS LAST, item_name ASC
        """,
    )
    return [
        {
            "item_id": int(row["item_id"]),
            "guild_id": _optional_int(row["guild_id"]),
            "item_name": str(row["item_name"]),
            "category": row["category"] or "",
            "default_price": _decimal(row["default_price"]),
            "is_bid_item": bool(row["is_bid_item"]),
            "is_active": bool(row["is_active"]),
            "memo": row["memo"] or "",
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def upsert_item_price(
    guild_id: int,
    *,
    item_name: str,
    default_price: Decimal,
    category: str | None = None,
    memo: str | None = None,
    is_bid_item: bool = True,
) -> int:
    ensure_guild(guild_id)
    normalized_name = item_name.strip()
    if not normalized_name:
        raise ValueError("Item name must not be empty.")

    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT item_id
                FROM items
                WHERE LOWER(item_name) = LOWER(%s)
                ORDER BY
                    CASE WHEN guild_id IS NULL THEN 0 ELSE 1 END,
                    is_active DESC,
                    updated_at DESC,
                    item_id ASC
                LIMIT 1
                """,
                (normalized_name,),
            )
            row = cursor.fetchone()
            if row is None:
                cursor.execute(
                    """
                    INSERT INTO items (
                        guild_id,
                        item_name,
                        category,
                        default_price,
                        is_bid_item,
                        is_active,
                        memo,
                        updated_at
                    )
                    VALUES (NULL, %s, %s, %s, %s, TRUE, %s, CURRENT_TIMESTAMP)
                    RETURNING item_id
                    """,
                    (
                        normalized_name,
                        _blank_to_none(category),
                        _decimal(default_price),
                        is_bid_item,
                        _blank_to_none(memo),
                    ),
                )
                item_id = int(cursor.fetchone()["item_id"])
            else:
                item_id = int(row["item_id"])
                cursor.execute(
                    """
                    UPDATE items
                    SET guild_id = NULL,
                        item_name = %s,
                        category = %s,
                        default_price = %s,
                        is_bid_item = %s,
                        is_active = TRUE,
                        memo = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE item_id = %s
                    """,
                    (
                        normalized_name,
                        _blank_to_none(category),
                        _decimal(default_price),
                        is_bid_item,
                        _blank_to_none(memo),
                        item_id,
                    ),
                )
            _deactivate_duplicate_item_prices(cursor, item_id, normalized_name)
        connection.commit()
    return item_id


def update_item_price(
    guild_id: int,
    item_id: int,
    *,
    item_name: str,
    default_price: Decimal,
    category: str | None = None,
    memo: str | None = None,
    is_bid_item: bool = True,
) -> None:
    ensure_guild(guild_id)
    normalized_name = item_name.strip()
    if not normalized_name:
        raise ValueError("Item name must not be empty.")

    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE items
                SET guild_id = NULL,
                    item_name = %s,
                    category = %s,
                    default_price = %s,
                    is_bid_item = %s,
                    is_active = TRUE,
                    memo = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE item_id = %s
                """,
                (
                    normalized_name,
                    _blank_to_none(category),
                    _decimal(default_price),
                    is_bid_item,
                    _blank_to_none(memo),
                    item_id,
                ),
            )
            if cursor.rowcount == 0:
                raise ValueError("Item price setting was not found.")
            _deactivate_duplicate_item_prices(cursor, item_id, normalized_name)
        connection.commit()


def deactivate_item_price(guild_id: int, item_id: int) -> None:
    ensure_guild(guild_id)
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE items
                SET is_active = FALSE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE item_id = %s
                """,
                (item_id,),
            )
        connection.commit()


def get_latest_adena_rate(guild_id: int) -> Decimal:
    row = _fetchone(
        """
        SELECT adena_rate
        FROM loot_events
        WHERE guild_id = %s
          AND adena_rate > 0
        ORDER BY updated_at DESC, loot_event_id DESC
        LIMIT 1
        """,
        (guild_id,),
    )
    return _decimal(row["adena_rate"]) if row else Decimal("0")


def create_loot_drop(
    guild_id: int,
    *,
    attendance_id: int,
    item_id: int | None,
    item_name: str,
    cash_price_krw: Decimal,
    sale_price: Decimal,
    adena_rate: Decimal,
    buyer_name: str | None,
    memo: str | None,
    created_by_discord_id: int | None,
) -> int:
    ensure_guild(guild_id)
    normalized_item_name = item_name.strip()
    if item_id is None and not normalized_item_name:
        raise ValueError("Item name must not be empty.")

    cash_amount = _decimal(cash_price_krw)
    sale_amount = _decimal(sale_price)
    net_amount = sale_amount - (sale_amount * DEFAULT_DISTRIBUTION_FEE_RATE)
    with _connect() as connection:
        with connection.cursor() as cursor:
            session = _get_attendance_session_for_loot(cursor, guild_id, attendance_id)
            participants = _get_attendance_participants_for_loot(cursor, attendance_id)
            if not participants:
                raise ValueError("Attendance session has no participants.")

            resolved_item_id, resolved_item_name = _resolve_loot_item(
                cursor,
                guild_id,
                item_id,
                normalized_item_name,
                cash_amount,
            )
            started_at = str(session["started_at"])
            cursor.execute(
                """
                INSERT INTO loot_events (
                    guild_id,
                    attendance_id,
                    event_date,
                    event_time_label,
                    title,
                    memo,
                    adena_rate,
                    created_by_discord_id,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING loot_event_id
                """,
                (
                    guild_id,
                    attendance_id,
                    _date_label_from_text(started_at),
                    _time_label_from_text(started_at),
                    resolved_item_name,
                    _blank_to_none(memo),
                    _decimal(adena_rate),
                    created_by_discord_id,
                ),
            )
            loot_event_id = int(cursor.fetchone()["loot_event_id"])
            participant_counts: dict[int, int] = {}
            unclassified_alliance_id: int | None = None

            for participant in participants:
                alliance_id = _optional_int(participant["alliance_id"])
                if alliance_id is None:
                    if unclassified_alliance_id is None:
                        unclassified_alliance_id = _ensure_alliance_id(
                            cursor,
                            "미분류",
                        )
                    alliance_id = unclassified_alliance_id
                participant_counts[alliance_id] = participant_counts.get(alliance_id, 0) + 1
                cursor.execute(
                    """
                    INSERT INTO loot_event_participants (
                        loot_event_id,
                        user_id,
                        alliance_id,
                        attended_at,
                        source
                    )
                    VALUES (%s, %s, %s, %s, 'attendance')
                    ON CONFLICT (loot_event_id, user_id) DO UPDATE SET
                        alliance_id = EXCLUDED.alliance_id,
                        attended_at = EXCLUDED.attended_at,
                        source = EXCLUDED.source
                    """,
                    (
                        loot_event_id,
                        int(participant["user_id"]),
                        alliance_id,
                        started_at,
                    ),
                )

            for alliance_id, count in participant_counts.items():
                cursor.execute(
                    """
                    INSERT INTO loot_event_alliance_counts (
                        loot_event_id,
                        alliance_id,
                        participant_count
                    )
                    VALUES (%s, %s, %s)
                    ON CONFLICT (loot_event_id, alliance_id) DO UPDATE SET
                        participant_count = EXCLUDED.participant_count
                    """,
                    (loot_event_id, alliance_id, count),
                )

            cursor.execute(
                """
                INSERT INTO loot_event_items (
                    loot_event_id,
                    item_id,
                    item_name_snapshot,
                    buyer_name,
                    cash_price_krw,
                    sale_price,
                    fee_rate,
                    net_amount,
                    memo
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL)
                RETURNING loot_item_id
                """,
                (
                    loot_event_id,
                    resolved_item_id,
                    resolved_item_name,
                    _blank_to_none(buyer_name),
                    cash_amount,
                    sale_amount,
                    DEFAULT_DISTRIBUTION_FEE_RATE,
                    net_amount,
                ),
            )
            loot_item_id = int(cursor.fetchone()["loot_item_id"])
            _upsert_distribution_for_loot(
                cursor,
                guild_id,
                loot_event_id,
                loot_item_id,
                sale_amount,
                participant_counts,
            )
        connection.commit()
    return loot_event_id


def update_loot_drop(
    guild_id: int,
    loot_event_id: int,
    *,
    cash_price_krw: Decimal,
    sale_price: Decimal,
    adena_rate: Decimal,
    buyer_name: str | None,
    memo: str | None,
) -> None:
    cash_amount = _decimal(cash_price_krw)
    sale_amount = _decimal(sale_price)
    net_amount = sale_amount - (sale_amount * DEFAULT_DISTRIBUTION_FEE_RATE)
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT loot_event_id
                FROM loot_events
                WHERE guild_id = %s
                  AND loot_event_id = %s
                """,
                (guild_id, loot_event_id),
            )
            if cursor.fetchone() is None:
                raise ValueError("Loot event was not found.")

            cursor.execute(
                """
                SELECT loot_item_id
                FROM loot_event_items
                WHERE loot_event_id = %s
                ORDER BY loot_item_id ASC
                LIMIT 1
                """,
                (loot_event_id,),
            )
            item_row = cursor.fetchone()
            if item_row is None:
                raise ValueError("Loot event item was not found.")
            loot_item_id = int(item_row["loot_item_id"])

            cursor.execute(
                """
                UPDATE loot_events
                SET adena_rate = %s,
                    memo = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE loot_event_id = %s
                """,
                (_decimal(adena_rate), _blank_to_none(memo), loot_event_id),
            )
            cursor.execute(
                """
                UPDATE loot_event_items
                SET buyer_name = %s,
                    cash_price_krw = %s,
                    sale_price = %s,
                    fee_rate = %s,
                    net_amount = %s
                WHERE loot_item_id = %s
                """,
                (
                    _blank_to_none(buyer_name),
                    cash_amount,
                    sale_amount,
                    DEFAULT_DISTRIBUTION_FEE_RATE,
                    net_amount,
                    loot_item_id,
                ),
            )
            participant_counts = _get_loot_alliance_counts(cursor, loot_event_id)
            _upsert_distribution_for_loot(
                cursor,
                guild_id,
                loot_event_id,
                loot_item_id,
                sale_amount,
                participant_counts,
            )
        connection.commit()


def delete_loot_drop(guild_id: int, loot_event_id: int) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM distribution_batches db
                USING loot_events le
                WHERE db.loot_event_id = le.loot_event_id
                  AND le.guild_id = %s
                  AND le.loot_event_id = %s
                """,
                (guild_id, loot_event_id),
            )
            cursor.execute(
                """
                DELETE FROM loot_events
                WHERE guild_id = %s
                  AND loot_event_id = %s
                """,
                (guild_id, loot_event_id),
            )
            if cursor.rowcount == 0:
                raise ValueError("Loot event was not found.")
        connection.commit()


def get_loot_drop_events(guild_id: int, limit: int = 30) -> list[dict[str, Any]]:
    event_rows = _fetchall(
        """
        SELECT
            le.loot_event_id,
            le.guild_id,
            le.attendance_id,
            le.event_date,
            le.event_time_label,
            le.title,
            le.memo,
            le.adena_rate,
            le.created_by_discord_id,
            le.updated_at,
            s.started_at AS attendance_started_at,
            s.ended_at AS attendance_ended_at,
            li.loot_item_id,
            li.item_id,
            li.item_name_snapshot,
            li.buyer_name,
            li.cash_price_krw,
            li.sale_price,
            li.net_amount,
            db.distribution_id,
            db.total_sale_amount,
            db.total_net_amount,
            db.total_participant_count,
            db.fee_rate,
            db.fee_amount,
            db.status AS distribution_status
        FROM loot_events le
        LEFT JOIN attendance_sessions s ON s.attendance_id = le.attendance_id
        LEFT JOIN LATERAL (
            SELECT
                loot_item_id,
                item_id,
                item_name_snapshot,
                buyer_name,
                cash_price_krw,
                sale_price,
                net_amount
            FROM loot_event_items
            WHERE loot_event_id = le.loot_event_id
            ORDER BY loot_item_id ASC
            LIMIT 1
        ) li ON TRUE
        LEFT JOIN distribution_batches db ON db.loot_event_id = le.loot_event_id
        WHERE le.guild_id = %s
        ORDER BY
            le.event_date DESC,
            le.event_time_label DESC NULLS LAST,
            le.loot_event_id DESC
        LIMIT %s
        """,
        (guild_id, int(limit)),
    )
    event_ids = [int(row["loot_event_id"]) for row in event_rows]
    if not event_ids:
        return []

    payout_rows = _fetchall(
        """
        SELECT
            db.loot_event_id,
            p.alliance_id,
            COALESCE(a.alliance_name, '미분류') AS alliance_name,
            p.participant_count,
            p.gross_amount,
            p.net_amount,
            p.payout_status
        FROM distribution_batches db
        INNER JOIN distribution_alliance_payouts p
            ON p.distribution_id = db.distribution_id
        LEFT JOIN alliances a ON a.alliance_id = p.alliance_id
        WHERE db.loot_event_id = ANY(%s::bigint[])
        ORDER BY db.loot_event_id DESC, alliance_name ASC
        """,
        (event_ids,),
    )
    participant_rows = _fetchall(
        """
        SELECT
            lep.loot_event_id,
            COALESCE(a.alliance_name, '미분류') AS alliance_name,
            u.discord_nickname
        FROM loot_event_participants lep
        INNER JOIN users u ON u.user_id = lep.user_id
        LEFT JOIN alliances a ON a.alliance_id = lep.alliance_id
        WHERE lep.loot_event_id = ANY(%s::bigint[])
        ORDER BY lep.loot_event_id DESC, alliance_name ASC, u.discord_nickname ASC
        """,
        (event_ids,),
    )
    payouts_by_event: dict[int, list[dict[str, Any]]] = {event_id: [] for event_id in event_ids}
    for row in payout_rows:
        event_id = int(row["loot_event_id"])
        count = int(row["participant_count"] or 0)
        net_amount = _decimal(row["net_amount"])
        payouts_by_event.setdefault(event_id, []).append(
            {
                "alliance_id": _optional_int(row["alliance_id"]),
                "alliance_name": str(row["alliance_name"]),
                "participant_count": count,
                "gross_amount": _decimal(row["gross_amount"]),
                "net_amount": net_amount,
                "per_member_amount": _safe_divide(net_amount, count),
                "payout_status": str(row["payout_status"]),
            }
        )

    member_map: dict[int, dict[str, list[str]]] = {event_id: {} for event_id in event_ids}
    for row in participant_rows:
        event_id = int(row["loot_event_id"])
        alliance_name = str(row["alliance_name"])
        member_map.setdefault(event_id, {}).setdefault(alliance_name, []).append(
            str(row["discord_nickname"])
        )

    events: list[dict[str, Any]] = []
    for row in event_rows:
        loot_event_id = int(row["loot_event_id"])
        participant_count = int(row["total_participant_count"] or 0)
        total_net_amount = _decimal(row["total_net_amount"])
        alliances = [
            {
                "alliance_name": alliance_name,
                "count": len(members),
                "members": members,
            }
            for alliance_name, members in sorted(member_map.get(loot_event_id, {}).items())
        ]
        events.append(
            {
                "loot_event_id": loot_event_id,
                "attendance_id": _optional_int(row["attendance_id"]),
                "event_date": row["event_date"] or "",
                "event_time_label": row["event_time_label"] or "",
                "title": row["title"] or "",
                "memo": row["memo"] or "",
                "adena_rate": _decimal(row["adena_rate"]),
                "attendance_started_at": row["attendance_started_at"] or "",
                "attendance_ended_at": row["attendance_ended_at"] or "",
                "loot_item_id": _optional_int(row["loot_item_id"]),
                "item_id": _optional_int(row["item_id"]),
                "item_name": row["item_name_snapshot"] or "",
                "buyer_name": row["buyer_name"] or "",
                "cash_price_krw": _decimal(row["cash_price_krw"]),
                "sale_price": _decimal(row["sale_price"]),
                "net_amount": _decimal(row["net_amount"]),
                "distribution_id": _optional_int(row["distribution_id"]),
                "total_sale_amount": _decimal(row["total_sale_amount"]),
                "total_net_amount": total_net_amount,
                "total_participant_count": participant_count,
                "fee_rate": _decimal(row["fee_rate"]),
                "fee_amount": _decimal(row["fee_amount"]),
                "per_member_amount": _safe_divide(total_net_amount, participant_count),
                "distribution_status": row["distribution_status"] or "draft",
                "alliance_payouts": payouts_by_event.get(loot_event_id, []),
                "alliances": alliances,
                "updated_at": row["updated_at"],
            }
        )
    return events


def update_distribution_alliance_payout_status(
    guild_id: int,
    distribution_id: int,
    alliance_id: int,
    payout_status: str,
) -> None:
    if payout_status not in {"paid", "unpaid"}:
        raise ValueError("Payout status must be paid or unpaid.")

    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE distribution_alliance_payouts p
                SET payout_status = %s,
                    updated_at = CURRENT_TIMESTAMP
                FROM distribution_batches db
                WHERE p.distribution_id = db.distribution_id
                  AND db.guild_id = %s
                  AND p.distribution_id = %s
                  AND p.alliance_id = %s
                """,
                (payout_status, guild_id, distribution_id, alliance_id),
            )
            if cursor.rowcount == 0:
                raise ValueError("Distribution payout was not found.")
            cursor.execute(
                """
                UPDATE distribution_batches db
                SET status = CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM distribution_alliance_payouts p
                        WHERE p.distribution_id = db.distribution_id
                          AND p.payout_status <> 'paid'
                    )
                    THEN 'draft'
                    ELSE 'paid'
                END
                WHERE db.guild_id = %s
                  AND db.distribution_id = %s
                """,
                (guild_id, distribution_id),
            )
        connection.commit()


def _get_attendance_session_for_loot(
    cursor: psycopg2.extensions.cursor,
    guild_id: int,
    attendance_id: int,
) -> dict[str, Any]:
    cursor.execute(
        """
        SELECT attendance_id, guild_id, started_at, ended_at
        FROM attendance_sessions
        WHERE guild_id = %s
          AND attendance_id = %s
        """,
        (guild_id, attendance_id),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("Attendance session was not found.")
    return dict(row)


def _get_attendance_participants_for_loot(
    cursor: psycopg2.extensions.cursor,
    attendance_id: int,
) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT
            e.user_id,
            u.discord_id,
            u.discord_nickname,
            u.alliance_id
        FROM attendance_entries e
        INNER JOIN users u ON u.user_id = e.user_id
        WHERE e.attendance_id = %s
        ORDER BY u.discord_nickname ASC
        """,
        (attendance_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _resolve_loot_item(
    cursor: psycopg2.extensions.cursor,
    guild_id: int,
    item_id: int | None,
    item_name: str,
    fallback_price: Decimal,
) -> tuple[int | None, str]:
    if item_id is not None:
        cursor.execute(
            """
            SELECT item_id, item_name
            FROM items
            WHERE item_id = %s
              AND is_active = TRUE
            """,
            (item_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError("Item price setting was not found.")
        return int(row["item_id"]), str(row["item_name"])

    cursor.execute(
        """
        SELECT item_id, item_name
        FROM items
        WHERE LOWER(item_name) = LOWER(%s)
          AND is_active = TRUE
        ORDER BY
            CASE WHEN guild_id IS NULL THEN 0 ELSE 1 END,
            updated_at DESC,
            item_id ASC
        LIMIT 1
        """,
        (item_name,),
    )
    row = cursor.fetchone()
    if row is not None:
        return int(row["item_id"]), str(row["item_name"])

    cursor.execute(
        """
        INSERT INTO items (
            guild_id,
            item_name,
            default_price,
            is_bid_item,
            is_active,
            updated_at
        )
        VALUES (NULL, %s, %s, TRUE, TRUE, CURRENT_TIMESTAMP)
        RETURNING item_id, item_name
        """,
        (item_name, fallback_price),
    )
    inserted = cursor.fetchone()
    return int(inserted["item_id"]), str(inserted["item_name"])


def _upsert_distribution_for_loot(
    cursor: psycopg2.extensions.cursor,
    guild_id: int,
    loot_event_id: int,
    loot_item_id: int,
    total_amount: Decimal,
    participant_counts: dict[int, int],
) -> int:
    participant_total = sum(participant_counts.values())
    total_sale_amount = _decimal(total_amount)
    fee_rate = DEFAULT_DISTRIBUTION_FEE_RATE
    fee_amount = total_sale_amount * fee_rate
    total_net_amount = total_sale_amount - fee_amount
    cursor.execute(
        """
        SELECT distribution_id
        FROM distribution_batches
        WHERE loot_event_id = %s
        ORDER BY distribution_id ASC
        LIMIT 1
        """,
        (loot_event_id,),
    )
    distribution_row = cursor.fetchone()
    if distribution_row is None:
        cursor.execute(
            """
            INSERT INTO distribution_batches (
                guild_id,
                loot_event_id,
                total_sale_amount,
                total_net_amount,
                total_participant_count,
                fee_rate,
                fee_amount,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'draft')
            RETURNING distribution_id
            """,
            (
                guild_id,
                loot_event_id,
                total_sale_amount,
                total_net_amount,
                participant_total,
                fee_rate,
                fee_amount,
            ),
        )
        distribution_id = int(cursor.fetchone()["distribution_id"])
    else:
        distribution_id = int(distribution_row["distribution_id"])
        cursor.execute(
            """
            UPDATE distribution_batches
            SET guild_id = %s,
                total_sale_amount = %s,
                total_net_amount = %s,
                total_participant_count = %s,
                fee_rate = %s,
                fee_amount = %s
            WHERE distribution_id = %s
            """,
            (
                guild_id,
                total_sale_amount,
                total_net_amount,
                participant_total,
                fee_rate,
                fee_amount,
                distribution_id,
            ),
        )

    cursor.execute(
        """
        DELETE FROM distribution_lines
        WHERE distribution_id = %s
          AND line_type IN ('item', 'fee')
        """,
        (distribution_id,),
    )
    cursor.execute(
        """
        INSERT INTO distribution_lines (
            distribution_id,
            loot_item_id,
            line_type,
            amount,
            memo
        )
        VALUES (%s, %s, 'item', %s, NULL)
        """,
        (distribution_id, loot_item_id, total_sale_amount),
    )
    if fee_amount:
        cursor.execute(
            """
            INSERT INTO distribution_lines (
                distribution_id,
                loot_item_id,
                line_type,
                amount,
                memo
            )
            VALUES (%s, %s, 'fee', %s, '경리 수수료 10%%')
            """,
            (distribution_id, loot_item_id, fee_amount),
        )

    active_alliance_ids = list(participant_counts.keys())
    if active_alliance_ids:
        cursor.execute(
            """
            DELETE FROM distribution_alliance_payouts
            WHERE distribution_id = %s
              AND NOT (alliance_id = ANY(%s::bigint[]))
            """,
            (distribution_id, active_alliance_ids),
        )
    per_member_amount = _safe_divide(total_net_amount, participant_total)
    for alliance_id, count in participant_counts.items():
        alliance_amount = per_member_amount * Decimal(count)
        cursor.execute(
            """
            INSERT INTO distribution_alliance_payouts (
                distribution_id,
                alliance_id,
                participant_count,
                gross_amount,
                net_amount,
                payout_status,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, 'unpaid', CURRENT_TIMESTAMP)
            ON CONFLICT (distribution_id, alliance_id) DO UPDATE SET
                participant_count = EXCLUDED.participant_count,
                gross_amount = EXCLUDED.gross_amount,
                net_amount = EXCLUDED.net_amount,
                payout_status = distribution_alliance_payouts.payout_status,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                distribution_id,
                alliance_id,
                count,
                alliance_amount,
                alliance_amount,
            ),
        )
    return distribution_id


def _get_loot_alliance_counts(
    cursor: psycopg2.extensions.cursor,
    loot_event_id: int,
) -> dict[int, int]:
    cursor.execute(
        """
        SELECT alliance_id, participant_count
        FROM loot_event_alliance_counts
        WHERE loot_event_id = %s
        """,
        (loot_event_id,),
    )
    return {
        int(row["alliance_id"]): int(row["participant_count"] or 0)
        for row in cursor.fetchall()
    }


def _ensure_alliance_id(
    cursor: psycopg2.extensions.cursor,
    alliance_name: str,
) -> int:
    normalized_name = alliance_name.strip()
    cursor.execute(
        """
        INSERT INTO alliances (alliance_name, display_name, tag_name, is_active)
        VALUES (%s, %s, %s, TRUE)
        ON CONFLICT (alliance_name) DO UPDATE SET
            is_active = TRUE,
            updated_at = CURRENT_TIMESTAMP
        RETURNING alliance_id
        """,
        (normalized_name, normalized_name, normalized_name),
    )
    return int(cursor.fetchone()["alliance_id"])


def _deactivate_duplicate_item_prices(
    cursor: psycopg2.extensions.cursor,
    keep_item_id: int,
    item_name: str,
) -> None:
    cursor.execute(
        """
        UPDATE items
        SET is_active = FALSE,
            updated_at = CURRENT_TIMESTAMP
        WHERE item_id <> %s
          AND LOWER(item_name) = LOWER(%s)
          AND is_active = TRUE
        """,
        (keep_item_id, item_name),
    )


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


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _safe_divide(value: Decimal, divisor: int) -> Decimal:
    if divisor <= 0:
        return Decimal("0")
    return _decimal(value) / Decimal(divisor)


def _date_label_from_text(value: str) -> str:
    return value[:10] if len(value) >= 10 else value


def _time_label_from_text(value: str) -> str:
    if len(value) >= 16:
        return value[11:16]
    return value


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
        "ALTER TABLE items ADD COLUMN IF NOT EXISTS guild_id BIGINT REFERENCES guilds(guild_id) ON DELETE CASCADE",
        "ALTER TABLE loot_events ADD COLUMN IF NOT EXISTS attendance_id BIGINT REFERENCES attendance_sessions(attendance_id) ON DELETE SET NULL",
        "ALTER TABLE loot_events ADD COLUMN IF NOT EXISTS adena_rate NUMERIC(18, 6) NOT NULL DEFAULT 0",
        "ALTER TABLE loot_event_items ADD COLUMN IF NOT EXISTS cash_price_krw NUMERIC(18, 2) NOT NULL DEFAULT 0",
        "ALTER TABLE distribution_batches ADD COLUMN IF NOT EXISTS guild_id BIGINT REFERENCES guilds(guild_id) ON DELETE CASCADE",
        "ALTER TABLE distribution_batches ADD COLUMN IF NOT EXISTS fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0",
        "ALTER TABLE distribution_batches ADD COLUMN IF NOT EXISTS fee_amount NUMERIC(18, 2) NOT NULL DEFAULT 0",
        "ALTER TABLE distribution_alliance_payouts ADD COLUMN IF NOT EXISTS payout_status TEXT NOT NULL DEFAULT 'unpaid'",
        "ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE scheduled_report_settings ADD COLUMN IF NOT EXISTS run_time TEXT NOT NULL DEFAULT '00:00'",
        "ALTER TABLE scheduled_report_settings ADD COLUMN IF NOT EXISTS schedule_json JSONB NOT NULL DEFAULT '{\"type\":\"daily\",\"time\":\"00:00\",\"timezone\":\"Asia/Seoul\"}'::jsonb",
        "ALTER TABLE scheduled_report_settings ADD COLUMN IF NOT EXISTS query_json JSONB NOT NULL DEFAULT '{\"dataset\":\"attendance\",\"period\":\"today\",\"group_by\":\"alliance\",\"rank_target\":\"user\",\"metric\":\"attendance_count\",\"limit\":10}'::jsonb",
        "ALTER TABLE scheduled_report_settings ADD COLUMN IF NOT EXISTS render_json JSONB NOT NULL DEFAULT '{\"output\":\"grouped_ranking\",\"title\":\"금일 혈맹별 출석 랭킹 TOP10\",\"group_header\":\"{group_name}\",\"row\":\"{rank}. {label} - {value}회\",\"empty\":\"출석 기록 없음\"}'::jsonb",
    ]
    for sql in column_sql:
        cursor.execute(sql)
    cursor.execute("ALTER TABLE scheduled_report_settings DROP CONSTRAINT IF EXISTS chk_scheduled_report_frequency")
    cursor.execute("ALTER TABLE scheduled_report_settings DROP CONSTRAINT IF EXISTS chk_scheduled_report_period_type")
    cursor.execute("ALTER TABLE scheduled_report_settings DROP CONSTRAINT IF EXISTS chk_scheduled_report_subject_type")
    cursor.execute("ALTER TABLE scheduled_report_settings DROP CONSTRAINT IF EXISTS chk_scheduled_report_result_type")
    cursor.execute("ALTER TABLE items DROP CONSTRAINT IF EXISTS items_item_name_key")
    cursor.execute("DROP INDEX IF EXISTS idx_items_guild_name_unique")
    cursor.execute("DROP INDEX IF EXISTS idx_items_active_name_unique")
    cursor.execute(
        """
        WITH ranked AS (
            SELECT
                item_id,
                ROW_NUMBER() OVER (
                    PARTITION BY LOWER(item_name)
                    ORDER BY
                        CASE WHEN guild_id IS NULL THEN 0 ELSE 1 END,
                        updated_at DESC,
                        item_id ASC
                ) AS item_rank
            FROM items
            WHERE is_active = TRUE
        )
        UPDATE items i
        SET
            guild_id = NULL,
            is_active = (ranked.item_rank = 1),
            updated_at = CURRENT_TIMESTAMP
        FROM ranked
        WHERE i.item_id = ranked.item_id
          AND (i.guild_id IS NOT NULL OR ranked.item_rank > 1)
        """
    )
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_items_active_name_unique "
        "ON items(LOWER(item_name)) WHERE is_active = TRUE"
    )
    cursor.execute(
        """
        UPDATE distribution_batches db
        SET guild_id = le.guild_id
        FROM loot_events le
        WHERE db.loot_event_id = le.loot_event_id
          AND db.guild_id IS NULL
        """
    )
    cursor.execute(
        """
        UPDATE loot_event_items
        SET fee_rate = %s,
            net_amount = sale_price * (1 - %s)
        WHERE sale_price > 0
          AND fee_rate = 0
        """,
        (DEFAULT_DISTRIBUTION_FEE_RATE, DEFAULT_DISTRIBUTION_FEE_RATE),
    )
    cursor.execute(
        """
        UPDATE distribution_batches
        SET fee_rate = %s,
            fee_amount = total_sale_amount * %s,
            total_net_amount = total_sale_amount * (1 - %s)
        WHERE total_sale_amount > 0
          AND fee_rate = 0
        """,
        (
            DEFAULT_DISTRIBUTION_FEE_RATE,
            DEFAULT_DISTRIBUTION_FEE_RATE,
            DEFAULT_DISTRIBUTION_FEE_RATE,
        ),
    )
    cursor.execute(
        """
        UPDATE distribution_alliance_payouts p
        SET gross_amount = (
                db.total_net_amount / NULLIF(db.total_participant_count, 0)
            ) * p.participant_count,
            net_amount = (
                db.total_net_amount / NULLIF(db.total_participant_count, 0)
            ) * p.participant_count,
            updated_at = CURRENT_TIMESTAMP
        FROM distribution_batches db
        WHERE p.distribution_id = db.distribution_id
          AND db.total_participant_count > 0
          AND db.fee_rate = %s
        """,
        (DEFAULT_DISTRIBUTION_FEE_RATE,),
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_items_guild_active "
        "ON items(guild_id, is_active)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_loot_events_guild_attendance "
        "ON loot_events(guild_id, attendance_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_loot_events_guild_updated "
        "ON loot_events(guild_id, updated_at)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_distribution_batches_guild "
        "ON distribution_batches(guild_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_command_queue_guild_status "
        "ON bot_command_queue(guild_id, status, created_at)"
    )


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

CREATE TABLE IF NOT EXISTS guild_alliance_role_mappings (
    mapping_id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
    role_id BIGINT NOT NULL,
    role_name TEXT NOT NULL,
    alliance_id BIGINT NOT NULL REFERENCES alliances(alliance_id) ON DELETE CASCADE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (guild_id, role_id)
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

CREATE TABLE IF NOT EXISTS scheduled_report_settings (
    report_setting_id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
    created_by_discord_id BIGINT NOT NULL,
    updated_by_discord_id BIGINT,
    report_name TEXT,
    frequency TEXT NOT NULL,
    period_type TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    result_type TEXT NOT NULL,
    run_time TEXT NOT NULL DEFAULT '00:00',
    channel_id BIGINT NOT NULL,
    channel_name TEXT NOT NULL,
    schedule_json JSONB NOT NULL DEFAULT '{"type":"daily","time":"00:00","timezone":"Asia/Seoul"}'::jsonb,
    query_json JSONB NOT NULL DEFAULT '{"dataset":"attendance","period":"today","group_by":"alliance","rank_target":"user","metric":"attendance_count","limit":10}'::jsonb,
    render_json JSONB NOT NULL DEFAULT '{"output":"grouped_ranking","title":"금일 혈맹별 출석 랭킹 TOP10","group_header":"{group_name}","row":"{rank}. {label} - {value}회","empty":"출석 기록 없음"}'::jsonb,
    status TEXT NOT NULL DEFAULT 'off',
    last_sent_at TEXT,
    next_run_at TEXT,
    memo TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_scheduled_report_frequency
        CHECK (frequency IN ('daily', 'every_3_days', 'weekly', 'monthly')),
    CONSTRAINT chk_scheduled_report_period_type
        CHECK (period_type IN ('today', 'recent_7_days', 'yesterday', 'recent_3_days', 'this_week', 'this_month')),
    CONSTRAINT chk_scheduled_report_subject_type
        CHECK (subject_type IN ('user', 'alliance')),
    CONSTRAINT chk_scheduled_report_result_type
        CHECK (result_type IN ('ranking', 'all', 'grouped_ranking')),
    CONSTRAINT chk_scheduled_report_status
        CHECK (status IN ('on', 'off', 'delete'))
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
    guild_id BIGINT REFERENCES guilds(guild_id) ON DELETE CASCADE,
    item_name TEXT NOT NULL,
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
    attendance_id BIGINT REFERENCES attendance_sessions(attendance_id) ON DELETE SET NULL,
    event_date TEXT NOT NULL,
    event_time_label TEXT,
    title TEXT,
    memo TEXT,
    adena_rate NUMERIC(18, 6) NOT NULL DEFAULT 0,
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
    cash_price_krw NUMERIC(18, 2) NOT NULL DEFAULT 0,
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
    guild_id BIGINT REFERENCES guilds(guild_id) ON DELETE CASCADE,
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
CREATE INDEX IF NOT EXISTS idx_guild_alliance_role_mappings_guild
ON guild_alliance_role_mappings(guild_id);
CREATE INDEX IF NOT EXISTS idx_attendance_sessions_guild_started ON attendance_sessions(guild_id, started_at);
CREATE INDEX IF NOT EXISTS idx_attendance_entries_user_id ON attendance_entries(user_id);
CREATE INDEX IF NOT EXISTS idx_attendance_entries_attendance_id ON attendance_entries(attendance_id);
CREATE INDEX IF NOT EXISTS idx_bot_command_queue_status ON bot_command_queue(status, created_at);
CREATE INDEX IF NOT EXISTS idx_bot_command_queue_guild_status ON bot_command_queue(guild_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_scheduled_report_settings_guild_status
ON scheduled_report_settings(guild_id, status);
CREATE INDEX IF NOT EXISTS idx_scheduled_report_settings_next_run
ON scheduled_report_settings(status, next_run_at);
CREATE INDEX IF NOT EXISTS idx_attendance_live_sessions_guild_status ON attendance_live_sessions(guild_id, status);
CREATE INDEX IF NOT EXISTS idx_items_name ON items(item_name);
CREATE INDEX IF NOT EXISTS idx_loot_events_date ON loot_events(event_date);
"""
