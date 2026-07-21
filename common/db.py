from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_FLOOR
from typing import Any
from urllib.parse import quote

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json, RealDictCursor


load_dotenv()

DEFAULT_DISTRIBUTION_FEE_RATE = Decimal("0.10")
TEST_DB_FLAG = "--test"
KST = timezone(timedelta(hours=9))


def is_test_database_mode() -> bool:
    return TEST_DB_FLAG in sys.argv or os.getenv("LINEAGE_DB_TARGET", "").lower() in {
        "test",
        "remote",
    }


def _now_kst_text() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def _datetime_bound_text(value: str | datetime | None) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        normalized = value
        if normalized.tzinfo is not None:
            normalized = normalized.astimezone(KST).replace(tzinfo=None)
        return normalized.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


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

    def get_guild_visibility_map(self, guild_ids: list[int]) -> dict[int, bool]:
        return get_guild_visibility_map(guild_ids)

    def get_developer_guild_rows(self) -> list[dict[str, Any]]:
        return get_developer_guild_rows()

    def set_guild_enabled(self, guild_id: int, is_enabled: bool) -> None:
        set_guild_enabled(guild_id, is_enabled)

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

    def update_attendance_voice_channel_ids(
        self,
        guild_id: int,
        channel_ids: list[int],
    ) -> GuildSettings:
        return update_attendance_voice_channel_ids(guild_id, channel_ids)

    def get_recent_attendance_sessions(
        self,
        guild_id: int,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        return get_recent_attendance_sessions(guild_id, limit)

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

    def get_guild_bookkeepers(self, guild_id: int) -> list[dict[str, Any]]:
        return get_guild_bookkeepers(guild_id)

    def get_guild_bookkeeper_candidates(self, guild_id: int) -> list[dict[str, Any]]:
        return get_guild_bookkeeper_candidates(guild_id)

    def add_guild_bookkeeper(
        self,
        guild_id: int,
        user_id: int,
        *,
        added_by_discord_id: int | None,
    ) -> None:
        add_guild_bookkeeper(
            guild_id,
            user_id,
            added_by_discord_id=added_by_discord_id,
        )

    def delete_guild_bookkeeper(self, guild_id: int, user_id: int) -> None:
        delete_guild_bookkeeper(guild_id, user_id)

    def is_guild_bookkeeper(self, guild_id: int, discord_id: int) -> bool:
        return is_guild_bookkeeper(guild_id, discord_id)

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
        start_at: str | datetime | None = None,
        end_at: str | datetime | None = None,
    ) -> list[dict[str, Any]]:
        return get_attendance_status_sessions(
            guild_id,
            limit,
            offset,
            start_at,
            end_at,
        )

    def count_attendance_status_sessions(
        self,
        guild_id: int,
        start_at: str | datetime | None = None,
        end_at: str | datetime | None = None,
    ) -> int:
        return count_attendance_status_sessions(guild_id, start_at, end_at)

    def get_attendance_edit_candidates(
        self,
        guild_id: int,
        attendance_id: int,
    ) -> list[dict[str, Any]]:
        return get_attendance_edit_candidates(guild_id, attendance_id)

    def add_attendance_entry(
        self,
        guild_id: int,
        attendance_id: int,
        user_id: int,
    ) -> None:
        add_attendance_entry(guild_id, attendance_id, user_id)

    def delete_attendance_entry(
        self,
        guild_id: int,
        attendance_id: int,
        user_id: int,
    ) -> None:
        delete_attendance_entry(guild_id, attendance_id, user_id)

    def add_work_log(
        self,
        guild_id: int,
        *,
        actor_discord_id: int | None,
        actor_display_name: str,
        actor_role: str,
        action_type: str,
        target_type: str,
        target_id: int | None,
        summary: str,
        details: dict[str, Any] | None = None,
    ) -> int:
        return add_work_log(
            guild_id,
            actor_discord_id=actor_discord_id,
            actor_display_name=actor_display_name,
            actor_role=actor_role,
            action_type=action_type,
            target_type=target_type,
            target_id=target_id,
            summary=summary,
            details=details,
        )

    def get_work_logs(
        self,
        guild_id: int,
        *,
        action_type: str | None = None,
        limit: int = 120,
    ) -> list[dict[str, Any]]:
        return get_work_logs(guild_id, action_type=action_type, limit=limit)

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

    def get_bid_item_dashboard(self, guild_id: int) -> dict[str, Any]:
        return get_bid_item_dashboard(guild_id)

    def upsert_bid_item(
        self,
        guild_id: int,
        *,
        item_name: str,
        is_free: bool = False,
        bid_item_id: int | None = None,
    ) -> dict[str, Any]:
        return upsert_bid_item(
            guild_id,
            item_name=item_name,
            is_free=is_free,
            bid_item_id=bid_item_id,
        )

    def deactivate_bid_item(self, guild_id: int, bid_item_id: int) -> dict[str, Any]:
        return deactivate_bid_item(guild_id, bid_item_id)

    def set_bid_item_alliance_status(
        self,
        guild_id: int,
        bid_item_id: int,
        *,
        alliance_id: int,
        is_completed: bool,
        updated_by_discord_id: int | None,
    ) -> dict[str, Any]:
        return set_bid_item_alliance_status(
            guild_id,
            bid_item_id,
            alliance_id=alliance_id,
            is_completed=is_completed,
            updated_by_discord_id=updated_by_discord_id,
        )

    def import_bid_item_sheet(
        self,
        guild_id: int,
        items: list[str],
        completed: dict[str, set[str]],
    ) -> dict[str, int]:
        return import_bid_item_sheet(guild_id, items, completed)

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
        fee_rate: Decimal = DEFAULT_DISTRIBUTION_FEE_RATE,
        bookkeeper_fee_rate: Decimal | None = None,
        alliance_fee_rate: Decimal | None = None,
        memo: str | None,
        created_by_discord_id: int | None,
        excluded_alliance_ids: list[int] | None = None,
    ) -> int:
        return create_loot_drop(
            guild_id,
            attendance_id=attendance_id,
            item_id=item_id,
            item_name=item_name,
            cash_price_krw=cash_price_krw,
            sale_price=sale_price,
            adena_rate=adena_rate,
            fee_rate=fee_rate,
            bookkeeper_fee_rate=bookkeeper_fee_rate,
            alliance_fee_rate=alliance_fee_rate,
            memo=memo,
            created_by_discord_id=created_by_discord_id,
            excluded_alliance_ids=excluded_alliance_ids,
        )

    def create_basic_loot_drop(
        self,
        guild_id: int,
        *,
        attendance_id: int,
        item_name: str,
        created_by_discord_id: int | None,
    ) -> int:
        return create_basic_loot_drop(
            guild_id,
            attendance_id=attendance_id,
            item_name=item_name,
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
        fee_rate: Decimal = DEFAULT_DISTRIBUTION_FEE_RATE,
        bookkeeper_fee_rate: Decimal | None = None,
        alliance_fee_rate: Decimal | None = None,
        memo: str | None,
    ) -> None:
        update_loot_drop(
            guild_id,
            loot_event_id,
            cash_price_krw=cash_price_krw,
            sale_price=sale_price,
            adena_rate=adena_rate,
            fee_rate=fee_rate,
            bookkeeper_fee_rate=bookkeeper_fee_rate,
            alliance_fee_rate=alliance_fee_rate,
            memo=memo,
        )

    def delete_loot_drop(self, guild_id: int, loot_event_id: int) -> None:
        delete_loot_drop(guild_id, loot_event_id)

    def get_loot_drop_events(
        self,
        guild_id: int,
        limit: int = 30,
        start_at: str | datetime | None = None,
        end_at: str | datetime | None = None,
    ) -> list[dict[str, Any]]:
        return get_loot_drop_events(guild_id, limit, start_at, end_at)

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

    def update_all_distribution_alliance_payout_status(
        self,
        guild_id: int,
        distribution_id: int,
        payout_status: str,
    ) -> None:
        update_all_distribution_alliance_payout_status(
            guild_id,
            distribution_id,
            payout_status,
        )

    def get_alliance_payout_fee_rules(
        self,
        guild_id: int,
        alliance_id: int,
    ) -> list[dict[str, Any]]:
        return get_alliance_payout_fee_rules(guild_id, alliance_id)

    def create_alliance_payout_fee_rule(
        self,
        guild_id: int,
        alliance_id: int,
        *,
        rule_name: str,
        fee_rate: Decimal,
        created_by_discord_id: int | None,
    ) -> int:
        return create_alliance_payout_fee_rule(
            guild_id,
            alliance_id,
            rule_name=rule_name,
            fee_rate=fee_rate,
            created_by_discord_id=created_by_discord_id,
        )

    def deactivate_alliance_payout_fee_rule(
        self,
        guild_id: int,
        alliance_id: int,
        rule_id: int,
    ) -> None:
        deactivate_alliance_payout_fee_rule(guild_id, alliance_id, rule_id)

    def update_alliance_payout_fee_rule(
        self,
        guild_id: int,
        alliance_id: int,
        rule_id: int,
        *,
        rule_name: str,
        fee_rate: Decimal,
    ) -> None:
        update_alliance_payout_fee_rule(
            guild_id,
            alliance_id,
            rule_id,
            rule_name=rule_name,
            fee_rate=fee_rate,
        )

    def get_member_payout_groups(
        self,
        guild_id: int,
        alliance_id: int,
    ) -> dict[int, dict[str, Any]]:
        return get_member_payout_groups(guild_id, alliance_id)

    def settle_member_payout(
        self,
        guild_id: int,
        distribution_id: int,
        alliance_id: int,
        *,
        updated_by_discord_id: int | None,
    ) -> int:
        return settle_member_payout(
            guild_id,
            distribution_id,
            alliance_id,
            updated_by_discord_id=updated_by_discord_id,
        )

    def settle_all_member_payouts(
        self,
        guild_id: int,
        alliance_id: int,
        *,
        updated_by_discord_id: int | None,
    ) -> int:
        return settle_all_member_payouts(
            guild_id,
            alliance_id,
            updated_by_discord_id=updated_by_discord_id,
        )

    def update_member_payout_recipient_status(
        self,
        guild_id: int,
        distribution_id: int,
        alliance_id: int,
        user_id: int,
        payout_status: str,
        *,
        updated_by_discord_id: int | None,
    ) -> int:
        return update_member_payout_recipient_status(
            guild_id,
            distribution_id,
            alliance_id,
            user_id,
            payout_status,
            updated_by_discord_id=updated_by_discord_id,
        )

    def get_member_forfeiture_settlements(
        self,
        guild_id: int,
        alliance_ids: list[int],
    ) -> dict[tuple[int, int, int], dict[str, Any]]:
        return get_member_forfeiture_settlements(guild_id, alliance_ids)

    def settle_member_forfeitures(
        self,
        guild_id: int,
        alliance_id: int,
        *,
        settled_by_discord_id: int | None,
    ) -> int:
        return settle_member_forfeitures(
            guild_id,
            alliance_id,
            settled_by_discord_id=settled_by_discord_id,
        )

    def get_loot_fee_settlements(
        self,
        guild_id: int,
        distribution_ids: list[int],
    ) -> dict[tuple[int, int, str], dict[str, Any]]:
        return get_loot_fee_settlements(guild_id, distribution_ids)

    def settle_loot_fee(
        self,
        guild_id: int,
        distribution_id: int,
        *,
        alliance_id: int | None,
        fee_key: str,
        fee_label: str,
        fee_rate: Decimal,
        fee_amount: Decimal,
        settled_by_discord_id: int | None,
    ) -> None:
        settle_loot_fee(
            guild_id,
            distribution_id,
            alliance_id=alliance_id,
            fee_key=fee_key,
            fee_label=fee_label,
            fee_rate=fee_rate,
            fee_amount=fee_amount,
            settled_by_discord_id=settled_by_discord_id,
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
            _drop_obsolete_member_payout_tables(cursor)
            _drop_obsolete_loot_boss_schema(cursor)
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


def get_guild_visibility_map(guild_ids: list[int]) -> dict[int, bool]:
    if not guild_ids:
        return {}
    rows = _fetchall(
        """
        SELECT guild_id, is_enabled
        FROM guilds
        WHERE guild_id = ANY(%s::bigint[])
        """,
        ([int(guild_id) for guild_id in guild_ids],),
    )
    return {int(row["guild_id"]): bool(row["is_enabled"]) for row in rows}


def get_developer_guild_rows() -> list[dict[str, Any]]:
    return _fetchall(
        """
        SELECT
            g.guild_id,
            g.is_enabled,
            gs.admin_channel_id,
            gs.attendance_voice_channel_id,
            gs.log_channel_id,
            COUNT(DISTINCT s.attendance_id) AS session_count,
            COUNT(e.user_id) AS attendance_count,
            MAX(CASE WHEN e.user_id IS NOT NULL THEN s.started_at END)
                AS last_attendance_at,
            MAX(s.started_at) AS last_session_started_at
        FROM guilds g
        LEFT JOIN guild_settings gs ON gs.guild_id = g.guild_id
        LEFT JOIN attendance_sessions s ON s.guild_id = g.guild_id
        LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
        GROUP BY
            g.guild_id,
            g.is_enabled,
            gs.admin_channel_id,
            gs.attendance_voice_channel_id,
            gs.log_channel_id
        ORDER BY
            g.is_enabled DESC,
            MAX(CASE WHEN e.user_id IS NOT NULL THEN s.started_at END) DESC NULLS LAST,
            MAX(s.started_at) DESC NULLS LAST,
            g.guild_id ASC
        """
    )


def set_guild_enabled(guild_id: int, is_enabled: bool) -> None:
    ensure_guild(guild_id)
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE guilds
                SET is_enabled = %s
                WHERE guild_id = %s
                """,
                (bool(is_enabled), guild_id),
            )
        connection.commit()


def get_configured_guild_id() -> int | None:
    row = _fetchone("SELECT guild_id FROM guilds ORDER BY guild_id LIMIT 1")
    return int(row["guild_id"]) if row else None


def get_settings(guild_id: int) -> GuildSettings:
    ensure_guild(guild_id)
    row = _fetchone(
        """
        SELECT guild_id, admin_channel_id, attendance_voice_channel_id,
               attendance_voice_channel_ids, log_channel_id, timer,
               attendance_available_timer
        FROM guild_settings
        WHERE guild_id = %s
        """,
        (guild_id,),
    )
    if row is None:
        return GuildSettings(guild_id=guild_id)
    voice_channel_ids = tuple(_normalize_id_list(row["attendance_voice_channel_ids"]))
    legacy_voice_channel_id = _optional_int(row["attendance_voice_channel_id"])
    if not voice_channel_ids and legacy_voice_channel_id is not None:
        voice_channel_ids = (legacy_voice_channel_id,)
    return GuildSettings(
        guild_id=int(row["guild_id"]),
        admin_channel_id=_optional_int(row["admin_channel_id"]),
        attendance_voice_channel_id=voice_channel_ids[0] if voice_channel_ids else None,
        attendance_voice_channel_ids=voice_channel_ids,
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


def update_attendance_voice_channel_ids(
    guild_id: int,
    channel_ids: list[int],
) -> GuildSettings:
    normalized_ids = _normalize_id_list(channel_ids)
    ensure_guild(guild_id)
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE guild_settings
                SET
                    attendance_voice_channel_id = %s,
                    attendance_voice_channel_ids = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE guild_id = %s
                """,
                (
                    normalized_ids[0] if normalized_ids else None,
                    Json(normalized_ids),
                    guild_id,
                ),
            )
        connection.commit()
    return get_settings(guild_id)


def get_recent_attendance_sessions(
    guild_id: int,
    limit: int = 25,
) -> list[dict[str, Any]]:
    ensure_guild(guild_id)
    normalized_limit = max(1, min(int(limit), 100))
    return _fetchall(
        """
        SELECT
            s.attendance_id,
            s.started_at,
            s.ended_at,
            s.started_by_discord_id,
            COUNT(e.user_id)::int AS participant_count
        FROM attendance_sessions s
        LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
        WHERE s.guild_id = %s
        GROUP BY
            s.attendance_id,
            s.started_at,
            s.ended_at,
            s.started_by_discord_id
        ORDER BY s.started_at DESC, s.attendance_id DESC
        LIMIT %s
        """,
        (guild_id, normalized_limit),
    )


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


def _guild_member_user_rows_sql(exclude_bookkeepers: bool) -> str:
    exclusion = ""
    if exclude_bookkeepers:
        exclusion = """
          AND NOT EXISTS (
              SELECT 1
              FROM guild_bookkeepers gb
              WHERE gb.guild_id = %s
                AND gb.user_id = u.user_id
          )
        """
    return f"""
        SELECT
            u.user_id,
            u.discord_id,
            u.discord_nickname,
            COALESCE(a.alliance_name, '미분류') AS alliance_name
        FROM users u
        LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
        WHERE EXISTS (
            SELECT 1
            FROM attendance_entries e
            INNER JOIN attendance_sessions s
                ON s.attendance_id = e.attendance_id
            WHERE e.user_id = u.user_id
              AND s.guild_id = %s
        )
        {exclusion}
        ORDER BY alliance_name ASC, u.discord_nickname ASC
    """


def get_guild_bookkeepers(guild_id: int) -> list[dict[str, Any]]:
    rows = _fetchall(
        """
        SELECT
            gb.guild_id,
            gb.user_id,
            gb.discord_id,
            gb.added_by_discord_id,
            gb.updated_at,
            u.discord_nickname,
            COALESCE(a.alliance_name, '미분류') AS alliance_name
        FROM guild_bookkeepers gb
        INNER JOIN users u ON u.user_id = gb.user_id
        LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
        WHERE gb.guild_id = %s
        ORDER BY alliance_name ASC, u.discord_nickname ASC
        """,
        (guild_id,),
    )
    return [
        {
            "guild_id": int(row["guild_id"]),
            "user_id": int(row["user_id"]),
            "discord_id": int(row["discord_id"]),
            "discord_nickname": str(row["discord_nickname"]),
            "alliance_name": str(row["alliance_name"]),
            "added_by_discord_id": (
                int(row["added_by_discord_id"])
                if row["added_by_discord_id"] is not None
                else None
            ),
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def get_guild_bookkeeper_candidates(guild_id: int) -> list[dict[str, Any]]:
    rows = _fetchall(
        _guild_member_user_rows_sql(exclude_bookkeepers=True),
        (guild_id, guild_id),
    )
    return [
        {
            "user_id": int(row["user_id"]),
            "discord_id": int(row["discord_id"]),
            "discord_nickname": str(row["discord_nickname"]),
            "alliance_name": str(row["alliance_name"]),
        }
        for row in rows
    ]


def add_guild_bookkeeper(
    guild_id: int,
    user_id: int,
    *,
    added_by_discord_id: int | None,
) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                _guild_member_user_rows_sql(exclude_bookkeepers=False),
                (guild_id,),
            )
            candidates = {int(row["user_id"]): int(row["discord_id"]) for row in cursor.fetchall()}
            discord_id = candidates.get(int(user_id))
            if discord_id is None:
                raise ValueError("User was not found.")
            cursor.execute(
                """
                INSERT INTO guild_bookkeepers (
                    guild_id,
                    user_id,
                    discord_id,
                    added_by_discord_id
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET
                    discord_id = EXCLUDED.discord_id,
                    added_by_discord_id = EXCLUDED.added_by_discord_id,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (guild_id, user_id, discord_id, added_by_discord_id),
            )
        connection.commit()


def delete_guild_bookkeeper(guild_id: int, user_id: int) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM guild_bookkeepers
                WHERE guild_id = %s AND user_id = %s
                """,
                (guild_id, user_id),
            )
        connection.commit()


def is_guild_bookkeeper(guild_id: int, discord_id: int) -> bool:
    row = _fetchone(
        """
        SELECT 1
        FROM guild_bookkeepers
        WHERE guild_id = %s AND discord_id = %s
        LIMIT 1
        """,
        (guild_id, discord_id),
    )
    return row is not None


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


def count_attendance_status_sessions(
    guild_id: int,
    start_at: str | datetime | None = None,
    end_at: str | datetime | None = None,
) -> int:
    conditions = ["guild_id = %s"]
    params: list[Any] = [guild_id]
    start_bound = _datetime_bound_text(start_at)
    end_bound = _datetime_bound_text(end_at)
    if start_bound:
        conditions.append("started_at >= %s")
        params.append(start_bound)
    if end_bound:
        conditions.append("started_at <= %s")
        params.append(end_bound)
    row = _fetchone(
        f"""
        SELECT COUNT(*) AS session_count
        FROM attendance_sessions
        WHERE {' AND '.join(conditions)}
        """,
        tuple(params),
    )
    return int(row["session_count"] or 0) if row else 0


def get_attendance_status_sessions(
    guild_id: int,
    limit: int = 10,
    offset: int = 0,
    start_at: str | datetime | None = None,
    end_at: str | datetime | None = None,
) -> list[dict[str, Any]]:
    conditions = ["s.guild_id = %s"]
    params: list[Any] = [guild_id]
    start_bound = _datetime_bound_text(start_at)
    end_bound = _datetime_bound_text(end_at)
    if start_bound:
        conditions.append("s.started_at >= %s")
        params.append(start_bound)
    if end_bound:
        conditions.append("s.started_at <= %s")
        params.append(end_bound)
    params.extend([int(limit), int(offset)])
    session_rows = _fetchall(
        f"""
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
        WHERE {' AND '.join(conditions)}
        GROUP BY
            s.attendance_id,
            s.started_at,
            s.ended_at,
            s.started_by_discord_id
        ORDER BY s.started_at DESC
        LIMIT %s
        OFFSET %s
        """,
        tuple(params),
    )
    session_ids = [int(row["attendance_id"]) for row in session_rows]
    if not session_ids:
        return []

    participant_rows = _fetchall(
        """
        SELECT
            e.attendance_id,
            u.alliance_id,
            COALESCE(a.alliance_name, '미분류') AS alliance_name,
            u.user_id,
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
    grouped: dict[int, dict[int | None, dict[str, Any]]] = {
        attendance_id: {} for attendance_id in session_ids
    }
    for row in participant_rows:
        attendance_id = int(row["attendance_id"])
        alliance_name = str(row["alliance_name"])
        alliance_id = _optional_int(row["alliance_id"])
        group = grouped.setdefault(attendance_id, {}).setdefault(
            alliance_id,
            {
                "alliance_id": alliance_id,
                "alliance_name": alliance_name,
                "members": [],
            },
        )
        group["members"].append(
            {
                "user_id": int(row["user_id"]),
                "discord_id": int(row["discord_id"]),
                "discord_nickname": str(row["discord_nickname"]),
            }
        )

    sessions: list[dict[str, Any]] = []
    for row in session_rows:
        attendance_id = int(row["attendance_id"])
        alliances = [
            {
                "alliance_id": group["alliance_id"],
                "alliance_name": group["alliance_name"],
                "count": len(group["members"]),
                "members": group["members"],
            }
            for group in sorted(
                grouped.get(attendance_id, {}).values(),
                key=lambda item: str(item["alliance_name"]),
            )
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


def get_attendance_edit_candidates(
    guild_id: int,
    attendance_id: int,
) -> list[dict[str, Any]]:
    rows = _fetchall(
        """
        SELECT DISTINCT
            u.user_id,
            u.discord_id,
            u.discord_nickname,
            COALESCE(a.alliance_name, '미분류') AS alliance_name
        FROM users u
        INNER JOIN attendance_entries e ON e.user_id = u.user_id
        INNER JOIN attendance_sessions s ON s.attendance_id = e.attendance_id
        LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
        WHERE s.guild_id = %s
          AND NOT EXISTS (
              SELECT 1
              FROM attendance_entries current_entry
              WHERE current_entry.attendance_id = %s
                AND current_entry.user_id = u.user_id
          )
        ORDER BY alliance_name ASC, u.discord_nickname ASC
        """,
        (guild_id, attendance_id),
    )
    return [
        {
            "user_id": int(row["user_id"]),
            "discord_id": int(row["discord_id"]),
            "discord_nickname": str(row["discord_nickname"]),
            "alliance_name": str(row["alliance_name"]),
        }
        for row in rows
    ]


def add_attendance_entry(guild_id: int, attendance_id: int, user_id: int) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            _get_attendance_session_for_loot(cursor, guild_id, attendance_id)
            cursor.execute(
                """
                SELECT u.user_id
                FROM users u
                WHERE u.user_id = %s
                  AND EXISTS (
                      SELECT 1
                      FROM attendance_entries e
                      INNER JOIN attendance_sessions s
                          ON s.attendance_id = e.attendance_id
                      WHERE e.user_id = u.user_id
                        AND s.guild_id = %s
                  )
                """,
                (user_id, guild_id),
            )
            if cursor.fetchone() is None:
                raise ValueError("User was not found.")
            cursor.execute(
                """
                INSERT INTO attendance_entries (attendance_id, user_id)
                VALUES (%s, %s)
                ON CONFLICT (attendance_id, user_id) DO NOTHING
                """,
                (attendance_id, user_id),
            )
            _rebuild_loot_for_attendance(cursor, guild_id, attendance_id)
        connection.commit()


def delete_attendance_entry(guild_id: int, attendance_id: int, user_id: int) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            _get_attendance_session_for_loot(cursor, guild_id, attendance_id)
            cursor.execute(
                """
                DELETE FROM attendance_entries
                WHERE attendance_id = %s
                  AND user_id = %s
                """,
                (attendance_id, user_id),
            )
            _rebuild_loot_for_attendance(cursor, guild_id, attendance_id)
        connection.commit()


def add_work_log(
    guild_id: int,
    *,
    actor_discord_id: int | None,
    actor_display_name: str,
    actor_role: str,
    action_type: str,
    target_type: str,
    target_id: int | None,
    summary: str,
    details: dict[str, Any] | None = None,
) -> int:
    ensure_guild(guild_id)
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO work_logs (
                    guild_id,
                    actor_discord_id,
                    actor_display_name,
                    actor_role,
                    action_type,
                    target_type,
                    target_id,
                    summary,
                    details_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING work_log_id
                """,
                (
                    guild_id,
                    actor_discord_id,
                    actor_display_name.strip() or str(actor_discord_id or ""),
                    actor_role,
                    action_type,
                    target_type,
                    target_id,
                    summary.strip(),
                    Json(details or {}),
                ),
            )
            work_log_id = int(cursor.fetchone()["work_log_id"])
        connection.commit()
    return work_log_id


def get_work_logs(
    guild_id: int,
    *,
    action_type: str | None = None,
    limit: int = 120,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 300))
    params: list[Any] = [guild_id]
    where = "WHERE guild_id = %s"
    if action_type:
        where += " AND action_type = %s"
        params.append(action_type)
    params.append(safe_limit)
    rows = _fetchall(
        f"""
        SELECT
            work_log_id,
            guild_id,
            actor_discord_id,
            actor_display_name,
            actor_role,
            action_type,
            target_type,
            target_id,
            summary,
            details_json,
            created_at
        FROM work_logs
        {where}
        ORDER BY created_at DESC, work_log_id DESC
        LIMIT %s
        """,
        tuple(params),
    )
    return [
        {
            "work_log_id": int(row["work_log_id"]),
            "guild_id": int(row["guild_id"]),
            "actor_discord_id": (
                int(row["actor_discord_id"])
                if row["actor_discord_id"] is not None
                else None
            ),
            "actor_display_name": str(row["actor_display_name"]),
            "actor_role": str(row["actor_role"]),
            "action_type": str(row["action_type"]),
            "target_type": str(row["target_type"]),
            "target_id": (
                int(row["target_id"])
                if row["target_id"] is not None
                else None
            ),
            "summary": str(row["summary"]),
            "details": row["details_json"] or {},
            "created_at": row["created_at"],
        }
        for row in rows
    ]


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
                    '전체'::text AS group_name,
                    {label_expr} AS label,
                    {value_expr} AS value,
                    ROW_NUMBER() OVER (
                        ORDER BY {value_expr} DESC, {label_expr} ASC
                    ) AS rank
                FROM attendance_sessions s
                INNER JOIN attendance_entries e ON e.attendance_id = s.attendance_id
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
            """,
            (guild_id, start_at, end_at, safe_limit),
        )
    else:
        group_select = (
            "COALESCE(a.alliance_name, '미분류')"
            if group_by == "alliance"
            else "'전체'::text"
        )
        partition_sql = (
            f"PARTITION BY {group_select}"
            if group_by == "alliance"
            else ""
        )
        group_by_sql = (
            f"{group_select}, u.user_id, u.discord_nickname"
            if group_by == "alliance"
            else "u.user_id, u.discord_nickname"
        )
        value_expr = "COUNT(e.user_id)"
        rows = _fetchall(
            f"""
            WITH ranked AS (
                SELECT
                    {group_select} AS group_name,
                    u.discord_nickname AS label,
                    {value_expr} AS value,
                    ROW_NUMBER() OVER (
                        {partition_sql}
                        ORDER BY {value_expr} DESC, u.discord_nickname ASC
                    ) AS rank
                FROM attendance_sessions s
                INNER JOIN attendance_entries e ON e.attendance_id = s.attendance_id
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


def _bid_candidate_alliances(cursor: psycopg2.extensions.cursor, guild_id: int) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT DISTINCT
            a.alliance_id,
            a.alliance_name,
            a.sort_order
        FROM guild_alliance_role_mappings m
        INNER JOIN alliances a ON a.alliance_id = m.alliance_id
        WHERE m.guild_id = %s
          AND a.is_active = TRUE
        ORDER BY a.sort_order ASC NULLS LAST, a.alliance_name ASC
        """,
        (guild_id,),
    )
    return [
        {
            "alliance_id": int(row["alliance_id"]),
            "alliance_name": str(row["alliance_name"]),
            "sort_order": int(row["sort_order"] or 0),
        }
        for row in cursor.fetchall()
    ]


def _bid_items(cursor: psycopg2.extensions.cursor, guild_id: int) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT
            bid_item_id,
            item_name,
            sort_order,
            is_free,
            memo,
            updated_at
        FROM bid_items
        WHERE guild_id = %s
          AND is_active = TRUE
        ORDER BY is_free ASC, LOWER(item_name) ASC, item_name ASC, bid_item_id ASC
        """,
        (guild_id,),
    )
    return [
        {
            "bid_item_id": int(row["bid_item_id"]),
            "item_name": str(row["item_name"]),
            "sort_order": int(row["sort_order"] or 0),
            "is_free": bool(row["is_free"]),
            "bid_type_label": "무료나눔" if bool(row["is_free"]) else "유료",
            "memo": row["memo"] or "",
            "updated_at": row["updated_at"],
        }
        for row in cursor.fetchall()
    ]


def _bid_result_rows(
    cursor: psycopg2.extensions.cursor,
    guild_id: int,
    bid_item_ids: list[int],
) -> list[dict[str, Any]]:
    if not bid_item_ids:
        return []
    cursor.execute(
        """
        SELECT
            r.result_id,
            r.bid_item_id,
            r.alliance_id,
            a.alliance_name,
            r.cycle_no,
            r.selected_by_discord_id,
            r.selected_at,
            r.memo
        FROM bid_item_results r
        INNER JOIN alliances a ON a.alliance_id = r.alliance_id
        WHERE r.guild_id = %s
          AND r.bid_item_id = ANY(%s::bigint[])
        ORDER BY r.bid_item_id ASC, r.cycle_no DESC, r.selected_at DESC, r.result_id DESC
        """,
        (guild_id, bid_item_ids),
    )
    return [dict(row) for row in cursor.fetchall()]


def _bid_item_state(
    item: dict[str, Any],
    candidates: list[dict[str, Any]],
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    candidate_ids = [int(alliance["alliance_id"]) for alliance in candidates]
    candidate_by_id = {int(alliance["alliance_id"]): alliance for alliance in candidates}
    cycles = [int(row["cycle_no"] or 1) for row in results]
    current_cycle = max(cycles) if cycles else 1
    completed_rows = [
        row
        for row in results
        if int(row["cycle_no"] or 1) == current_cycle
        and int(row["alliance_id"]) in candidate_by_id
    ]
    completed_ids = {int(row["alliance_id"]) for row in completed_rows}
    has_completed_cycle = bool(candidate_ids) and len(completed_ids) >= len(candidate_ids)
    if has_completed_cycle:
        current_cycle += 1
        completed_rows = []
        completed_ids = set()
    remaining = [
        alliance
        for alliance in candidates
        if int(alliance["alliance_id"]) not in completed_ids
    ]
    completed_by_alliance = {
        int(row["alliance_id"]): row for row in completed_rows
    }
    history_by_alliance: dict[int, list[dict[str, Any]]] = {
        alliance_id: [] for alliance_id in candidate_ids
    }
    for row in results:
        alliance_id = int(row["alliance_id"])
        if alliance_id in history_by_alliance:
            history_by_alliance[alliance_id].append(row)
    history_by_cycle: dict[int, list[dict[str, Any]]] = {}
    for row in results:
        alliance_id = int(row["alliance_id"])
        if alliance_id not in candidate_by_id:
            continue
        history_by_cycle.setdefault(int(row["cycle_no"] or 1), []).append(row)
    cycle_history = []
    for cycle_no in sorted(history_by_cycle, reverse=True):
        rows = sorted(
            history_by_cycle[cycle_no],
            key=lambda row: (
                int(candidate_by_id[int(row["alliance_id"])].get("sort_order", 0))
                if "sort_order" in candidate_by_id[int(row["alliance_id"])]
                else 0,
                str(row["alliance_name"]),
            ),
        )
        cycle_history.append(
            {
                "cycle_no": cycle_no,
                "records": [
                    {
                        "result_id": int(row["result_id"]),
                        "alliance_id": int(row["alliance_id"]),
                        "alliance_name": str(row["alliance_name"]),
                        "selected_at": str(row["selected_at"] or ""),
                        "selected_by_discord_id": _optional_int(row["selected_by_discord_id"]),
                        "memo": row.get("memo") or "",
                    }
                    for row in rows
                ],
            }
        )
    alliance_statuses = []
    for alliance in candidates:
        alliance_id = int(alliance["alliance_id"])
        current_row = completed_by_alliance.get(alliance_id)
        is_completed = current_row is not None
        alliance_statuses.append(
            {
                "alliance_id": alliance_id,
                "alliance_name": str(alliance["alliance_name"]),
                "is_completed": is_completed,
                "status_label": "완료" if is_completed else "대기",
                "next_is_completed": not is_completed,
                "next_label": "해제" if is_completed else "완료 처리",
                "selected_at": str((current_row or {}).get("selected_at") or ""),
                "cycle_no": current_cycle,
                "history": [
                    {
                        "result_id": int(history["result_id"]),
                        "cycle_no": int(history["cycle_no"] or 1),
                        "selected_at": str(history["selected_at"] or ""),
                        "selected_by_discord_id": _optional_int(history["selected_by_discord_id"]),
                        "memo": history.get("memo") or "",
                    }
                    for history in history_by_alliance.get(alliance_id, [])
                ],
            }
        )
    return {
        **item,
        "cycle_no": current_cycle,
        "has_completed_cycle": has_completed_cycle,
        "candidate_count": len(candidate_ids),
        "completed_count": len(completed_ids),
        "remaining_count": len(remaining),
        "progress_text": f"{len(completed_ids)} / {len(candidate_ids)}",
        "completed_alliances": [
            {
                "alliance_id": int(row["alliance_id"]),
                "alliance_name": str(row["alliance_name"]),
                "selected_at": str(row["selected_at"] or ""),
            }
            for row in completed_rows
        ],
        "remaining_alliances": remaining,
        "history": [
            {
                "result_id": int(row["result_id"]),
                "alliance_id": int(row["alliance_id"]),
                "alliance_name": str(row["alliance_name"]),
                "cycle_no": int(row["cycle_no"] or 1),
                "selected_at": str(row["selected_at"] or ""),
                "selected_by_discord_id": _optional_int(row["selected_by_discord_id"]),
            }
            for row in results[:8]
        ],
        "alliance_statuses": alliance_statuses,
        "cycle_history": cycle_history,
    }


def get_bid_item_dashboard(guild_id: int) -> dict[str, Any]:
    ensure_guild(guild_id)
    with _connect() as connection:
        with connection.cursor() as cursor:
            candidates = _bid_candidate_alliances(cursor, guild_id)
            items = _bid_items(cursor, guild_id)
            bid_item_ids = [int(item["bid_item_id"]) for item in items]
            results = _bid_result_rows(cursor, guild_id, bid_item_ids)

    results_by_item: dict[int, list[dict[str, Any]]] = {
        int(item["bid_item_id"]): [] for item in items
    }
    for result in results:
        results_by_item.setdefault(int(result["bid_item_id"]), []).append(result)
    states = [
        _bid_item_state(item, candidates, results_by_item.get(int(item["bid_item_id"]), []))
        for item in items
    ]
    table_rows = []
    for alliance in candidates:
        alliance_id = int(alliance["alliance_id"])
        cells = []
        for item in states:
            status = next(
                (
                    entry
                    for entry in item["alliance_statuses"]
                    if int(entry["alliance_id"]) == alliance_id
                ),
                None,
            )
            cells.append(
                {
                    **(status or {}),
                    "bid_item_id": int(item["bid_item_id"]),
                    "item_name": item["item_name"],
                    "is_free": bool(item["is_free"]),
                    "cycle_no": int(item["cycle_no"]),
                    "progress_text": item["progress_text"],
                }
            )
        table_rows.append({**alliance, "cells": cells})
    ready_count = sum(1 for item in states if item["remaining_count"] > 0)
    return {
        "items": states,
        "alliances": candidates,
        "table_rows": table_rows,
        "summary": {
            "item_count": len(states),
            "alliance_count": len(candidates),
            "ready_count": ready_count,
            "completed_count": len(states) - ready_count,
        },
    }


def upsert_bid_item(
    guild_id: int,
    *,
    item_name: str,
    is_free: bool = False,
    bid_item_id: int | None = None,
) -> dict[str, Any]:
    ensure_guild(guild_id)
    normalized_name = item_name.strip()
    if not normalized_name:
        raise ValueError("아이템 이름을 입력해주세요.")

    with _connect() as connection:
        with connection.cursor() as cursor:
            if bid_item_id:
                cursor.execute(
                    """
                    UPDATE bid_items
                    SET item_name = %s,
                        is_free = %s,
                        is_active = TRUE,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE guild_id = %s
                      AND bid_item_id = %s
                    RETURNING bid_item_id, item_name, sort_order, is_free
                    """,
                    (normalized_name, is_free, guild_id, bid_item_id),
                )
                row = cursor.fetchone()
                if row is None:
                    raise ValueError("입찰 아이템을 찾을 수 없습니다.")
            else:
                cursor.execute(
                    """
                    SELECT COALESCE(MAX(sort_order), 0) + 1 AS next_sort_order
                    FROM bid_items
                    WHERE guild_id = %s
                    """,
                    (guild_id,),
                )
                next_sort_order = int(cursor.fetchone()["next_sort_order"] or 1)
                cursor.execute(
                    """
                    INSERT INTO bid_items (
                        guild_id,
                        item_name,
                        sort_order,
                        is_free,
                        is_active,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                    ON CONFLICT (guild_id, (lower(item_name)))
                    DO UPDATE SET
                        item_name = EXCLUDED.item_name,
                        is_free = EXCLUDED.is_free,
                        is_active = TRUE,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING bid_item_id, item_name, sort_order, is_free
                    """,
                    (guild_id, normalized_name, next_sort_order, is_free),
                )
                row = cursor.fetchone()
        connection.commit()
    return {
        "bid_item_id": int(row["bid_item_id"]),
        "item_name": str(row["item_name"]),
        "sort_order": int(row["sort_order"] or 0),
        "is_free": bool(row["is_free"]),
    }


def deactivate_bid_item(guild_id: int, bid_item_id: int) -> dict[str, Any]:
    ensure_guild(guild_id)
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE bid_items
                SET is_active = FALSE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE guild_id = %s
                  AND bid_item_id = %s
                  AND is_active = TRUE
                RETURNING bid_item_id, item_name, is_free
                """,
                (guild_id, bid_item_id),
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError("입찰 아이템을 찾을 수 없습니다.")
        connection.commit()
    return {
        "bid_item_id": int(row["bid_item_id"]),
        "item_name": str(row["item_name"]),
        "is_free": bool(row["is_free"]),
    }


def set_bid_item_alliance_status(
    guild_id: int,
    bid_item_id: int,
    *,
    alliance_id: int,
    is_completed: bool,
    updated_by_discord_id: int | None,
) -> dict[str, Any]:
    ensure_guild(guild_id)
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (bid_item_id,))
            candidates = _bid_candidate_alliances(cursor, guild_id)
            if not candidates:
                raise ValueError("등록된 혈맹 역할 매핑이 없습니다.")
            candidate_by_id = {
                int(candidate["alliance_id"]): candidate for candidate in candidates
            }
            alliance_id = int(alliance_id)
            if alliance_id not in candidate_by_id:
                raise ValueError("입찰 대상 혈맹이 아닙니다.")
            cursor.execute(
                """
                SELECT bid_item_id, item_name, sort_order, memo, updated_at
                FROM bid_items
                WHERE guild_id = %s
                  AND bid_item_id = %s
                  AND is_active = TRUE
                """,
                (guild_id, bid_item_id),
            )
            item_row = cursor.fetchone()
            if item_row is None:
                raise ValueError("입찰 아이템을 찾을 수 없습니다.")
            item = {
                "bid_item_id": int(item_row["bid_item_id"]),
                "item_name": str(item_row["item_name"]),
                "sort_order": int(item_row["sort_order"] or 0),
                "memo": item_row["memo"] or "",
                "updated_at": item_row["updated_at"],
            }
            results = _bid_result_rows(cursor, guild_id, [bid_item_id])
            state = _bid_item_state(item, candidates, results)
            selected_at = ""
            result_id = None
            if is_completed:
                selected_at = _now_kst_text()
                cursor.execute(
                    """
                    INSERT INTO bid_item_results (
                        guild_id,
                        bid_item_id,
                        alliance_id,
                        cycle_no,
                        selected_by_discord_id,
                        selected_at,
                        memo
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (guild_id, bid_item_id, alliance_id, cycle_no)
                    DO UPDATE SET
                        selected_by_discord_id = EXCLUDED.selected_by_discord_id,
                        selected_at = EXCLUDED.selected_at,
                        memo = EXCLUDED.memo
                    RETURNING result_id
                    """,
                    (
                        guild_id,
                        bid_item_id,
                        alliance_id,
                        int(state["cycle_no"]),
                        updated_by_discord_id,
                        selected_at,
                        "manual",
                    ),
                )
                result_id = int(cursor.fetchone()["result_id"])
            else:
                cursor.execute(
                    """
                    DELETE FROM bid_item_results
                    WHERE guild_id = %s
                      AND bid_item_id = %s
                      AND alliance_id = %s
                      AND cycle_no = %s
                    RETURNING result_id
                    """,
                    (guild_id, bid_item_id, alliance_id, int(state["cycle_no"])),
                )
                deleted = cursor.fetchone()
                result_id = int(deleted["result_id"]) if deleted else None
            results = _bid_result_rows(cursor, guild_id, [bid_item_id])
            state = _bid_item_state(item, candidates, results)
        connection.commit()
    alliance_state = next(
        (
            status
            for status in state["alliance_statuses"]
            if int(status["alliance_id"]) == alliance_id
        ),
        None,
    )
    return {
        "result_id": result_id,
        "bid_item_id": bid_item_id,
        "item_name": item["item_name"],
        "alliance_id": alliance_id,
        "alliance_name": str(candidate_by_id[alliance_id]["alliance_name"]),
        "cycle_no": int(state["cycle_no"]),
        "selected_at": selected_at,
        "is_completed": bool((alliance_state or {}).get("is_completed", is_completed)),
        "status_label": (alliance_state or {}).get("status_label") or ("완료" if is_completed else "대기"),
        "next_is_completed": (alliance_state or {}).get("next_is_completed", not is_completed),
        "next_label": (alliance_state or {}).get("next_label") or ("해제" if is_completed else "완료 처리"),
        "item": {
            "bid_item_id": bid_item_id,
            "cycle_no": state["cycle_no"],
            "progress_text": state["progress_text"],
            "completed_count": state["completed_count"],
            "candidate_count": state["candidate_count"],
            "remaining_count": state["remaining_count"],
            "alliance_statuses": state["alliance_statuses"],
            "cycle_history": state["cycle_history"],
        },
    }


def import_bid_item_sheet(
    guild_id: int,
    items: list[str],
    completed: dict[str, set[str]],
) -> dict[str, int]:
    ensure_guild(guild_id)
    normalized_items = [item.strip() for item in items if item and item.strip()]
    imported_items = 0
    imported_results = 0
    with _connect() as connection:
        with connection.cursor() as cursor:
            candidates = _bid_candidate_alliances(cursor, guild_id)
            alliance_by_name = {
                str(alliance["alliance_name"]).strip(): int(alliance["alliance_id"])
                for alliance in candidates
            }
            for index, item_name in enumerate(normalized_items, start=1):
                cursor.execute(
                    """
                    INSERT INTO bid_items (
                        guild_id,
                        item_name,
                        sort_order,
                        is_active,
                        updated_at
                    )
                    VALUES (%s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                    ON CONFLICT (guild_id, (lower(item_name)))
                    DO UPDATE SET
                        item_name = EXCLUDED.item_name,
                        sort_order = COALESCE(bid_items.sort_order, EXCLUDED.sort_order),
                        is_active = TRUE,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING bid_item_id
                    """,
                    (guild_id, item_name, index),
                )
                bid_item_id = int(cursor.fetchone()["bid_item_id"])
                imported_items += 1
                for alliance_name in sorted(completed.get(item_name, set())):
                    alliance_id = alliance_by_name.get(alliance_name.strip())
                    if alliance_id is None:
                        continue
                    cursor.execute(
                        """
                        INSERT INTO bid_item_results (
                            guild_id,
                            bid_item_id,
                            alliance_id,
                            cycle_no,
                            selected_at,
                            memo
                        )
                        VALUES (%s, %s, %s, 1, %s, 'xlsx import')
                        ON CONFLICT (guild_id, bid_item_id, alliance_id, cycle_no)
                        DO NOTHING
                        """,
                        (guild_id, bid_item_id, alliance_id, _now_kst_text()),
                    )
                    imported_results += cursor.rowcount
        connection.commit()
    return {"items": imported_items, "results": imported_results}


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


def _normalize_distribution_fee_rates(
    fee_rate: Decimal,
    bookkeeper_fee_rate: Decimal | None = None,
    alliance_fee_rate: Decimal | None = None,
) -> tuple[Decimal, Decimal, Decimal]:
    if bookkeeper_fee_rate is None and alliance_fee_rate is None:
        bookkeeper_rate = _decimal(fee_rate)
        alliance_rate = Decimal("0")
    else:
        bookkeeper_rate = _decimal(bookkeeper_fee_rate or Decimal("0"))
        alliance_rate = _decimal(alliance_fee_rate or Decimal("0"))
    if bookkeeper_rate < 0 or alliance_rate < 0:
        raise ValueError("Fee rate must not be negative.")
    return bookkeeper_rate + alliance_rate, bookkeeper_rate, alliance_rate


def create_basic_loot_drop(
    guild_id: int,
    *,
    attendance_id: int,
    item_name: str,
    created_by_discord_id: int | None,
) -> int:
    ensure_guild(guild_id)
    normalized_item_name = item_name.strip()
    if not normalized_item_name:
        raise ValueError("Item name must not be empty.")

    with _connect() as connection:
        with connection.cursor() as cursor:
            session = _get_attendance_session_for_loot(cursor, guild_id, attendance_id)
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
                    excluded_alliance_ids,
                    created_by_discord_id,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NULL, 0, %s, %s, CURRENT_TIMESTAMP)
                RETURNING loot_event_id
                """,
                (
                    guild_id,
                    attendance_id,
                    _date_label_from_text(started_at),
                    _time_label_from_text(started_at),
                    normalized_item_name,
                    Json([]),
                    created_by_discord_id,
                ),
            )
            loot_event_id = int(cursor.fetchone()["loot_event_id"])
            cursor.execute(
                """
                INSERT INTO loot_event_items (
                    loot_event_id,
                    item_id,
                    item_name_snapshot,
                    cash_price_krw,
                    sale_price,
                    fee_rate,
                    bookkeeper_fee_rate,
                    alliance_fee_rate,
                    net_amount
                )
                VALUES (%s, NULL, %s, 0, 0, 0, 0, 0, 0)
                """,
                (loot_event_id, normalized_item_name),
            )
        connection.commit()
    return loot_event_id


def create_loot_drop(
    guild_id: int,
    *,
    attendance_id: int,
    item_id: int | None,
    item_name: str,
    cash_price_krw: Decimal,
    sale_price: Decimal,
    adena_rate: Decimal,
    fee_rate: Decimal = DEFAULT_DISTRIBUTION_FEE_RATE,
    bookkeeper_fee_rate: Decimal | None = None,
    alliance_fee_rate: Decimal | None = None,
    memo: str | None,
    created_by_discord_id: int | None,
    excluded_alliance_ids: list[int] | None = None,
) -> int:
    ensure_guild(guild_id)
    normalized_item_name = item_name.strip()
    if item_id is None and not normalized_item_name:
        raise ValueError("Item name must not be empty.")

    cash_amount = _decimal(cash_price_krw)
    sale_amount = _decimal(sale_price)
    normalized_fee_rate, bookkeeper_rate, alliance_rate = _normalize_distribution_fee_rates(
        fee_rate,
        bookkeeper_fee_rate,
        alliance_fee_rate,
    )
    net_amount = sale_amount - (sale_amount * normalized_fee_rate)
    excluded_ids = _normalize_alliance_id_list(excluded_alliance_ids)
    excluded_id_set = set(excluded_ids)
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
                    excluded_alliance_ids,
                    created_by_discord_id,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
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
                    Json(excluded_ids),
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
                if alliance_id in excluded_id_set:
                    continue
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

            if not participant_counts:
                raise ValueError("분배 대상 혈맹을 1개 이상 포함해주세요.")

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
                    cash_price_krw,
                    sale_price,
                    fee_rate,
                    bookkeeper_fee_rate,
                    alliance_fee_rate,
                    net_amount
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING loot_item_id
                """,
                (
                    loot_event_id,
                    resolved_item_id,
                    resolved_item_name,
                    cash_amount,
                    sale_amount,
                    normalized_fee_rate,
                    bookkeeper_rate,
                    alliance_rate,
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
                normalized_fee_rate,
                bookkeeper_rate,
                alliance_rate,
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
    fee_rate: Decimal = DEFAULT_DISTRIBUTION_FEE_RATE,
    bookkeeper_fee_rate: Decimal | None = None,
    alliance_fee_rate: Decimal | None = None,
    memo: str | None,
) -> None:
    cash_amount = _decimal(cash_price_krw)
    sale_amount = _decimal(sale_price)
    normalized_fee_rate, bookkeeper_rate, alliance_rate = _normalize_distribution_fee_rates(
        fee_rate,
        bookkeeper_fee_rate,
        alliance_fee_rate,
    )
    net_amount = sale_amount - (sale_amount * normalized_fee_rate)
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
                SET cash_price_krw = %s,
                    sale_price = %s,
                    fee_rate = %s,
                    bookkeeper_fee_rate = %s,
                    alliance_fee_rate = %s,
                    net_amount = %s
                WHERE loot_item_id = %s
                """,
                (
                    cash_amount,
                    sale_amount,
                    normalized_fee_rate,
                    bookkeeper_rate,
                    alliance_rate,
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
                normalized_fee_rate,
                bookkeeper_rate,
                alliance_rate,
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


def get_loot_drop_events(
    guild_id: int,
    limit: int = 30,
    start_at: str | datetime | None = None,
    end_at: str | datetime | None = None,
) -> list[dict[str, Any]]:
    conditions = ["le.guild_id = %s"]
    params: list[Any] = [guild_id]
    start_bound = _datetime_bound_text(start_at)
    end_bound = _datetime_bound_text(end_at)
    if start_bound:
        conditions.append("s.started_at >= %s")
        params.append(start_bound)
    if end_bound:
        conditions.append("s.started_at <= %s")
        params.append(end_bound)
    params.append(int(limit))
    event_rows = _fetchall(
        f"""
        SELECT
            le.loot_event_id,
            le.guild_id,
            le.attendance_id,
            le.event_date,
            le.event_time_label,
            le.title,
            le.memo,
            le.adena_rate,
            le.excluded_alliance_ids,
            le.created_by_discord_id,
            le.updated_at,
            s.started_at AS attendance_started_at,
            s.ended_at AS attendance_ended_at,
            li.loot_item_id,
            li.item_id,
            li.item_name_snapshot,
            li.cash_price_krw,
            li.sale_price,
            li.net_amount,
            db.distribution_id,
            db.total_sale_amount,
            db.total_net_amount,
            db.total_participant_count,
            db.fee_rate,
            db.fee_amount,
            db.bookkeeper_fee_rate,
            db.bookkeeper_fee_amount,
            db.alliance_fee_rate,
            db.alliance_fee_amount
        FROM loot_events le
        LEFT JOIN attendance_sessions s ON s.attendance_id = le.attendance_id
        LEFT JOIN LATERAL (
            SELECT
                loot_item_id,
                item_id,
                item_name_snapshot,
                cash_price_krw,
                sale_price,
                net_amount
            FROM loot_event_items
            WHERE loot_event_id = le.loot_event_id
            ORDER BY loot_item_id ASC
            LIMIT 1
        ) li ON TRUE
        LEFT JOIN distribution_batches db ON db.loot_event_id = le.loot_event_id
        WHERE {' AND '.join(conditions)}
        ORDER BY
            le.event_date DESC,
            le.event_time_label DESC NULLS LAST,
            le.loot_event_id DESC
        LIMIT %s
        """,
        tuple(params),
    )
    event_ids = [int(row["loot_event_id"]) for row in event_rows]
    if not event_ids:
        return []
    excluded_by_event = {
        int(row["loot_event_id"]): _normalize_alliance_id_list(
            row.get("excluded_alliance_ids"),
        )
        for row in event_rows
    }
    excluded_ids = sorted(
        {
            alliance_id
            for ids in excluded_by_event.values()
            for alliance_id in ids
        }
    )
    excluded_name_map: dict[int, str] = {}
    if excluded_ids:
        excluded_rows = _fetchall(
            """
            SELECT alliance_id, alliance_name
            FROM alliances
            WHERE alliance_id = ANY(%s::bigint[])
            """,
            (excluded_ids,),
        )
        excluded_name_map = {
            int(row["alliance_id"]): str(row["alliance_name"])
            for row in excluded_rows
        }

    payout_rows = _fetchall(
        """
        SELECT
            db.loot_event_id,
            p.alliance_id,
            COALESCE(a.alliance_name, '미분류') AS alliance_name,
            p.participant_count,
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
            u.user_id,
            u.discord_id,
            COALESCE(a.alliance_name, '미분류') AS alliance_name,
            lep.alliance_id,
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
                "net_amount": net_amount,
                "per_member_amount": _safe_divide(net_amount, count),
                "payout_status": str(row["payout_status"]),
            }
        )

    member_map: dict[int, dict[int | None, dict[str, Any]]] = {
        event_id: {} for event_id in event_ids
    }
    for row in participant_rows:
        event_id = int(row["loot_event_id"])
        alliance_name = str(row["alliance_name"])
        alliance_id = _optional_int(row["alliance_id"])
        group = member_map.setdefault(event_id, {}).setdefault(
            alliance_id,
            {
                "alliance_id": alliance_id,
                "alliance_name": alliance_name,
                "members": [],
            },
        )
        group["members"].append(
            {
                "user_id": int(row["user_id"]),
                "discord_id": int(row["discord_id"]),
                "discord_nickname": str(row["discord_nickname"]),
                "alliance_id": alliance_id,
            }
        )

    events: list[dict[str, Any]] = []
    for row in event_rows:
        loot_event_id = int(row["loot_event_id"])
        participant_count = int(row["total_participant_count"] or 0)
        total_net_amount = _decimal(row["total_net_amount"])
        excluded_alliance_ids = excluded_by_event.get(loot_event_id, [])
        excluded_alliances = [
            {
                "alliance_id": alliance_id,
                "alliance_name": excluded_name_map.get(alliance_id, str(alliance_id)),
            }
            for alliance_id in excluded_alliance_ids
        ]
        alliances = [
            {
                "alliance_id": group["alliance_id"],
                "alliance_name": group["alliance_name"],
                "count": len(group["members"]),
                "members": group["members"],
            }
            for group in sorted(
                member_map.get(loot_event_id, {}).values(),
                key=lambda item: str(item["alliance_name"]),
            )
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
                "cash_price_krw": _decimal(row["cash_price_krw"]),
                "sale_price": _decimal(row["sale_price"]),
                "net_amount": _decimal(row["net_amount"]),
                "distribution_id": _optional_int(row["distribution_id"]),
                "total_sale_amount": _decimal(row["total_sale_amount"]),
                "total_net_amount": total_net_amount,
                "total_participant_count": participant_count,
                "fee_rate": _decimal(row["fee_rate"]),
                "fee_amount": _decimal(row["fee_amount"]),
                "bookkeeper_fee_rate": _decimal(row["bookkeeper_fee_rate"]),
                "bookkeeper_fee_amount": _decimal(row["bookkeeper_fee_amount"]),
                "alliance_fee_rate": _decimal(row["alliance_fee_rate"]),
                "alliance_fee_amount": _decimal(row["alliance_fee_amount"]),
                "per_member_amount": _safe_divide(total_net_amount, participant_count),
                "alliance_payouts": payouts_by_event.get(loot_event_id, []),
                "alliances": alliances,
                "excluded_alliance_ids": excluded_alliance_ids,
                "excluded_alliances": excluded_alliances,
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
        connection.commit()


def update_all_distribution_alliance_payout_status(
    guild_id: int,
    distribution_id: int,
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
                """,
                (payout_status, guild_id, distribution_id),
            )
            if cursor.rowcount == 0:
                raise ValueError("Distribution payout was not found.")
        connection.commit()


def get_alliance_payout_fee_rules(
    guild_id: int,
    alliance_id: int,
) -> list[dict[str, Any]]:
    rows = _fetchall(
        """
        SELECT
            rule_id,
            guild_id,
            alliance_id,
            rule_name,
            fee_rate,
            sort_order,
            updated_at
        FROM alliance_payout_fee_rules
        WHERE guild_id = %s
          AND alliance_id = %s
          AND is_active = TRUE
        ORDER BY sort_order ASC, rule_id ASC
        """,
        (guild_id, alliance_id),
    )
    return [
        {
            "rule_id": int(row["rule_id"]),
            "guild_id": int(row["guild_id"]),
            "alliance_id": int(row["alliance_id"]),
            "rule_name": str(row["rule_name"]),
            "fee_rate": _decimal(row["fee_rate"]),
            "sort_order": int(row["sort_order"] or 0),
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def create_alliance_payout_fee_rule(
    guild_id: int,
    alliance_id: int,
    *,
    rule_name: str,
    fee_rate: Decimal,
    created_by_discord_id: int | None,
) -> int:
    normalized_name = rule_name.strip()
    normalized_fee_rate = _decimal(fee_rate)
    if not normalized_name:
        raise ValueError("Fee rule name must not be empty.")
    if normalized_fee_rate < 0:
        raise ValueError("Fee rule rate must not be negative.")

    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COALESCE(MAX(sort_order), 0) + 1 AS next_order
                FROM alliance_payout_fee_rules
                WHERE guild_id = %s
                  AND alliance_id = %s
                """,
                (guild_id, alliance_id),
            )
            next_order = int(cursor.fetchone()["next_order"] or 1)
            cursor.execute(
                """
                INSERT INTO alliance_payout_fee_rules (
                    guild_id,
                    alliance_id,
                    rule_name,
                    fee_rate,
                    sort_order,
                    is_active,
                    created_by_discord_id,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, TRUE, %s, CURRENT_TIMESTAMP)
                RETURNING rule_id
                """,
                (
                    guild_id,
                    alliance_id,
                    normalized_name,
                    normalized_fee_rate,
                    next_order,
                    created_by_discord_id,
                ),
            )
            rule_id = int(cursor.fetchone()["rule_id"])
        connection.commit()
    return rule_id


def deactivate_alliance_payout_fee_rule(
    guild_id: int,
    alliance_id: int,
    rule_id: int,
) -> None:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE alliance_payout_fee_rules
                SET is_active = FALSE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE guild_id = %s
                  AND alliance_id = %s
                  AND rule_id = %s
                """,
                (guild_id, alliance_id, rule_id),
            )
            if cursor.rowcount == 0:
                raise ValueError("Fee rule was not found.")
        connection.commit()


def update_alliance_payout_fee_rule(
    guild_id: int,
    alliance_id: int,
    rule_id: int,
    *,
    rule_name: str,
    fee_rate: Decimal,
) -> None:
    normalized_name = rule_name.strip()
    normalized_fee_rate = _decimal(fee_rate)
    if not normalized_name:
        raise ValueError("Fee rule name must not be empty.")
    if normalized_fee_rate < 0:
        raise ValueError("Fee rule rate must not be negative.")

    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE alliance_payout_fee_rules
                SET rule_name = %s,
                    fee_rate = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE guild_id = %s
                  AND alliance_id = %s
                  AND rule_id = %s
                  AND is_active = TRUE
                """,
                (
                    normalized_name,
                    normalized_fee_rate,
                    guild_id,
                    alliance_id,
                    rule_id,
                ),
            )
            if cursor.rowcount == 0:
                raise ValueError("Fee rule was not found.")
        connection.commit()


def get_member_payout_groups(
    guild_id: int,
    alliance_id: int,
) -> dict[int, dict[str, Any]]:
    snapshot_rows = _fetchall(
        """
        SELECT
            mprs.snapshot_id,
            mprs.distribution_id,
            mprs.alliance_id,
            mprs.rule_name_snapshot,
            mprs.fee_rate_snapshot,
            mprs.sort_order
        FROM member_payout_rule_snapshots mprs
        INNER JOIN distribution_batches db
            ON db.distribution_id = mprs.distribution_id
        WHERE db.guild_id = %s
          AND mprs.alliance_id = %s
        ORDER BY mprs.distribution_id DESC, mprs.sort_order ASC, mprs.snapshot_id ASC
        """,
        (guild_id, alliance_id),
    )
    status_rows = _fetchall(
        """
        SELECT
            mps.distribution_id,
            mps.alliance_id,
            mps.user_id,
            mps.is_paid,
            mps.payout_status,
            mps.updated_at
        FROM member_payout_statuses mps
        INNER JOIN distribution_batches db
            ON db.distribution_id = mps.distribution_id
        WHERE db.guild_id = %s
          AND mps.alliance_id = %s
        ORDER BY mps.distribution_id DESC, mps.user_id ASC
        """,
        (guild_id, alliance_id),
    )

    groups: dict[int, dict[str, Any]] = {}
    for row in snapshot_rows:
        distribution_id = int(row["distribution_id"])
        group = groups.setdefault(
            distribution_id,
            {
                "distribution_id": distribution_id,
                "alliance_id": int(row["alliance_id"]),
                "fee_lines": [],
                "statuses": {},
                "status_updated_at": {},
            },
        )
        group["fee_lines"].append(
            {
                "snapshot_id": int(row["snapshot_id"]),
                "rule_name": str(row["rule_name_snapshot"]),
                "fee_rate": _decimal(row["fee_rate_snapshot"]),
                "sort_order": int(row["sort_order"] or 0),
            }
        )
    for row in status_rows:
        distribution_id = int(row["distribution_id"])
        group = groups.setdefault(
            distribution_id,
            {
                "distribution_id": distribution_id,
                "alliance_id": int(row["alliance_id"]),
                "fee_lines": [],
                "statuses": {},
                "status_updated_at": {},
            },
        )
        payout_status = str(row.get("payout_status") or "").lower()
        if payout_status not in {"paid", "unpaid", "forfeited"}:
            payout_status = "paid" if bool(row["is_paid"]) else "unpaid"
        user_id = int(row["user_id"])
        group["statuses"][user_id] = payout_status
        group["status_updated_at"][user_id] = row.get("updated_at")
    return groups


def settle_member_payout(
    guild_id: int,
    distribution_id: int,
    alliance_id: int,
    *,
    updated_by_discord_id: int | None,
) -> int:
    with _connect() as connection:
        with connection.cursor() as cursor:
            payout = _get_distribution_alliance_payout_for_settlement(
                cursor,
                guild_id,
                distribution_id,
                alliance_id,
            )
            participants = _get_loot_participants_for_alliance(
                cursor,
                int(payout["loot_event_id"]),
                alliance_id,
            )
            if not participants:
                raise ValueError("Member payout has no recipients.")

            _ensure_member_payout_rule_snapshot(
                cursor,
                guild_id,
                distribution_id,
                alliance_id,
            )
            for participant in participants:
                cursor.execute(
                    """
                    INSERT INTO member_payout_statuses (
                        distribution_id,
                        alliance_id,
                        user_id,
                        is_paid,
                        payout_status,
                        updated_at
                    )
                    VALUES (%s, %s, %s, TRUE, 'paid', CURRENT_TIMESTAMP)
                    ON CONFLICT (distribution_id, alliance_id, user_id)
                    DO UPDATE SET
                        is_paid = TRUE,
                        payout_status = 'paid',
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (distribution_id, alliance_id, int(participant["user_id"])),
                )
        connection.commit()
    return distribution_id


def update_member_payout_recipient_status(
    guild_id: int,
    distribution_id: int,
    alliance_id: int,
    user_id: int,
    payout_status: str,
    *,
    updated_by_discord_id: int | None,
) -> int:
    if payout_status not in {"paid", "unpaid", "forfeited"}:
        raise ValueError("Member payout status must be paid, unpaid, or forfeited.")

    with _connect() as connection:
        with connection.cursor() as cursor:
            payout = _get_distribution_alliance_payout_for_settlement(
                cursor,
                guild_id,
                distribution_id,
                alliance_id,
            )
            cursor.execute(
                """
                SELECT 1
                FROM loot_event_participants
                WHERE loot_event_id = %s
                  AND alliance_id = %s
                  AND user_id = %s
                """,
                (int(payout["loot_event_id"]), alliance_id, user_id),
            )
            if cursor.fetchone() is None:
                raise ValueError("Member payout recipient was not found.")

            _ensure_member_payout_rule_snapshot(
                cursor,
                guild_id,
                distribution_id,
                alliance_id,
            )
            cursor.execute(
                """
                INSERT INTO member_payout_statuses (
                    distribution_id,
                    alliance_id,
                    user_id,
                    is_paid,
                    payout_status,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (distribution_id, alliance_id, user_id)
                DO UPDATE SET
                    is_paid = EXCLUDED.is_paid,
                    payout_status = EXCLUDED.payout_status,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    distribution_id,
                    alliance_id,
                    user_id,
                    payout_status == "paid",
                    payout_status,
                ),
            )
        connection.commit()
    return distribution_id


def get_member_forfeiture_settlements(
    guild_id: int,
    alliance_ids: list[int],
) -> dict[tuple[int, int, int], dict[str, Any]]:
    normalized_ids = _normalize_alliance_id_list(alliance_ids)
    if not normalized_ids:
        return {}
    rows = _fetchall(
        """
        SELECT
            mfs.distribution_id,
            mfs.alliance_id,
            mfs.user_id,
            mfs.settled_by_discord_id,
            mfs.settled_at
        FROM member_forfeiture_settlements mfs
        INNER JOIN distribution_batches db
            ON db.distribution_id = mfs.distribution_id
        WHERE db.guild_id = %s
          AND mfs.alliance_id = ANY(%s::bigint[])
        """,
        (guild_id, normalized_ids),
    )
    return {
        (
            int(row["distribution_id"]),
            int(row["alliance_id"]),
            int(row["user_id"]),
        ): dict(row)
        for row in rows
    }


def settle_member_forfeitures(
    guild_id: int,
    alliance_id: int,
    *,
    settled_by_discord_id: int | None,
) -> int:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO member_forfeiture_settlements (
                    distribution_id,
                    alliance_id,
                    user_id,
                    settled_by_discord_id,
                    settled_at
                )
                SELECT
                    mps.distribution_id,
                    mps.alliance_id,
                    mps.user_id,
                    %s,
                    CURRENT_TIMESTAMP
                FROM member_payout_statuses mps
                INNER JOIN distribution_batches db
                    ON db.distribution_id = mps.distribution_id
                LEFT JOIN member_forfeiture_settlements mfs
                    ON mfs.distribution_id = mps.distribution_id
                   AND mfs.alliance_id = mps.alliance_id
                   AND mfs.user_id = mps.user_id
                WHERE db.guild_id = %s
                  AND mps.alliance_id = %s
                  AND mps.payout_status = 'forfeited'
                  AND mfs.distribution_id IS NULL
                ON CONFLICT (distribution_id, alliance_id, user_id)
                DO NOTHING
                """,
                (settled_by_discord_id, guild_id, alliance_id),
            )
            settled_count = int(cursor.rowcount or 0)
        connection.commit()
    return settled_count


def get_loot_fee_settlements(
    guild_id: int,
    distribution_ids: list[int],
) -> dict[tuple[int, int, str], dict[str, Any]]:
    normalized_distribution_ids = sorted(
        {
            int(distribution_id)
            for distribution_id in distribution_ids
            if int(distribution_id or 0) > 0
        }
    )
    if not normalized_distribution_ids:
        return {}
    rows = _fetchall(
        """
        SELECT
            lfs.distribution_id,
            lfs.alliance_id,
            lfs.fee_key,
            lfs.fee_label,
            lfs.fee_rate,
            lfs.fee_amount,
            lfs.settled_by_discord_id,
            lfs.settled_at
        FROM loot_fee_settlements lfs
        INNER JOIN distribution_batches db
            ON db.distribution_id = lfs.distribution_id
        WHERE db.guild_id = %s
          AND lfs.distribution_id = ANY(%s::bigint[])
        """,
        (guild_id, normalized_distribution_ids),
    )
    return {
        (
            int(row["distribution_id"]),
            int(row["alliance_id"] or 0),
            str(row["fee_key"]),
        ): {
            "distribution_id": int(row["distribution_id"]),
            "alliance_id": int(row["alliance_id"] or 0),
            "fee_key": str(row["fee_key"]),
            "fee_label": str(row["fee_label"]),
            "fee_rate": _decimal(row["fee_rate"]),
            "fee_amount": _decimal(row["fee_amount"]),
            "settled_by_discord_id": _optional_int(row["settled_by_discord_id"]),
            "settled_at": row["settled_at"],
        }
        for row in rows
    }


def settle_loot_fee(
    guild_id: int,
    distribution_id: int,
    *,
    alliance_id: int | None,
    fee_key: str,
    fee_label: str,
    fee_rate: Decimal,
    fee_amount: Decimal,
    settled_by_discord_id: int | None,
) -> None:
    normalized_fee_key = str(fee_key or "").strip()
    normalized_alliance_id = int(alliance_id or 0)
    if int(distribution_id or 0) <= 0:
        raise ValueError("Distribution id is required.")
    if not normalized_fee_key:
        raise ValueError("Fee key is required.")

    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM distribution_batches
                WHERE guild_id = %s
                  AND distribution_id = %s
                """,
                (guild_id, distribution_id),
            )
            if cursor.fetchone() is None:
                raise ValueError("Distribution was not found.")
            if normalized_alliance_id > 0:
                cursor.execute(
                    """
                    SELECT net_amount
                    FROM distribution_alliance_payouts
                    WHERE distribution_id = %s
                      AND alliance_id = %s
                    """,
                    (distribution_id, normalized_alliance_id),
                )
                payout_row = cursor.fetchone()
                if payout_row is None:
                    raise ValueError("Alliance payout was not found.")
                _ensure_member_payout_rule_snapshot(
                    cursor,
                    guild_id,
                    distribution_id,
                    normalized_alliance_id,
                )
                cursor.execute(
                    """
                    SELECT
                        snapshot_id,
                        rule_name_snapshot,
                        fee_rate_snapshot,
                        sort_order
                    FROM member_payout_rule_snapshots
                    WHERE distribution_id = %s
                      AND alliance_id = %s
                    ORDER BY sort_order ASC, snapshot_id ASC
                    """,
                    (distribution_id, normalized_alliance_id),
                )
                matched_fee: dict[str, Any] | None = None
                for row in cursor.fetchall():
                    candidate_key = _loot_internal_fee_key(
                        str(row["rule_name_snapshot"]),
                        int(row["sort_order"] or 0),
                    )
                    legacy_candidate_key = f"internal:{int(row['snapshot_id'] or 0)}"
                    if normalized_fee_key in {candidate_key, legacy_candidate_key}:
                        matched_fee = dict(row)
                        normalized_fee_key = candidate_key
                        break
                if matched_fee is None:
                    raise ValueError("Fee rule was not found.")
                normalized_fee_label = str(matched_fee["rule_name_snapshot"])
                normalized_fee_rate = _decimal(matched_fee["fee_rate_snapshot"])
                normalized_fee_amount = _loot_floor_amount(
                    _decimal(payout_row["net_amount"]) * normalized_fee_rate
                )
            else:
                cursor.execute(
                    """
                    SELECT
                        bookkeeper_fee_rate,
                        bookkeeper_fee_amount,
                        alliance_fee_rate,
                        alliance_fee_amount
                    FROM distribution_batches
                    WHERE guild_id = %s
                      AND distribution_id = %s
                    """,
                    (guild_id, distribution_id),
                )
                distribution_row = cursor.fetchone()
                if distribution_row is None:
                    raise ValueError("Distribution was not found.")
                if normalized_fee_key == "bookkeeper":
                    normalized_fee_label = "경리 수수료"
                    normalized_fee_rate = _decimal(distribution_row["bookkeeper_fee_rate"])
                    normalized_fee_amount = _loot_floor_amount(
                        distribution_row["bookkeeper_fee_amount"]
                    )
                elif normalized_fee_key == "alliance":
                    normalized_fee_label = "연합 수수료"
                    normalized_fee_rate = _decimal(distribution_row["alliance_fee_rate"])
                    normalized_fee_amount = _loot_floor_amount(
                        distribution_row["alliance_fee_amount"]
                    )
                else:
                    raise ValueError("Fee key was not found.")
            if normalized_fee_amount <= 0:
                raise ValueError("Fee amount must be positive.")
            cursor.execute(
                """
                INSERT INTO loot_fee_settlements (
                    distribution_id,
                    alliance_id,
                    fee_key,
                    fee_label,
                    fee_rate,
                    fee_amount,
                    settled_by_discord_id,
                    settled_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (distribution_id, alliance_id, fee_key)
                DO UPDATE SET
                    fee_label = EXCLUDED.fee_label,
                    fee_rate = EXCLUDED.fee_rate,
                    fee_amount = EXCLUDED.fee_amount,
                    settled_by_discord_id = EXCLUDED.settled_by_discord_id,
                    settled_at = CURRENT_TIMESTAMP
                """,
                (
                    distribution_id,
                    normalized_alliance_id,
                    normalized_fee_key,
                    normalized_fee_label,
                    normalized_fee_rate,
                    normalized_fee_amount,
                    settled_by_discord_id,
                ),
            )
        connection.commit()


def _loot_internal_fee_key(rule_name: str, sort_order: int) -> str:
    return f"internal:{int(sort_order or 0)}:{str(rule_name or '').strip()}"


def _loot_floor_amount(value: Any) -> Decimal:
    return _decimal(value).quantize(Decimal("1"), rounding=ROUND_FLOOR)


def _ensure_member_payout_rule_snapshot(
    cursor: psycopg2.extensions.cursor,
    guild_id: int,
    distribution_id: int,
    alliance_id: int,
) -> None:
    snapshot_is_locked = _member_payout_rule_snapshot_is_locked(
        cursor,
        distribution_id,
        alliance_id,
    )
    cursor.execute(
        """
        SELECT 1
        FROM member_payout_rule_snapshots
        WHERE distribution_id = %s
          AND alliance_id = %s
        LIMIT 1
        """,
        (distribution_id, alliance_id),
    )
    snapshot_exists = cursor.fetchone() is not None
    if snapshot_exists and snapshot_is_locked:
        return
    if snapshot_exists:
        cursor.execute(
            """
            DELETE FROM member_payout_rule_snapshots
            WHERE distribution_id = %s
              AND alliance_id = %s
            """,
            (distribution_id, alliance_id),
        )
    cursor.execute(
        """
        INSERT INTO member_payout_rule_snapshots (
            distribution_id,
            alliance_id,
            rule_name_snapshot,
            fee_rate_snapshot,
            sort_order,
            created_at
        )
        SELECT
            %s,
            %s,
            rule_name,
            fee_rate,
            sort_order,
            CURRENT_TIMESTAMP
        FROM alliance_payout_fee_rules
        WHERE guild_id = %s
          AND alliance_id = %s
          AND is_active = TRUE
        ON CONFLICT DO NOTHING
        """,
        (distribution_id, alliance_id, guild_id, alliance_id),
    )


def _member_payout_rule_snapshot_is_locked(
    cursor: psycopg2.extensions.cursor,
    distribution_id: int,
    alliance_id: int,
) -> bool:
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM member_payout_statuses
            WHERE distribution_id = %s
              AND alliance_id = %s
              AND (
                  is_paid = TRUE
                  OR COALESCE(payout_status, 'unpaid') <> 'unpaid'
              )
            UNION ALL
            SELECT 1
            FROM loot_fee_settlements
            WHERE distribution_id = %s
              AND alliance_id = %s
        ) AS is_locked
        """,
        (distribution_id, alliance_id, distribution_id, alliance_id),
    )
    row = cursor.fetchone()
    return bool(row and row["is_locked"])


def settle_all_member_payouts(
    guild_id: int,
    alliance_id: int,
    *,
    updated_by_discord_id: int | None,
) -> int:
    rows = _fetchall(
        """
        SELECT db.distribution_id
        FROM distribution_batches db
        INNER JOIN distribution_alliance_payouts p
            ON p.distribution_id = db.distribution_id
        WHERE db.guild_id = %s
          AND p.alliance_id = %s
          AND EXISTS (
              SELECT 1
              FROM loot_event_participants lep
              WHERE lep.loot_event_id = db.loot_event_id
                AND lep.alliance_id = p.alliance_id
          )
          AND EXISTS (
              SELECT 1
              FROM loot_event_participants lep
              WHERE lep.loot_event_id = db.loot_event_id
                AND lep.alliance_id = p.alliance_id
                AND NOT EXISTS (
                    SELECT 1
                    FROM member_payout_statuses mps
                    WHERE mps.distribution_id = db.distribution_id
                      AND mps.alliance_id = p.alliance_id
                      AND mps.user_id = lep.user_id
                      AND (
                          mps.is_paid = TRUE
                          OR COALESCE(mps.payout_status, 'unpaid') <> 'unpaid'
                      )
                )
          )
        ORDER BY db.distribution_id ASC
        """,
        (guild_id, alliance_id),
    )
    completed_count = 0
    for row in rows:
        settle_member_payout(
            guild_id,
            int(row["distribution_id"]),
            alliance_id,
            updated_by_discord_id=updated_by_discord_id,
        )
        completed_count += 1
    return completed_count


def _get_distribution_alliance_payout_for_settlement(
    cursor: psycopg2.extensions.cursor,
    guild_id: int,
    distribution_id: int,
    alliance_id: int,
) -> dict[str, Any]:
    cursor.execute(
        """
        SELECT
            db.distribution_id,
            db.loot_event_id,
            p.alliance_id,
            p.net_amount,
            le.title,
            COALESCE(li.item_id, 0) AS item_id,
            COALESCE(li.item_name_snapshot, le.title, '') AS item_name
        FROM distribution_batches db
        INNER JOIN distribution_alliance_payouts p
            ON p.distribution_id = db.distribution_id
        INNER JOIN loot_events le
            ON le.loot_event_id = db.loot_event_id
        LEFT JOIN LATERAL (
            SELECT item_id, item_name_snapshot
            FROM loot_event_items
            WHERE loot_event_id = le.loot_event_id
            ORDER BY loot_item_id ASC
            LIMIT 1
        ) li ON TRUE
        WHERE db.guild_id = %s
          AND db.distribution_id = %s
          AND p.alliance_id = %s
        """,
        (guild_id, distribution_id, alliance_id),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError("Distribution payout was not found.")
    return dict(row)


def _get_alliance_payout_fee_rules_for_update(
    cursor: psycopg2.extensions.cursor,
    guild_id: int,
    alliance_id: int,
) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT rule_name, fee_rate, sort_order
        FROM alliance_payout_fee_rules
        WHERE guild_id = %s
          AND alliance_id = %s
          AND is_active = TRUE
        ORDER BY sort_order ASC, rule_id ASC
        """,
        (guild_id, alliance_id),
    )
    return [dict(row) for row in cursor.fetchall()]


def _get_loot_participants_for_alliance(
    cursor: psycopg2.extensions.cursor,
    loot_event_id: int,
    alliance_id: int,
) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT
            u.user_id,
            u.discord_nickname
        FROM loot_event_participants lep
        INNER JOIN users u ON u.user_id = lep.user_id
        WHERE lep.loot_event_id = %s
          AND lep.alliance_id = %s
        ORDER BY u.discord_nickname ASC
        """,
        (loot_event_id, alliance_id),
    )
    return [dict(row) for row in cursor.fetchall()]


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


def _rebuild_loot_for_attendance(
    cursor: psycopg2.extensions.cursor,
    guild_id: int,
    attendance_id: int,
) -> None:
    cursor.execute(
        """
        SELECT
            le.loot_event_id,
            li.loot_item_id,
            li.sale_price,
            li.fee_rate,
            li.bookkeeper_fee_rate,
            li.alliance_fee_rate,
            le.excluded_alliance_ids
        FROM loot_events le
        INNER JOIN loot_event_items li ON li.loot_event_id = le.loot_event_id
        WHERE le.guild_id = %s
          AND le.attendance_id = %s
        ORDER BY le.loot_event_id ASC, li.loot_item_id ASC
        """,
        (guild_id, attendance_id),
    )
    loot_rows = [dict(row) for row in cursor.fetchall()]
    if not loot_rows:
        return

    session = _get_attendance_session_for_loot(cursor, guild_id, attendance_id)
    participants = _get_attendance_participants_for_loot(cursor, attendance_id)
    unclassified_alliance_id: int | None = None

    for loot_row in loot_rows:
        loot_event_id = int(loot_row["loot_event_id"])
        excluded_id_set = set(_normalize_alliance_id_list(loot_row.get("excluded_alliance_ids")))
        participant_counts: dict[int, int] = {}
        cursor.execute(
            """
            DELETE FROM loot_event_participants
            WHERE loot_event_id = %s
            """,
            (loot_event_id,),
        )
        cursor.execute(
            """
            DELETE FROM loot_event_alliance_counts
            WHERE loot_event_id = %s
            """,
            (loot_event_id,),
        )
        for participant in participants:
            alliance_id = _optional_int(participant["alliance_id"])
            if alliance_id is None:
                if unclassified_alliance_id is None:
                    unclassified_alliance_id = _ensure_alliance_id(cursor, "미분류")
                alliance_id = unclassified_alliance_id
            if alliance_id in excluded_id_set:
                continue
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
                VALUES (%s, %s, %s, %s, 'attendance_edit')
                ON CONFLICT (loot_event_id, user_id) DO UPDATE SET
                    alliance_id = EXCLUDED.alliance_id,
                    attended_at = EXCLUDED.attended_at,
                    source = EXCLUDED.source
                """,
                (
                    loot_event_id,
                    int(participant["user_id"]),
                    alliance_id,
                    str(session["started_at"]),
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

        distribution_id = _upsert_distribution_for_loot(
            cursor,
            guild_id,
            loot_event_id,
            int(loot_row["loot_item_id"]),
            _decimal(loot_row["sale_price"]),
            participant_counts,
            _decimal(loot_row["fee_rate"]),
            _decimal(loot_row["bookkeeper_fee_rate"]),
            _decimal(loot_row["alliance_fee_rate"]),
        )
        cursor.execute(
            """
            DELETE FROM member_payout_statuses
            WHERE distribution_id = %s
            """,
            (distribution_id,),
        )
        cursor.execute(
            """
            DELETE FROM member_payout_rule_snapshots
            WHERE distribution_id = %s
            """,
            (distribution_id,),
        )


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
    fee_rate: Decimal,
    bookkeeper_fee_rate: Decimal | None = None,
    alliance_fee_rate: Decimal | None = None,
) -> int:
    participant_total = sum(participant_counts.values())
    total_sale_amount = _decimal(total_amount)
    fee_rate, bookkeeper_fee_rate, alliance_fee_rate = _normalize_distribution_fee_rates(
        fee_rate,
        bookkeeper_fee_rate,
        alliance_fee_rate,
    )
    bookkeeper_fee_amount = total_sale_amount * bookkeeper_fee_rate
    alliance_fee_amount = total_sale_amount * alliance_fee_rate
    fee_amount = bookkeeper_fee_amount + alliance_fee_amount
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
                bookkeeper_fee_rate,
                bookkeeper_fee_amount,
                alliance_fee_rate,
                alliance_fee_amount
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                bookkeeper_fee_rate,
                bookkeeper_fee_amount,
                alliance_fee_rate,
                alliance_fee_amount,
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
                fee_amount = %s,
                bookkeeper_fee_rate = %s,
                bookkeeper_fee_amount = %s,
                alliance_fee_rate = %s,
                alliance_fee_amount = %s
            WHERE distribution_id = %s
            """,
            (
                guild_id,
                total_sale_amount,
                total_net_amount,
                participant_total,
                fee_rate,
                fee_amount,
                bookkeeper_fee_rate,
                bookkeeper_fee_amount,
                alliance_fee_rate,
                alliance_fee_amount,
                distribution_id,
            ),
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
    else:
        cursor.execute(
            """
            DELETE FROM distribution_alliance_payouts
            WHERE distribution_id = %s
            """,
            (distribution_id,),
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
                net_amount,
                payout_status,
                updated_at
            )
            VALUES (%s, %s, %s, %s, 'unpaid', CURRENT_TIMESTAMP)
            ON CONFLICT (distribution_id, alliance_id) DO UPDATE SET
                participant_count = EXCLUDED.participant_count,
                net_amount = EXCLUDED.net_amount,
                payout_status = distribution_alliance_payouts.payout_status,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                distribution_id,
                alliance_id,
                count,
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


def _normalize_id_list(value: Any) -> list[int]:
    if value is None:
        return []
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return []
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = value.split(",")
    else:
        parsed = value
    if not isinstance(parsed, (list, tuple, set)):
        parsed = [parsed]
    normalized: list[int] = []
    seen: set[int] = set()
    for item in parsed:
        try:
            item_id = int(item)
        except (TypeError, ValueError):
            continue
        if item_id <= 0 or item_id in seen:
            continue
        seen.add(item_id)
        normalized.append(item_id)
    return normalized


def _normalize_alliance_id_list(value: Any) -> list[int]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        try:
            import json

            value = json.loads(value)
        except (TypeError, ValueError):
            return []
    if not isinstance(value, (list, tuple, set)):
        return []
    normalized: list[int] = []
    seen: set[int] = set()
    for item in value:
        try:
            alliance_id = _optional_int(item)
        except (TypeError, ValueError):
            alliance_id = None
        if alliance_id is None or alliance_id <= 0 or alliance_id in seen:
            continue
        normalized.append(alliance_id)
        seen.add(alliance_id)
    return normalized


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
        "ALTER TABLE guilds ADD COLUMN IF NOT EXISTS is_enabled BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE items ADD COLUMN IF NOT EXISTS guild_id BIGINT REFERENCES guilds(guild_id) ON DELETE CASCADE",
        "ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS attendance_voice_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb",
        "ALTER TABLE loot_events ADD COLUMN IF NOT EXISTS attendance_id BIGINT REFERENCES attendance_sessions(attendance_id) ON DELETE SET NULL",
        "ALTER TABLE loot_events ADD COLUMN IF NOT EXISTS adena_rate NUMERIC(18, 6) NOT NULL DEFAULT 0",
        "ALTER TABLE loot_events ADD COLUMN IF NOT EXISTS excluded_alliance_ids JSONB NOT NULL DEFAULT '[]'::jsonb",
        "ALTER TABLE loot_event_items ADD COLUMN IF NOT EXISTS cash_price_krw NUMERIC(18, 2) NOT NULL DEFAULT 0",
        "ALTER TABLE loot_event_items ADD COLUMN IF NOT EXISTS bookkeeper_fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0",
        "ALTER TABLE loot_event_items ADD COLUMN IF NOT EXISTS alliance_fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0",
        "ALTER TABLE distribution_batches ADD COLUMN IF NOT EXISTS guild_id BIGINT REFERENCES guilds(guild_id) ON DELETE CASCADE",
        "ALTER TABLE distribution_batches ADD COLUMN IF NOT EXISTS fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0",
        "ALTER TABLE distribution_batches ADD COLUMN IF NOT EXISTS fee_amount NUMERIC(18, 2) NOT NULL DEFAULT 0",
        "ALTER TABLE distribution_batches ADD COLUMN IF NOT EXISTS bookkeeper_fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0",
        "ALTER TABLE distribution_batches ADD COLUMN IF NOT EXISTS bookkeeper_fee_amount NUMERIC(18, 2) NOT NULL DEFAULT 0",
        "ALTER TABLE distribution_batches ADD COLUMN IF NOT EXISTS alliance_fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0",
        "ALTER TABLE distribution_batches ADD COLUMN IF NOT EXISTS alliance_fee_amount NUMERIC(18, 2) NOT NULL DEFAULT 0",
        "ALTER TABLE distribution_alliance_payouts ADD COLUMN IF NOT EXISTS payout_status TEXT NOT NULL DEFAULT 'unpaid'",
        "ALTER TABLE member_payout_statuses ADD COLUMN IF NOT EXISTS payout_status TEXT NOT NULL DEFAULT 'unpaid'",
        "ALTER TABLE alliance_payout_fee_rules ADD COLUMN IF NOT EXISTS created_by_discord_id BIGINT",
        "ALTER TABLE alliance_payout_fee_rules ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "ALTER TABLE scheduled_report_settings ADD COLUMN IF NOT EXISTS run_time TEXT NOT NULL DEFAULT '00:00'",
        "ALTER TABLE scheduled_report_settings ADD COLUMN IF NOT EXISTS schedule_json JSONB NOT NULL DEFAULT '{\"type\":\"daily\",\"time\":\"00:00\",\"timezone\":\"Asia/Seoul\"}'::jsonb",
        "ALTER TABLE scheduled_report_settings ADD COLUMN IF NOT EXISTS query_json JSONB NOT NULL DEFAULT '{\"dataset\":\"attendance\",\"period\":\"today\",\"group_by\":\"alliance\",\"rank_target\":\"user\",\"metric\":\"attendance_count\",\"limit\":10}'::jsonb",
        "ALTER TABLE scheduled_report_settings ADD COLUMN IF NOT EXISTS render_json JSONB NOT NULL DEFAULT '{\"output\":\"grouped_ranking\",\"title\":\"금일 혈맹별 출석 랭킹 TOP10\",\"group_header\":\"{group_name}\",\"row\":\"{rank}. {label} - {value}회\",\"empty\":\"출석 기록 없음\"}'::jsonb",
        "ALTER TABLE bid_items ADD COLUMN IF NOT EXISTS is_free BOOLEAN NOT NULL DEFAULT FALSE",
    ]
    for sql in column_sql:
        cursor.execute(sql)
    cursor.execute(
        """
        UPDATE member_payout_statuses
        SET payout_status = CASE WHEN is_paid = TRUE THEN 'paid' ELSE 'unpaid' END
        WHERE payout_status NOT IN ('paid', 'unpaid', 'forfeited')
           OR (is_paid = TRUE AND payout_status = 'unpaid')
        """
    )
    cursor.execute(
        """
        UPDATE guild_settings
        SET attendance_voice_channel_ids = jsonb_build_array(attendance_voice_channel_id)
        WHERE attendance_voice_channel_id IS NOT NULL
          AND attendance_voice_channel_ids = '[]'::jsonb
        """
    )
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
        UPDATE loot_event_items
        SET bookkeeper_fee_rate = fee_rate,
            alliance_fee_rate = 0
        WHERE fee_rate > 0
          AND bookkeeper_fee_rate = 0
          AND alliance_fee_rate = 0
        """
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
        UPDATE distribution_batches
        SET bookkeeper_fee_rate = fee_rate,
            bookkeeper_fee_amount = fee_amount,
            alliance_fee_rate = 0,
            alliance_fee_amount = 0
        WHERE fee_rate > 0
          AND bookkeeper_fee_rate = 0
          AND alliance_fee_rate = 0
        """
    )
    cursor.execute(
        """
        UPDATE distribution_alliance_payouts p
        SET net_amount = (
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
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_bid_items_guild_name_unique "
        "ON bid_items(guild_id, LOWER(item_name))"
    )
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_bid_results_unique_cycle "
        "ON bid_item_results(guild_id, bid_item_id, alliance_id, cycle_no)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_bid_results_item_cycle "
        "ON bid_item_results(guild_id, bid_item_id, cycle_no)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_guild_bookkeepers_discord "
        "ON guild_bookkeepers(guild_id, discord_id)"
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
        "CREATE INDEX IF NOT EXISTS idx_loot_events_guild_date_order "
        "ON loot_events(guild_id, event_date DESC, event_time_label DESC, loot_event_id DESC)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_distribution_batches_guild "
        "ON distribution_batches(guild_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_distribution_batches_guild_distribution "
        "ON distribution_batches(guild_id, distribution_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_distribution_batches_loot_event "
        "ON distribution_batches(loot_event_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_distribution_alliance_payouts_distribution "
        "ON distribution_alliance_payouts(distribution_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_loot_event_items_event "
        "ON loot_event_items(loot_event_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_loot_event_participants_event "
        "ON loot_event_participants(loot_event_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_loot_event_participants_event_alliance "
        "ON loot_event_participants(loot_event_id, alliance_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_member_payout_rule_snapshots_distribution_alliance "
        "ON member_payout_rule_snapshots(distribution_id, alliance_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_member_payout_rule_snapshots_alliance_distribution "
        "ON member_payout_rule_snapshots(alliance_id, distribution_id)"
    )
    cursor.execute(
        """
        DELETE FROM member_payout_rule_snapshots newer
        USING member_payout_rule_snapshots older
        WHERE newer.snapshot_id > older.snapshot_id
          AND newer.distribution_id = older.distribution_id
          AND newer.alliance_id = older.alliance_id
          AND newer.rule_name_snapshot = older.rule_name_snapshot
          AND newer.sort_order = older.sort_order
        """
    )
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_member_payout_rule_snapshots_unique "
        "ON member_payout_rule_snapshots(distribution_id, alliance_id, rule_name_snapshot, sort_order)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_member_payout_statuses_alliance_paid "
        "ON member_payout_statuses(alliance_id, is_paid)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_member_payout_statuses_alliance_status "
        "ON member_payout_statuses(alliance_id, payout_status)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_member_payout_statuses_alliance_distribution "
        "ON member_payout_statuses(alliance_id, distribution_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_member_forfeiture_settlements_alliance "
        "ON member_forfeiture_settlements(alliance_id, settled_at)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_member_forfeiture_settlements_alliance_distribution "
        "ON member_forfeiture_settlements(alliance_id, distribution_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_loot_fee_settlements_distribution "
        "ON loot_fee_settlements(distribution_id, alliance_id, fee_key)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_alliance_payout_fee_rules_active "
        "ON alliance_payout_fee_rules(guild_id, alliance_id, is_active)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_command_queue_guild_status "
        "ON bot_command_queue(guild_id, status, created_at)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_work_logs_guild_created "
        "ON work_logs(guild_id, created_at DESC)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_work_logs_guild_action "
        "ON work_logs(guild_id, action_type, created_at DESC)"
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
        "items": ("created_at",),
        "loot_events": ("created_at",),
    }
    for table_name, column_names in redundant_columns.items():
        for column_name in column_names:
            cursor.execute(f"ALTER TABLE IF EXISTS {table_name} DROP COLUMN IF EXISTS {column_name}")


def _drop_obsolete_member_payout_tables(cursor: psycopg2.extensions.cursor) -> None:
    for table_name in (
        "payout_transactions",
        "member_payout_fee_statuses",
        "member_payout_fee_lines",
        "member_payout_recipients",
        "member_payout_items",
        "member_payout_groups",
    ):
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")


def _drop_obsolete_loot_boss_schema(cursor: psycopg2.extensions.cursor) -> None:
    for table_name in (
        "boss_attendance_snapshot_rows",
        "boss_hunt_participants",
        "boss_spawn_schedules",
        "boss_attendance_snapshots",
        "boss_hunt_sessions",
        "bosses",
        "distribution_lines",
    ):
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

    obsolete_columns = {
        "loot_event_items": ("buyer_name", "buyer_alliance_id", "memo"),
        "distribution_batches": ("status", "closed_at", "created_at"),
        "distribution_alliance_payouts": ("gross_amount", "payout_method", "memo"),
    }
    for table_name, column_names in obsolete_columns.items():
        for column_name in column_names:
            cursor.execute(
                f"ALTER TABLE IF EXISTS {table_name} DROP COLUMN IF EXISTS {column_name}"
            )


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS guilds (
    guild_id BIGINT PRIMARY KEY,
    is_enabled BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS guild_settings (
    guild_id BIGINT PRIMARY KEY REFERENCES guilds(guild_id) ON DELETE CASCADE,
    admin_channel_id BIGINT,
    attendance_voice_channel_id BIGINT,
    attendance_voice_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
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

	CREATE TABLE IF NOT EXISTS guild_bookkeepers (
	    guild_id BIGINT NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
	    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
	    discord_id BIGINT NOT NULL,
	    added_by_discord_id BIGINT,
	    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	    PRIMARY KEY (guild_id, user_id),
	    UNIQUE (guild_id, discord_id)
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

CREATE TABLE IF NOT EXISTS work_logs (
    work_log_id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
    actor_discord_id BIGINT,
    actor_display_name TEXT NOT NULL,
    actor_role TEXT NOT NULL,
    action_type TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id BIGINT,
    summary TEXT NOT NULL,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
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

CREATE TABLE IF NOT EXISTS bid_items (
    bid_item_id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
    item_name TEXT NOT NULL,
    sort_order INTEGER,
    is_free BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    memo TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bid_item_results (
    result_id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
    bid_item_id BIGINT NOT NULL REFERENCES bid_items(bid_item_id) ON DELETE CASCADE,
    alliance_id BIGINT NOT NULL REFERENCES alliances(alliance_id) ON DELETE CASCADE,
    cycle_no INTEGER NOT NULL DEFAULT 1,
    selected_by_discord_id BIGINT,
    selected_at TEXT NOT NULL,
    memo TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_bid_item_results_cycle CHECK (cycle_no >= 1)
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
    excluded_alliance_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_by_discord_id BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS loot_event_items (
    loot_item_id BIGSERIAL PRIMARY KEY,
    loot_event_id BIGINT NOT NULL REFERENCES loot_events(loot_event_id) ON DELETE CASCADE,
    item_id BIGINT REFERENCES items(item_id),
    item_name_snapshot TEXT NOT NULL,
    cash_price_krw NUMERIC(18, 2) NOT NULL DEFAULT 0,
    sale_price NUMERIC(18, 2) NOT NULL DEFAULT 0,
    fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0,
    bookkeeper_fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0,
    alliance_fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0,
    net_amount NUMERIC(18, 2) NOT NULL DEFAULT 0
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
    bookkeeper_fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0,
    bookkeeper_fee_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    alliance_fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0,
    alliance_fee_amount NUMERIC(18, 2) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS distribution_alliance_payouts (
    distribution_id BIGINT NOT NULL REFERENCES distribution_batches(distribution_id) ON DELETE CASCADE,
    alliance_id BIGINT NOT NULL REFERENCES alliances(alliance_id),
    participant_count INTEGER NOT NULL DEFAULT 0,
    net_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    payout_status TEXT NOT NULL DEFAULT 'unpaid',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (distribution_id, alliance_id)
);

CREATE TABLE IF NOT EXISTS alliance_payout_fee_rules (
    rule_id BIGSERIAL PRIMARY KEY,
    guild_id BIGINT NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
    alliance_id BIGINT NOT NULL REFERENCES alliances(alliance_id) ON DELETE CASCADE,
    rule_name TEXT NOT NULL,
    fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0,
    sort_order INTEGER NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_by_discord_id BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS member_payout_rule_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    distribution_id BIGINT NOT NULL REFERENCES distribution_batches(distribution_id) ON DELETE CASCADE,
    alliance_id BIGINT NOT NULL REFERENCES alliances(alliance_id) ON DELETE CASCADE,
    rule_name_snapshot TEXT NOT NULL,
    fee_rate_snapshot NUMERIC(8, 6) NOT NULL DEFAULT 0,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS member_payout_statuses (
    distribution_id BIGINT NOT NULL REFERENCES distribution_batches(distribution_id) ON DELETE CASCADE,
    alliance_id BIGINT NOT NULL REFERENCES alliances(alliance_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    is_paid BOOLEAN NOT NULL DEFAULT FALSE,
    payout_status TEXT NOT NULL DEFAULT 'unpaid',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (distribution_id, alliance_id, user_id)
);

CREATE TABLE IF NOT EXISTS member_forfeiture_settlements (
    distribution_id BIGINT NOT NULL REFERENCES distribution_batches(distribution_id) ON DELETE CASCADE,
    alliance_id BIGINT NOT NULL REFERENCES alliances(alliance_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    settled_by_discord_id BIGINT,
    settled_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (distribution_id, alliance_id, user_id)
);

CREATE TABLE IF NOT EXISTS loot_fee_settlements (
    distribution_id BIGINT NOT NULL REFERENCES distribution_batches(distribution_id) ON DELETE CASCADE,
    alliance_id BIGINT NOT NULL DEFAULT 0,
    fee_key TEXT NOT NULL,
    fee_label TEXT NOT NULL,
    fee_rate NUMERIC(8, 6) NOT NULL DEFAULT 0,
    fee_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    settled_by_discord_id BIGINT,
    settled_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (distribution_id, alliance_id, fee_key)
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_bid_items_guild_name_unique ON bid_items(guild_id, LOWER(item_name));
CREATE UNIQUE INDEX IF NOT EXISTS idx_bid_results_unique_cycle ON bid_item_results(guild_id, bid_item_id, alliance_id, cycle_no);
CREATE INDEX IF NOT EXISTS idx_bid_results_item_cycle ON bid_item_results(guild_id, bid_item_id, cycle_no);
CREATE INDEX IF NOT EXISTS idx_loot_events_guild_attendance ON loot_events(guild_id, attendance_id);
CREATE INDEX IF NOT EXISTS idx_loot_events_date ON loot_events(event_date);
CREATE INDEX IF NOT EXISTS idx_loot_events_guild_date_order ON loot_events(guild_id, event_date DESC, event_time_label DESC, loot_event_id DESC);
CREATE INDEX IF NOT EXISTS idx_distribution_batches_guild ON distribution_batches(guild_id);
CREATE INDEX IF NOT EXISTS idx_distribution_batches_guild_distribution ON distribution_batches(guild_id, distribution_id);
CREATE INDEX IF NOT EXISTS idx_distribution_batches_loot_event ON distribution_batches(loot_event_id);
CREATE INDEX IF NOT EXISTS idx_distribution_alliance_payouts_distribution ON distribution_alliance_payouts(distribution_id);
CREATE INDEX IF NOT EXISTS idx_loot_event_items_event ON loot_event_items(loot_event_id);
CREATE INDEX IF NOT EXISTS idx_loot_event_participants_event ON loot_event_participants(loot_event_id);
CREATE INDEX IF NOT EXISTS idx_loot_event_participants_event_alliance ON loot_event_participants(loot_event_id, alliance_id);
CREATE INDEX IF NOT EXISTS idx_member_payout_rule_snapshots_distribution_alliance ON member_payout_rule_snapshots(distribution_id, alliance_id);
CREATE INDEX IF NOT EXISTS idx_member_payout_rule_snapshots_alliance_distribution ON member_payout_rule_snapshots(alliance_id, distribution_id);
CREATE INDEX IF NOT EXISTS idx_member_payout_statuses_alliance_status ON member_payout_statuses(alliance_id, payout_status);
CREATE INDEX IF NOT EXISTS idx_member_payout_statuses_alliance_distribution ON member_payout_statuses(alliance_id, distribution_id);
CREATE INDEX IF NOT EXISTS idx_member_forfeiture_settlements_alliance ON member_forfeiture_settlements(alliance_id, settled_at);
CREATE INDEX IF NOT EXISTS idx_member_forfeiture_settlements_alliance_distribution ON member_forfeiture_settlements(alliance_id, distribution_id);
CREATE INDEX IF NOT EXISTS idx_loot_fee_settlements_distribution ON loot_fee_settlements(distribution_id, alliance_id, fee_key);
"""
