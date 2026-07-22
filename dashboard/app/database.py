from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy import inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from dashboard.app.config import get_settings


settings = get_settings()

EXPECTED_TABLES = frozenset(
    {
        "alliance_access_policies",
        "alliances",
        "attendance_entries",
        "attendance_sessions",
        "audit_action_types",
        "audit_actors",
        "audit_entity_types",
        "audit_event_contexts",
        "audit_events",
        "bid_item_results",
        "bid_items",
        "catalog_item_versions",
        "guild_alliance_role_mappings",
        "guild_settings",
        "guild_user_assignments",
        "guilds",
        "items",
        "scheduled_report_settings",
        "schema_migrations",
        "settlement_drop_excluded_alliances",
        "settlement_drop_participants",
        "settlement_drops",
        "settlement_fee_rule_versions",
        "settlement_fee_rules",
        "settlement_payout_objects",
        "treasury_accounts",
        "treasury_categories",
        "treasury_entries",
        "treasury_source_types",
        "users",
    }
)

engine: AsyncEngine = create_async_engine(
    settings.database_url,
    echo=settings.db_echo,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = async_sessionmaker(
    engine,
    expire_on_commit=False,
    autoflush=False,
)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session


async def ping_database() -> None:
    async with engine.begin() as connection:
        await connection.execute(text("SELECT 1"))


async def ensure_settings_schema() -> None:
    statements = (
        """
        CREATE TABLE IF NOT EXISTS guild_user_assignments (
            assignment_id BIGSERIAL PRIMARY KEY,
            guild_id BIGINT NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
            discord_user_id BIGINT NOT NULL,
            scope_code SMALLINT NOT NULL CHECK (scope_code IN (1, 2, 3)),
            alliance_id BIGINT REFERENCES alliances(alliance_id) ON DELETE CASCADE,
            assigned_by_discord_user_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT chk_guild_user_assignment_scope
                CHECK (
                    (scope_code = 1 AND alliance_id IS NULL)
                    OR (scope_code IN (2, 3) AND alliance_id IS NOT NULL)
                )
        )
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_guild_alliance_manager_user
        ON guild_user_assignments (guild_id, discord_user_id, scope_code)
        WHERE scope_code = 1
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_guild_clan_assignment_user
        ON guild_user_assignments (guild_id, alliance_id, discord_user_id, scope_code)
        WHERE scope_code IN (2, 3)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_guild_user_assignments_lookup
        ON guild_user_assignments (guild_id, scope_code, alliance_id)
        """,
        """
        CREATE TABLE IF NOT EXISTS alliance_access_policies (
            guild_id BIGINT NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
            alliance_id BIGINT NOT NULL REFERENCES alliances(alliance_id) ON DELETE CASCADE,
            distribution_visibility_code SMALLINT NOT NULL DEFAULT 2
                CHECK (distribution_visibility_code IN (1, 2, 3)),
            treasury_visibility_code SMALLINT NOT NULL DEFAULT 3
                CHECK (treasury_visibility_code IN (1, 2, 3)),
            user_access_code SMALLINT NOT NULL DEFAULT 2
                CHECK (user_access_code IN (1, 2, 3)),
            updated_by_discord_user_id BIGINT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (guild_id, alliance_id)
        )
        """,
    )
    async with engine.begin() as connection:
        for statement in statements:
            await connection.execute(text(statement))


async def apply_local_schema_cleanup() -> bool:
    """Remove legacy tables only from this branch's local PostgreSQL test database."""
    database_url = make_url(settings.database_url)
    if database_url.host not in {"127.0.0.1", "localhost", "::1"} or database_url.database != "testdb":
        return False

    async with engine.begin() as connection:
        table_names = set(await connection.run_sync(lambda sync_connection: inspect(sync_connection).get_table_names()))
        if "schema_migrations" not in table_names:
            await connection.execute(
                text("""
                    CREATE TABLE schema_migrations (
                        version INTEGER PRIMARY KEY,
                        applied_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT)
                    )
                """)
            )

        already_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 4")
        )
        if already_applied:
            return False

        if "guild_bookkeepers" in table_names:
            await connection.execute(
                text("""
                    INSERT INTO guild_user_assignments (
                        guild_id, discord_user_id, scope_code, alliance_id,
                        assigned_by_discord_user_id, created_at, updated_at
                    )
                    SELECT guild_id, discord_id, 1, NULL,
                           added_by_discord_id, updated_at, updated_at
                    FROM guild_bookkeepers
                    ON CONFLICT DO NOTHING
                """)
            )

        await connection.execute(text("ALTER TABLE guilds ADD COLUMN IF NOT EXISTS guild_name TEXT"))
        await connection.execute(text("ALTER TABLE guilds ADD COLUMN IF NOT EXISTS owner_discord_id BIGINT"))
        await connection.execute(text("ALTER TABLE guilds ADD COLUMN IF NOT EXISTS icon_hash TEXT"))
        await connection.execute(text("ALTER TABLE guilds ADD COLUMN IF NOT EXISTS discord_synced_at TIMESTAMPTZ"))

        await connection.execute(text("ALTER TABLE guild_settings DROP COLUMN IF EXISTS attendance_voice_channel_ids"))
        for column_name in ("class_name", "attribute_name", "position_name", "phone", "memo"):
            await connection.execute(text(f'ALTER TABLE users DROP COLUMN IF EXISTS "{column_name}"'))
        for column_name in ("category", "is_bid_item", "sort_order", "memo"):
            await connection.execute(text(f'ALTER TABLE items DROP COLUMN IF EXISTS "{column_name}"'))
        await connection.execute(
            text("""
                ALTER TABLE items
                ALTER COLUMN default_price TYPE BIGINT
                USING CASE WHEN default_price IS NULL THEN NULL ELSE TRUNC(default_price)::BIGINT END
            """)
        )

        legacy_tables = (
            "alliance_item_bid_statuses",
            "attendance_live_participants",
            "attendance_live_sessions",
            "bot_command_queue",
            "discord_message_links",
            "guild_bookkeepers",
            "item_bid_rules",
            "item_categories",
            "item_price_rules",
            "notifications",
            "web_admins",
            "websocket_events",
        )
        for table_name in legacy_tables:
            await connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))

        await connection.execute(
            text("""
                INSERT INTO schema_migrations(version, applied_at)
                VALUES (4, EXTRACT(EPOCH FROM NOW())::BIGINT)
            """)
        )
    return True


async def close_database() -> None:
    await engine.dispose()
