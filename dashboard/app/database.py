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
        "settlement_drop_sales",
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

    changed = False
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

        cleanup_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 4")
        )
        if not cleanup_applied:
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
            changed = True

        index_cleanup_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 5")
        )
        if not index_cleanup_applied:
            await connection.execute(text("DROP INDEX IF EXISTS idx_users_discord_id"))
            await connection.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS idx_users_alliance_active_name
                    ON users (alliance_id, is_active, discord_nickname)
                """)
            )
            await connection.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS idx_items_guild_active_name
                    ON items (guild_id, is_active, item_name)
                """)
            )
            await connection.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS idx_fee_rules_guild_scope_alliance
                    ON settlement_fee_rules (guild_id, scope_code, alliance_id, is_active)
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (5, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        alliance_treasury_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 6")
        )
        if not alliance_treasury_applied:
            await connection.execute(
                text("""
                    ALTER TABLE treasury_accounts
                    ADD COLUMN IF NOT EXISTS account_scope_code SMALLINT
                """)
            )
            await connection.execute(
                text("""
                    UPDATE treasury_accounts
                    SET account_scope_code = 2
                    WHERE account_scope_code IS NULL
                """)
            )
            await connection.execute(
                text("""
                    ALTER TABLE treasury_accounts
                    ALTER COLUMN account_scope_code SET DEFAULT 2,
                    ALTER COLUMN account_scope_code SET NOT NULL,
                    ALTER COLUMN alliance_id DROP NOT NULL
                """)
            )
            await connection.execute(
                text("""
                    ALTER TABLE treasury_accounts
                    DROP CONSTRAINT IF EXISTS treasury_accounts_guild_id_alliance_id_key
                """)
            )
            await connection.execute(
                text("""
                    ALTER TABLE treasury_accounts
                    DROP CONSTRAINT IF EXISTS uq_treasury_account_guild_alliance,
                    DROP CONSTRAINT IF EXISTS chk_treasury_account_scope
                """)
            )
            await connection.execute(
                text("""
                    ALTER TABLE treasury_accounts
                    ADD CONSTRAINT chk_treasury_account_scope
                    CHECK (
                        (account_scope_code = 1 AND alliance_id IS NULL)
                        OR (account_scope_code = 2 AND alliance_id IS NOT NULL)
                    )
                """)
            )
            await connection.execute(
                text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_treasury_account_alliance_scope
                    ON treasury_accounts (guild_id)
                    WHERE account_scope_code = 1
                """)
            )
            await connection.execute(
                text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_treasury_account_clan_scope
                    ON treasury_accounts (guild_id, alliance_id)
                    WHERE account_scope_code = 2
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO treasury_accounts (
                        guild_id, alliance_id, account_scope_code,
                        current_balance, updated_at
                    )
                    SELECT guild_id, NULL, 1, 0,
                           EXTRACT(EPOCH FROM NOW())::BIGINT
                    FROM guilds
                    ON CONFLICT (guild_id) WHERE account_scope_code = 1
                    DO NOTHING
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO treasury_categories (
                        guild_id, direction, category_name, is_active
                    )
                    SELECT guild_id, 1, '연합비 입금', TRUE FROM guilds
                    UNION ALL
                    SELECT guild_id, -1, '연합비 지출', TRUE FROM guilds
                    ON CONFLICT (guild_id, direction, category_name)
                    DO NOTHING
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (6, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        treasury_category_scope_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 7")
        )
        if not treasury_category_scope_applied:
            await connection.execute(
                text("""
                    ALTER TABLE treasury_categories
                    ADD COLUMN IF NOT EXISTS account_scope_code SMALLINT
                """)
            )
            await connection.execute(
                text("""
                    UPDATE treasury_categories
                    SET account_scope_code = CASE
                        WHEN category_name IN ('연합비 입금', '연합비 지출') THEN 1
                        ELSE 2
                    END
                    WHERE account_scope_code IS NULL
                """)
            )
            await connection.execute(
                text("""
                    ALTER TABLE treasury_categories
                    ALTER COLUMN account_scope_code SET DEFAULT 2,
                    ALTER COLUMN account_scope_code SET NOT NULL,
                    DROP CONSTRAINT IF EXISTS treasury_categories_guild_id_direction_category_name_key,
                    DROP CONSTRAINT IF EXISTS uq_treasury_category_guild_direction_name,
                    DROP CONSTRAINT IF EXISTS chk_treasury_category_scope
                """)
            )
            await connection.execute(
                text("""
                    ALTER TABLE treasury_categories
                    ADD CONSTRAINT chk_treasury_category_scope
                    CHECK (account_scope_code IN (1, 2)),
                    ADD CONSTRAINT uq_treasury_category_scope_name
                    UNIQUE (guild_id, account_scope_code, direction, category_name)
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (7, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        await connection.execute(
            text("""
                INSERT INTO treasury_accounts (
                    guild_id, alliance_id, account_scope_code, current_balance, updated_at
                )
                SELECT guild_id, NULL, 1, 0, EXTRACT(EPOCH FROM NOW())::BIGINT
                FROM guilds
                ON CONFLICT (guild_id) WHERE account_scope_code = 1
                DO NOTHING
            """)
        )
        await connection.execute(
            text("""
                INSERT INTO treasury_categories (
                    guild_id, account_scope_code, direction, category_name, is_active
                )
                SELECT g.guild_id, 1, category.direction, category.category_name, TRUE
                FROM guilds g
                CROSS JOIN (
                    VALUES
                        (1, '연합비 입금'),
                        (1, '이벤트비 입금'),
                        (1, '수수료 입금'),
                        (1, '운영비 입금'),
                        (1, '기타 입금'),
                        (-1, '연합비 지출'),
                        (-1, '이벤트비 지출'),
                        (-1, '운영비 지출'),
                        (-1, '환급'),
                        (-1, '기타 지출')
                ) AS category(direction, category_name)
                ON CONFLICT (guild_id, account_scope_code, direction, category_name)
                DO NOTHING
            """)
        )

        drop_sales_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 8")
        )
        if not drop_sales_applied:
            await connection.execute(
                text("""
                    CREATE TABLE settlement_drop_sales (
                        drop_id BIGINT PRIMARY KEY
                            REFERENCES settlement_drops(drop_id) ON DELETE CASCADE,
                        status_code SMALLINT NOT NULL DEFAULT 0,
                        buyer_alliance_id BIGINT
                            REFERENCES alliances(alliance_id) ON DELETE RESTRICT,
                        buyer_user_id BIGINT
                            REFERENCES users(user_id) ON DELETE SET NULL,
                        completed_at BIGINT,
                        completed_by_user_id BIGINT
                            REFERENCES users(user_id) ON DELETE SET NULL,
                        created_at BIGINT NOT NULL,
                        updated_at BIGINT NOT NULL,
                        CONSTRAINT chk_drop_sale_status
                            CHECK (status_code IN (0, 1)),
                        CONSTRAINT chk_drop_sale_completion
                            CHECK (
                                (
                                    status_code = 0
                                    AND buyer_alliance_id IS NULL
                                    AND buyer_user_id IS NULL
                                    AND completed_at IS NULL
                                    AND completed_by_user_id IS NULL
                                )
                                OR (
                                    status_code = 1
                                    AND buyer_alliance_id IS NOT NULL
                                    AND completed_at IS NOT NULL
                                )
                            )
                    )
                """)
            )
            await connection.execute(
                text("""
                    CREATE INDEX idx_drop_sales_status_time
                    ON settlement_drop_sales (status_code, updated_at DESC, drop_id DESC)
                """)
            )
            await connection.execute(
                text("""
                    CREATE INDEX idx_drop_sales_buyer_alliance_time
                    ON settlement_drop_sales (
                        buyer_alliance_id, completed_at DESC, drop_id DESC
                    )
                    WHERE status_code = 1
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO settlement_drop_sales (
                        drop_id, status_code, buyer_alliance_id, buyer_user_id,
                        completed_at, completed_by_user_id, created_at, updated_at
                    )
                    SELECT drop_id, 0, NULL, NULL, NULL, NULL,
                           occurred_at, occurred_at
                    FROM settlement_drops
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO audit_entity_types(entity_type_id, entity_code)
                    VALUES (7, 'sale')
                    ON CONFLICT (entity_code) DO NOTHING
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO audit_action_types(
                        action_type_id, action_code, entity_type_id
                    ) VALUES
                        (16, 'sale_complete', 7),
                        (17, 'sale_update', 7),
                        (18, 'sale_reopen', 7)
                    ON CONFLICT (action_code) DO NOTHING
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (8, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        attendance_end_time_cleanup_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 9")
        )
        if not attendance_end_time_cleanup_applied:
            await connection.execute(
                text("ALTER TABLE attendance_sessions DROP COLUMN IF EXISTS ended_at")
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (9, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        item_guild_scope_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 10")
        )
        if not item_guild_scope_applied:
            await connection.execute(
                text("""
                    WITH default_guild AS (
                        SELECT g.guild_id
                        FROM guilds g
                        LEFT JOIN settlement_drops d ON d.guild_id = g.guild_id
                        GROUP BY g.guild_id
                        ORDER BY COUNT(d.drop_id) DESC, g.guild_id
                        LIMIT 1
                    )
                    UPDATE items i
                    SET guild_id = COALESCE(
                        (
                            SELECT d.guild_id
                            FROM catalog_item_versions v
                            JOIN settlement_drops d ON d.item_version_id = v.item_version_id
                            WHERE v.item_id = i.item_id
                            GROUP BY d.guild_id
                            ORDER BY COUNT(d.drop_id) DESC, d.guild_id
                            LIMIT 1
                        ),
                        (SELECT guild_id FROM default_guild)
                    )
                    WHERE i.guild_id IS NULL
                """)
            )
            remaining_items = int(
                await connection.scalar(
                    text("SELECT COUNT(*) FROM items WHERE guild_id IS NULL")
                )
                or 0
            )
            if remaining_items:
                raise RuntimeError("길드가 없는 아이템을 귀속할 서버가 없습니다.")
            await connection.execute(
                text("ALTER TABLE items ALTER COLUMN guild_id SET NOT NULL")
            )
            await connection.execute(
                text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_items_guild_lower_name
                    ON items (guild_id, LOWER(item_name))
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (10, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        item_active_state_cleanup_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 11")
        )
        if not item_active_state_cleanup_applied:
            await connection.execute(text("DROP INDEX IF EXISTS idx_items_guild_active_name"))
            await connection.execute(text("ALTER TABLE items DROP COLUMN IF EXISTS is_active"))
            await connection.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS idx_items_guild_name
                    ON items (guild_id, item_name)
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (11, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        item_status_code_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 12")
        )
        if not item_status_code_applied:
            await connection.execute(
                text("""
                    ALTER TABLE items
                    ADD COLUMN IF NOT EXISTS status_code SMALLINT NOT NULL DEFAULT 1
                """)
            )
            await connection.execute(
                text("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1
                            FROM pg_constraint
                            WHERE conname = 'chk_items_status_code'
                              AND conrelid = 'items'::regclass
                        ) THEN
                            ALTER TABLE items
                            ADD CONSTRAINT chk_items_status_code
                            CHECK (status_code IN (0, 1));
                        END IF;
                    END
                    $$
                """)
            )
            await connection.execute(text("DROP INDEX IF EXISTS idx_items_guild_name"))
            await connection.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS idx_items_guild_status_name
                    ON items (guild_id, status_code, item_name)
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (12, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True
    return changed


async def close_database() -> None:
    await engine.dispose()
