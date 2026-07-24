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
        "treasury_distribution_recipients",
        "treasury_distributions",
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
            discord_display_name TEXT,
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
        ALTER TABLE guild_user_assignments
        ADD COLUMN IF NOT EXISTS discord_display_name TEXT
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
                CHECK (distribution_visibility_code IN (1, 2)),
            treasury_visibility_code SMALLINT NOT NULL DEFAULT 2
                CHECK (treasury_visibility_code IN (1, 2)),
            user_access_code SMALLINT NOT NULL DEFAULT 2
                CHECK (user_access_code IN (2, 3)),
            updated_by_discord_user_id BIGINT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (guild_id, alliance_id)
        )
        """,
        """
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
            run_time TEXT NOT NULL,
            channel_id BIGINT NOT NULL,
            channel_name TEXT NOT NULL,
            schedule_json JSONB NOT NULL,
            query_json JSONB NOT NULL,
            render_json JSONB NOT NULL,
            status TEXT NOT NULL DEFAULT 'on'
                CHECK (status IN ('on', 'off', 'delete')),
            last_sent_at TEXT,
            next_run_at TEXT,
            memo TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_scheduled_report_settings_active
        ON scheduled_report_settings (status, next_run_at, guild_id)
        WHERE status <> 'delete'
        """,
        """
        INSERT INTO treasury_source_types(source_type_id, source_code)
        SELECT COALESCE(MAX(source_type_id), 0) + 1,
               'alliance_distribution_receipt'
        FROM treasury_source_types
        ON CONFLICT (source_code) DO NOTHING
        """,
        """
        INSERT INTO audit_entity_types(entity_type_id, entity_code)
        VALUES
            (8, 'fee_rule'),
            (9, 'configuration'),
            (10, 'assignment'),
            (11, 'report')
        ON CONFLICT (entity_code) DO NOTHING
        """,
        """
        INSERT INTO audit_action_types(
            action_type_id, action_code, entity_type_id
        ) VALUES
            (19, 'fee_rule_create', 8),
            (20, 'fee_rule_update', 8),
            (21, 'guild_update', 9),
            (22, 'attendance_settings_update', 9),
            (23, 'alliance_mapping_create', 9),
            (24, 'alliance_mapping_delete', 9),
            (25, 'assignment_create', 10),
            (26, 'assignment_delete', 10),
            (27, 'clan_policy_update', 9),
            (28, 'treasury_distribution_create', 6),
            (29, 'report_create', 11),
            (30, 'report_update', 11),
            (31, 'report_status', 11)
        ON CONFLICT (action_code) DO NOTHING
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
                        (1, '연합 수수료'),
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

        treasury_distribution_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 13")
        )
        if not treasury_distribution_applied:
            await connection.execute(
                text("""
                    INSERT INTO treasury_source_types(source_type_id, source_code)
                    SELECT COALESCE(MAX(source_type_id), 0) + 1,
                           'treasury_distribution'
                    FROM treasury_source_types
                    ON CONFLICT (source_code) DO NOTHING
                """)
            )
            await connection.execute(
                text("""
                    CREATE TABLE treasury_distributions (
                        treasury_distribution_id BIGSERIAL PRIMARY KEY,
                        treasury_account_id BIGINT NOT NULL
                            REFERENCES treasury_accounts(treasury_account_id) ON DELETE RESTRICT,
                        requested_amount BIGINT NOT NULL
                            CHECK (requested_amount > 0),
                        per_recipient_amount BIGINT NOT NULL
                            CHECK (per_recipient_amount > 0),
                        distributed_amount BIGINT NOT NULL
                            CHECK (distributed_amount > 0),
                        recipient_count INTEGER NOT NULL
                            CHECK (recipient_count > 0),
                        memo TEXT,
                        created_at BIGINT NOT NULL
                            DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT),
                        created_by_user_id BIGINT
                            REFERENCES users(user_id) ON DELETE SET NULL,
                        CONSTRAINT chk_treasury_distribution_amounts
                            CHECK (
                                distributed_amount = per_recipient_amount * recipient_count
                                AND distributed_amount <= requested_amount
                            )
                    )
                """)
            )
            await connection.execute(
                text("""
                    CREATE TABLE treasury_distribution_recipients (
                        treasury_distribution_id BIGINT NOT NULL
                            REFERENCES treasury_distributions(treasury_distribution_id)
                            ON DELETE CASCADE,
                        user_id BIGINT NOT NULL
                            REFERENCES users(user_id) ON DELETE RESTRICT,
                        status_code SMALLINT NOT NULL DEFAULT 0
                            CHECK (status_code IN (0, 1)),
                        completed_at BIGINT,
                        PRIMARY KEY (treasury_distribution_id, user_id),
                        CONSTRAINT chk_treasury_distribution_recipient_completion
                            CHECK (
                                (status_code = 0 AND completed_at IS NULL)
                                OR (status_code = 1 AND completed_at IS NOT NULL)
                            )
                    )
                """)
            )
            await connection.execute(
                text("""
                    CREATE INDEX idx_treasury_distributions_account_time
                    ON treasury_distributions (
                        treasury_account_id, created_at DESC, treasury_distribution_id DESC
                    )
                """)
            )
            await connection.execute(
                text("""
                    CREATE INDEX idx_treasury_distribution_recipients_status
                    ON treasury_distribution_recipients (
                        treasury_distribution_id, status_code, user_id
                    )
                """)
            )
            await connection.execute(
                text("""
                    CREATE INDEX idx_treasury_distribution_recipients_user
                    ON treasury_distribution_recipients (user_id, status_code)
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (13, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        bid_catalog_unification_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 14")
        )
        if not bid_catalog_unification_applied:
            await connection.execute(
                text("""
                    ALTER TABLE bid_item_results
                    ADD COLUMN IF NOT EXISTS item_id BIGINT
                """)
            )
            await connection.execute(
                text("""
                    UPDATE bid_item_results r
                    SET item_id = i.item_id
                    FROM bid_items b
                    JOIN items i
                      ON i.guild_id = b.guild_id
                     AND LOWER(i.item_name) = LOWER(b.item_name)
                    WHERE r.bid_item_id = b.bid_item_id
                      AND r.item_id IS NULL
                """)
            )
            await connection.execute(
                text("DELETE FROM bid_item_results WHERE item_id IS NULL")
            )
            await connection.execute(
                text("""
                    ALTER TABLE bid_item_results
                    DROP CONSTRAINT IF EXISTS bid_item_results_bid_item_id_fkey
                """)
            )
            await connection.execute(text("DROP INDEX IF EXISTS idx_bid_results_item_cycle"))
            await connection.execute(text("DROP INDEX IF EXISTS idx_bid_results_unique_cycle"))
            await connection.execute(
                text("""
                    ALTER TABLE bid_item_results
                    ALTER COLUMN item_id SET NOT NULL,
                    DROP COLUMN IF EXISTS bid_item_id,
                    ADD CONSTRAINT bid_item_results_item_id_fkey
                        FOREIGN KEY (item_id)
                        REFERENCES items(item_id) ON DELETE CASCADE
                """)
            )
            await connection.execute(
                text("""
                    CREATE INDEX idx_bid_results_item_time
                    ON bid_item_results (
                        guild_id, item_id, selected_at DESC, result_id DESC
                    )
                """)
            )
            await connection.execute(
                text("""
                    CREATE UNIQUE INDEX idx_bid_results_unique_cycle
                    ON bid_item_results (
                        guild_id, item_id, alliance_id, cycle_no
                    )
                """)
            )
            await connection.execute(text("DROP TABLE bid_items"))
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (14, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        fixed_treasury_fee_rules_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 15")
        )
        if not fixed_treasury_fee_rules_applied:
            await connection.execute(
                text("""
                    ALTER TABLE settlement_fee_rules
                    ADD COLUMN IF NOT EXISTS fixed_code TEXT
                """)
            )
            await connection.execute(
                text("""
                    WITH candidates AS (
                        SELECT r.fee_rule_id,
                               CASE
                                   WHEN r.scope_code = 1
                                    AND r.alliance_id IS NULL
                                    AND latest.rule_name = '연합 수수료'
                                       THEN 'alliance_fee'
                                   WHEN r.scope_code = 2
                                    AND r.alliance_id IS NOT NULL
                                    AND latest.rule_name = '혈비'
                                       THEN 'clan_fund'
                               END AS fixed_code,
                               ROW_NUMBER() OVER (
                                   PARTITION BY
                                       r.guild_id,
                                       r.scope_code,
                                       CASE WHEN r.scope_code = 1 THEN NULL ELSE r.alliance_id END
                                   ORDER BY r.fee_rule_id DESC
                               ) AS position
                        FROM settlement_fee_rules r
                        JOIN LATERAL (
                            SELECT v.rule_name
                            FROM settlement_fee_rule_versions v
                            WHERE v.fee_rule_id = r.fee_rule_id
                            ORDER BY v.valid_from DESC, v.fee_rule_version_id DESC
                            LIMIT 1
                        ) latest ON TRUE
                        WHERE (r.scope_code = 1 AND latest.rule_name = '연합 수수료')
                           OR (r.scope_code = 2 AND latest.rule_name = '혈비')
                    )
                    UPDATE settlement_fee_rules r
                    SET fixed_code = candidates.fixed_code,
                        is_active = TRUE
                    FROM candidates
                    WHERE r.fee_rule_id = candidates.fee_rule_id
                      AND candidates.fixed_code IS NOT NULL
                      AND candidates.position = 1
                """)
            )
            await connection.execute(
                text("""
                    WITH duplicate_fixed_rules AS (
                        SELECT r.fee_rule_id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY r.guild_id, r.scope_code, r.alliance_id
                                   ORDER BY r.fee_rule_id DESC
                               ) AS position
                        FROM settlement_fee_rules r
                        JOIN LATERAL (
                            SELECT v.rule_name
                            FROM settlement_fee_rule_versions v
                            WHERE v.fee_rule_id = r.fee_rule_id
                            ORDER BY v.valid_from DESC, v.fee_rule_version_id DESC
                            LIMIT 1
                        ) latest ON TRUE
                        WHERE (r.scope_code = 1 AND latest.rule_name = '연합 수수료')
                           OR (r.scope_code = 2 AND latest.rule_name = '혈비')
                    )
                    UPDATE settlement_fee_rules r
                    SET is_active = FALSE
                    FROM duplicate_fixed_rules duplicate
                    WHERE r.fee_rule_id = duplicate.fee_rule_id
                      AND duplicate.position > 1
                """)
            )
            await connection.execute(
                text("""
                    WITH inserted AS (
                        INSERT INTO settlement_fee_rules (
                            guild_id, alliance_id, scope_code, is_active, fixed_code
                        )
                        SELECT g.guild_id, NULL, 1, TRUE, 'alliance_fee'
                        FROM guilds g
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM settlement_fee_rules r
                            WHERE r.guild_id = g.guild_id
                              AND r.fixed_code = 'alliance_fee'
                        )
                        RETURNING fee_rule_id
                    )
                    INSERT INTO settlement_fee_rule_versions (
                        fee_rule_id, rule_name, rate_ppm, valid_from
                    )
                    SELECT fee_rule_id, '연합 수수료', 0,
                           EXTRACT(EPOCH FROM NOW())::BIGINT
                    FROM inserted
                """)
            )
            await connection.execute(
                text("""
                    WITH mapped_alliances AS (
                        SELECT DISTINCT guild_id, alliance_id
                        FROM guild_alliance_role_mappings
                    ),
                    inserted AS (
                        INSERT INTO settlement_fee_rules (
                            guild_id, alliance_id, scope_code, is_active, fixed_code
                        )
                        SELECT mapped.guild_id, mapped.alliance_id, 2, TRUE, 'clan_fund'
                        FROM mapped_alliances mapped
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM settlement_fee_rules r
                            WHERE r.guild_id = mapped.guild_id
                              AND r.alliance_id = mapped.alliance_id
                              AND r.fixed_code = 'clan_fund'
                        )
                        RETURNING fee_rule_id
                    )
                    INSERT INTO settlement_fee_rule_versions (
                        fee_rule_id, rule_name, rate_ppm, valid_from
                    )
                    SELECT fee_rule_id, '혈비', 0,
                           EXTRACT(EPOCH FROM NOW())::BIGINT
                    FROM inserted
                """)
            )
            await connection.execute(
                text("""
                    ALTER TABLE settlement_fee_rules
                    DROP CONSTRAINT IF EXISTS chk_settlement_fee_rule_fixed_scope,
                    ADD CONSTRAINT chk_settlement_fee_rule_fixed_scope
                    CHECK (
                        fixed_code IS NULL
                        OR (
                            fixed_code = 'alliance_fee'
                            AND scope_code = 1
                            AND alliance_id IS NULL
                        )
                        OR (
                            fixed_code = 'clan_fund'
                            AND scope_code = 2
                            AND alliance_id IS NOT NULL
                        )
                    )
                """)
            )
            await connection.execute(
                text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_fee_rules_fixed_alliance
                    ON settlement_fee_rules (guild_id)
                    WHERE fixed_code = 'alliance_fee'
                """)
            )
            await connection.execute(
                text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_fee_rules_fixed_clan
                    ON settlement_fee_rules (guild_id, alliance_id)
                    WHERE fixed_code = 'clan_fund'
                """)
            )
            await connection.execute(
                text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_payout_fee_drop_rule
                    ON settlement_payout_objects (drop_id, fee_rule_version_id)
                    WHERE object_code = 3 AND parent_payout_object_id IS NULL
                """)
            )
            await connection.execute(
                text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_payout_fee_parent_rule
                    ON settlement_payout_objects (
                        parent_payout_object_id, fee_rule_version_id
                    )
                    WHERE object_code = 3 AND parent_payout_object_id IS NOT NULL
                """)
            )
            await connection.execute(
                text("""
                    CREATE OR REPLACE FUNCTION ensure_guild_fixed_fee_rule()
                    RETURNS TRIGGER AS $$
                    DECLARE
                        new_rule_id BIGINT;
                    BEGIN
                        INSERT INTO settlement_fee_rules (
                            guild_id, alliance_id, scope_code, is_active, fixed_code
                        ) VALUES (
                            NEW.guild_id, NULL, 1, TRUE, 'alliance_fee'
                        )
                        ON CONFLICT (guild_id)
                            WHERE fixed_code = 'alliance_fee'
                        DO NOTHING
                        RETURNING fee_rule_id INTO new_rule_id;

                        IF new_rule_id IS NOT NULL THEN
                            INSERT INTO settlement_fee_rule_versions (
                                fee_rule_id, rule_name, rate_ppm, valid_from
                            ) VALUES (
                                new_rule_id, '연합 수수료', 0,
                                EXTRACT(EPOCH FROM NOW())::BIGINT
                            );
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                """)
            )
            await connection.execute(
                text("""
                    DROP TRIGGER IF EXISTS trg_ensure_guild_fixed_fee_rule ON guilds
                """)
            )
            await connection.execute(
                text("""
                    CREATE TRIGGER trg_ensure_guild_fixed_fee_rule
                    AFTER INSERT ON guilds
                    FOR EACH ROW EXECUTE FUNCTION ensure_guild_fixed_fee_rule()
                """)
            )
            await connection.execute(
                text("""
                    CREATE OR REPLACE FUNCTION ensure_clan_fixed_fee_rule()
                    RETURNS TRIGGER AS $$
                    DECLARE
                        new_rule_id BIGINT;
                    BEGIN
                        INSERT INTO settlement_fee_rules (
                            guild_id, alliance_id, scope_code, is_active, fixed_code
                        ) VALUES (
                            NEW.guild_id, NEW.alliance_id, 2, TRUE, 'clan_fund'
                        )
                        ON CONFLICT (guild_id, alliance_id)
                            WHERE fixed_code = 'clan_fund'
                        DO NOTHING
                        RETURNING fee_rule_id INTO new_rule_id;

                        IF new_rule_id IS NOT NULL THEN
                            INSERT INTO settlement_fee_rule_versions (
                                fee_rule_id, rule_name, rate_ppm, valid_from
                            ) VALUES (
                                new_rule_id, '혈비', 0,
                                EXTRACT(EPOCH FROM NOW())::BIGINT
                            );
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                """)
            )
            await connection.execute(
                text("""
                    DROP TRIGGER IF EXISTS trg_ensure_clan_fixed_fee_rule
                    ON guild_alliance_role_mappings
                """)
            )
            await connection.execute(
                text("""
                    CREATE TRIGGER trg_ensure_clan_fixed_fee_rule
                    AFTER INSERT OR UPDATE OF guild_id, alliance_id
                    ON guild_alliance_role_mappings
                    FOR EACH ROW EXECUTE FUNCTION ensure_clan_fixed_fee_rule()
                """)
            )
            await connection.execute(
                text("""
                    CREATE OR REPLACE FUNCTION protect_fixed_fee_rule()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        IF TG_OP = 'DELETE' AND OLD.fixed_code IS NOT NULL THEN
                            RAISE EXCEPTION 'fixed fee rule cannot be deleted';
                        END IF;
                        IF TG_OP = 'UPDATE' AND OLD.fixed_code IS NOT NULL THEN
                            NEW.guild_id := OLD.guild_id;
                            NEW.alliance_id := OLD.alliance_id;
                            NEW.scope_code := OLD.scope_code;
                            NEW.fixed_code := OLD.fixed_code;
                            NEW.is_active := TRUE;
                        END IF;
                        IF TG_OP = 'DELETE' THEN
                            RETURN OLD;
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                """)
            )
            await connection.execute(
                text("""
                    DROP TRIGGER IF EXISTS trg_protect_fixed_fee_rule
                    ON settlement_fee_rules
                """)
            )
            await connection.execute(
                text("""
                    CREATE TRIGGER trg_protect_fixed_fee_rule
                    BEFORE UPDATE OR DELETE ON settlement_fee_rules
                    FOR EACH ROW EXECUTE FUNCTION protect_fixed_fee_rule()
                """)
            )
            await connection.execute(
                text("""
                    CREATE OR REPLACE FUNCTION normalize_fixed_fee_rule_version()
                    RETURNS TRIGGER AS $$
                    DECLARE
                        rule_fixed_code TEXT;
                    BEGIN
                        SELECT fixed_code INTO rule_fixed_code
                        FROM settlement_fee_rules
                        WHERE fee_rule_id = NEW.fee_rule_id;
                        IF rule_fixed_code = 'alliance_fee' THEN
                            NEW.rule_name := '연합 수수료';
                        ELSIF rule_fixed_code = 'clan_fund' THEN
                            NEW.rule_name := '혈비';
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                """)
            )
            await connection.execute(
                text("""
                    DROP TRIGGER IF EXISTS trg_normalize_fixed_fee_rule_version
                    ON settlement_fee_rule_versions
                """)
            )
            await connection.execute(
                text("""
                    CREATE TRIGGER trg_normalize_fixed_fee_rule_version
                    BEFORE INSERT OR UPDATE ON settlement_fee_rule_versions
                    FOR EACH ROW EXECUTE FUNCTION normalize_fixed_fee_rule_version()
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO treasury_categories (
                        guild_id, account_scope_code, direction,
                        category_name, is_active
                    )
                    SELECT guild_id, 1, 1, '연합 수수료', TRUE FROM guilds
                    UNION ALL
                    SELECT guild_id, 2, 1, '혈비', TRUE FROM guilds
                    ON CONFLICT (
                        guild_id, account_scope_code, direction, category_name
                    ) DO UPDATE SET is_active = TRUE
                """)
            )
            await connection.execute(
                text("ALTER TABLE treasury_entries DISABLE TRIGGER USER")
            )
            await connection.execute(
                text("""
                    DELETE FROM treasury_entries entry
                    WHERE entry.source_type_id IN (3, 4)
                      AND NOT EXISTS (
                          SELECT 1
                          FROM settlement_payout_objects payout
                          JOIN settlement_fee_rule_versions version
                            ON version.fee_rule_version_id = payout.fee_rule_version_id
                          JOIN settlement_fee_rules rule
                            ON rule.fee_rule_id = version.fee_rule_id
                          WHERE payout.payout_object_id = entry.source_id
                            AND (
                                (
                                    entry.source_type_id = 4
                                    AND rule.fixed_code = 'alliance_fee'
                                )
                                OR (
                                    entry.source_type_id = 3
                                    AND rule.fixed_code = 'clan_fund'
                                )
                            )
                      )
                """)
            )
            await connection.execute(
                text("""
                    UPDATE treasury_entries entry
                    SET treasury_category_id = category.treasury_category_id
                    FROM treasury_accounts account,
                         treasury_categories category
                    WHERE account.treasury_account_id = entry.treasury_account_id
                      AND category.guild_id = account.guild_id
                      AND category.account_scope_code = account.account_scope_code
                      AND category.direction = 1
                      AND category.category_name = CASE
                          WHEN entry.source_type_id = 4 THEN '연합 수수료'
                          ELSE '혈비'
                      END
                      AND entry.source_type_id IN (3, 4)
                """)
            )
            await connection.execute(
                text("""
                    WITH running_balances AS (
                        SELECT treasury_entry_id,
                               SUM(direction * amount_adena) OVER (
                                   PARTITION BY treasury_account_id
                                   ORDER BY occurred_at, treasury_entry_id
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                               ) AS balance_after
                        FROM treasury_entries
                    )
                    UPDATE treasury_entries entry
                    SET balance_after = running.balance_after
                    FROM running_balances running
                    WHERE running.treasury_entry_id = entry.treasury_entry_id
                """)
            )
            await connection.execute(
                text("""
                    UPDATE treasury_accounts account
                    SET current_balance = COALESCE((
                            SELECT SUM(entry.direction * entry.amount_adena)
                            FROM treasury_entries entry
                            WHERE entry.treasury_account_id =
                                  account.treasury_account_id
                        ), 0),
                        updated_at = EXTRACT(EPOCH FROM NOW())::BIGINT
                """)
            )
            await connection.execute(
                text("ALTER TABLE treasury_entries ENABLE TRIGGER USER")
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (15, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        fixed_treasury_category_cleanup_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 16")
        )
        if not fixed_treasury_category_cleanup_applied:
            await connection.execute(
                text("""
                    DELETE FROM treasury_categories category
                    WHERE category.category_name = '수수료 입금'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM treasury_entries entry
                          WHERE entry.treasury_category_id =
                                category.treasury_category_id
                      )
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (16, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        treasury_distribution_target_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 17")
        )
        if not treasury_distribution_target_applied:
            await connection.execute(
                text("""
                    ALTER TABLE treasury_distribution_recipients
                    DROP CONSTRAINT treasury_distribution_recipients_pkey
                """)
            )
            await connection.execute(
                text("""
                    ALTER TABLE treasury_distribution_recipients
                    DROP CONSTRAINT chk_treasury_distribution_recipient_completion
                """)
            )
            await connection.execute(
                text("""
                    ALTER TABLE treasury_distribution_recipients
                    DROP CONSTRAINT treasury_distribution_recipients_status_code_check
                """)
            )
            await connection.execute(
                text("""
                    ALTER TABLE treasury_distribution_recipients
                    ALTER COLUMN user_id DROP NOT NULL,
                    ADD COLUMN treasury_distribution_recipient_id BIGSERIAL,
                    ADD COLUMN alliance_id BIGINT
                        REFERENCES alliances(alliance_id) ON DELETE RESTRICT
                """)
            )
            await connection.execute(
                text("""
                    ALTER TABLE treasury_distribution_recipients
                    ADD PRIMARY KEY (treasury_distribution_recipient_id),
                    ADD CONSTRAINT chk_treasury_distribution_recipient_target
                        CHECK ((user_id IS NULL) <> (alliance_id IS NULL)),
                    ADD CONSTRAINT chk_treasury_distribution_recipient_status
                        CHECK (status_code IN (0, 1, 2)),
                    ADD CONSTRAINT chk_treasury_distribution_recipient_completion
                        CHECK (
                            (status_code = 0 AND completed_at IS NULL)
                            OR (status_code IN (1, 2) AND completed_at IS NOT NULL)
                        )
                """)
            )
            await connection.execute(
                text("""
                    CREATE UNIQUE INDEX
                        uq_treasury_distribution_recipient_user
                    ON treasury_distribution_recipients (
                        treasury_distribution_id, user_id
                    )
                    WHERE user_id IS NOT NULL
                """)
            )
            await connection.execute(
                text("""
                    CREATE UNIQUE INDEX
                        uq_treasury_distribution_recipient_alliance
                    ON treasury_distribution_recipients (
                        treasury_distribution_id, alliance_id
                    )
                    WHERE alliance_id IS NOT NULL
                """)
            )
            await connection.execute(
                text("""
                    CREATE INDEX idx_treasury_distribution_recipients_alliance
                    ON treasury_distribution_recipients (alliance_id, status_code)
                    WHERE alliance_id IS NOT NULL
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (17, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        treasury_distribution_forfeiture_source_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 18")
        )
        if not treasury_distribution_forfeiture_source_applied:
            await connection.execute(
                text("""
                    INSERT INTO treasury_source_types(source_type_id, source_code)
                    SELECT COALESCE(MAX(source_type_id), 0) + 1,
                           'treasury_distribution_forfeiture'
                    FROM treasury_source_types
                    ON CONFLICT (source_code) DO NOTHING
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (18, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        alliance_distribution_receipt_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 19")
        )
        if not alliance_distribution_receipt_applied:
            await connection.execute(
                text("""
                    INSERT INTO treasury_source_types(source_type_id, source_code)
                    SELECT COALESCE(MAX(source_type_id), 0) + 1,
                           'alliance_distribution_receipt'
                    FROM treasury_source_types
                    ON CONFLICT (source_code) DO NOTHING
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (19, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        fee_history_index_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 20")
        )
        if not fee_history_index_applied:
            await connection.execute(
                text("""
                    CREATE INDEX IF NOT EXISTS idx_payout_fee_history
                    ON settlement_payout_objects (
                        fee_rule_version_id, status_code, payout_object_id DESC
                    )
                    WHERE object_code = 3
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (20, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        forfeiture_category_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 21")
        )
        if not forfeiture_category_applied:
            await connection.execute(
                text("""
                    DELETE FROM treasury_categories replacement
                    WHERE replacement.category_name = '귀속'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM treasury_entries entry
                          WHERE entry.treasury_category_id =
                                replacement.treasury_category_id
                      )
                      AND EXISTS (
                          SELECT 1
                          FROM treasury_categories previous
                          WHERE previous.guild_id = replacement.guild_id
                            AND previous.account_scope_code =
                                replacement.account_scope_code
                            AND previous.direction = replacement.direction
                            AND previous.category_name = '귀속 혈비'
                      )
                """)
            )
            await connection.execute(
                text("""
                    UPDATE treasury_categories previous
                    SET category_name = '귀속'
                    WHERE previous.category_name = '귀속 혈비'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM treasury_categories replacement
                          WHERE replacement.guild_id = previous.guild_id
                            AND replacement.account_scope_code =
                                previous.account_scope_code
                            AND replacement.direction = previous.direction
                            AND replacement.category_name = '귀속'
                      )
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (21, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True

        access_policy_cleanup_applied = await connection.scalar(
            text("SELECT 1 FROM schema_migrations WHERE version = 22")
        )
        if not access_policy_cleanup_applied:
            await connection.execute(
                text("""
                    UPDATE alliance_access_policies
                    SET distribution_visibility_code = 2
                    WHERE distribution_visibility_code = 3
                """)
            )
            await connection.execute(
                text("""
                    UPDATE alliance_access_policies
                    SET treasury_visibility_code = 2
                    WHERE treasury_visibility_code = 3
                """)
            )
            await connection.execute(
                text("""
                    UPDATE alliance_access_policies
                    SET user_access_code = 3
                    WHERE user_access_code = 1
                """)
            )
            await connection.execute(
                text("""
                    DO $$
                    DECLARE policy_constraint RECORD;
                    BEGIN
                        FOR policy_constraint IN
                            SELECT constraint_name
                            FROM information_schema.constraint_column_usage
                            WHERE table_schema = 'public'
                              AND table_name = 'alliance_access_policies'
                              AND column_name IN (
                                  'distribution_visibility_code',
                                  'treasury_visibility_code',
                                  'user_access_code'
                              )
                        LOOP
                            IF EXISTS (
                                SELECT 1
                                FROM pg_constraint
                                WHERE conname = policy_constraint.constraint_name
                                  AND contype = 'c'
                            ) THEN
                                EXECUTE format(
                                    'ALTER TABLE alliance_access_policies DROP CONSTRAINT %I',
                                    policy_constraint.constraint_name
                                );
                            END IF;
                        END LOOP;
                    END
                    $$;
                """)
            )
            await connection.execute(
                text("""
                    ALTER TABLE alliance_access_policies
                        ALTER COLUMN distribution_visibility_code SET DEFAULT 2,
                        ALTER COLUMN treasury_visibility_code SET DEFAULT 2,
                        ALTER COLUMN user_access_code SET DEFAULT 2,
                        ADD CONSTRAINT chk_alliance_access_distribution_visibility
                            CHECK (distribution_visibility_code IN (1, 2)),
                        ADD CONSTRAINT chk_alliance_access_treasury_visibility
                            CHECK (treasury_visibility_code IN (1, 2)),
                        ADD CONSTRAINT chk_alliance_access_user_access
                            CHECK (user_access_code IN (2, 3))
                """)
            )
            await connection.execute(
                text("""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES (22, EXTRACT(EPOCH FROM NOW())::BIGINT)
                """)
            )
            changed = True
    return changed


async def close_database() -> None:
    await engine.dispose()
