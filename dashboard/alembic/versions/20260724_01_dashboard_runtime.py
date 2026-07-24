"""Create dashboard runtime configuration tables.

Revision ID: 20260724_01
Revises:
Create Date: 2026-07-24
"""
from __future__ import annotations

from alembic import op


revision = "20260724_01"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
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
    for statement in statements:
        op.execute(statement)


def downgrade() -> None:
    # These tables may predate Alembic on existing installations.
    pass
