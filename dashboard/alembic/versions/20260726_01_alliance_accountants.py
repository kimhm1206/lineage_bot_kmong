"""Add alliance accountant assignments.

Revision ID: 20260726_01
Revises: 20260724_03
Create Date: 2026-07-26
"""
from __future__ import annotations

from alembic import op


revision = "20260726_01"
down_revision = "20260724_03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE guild_user_assignments
            DROP CONSTRAINT IF EXISTS guild_user_assignments_scope_code_check,
            DROP CONSTRAINT IF EXISTS chk_guild_user_assignment_scope,
            DROP CONSTRAINT IF EXISTS chk_guild_user_assignment_scope_code
        """
    )
    op.execute(
        """
        ALTER TABLE guild_user_assignments
            ADD CONSTRAINT chk_guild_user_assignment_scope_code
                CHECK (scope_code IN (1, 2, 3, 4)),
            ADD CONSTRAINT chk_guild_user_assignment_scope
                CHECK (
                    (scope_code IN (1, 4) AND alliance_id IS NULL)
                    OR (scope_code IN (2, 3) AND alliance_id IS NOT NULL)
                )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_guild_alliance_accountant_user
        ON guild_user_assignments (guild_id, discord_user_id, scope_code)
        WHERE scope_code = 4
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM guild_user_assignments
        WHERE scope_code = 4
        """
    )
    op.execute(
        """
        DROP INDEX IF EXISTS uq_guild_alliance_accountant_user
        """
    )
    op.execute(
        """
        ALTER TABLE guild_user_assignments
            DROP CONSTRAINT IF EXISTS chk_guild_user_assignment_scope,
            DROP CONSTRAINT IF EXISTS chk_guild_user_assignment_scope_code
        """
    )
    op.execute(
        """
        ALTER TABLE guild_user_assignments
            ADD CONSTRAINT chk_guild_user_assignment_scope_code
                CHECK (scope_code IN (1, 2, 3)),
            ADD CONSTRAINT chk_guild_user_assignment_scope
                CHECK (
                    (scope_code = 1 AND alliance_id IS NULL)
                    OR (scope_code IN (2, 3) AND alliance_id IS NOT NULL)
                )
        """
    )
