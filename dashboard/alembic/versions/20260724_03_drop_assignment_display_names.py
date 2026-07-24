"""Remove redundant assignment display-name snapshots.

Revision ID: 20260724_03
Revises: 20260724_02
Create Date: 2026-07-24
"""
from __future__ import annotations

from alembic import op


revision = "20260724_03"
down_revision = "20260724_02"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE guild_user_assignments
        DROP COLUMN IF EXISTS discord_display_name
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE guild_user_assignments
        ADD COLUMN IF NOT EXISTS discord_display_name TEXT
        """
    )
