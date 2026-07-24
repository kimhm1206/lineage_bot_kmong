"""Persist Discord display names for operating assignments.

Revision ID: 20260724_02
Revises: 20260724_01
Create Date: 2026-07-24
"""
from __future__ import annotations

from alembic import op


revision = "20260724_02"
down_revision = "20260724_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE guild_user_assignments
        ADD COLUMN IF NOT EXISTS discord_display_name TEXT
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE guild_user_assignments
        DROP COLUMN IF EXISTS discord_display_name
        """
    )
