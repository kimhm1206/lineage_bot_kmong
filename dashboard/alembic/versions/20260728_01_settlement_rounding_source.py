"""Add a treasury source type for settlement rounding remainders.

Revision ID: 20260728_01
Revises: 20260726_01
Create Date: 2026-07-28
"""
from __future__ import annotations

from alembic import op


revision = "20260728_01"
down_revision = "20260726_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO treasury_source_types(source_type_id, source_code)
        SELECT COALESCE(MAX(source_type_id), 0) + 1, 'settlement_rounding'
        FROM treasury_source_types
        ON CONFLICT (source_code) DO NOTHING
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM treasury_source_types source_type
        WHERE source_type.source_code = 'settlement_rounding'
          AND NOT EXISTS (
              SELECT 1
              FROM treasury_entries entry
              WHERE entry.source_type_id = source_type.source_type_id
          )
        """
    )
