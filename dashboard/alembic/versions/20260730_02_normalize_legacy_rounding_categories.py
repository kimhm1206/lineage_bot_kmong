"""Normalize legacy numeric settlement rounding categories.

Revision ID: 20260730_02
Revises: 20260730_01
Create Date: 2026-07-30
"""
from __future__ import annotations

from alembic import op


revision = "20260730_02"
down_revision = "20260730_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO treasury_categories (
            guild_id, account_scope_code, direction, category_name, is_active
        )
        SELECT DISTINCT
               guild_id, account_scope_code, direction,
               '분배 후 나머지', TRUE
        FROM treasury_categories
        WHERE category_name LIKE '분배 후 나머지[%]'
        ON CONFLICT (
            guild_id, account_scope_code, direction, category_name
        ) DO UPDATE SET is_active = TRUE
        """
    )
    op.execute(
        "ALTER TABLE treasury_entries "
        "DISABLE TRIGGER trg_treasury_entry_no_update"
    )
    op.execute(
        """
        UPDATE treasury_entries entry
        SET treasury_category_id = canonical.treasury_category_id,
            memo = '분배 후 나머지 Drop#'
                   || REGEXP_REPLACE(
                       TRIM(
                           TRAILING ']' FROM
                           SPLIT_PART(legacy.category_name, '[', 2)
                       ),
                       '^Drop#',
                       ''
                   )
                   || CASE WHEN entry.direction = -1 THEN ' 취소' ELSE '' END
        FROM treasury_categories legacy,
             treasury_categories canonical
        WHERE entry.treasury_category_id = legacy.treasury_category_id
          AND legacy.category_name LIKE '분배 후 나머지[%]'
          AND canonical.guild_id = legacy.guild_id
          AND canonical.account_scope_code = legacy.account_scope_code
          AND canonical.direction = legacy.direction
          AND canonical.category_name = '분배 후 나머지'
        """
    )
    op.execute(
        "ALTER TABLE treasury_entries "
        "ENABLE TRIGGER trg_treasury_entry_no_update"
    )
    op.execute(
        """
        DELETE FROM treasury_categories
        WHERE category_name LIKE '분배 후 나머지[%]'
          AND NOT EXISTS (
              SELECT 1
              FROM treasury_entries entry
              WHERE entry.treasury_category_id =
                    treasury_categories.treasury_category_id
          )
        """
    )


def downgrade() -> None:
    # The normalized category remains valid for earlier application versions.
    pass
