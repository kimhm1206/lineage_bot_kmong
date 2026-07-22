from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.database import EXPECTED_TABLES


DATA_TABLES = (
    ("guilds", "서버"),
    ("alliances", "혈맹"),
    ("guild_alliance_role_mappings", "혈맹 역할 매핑"),
    ("guild_user_assignments", "운영 담당자"),
    ("users", "유저"),
    ("attendance_sessions", "출석 회차"),
    ("attendance_entries", "출석 인원"),
    ("items", "아이템"),
    ("settlement_drops", "드랍"),
    ("settlement_payout_objects", "분배 객체"),
    ("bid_items", "입찰 아이템"),
    ("bid_item_results", "입찰 결과"),
    ("treasury_entries", "혈비 원장"),
    ("audit_events", "작업 로그"),
)


async def database_overview(session: AsyncSession) -> dict[str, Any]:
    database_name = await session.scalar(text("SELECT current_database()"))
    actual_tables = set(
        (
            await session.execute(
                text("""
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = 'public'
                    ORDER BY tablename
                """)
            )
        ).scalars()
    )

    count_query = " UNION ALL ".join(
        f"SELECT '{table_name}' AS table_name, COUNT(*)::BIGINT AS row_count FROM {table_name}"
        for table_name, _ in DATA_TABLES
        if table_name in actual_tables
    )
    row_counts: dict[str, int] = {}
    if count_query:
        rows = (await session.execute(text(count_query))).mappings().all()
        row_counts = {row["table_name"]: row["row_count"] for row in rows}

    versions = []
    if "schema_migrations" in actual_tables:
        versions = list(
            (
                await session.execute(
                    text("SELECT version, applied_at FROM schema_migrations ORDER BY version DESC")
                )
            ).mappings()
        )

    return {
        "database_name": database_name,
        "table_count": len(actual_tables),
        "expected_table_count": len(EXPECTED_TABLES),
        "missing_tables": sorted(EXPECTED_TABLES - actual_tables),
        "unexpected_tables": sorted(actual_tables - EXPECTED_TABLES),
        "schema_versions": [dict(row) for row in versions],
        "data_counts": [
            {
                "table_name": table_name,
                "label": label,
                "row_count": row_counts.get(table_name, 0),
            }
            for table_name, label in DATA_TABLES
            if table_name in actual_tables
        ],
    }
