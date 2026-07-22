from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


SCOPE_ALLIANCE_MANAGER = 1
SCOPE_CLAN_MANAGER = 2
SCOPE_CLAN_ACCOUNTANT = 3


async def list_guilds(session: AsyncSession) -> list[dict[str, Any]]:
    result = await session.execute(
        text("""
            SELECT g.guild_id, g.is_enabled,
                   gs.admin_channel_id, gs.attendance_voice_channel_id,
                   gs.log_channel_id, gs.timer, gs.attendance_available_timer
            FROM guilds g
            LEFT JOIN guild_settings gs ON gs.guild_id = g.guild_id
            ORDER BY g.is_enabled DESC, g.guild_id
        """)
    )
    return [dict(row) for row in result.mappings()]


async def upsert_guild(session: AsyncSession, guild_id: int, *, is_enabled: bool = True) -> None:
    await session.execute(
        text("""
            INSERT INTO guilds (guild_id, is_enabled)
            VALUES (:guild_id, :is_enabled)
            ON CONFLICT (guild_id) DO UPDATE SET is_enabled = EXCLUDED.is_enabled
        """),
        {"guild_id": guild_id, "is_enabled": is_enabled},
    )
    await session.commit()


async def save_attendance_settings(
    session: AsyncSession,
    *,
    guild_id: int,
    admin_channel_id: int | None,
    voice_channel_id: int | None,
    log_channel_id: int | None,
    timer: int,
    attendance_available_timer: int,
) -> None:
    await session.execute(
        text("""
            INSERT INTO guild_settings (
                guild_id, admin_channel_id, attendance_voice_channel_id,
                attendance_voice_channel_ids, log_channel_id, timer,
                attendance_available_timer, updated_at
            ) VALUES (
                :guild_id, CAST(:admin_channel_id AS BIGINT), CAST(:voice_channel_id AS BIGINT),
                CASE WHEN CAST(:voice_channel_id AS BIGINT) IS NULL THEN '[]'::jsonb
                     ELSE jsonb_build_array(CAST(:voice_channel_id AS BIGINT)) END,
                CAST(:log_channel_id AS BIGINT), :timer, :attendance_available_timer, NOW()
            )
            ON CONFLICT (guild_id) DO UPDATE SET
                admin_channel_id = EXCLUDED.admin_channel_id,
                attendance_voice_channel_id = EXCLUDED.attendance_voice_channel_id,
                attendance_voice_channel_ids = EXCLUDED.attendance_voice_channel_ids,
                log_channel_id = EXCLUDED.log_channel_id,
                timer = EXCLUDED.timer,
                attendance_available_timer = EXCLUDED.attendance_available_timer,
                updated_at = NOW()
        """),
        {
            "guild_id": guild_id,
            "admin_channel_id": admin_channel_id,
            "voice_channel_id": voice_channel_id,
            "log_channel_id": log_channel_id,
            "timer": timer,
            "attendance_available_timer": attendance_available_timer,
        },
    )
    await session.commit()


async def list_guild_alliances(session: AsyncSession, guild_id: int) -> list[dict[str, Any]]:
    result = await session.execute(
        text("""
            SELECT DISTINCT a.alliance_id, a.alliance_name, a.display_name,
                            a.color, a.sort_order,
                            COALESCE(a.sort_order, 2147483647) AS resolved_sort_order
            FROM alliances a
            JOIN guild_alliance_role_mappings m ON m.alliance_id = a.alliance_id
            WHERE m.guild_id = :guild_id AND a.is_active = TRUE
            ORDER BY resolved_sort_order, a.alliance_name
        """),
        {"guild_id": guild_id},
    )
    return [dict(row) for row in result.mappings()]


async def list_role_mappings(session: AsyncSession, guild_id: int) -> list[dict[str, Any]]:
    result = await session.execute(
        text("""
            SELECT m.mapping_id, m.role_id, m.role_name, m.alliance_id, a.alliance_name
            FROM guild_alliance_role_mappings m
            JOIN alliances a ON a.alliance_id = m.alliance_id
            WHERE m.guild_id = :guild_id
            ORDER BY COALESCE(a.sort_order, 2147483647), a.alliance_name, m.role_name
        """),
        {"guild_id": guild_id},
    )
    return [dict(row) for row in result.mappings()]


async def save_role_mapping(
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_name: str,
    role_id: int,
    role_name: str,
) -> None:
    alliance_id = await session.scalar(
        text("""
            INSERT INTO alliances (alliance_name, display_name, is_active, updated_at)
            VALUES (:name, :name, TRUE, NOW())
            ON CONFLICT (alliance_name) DO UPDATE
            SET display_name = COALESCE(alliances.display_name, EXCLUDED.display_name),
                is_active = TRUE,
                updated_at = NOW()
            RETURNING alliance_id
        """),
        {"name": alliance_name},
    )
    await session.execute(
        text("""
            INSERT INTO guild_alliance_role_mappings (
                guild_id, role_id, role_name, alliance_id, updated_at
            ) VALUES (:guild_id, :role_id, :role_name, :alliance_id, NOW())
            ON CONFLICT (guild_id, role_id) DO UPDATE
            SET role_name = EXCLUDED.role_name,
                alliance_id = EXCLUDED.alliance_id,
                updated_at = NOW()
        """),
        {
            "guild_id": guild_id,
            "role_id": role_id,
            "role_name": role_name,
            "alliance_id": alliance_id,
        },
    )
    await session.commit()


async def delete_role_mapping(session: AsyncSession, *, guild_id: int, mapping_id: int) -> None:
    await session.execute(
        text("DELETE FROM guild_alliance_role_mappings WHERE guild_id = :guild_id AND mapping_id = :mapping_id"),
        {"guild_id": guild_id, "mapping_id": mapping_id},
    )
    await session.commit()


async def list_assignments(session: AsyncSession, guild_id: int) -> list[dict[str, Any]]:
    result = await session.execute(
        text("""
            SELECT ua.assignment_id, ua.discord_user_id, ua.scope_code,
                   ua.alliance_id, a.alliance_name
            FROM guild_user_assignments ua
            LEFT JOIN alliances a ON a.alliance_id = ua.alliance_id
            WHERE ua.guild_id = :guild_id
            ORDER BY ua.scope_code, a.alliance_name NULLS FIRST, ua.discord_user_id
        """),
        {"guild_id": guild_id},
    )
    return [dict(row) for row in result.mappings()]


async def add_assignment(
    session: AsyncSession,
    *,
    guild_id: int,
    discord_user_id: int,
    scope_code: int,
    alliance_id: int | None,
) -> None:
    await session.execute(
        text("""
            INSERT INTO guild_user_assignments (
                guild_id, discord_user_id, scope_code, alliance_id, updated_at
            ) VALUES (:guild_id, :discord_user_id, :scope_code, :alliance_id, NOW())
            ON CONFLICT DO NOTHING
        """),
        {
            "guild_id": guild_id,
            "discord_user_id": discord_user_id,
            "scope_code": scope_code,
            "alliance_id": alliance_id,
        },
    )
    await session.commit()


async def delete_assignment(session: AsyncSession, *, guild_id: int, assignment_id: int) -> None:
    await session.execute(
        text("DELETE FROM guild_user_assignments WHERE guild_id = :guild_id AND assignment_id = :assignment_id"),
        {"guild_id": guild_id, "assignment_id": assignment_id},
    )
    await session.commit()


async def get_policy(session: AsyncSession, *, guild_id: int, alliance_id: int) -> dict[str, Any]:
    result = await session.execute(
        text("""
            SELECT distribution_visibility_code, treasury_visibility_code, user_access_code
            FROM alliance_access_policies
            WHERE guild_id = :guild_id AND alliance_id = :alliance_id
        """),
        {"guild_id": guild_id, "alliance_id": alliance_id},
    )
    row = result.mappings().one_or_none()
    return dict(row) if row else {
        "distribution_visibility_code": 2,
        "treasury_visibility_code": 3,
        "user_access_code": 2,
    }


async def save_policy(
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int,
    distribution_visibility_code: int,
    treasury_visibility_code: int,
    user_access_code: int,
) -> None:
    await session.execute(
        text("""
            INSERT INTO alliance_access_policies (
                guild_id, alliance_id, distribution_visibility_code,
                treasury_visibility_code, user_access_code, updated_at
            ) VALUES (
                :guild_id, :alliance_id, :distribution_visibility_code,
                :treasury_visibility_code, :user_access_code, NOW()
            )
            ON CONFLICT (guild_id, alliance_id) DO UPDATE SET
                distribution_visibility_code = EXCLUDED.distribution_visibility_code,
                treasury_visibility_code = EXCLUDED.treasury_visibility_code,
                user_access_code = EXCLUDED.user_access_code,
                updated_at = NOW()
        """),
        {
            "guild_id": guild_id,
            "alliance_id": alliance_id,
            "distribution_visibility_code": distribution_visibility_code,
            "treasury_visibility_code": treasury_visibility_code,
            "user_access_code": user_access_code,
        },
    )
    await session.commit()
