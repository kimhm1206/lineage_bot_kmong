from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.services import audit_service

SCOPE_ALLIANCE_MANAGER = 1
SCOPE_CLAN_MANAGER = 2
SCOPE_CLAN_ACCOUNTANT = 3


async def list_guilds(session: AsyncSession) -> list[dict[str, Any]]:
    result = await session.execute(
        text("""
            SELECT g.guild_id, g.is_enabled, g.guild_name,
                   g.owner_discord_id, g.icon_hash, g.discord_synced_at,
                   gs.admin_channel_id, gs.attendance_voice_channel_id,
                   gs.log_channel_id, gs.timer, gs.attendance_available_timer
            FROM guilds g
            LEFT JOIN guild_settings gs ON gs.guild_id = g.guild_id
            ORDER BY g.is_enabled DESC, g.guild_id
        """)
    )
    return [dict(row) for row in result.mappings()]


async def upsert_guild(
    session: AsyncSession,
    guild_id: int,
    *,
    is_enabled: bool = True,
    guild_name: str | None = None,
    owner_discord_id: int | None = None,
    icon_hash: str | None = None,
) -> None:
    await session.execute(
        text("""
            INSERT INTO guilds (
                guild_id, is_enabled, guild_name, owner_discord_id,
                icon_hash, discord_synced_at
            ) VALUES (
                :guild_id, :is_enabled, :guild_name, :owner_discord_id,
                :icon_hash, CASE WHEN :guild_name IS NULL THEN NULL ELSE NOW() END
            )
            ON CONFLICT (guild_id) DO UPDATE SET
                is_enabled = EXCLUDED.is_enabled,
                guild_name = COALESCE(EXCLUDED.guild_name, guilds.guild_name),
                owner_discord_id = COALESCE(EXCLUDED.owner_discord_id, guilds.owner_discord_id),
                icon_hash = CASE
                    WHEN EXCLUDED.guild_name IS NULL THEN guilds.icon_hash
                    ELSE EXCLUDED.icon_hash
                END,
                discord_synced_at = CASE
                    WHEN EXCLUDED.guild_name IS NULL THEN guilds.discord_synced_at
                    ELSE NOW()
                END
        """),
        {
            "guild_id": guild_id,
            "is_enabled": is_enabled,
            "guild_name": guild_name,
            "owner_discord_id": owner_discord_id,
            "icon_hash": icon_hash,
        },
    )
    await audit_service.record_event(
        session,
        guild_id=guild_id,
        action_code="guild_update",
        target_id=guild_id,
        state_code=1 if is_enabled else 0,
    )
    await session.commit()


async def update_guild_metadata(
    session: AsyncSession,
    guilds: list[dict[str, Any]],
) -> None:
    for guild in guilds:
        await session.execute(
            text("""
                UPDATE guilds
                SET guild_name = :guild_name,
                    owner_discord_id = :owner_discord_id,
                    icon_hash = :icon_hash,
                    discord_synced_at = NOW()
                WHERE guild_id = :guild_id
            """),
            guild,
        )
        await audit_service.record_event(
            session,
            guild_id=int(guild["guild_id"]),
            action_code="guild_update",
            target_id=int(guild["guild_id"]),
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
) -> int | None:
    previous_admin_channel_id = await session.scalar(
        text("""
            SELECT admin_channel_id
            FROM guild_settings
            WHERE guild_id = :guild_id
        """),
        {"guild_id": guild_id},
    )
    await session.execute(
        text("""
            INSERT INTO guild_settings (
                guild_id, admin_channel_id, attendance_voice_channel_id,
                log_channel_id, timer, attendance_available_timer, updated_at
            ) VALUES (
                :guild_id, CAST(:admin_channel_id AS BIGINT), CAST(:voice_channel_id AS BIGINT),
                CAST(:log_channel_id AS BIGINT), :timer, :attendance_available_timer, NOW()
            )
            ON CONFLICT (guild_id) DO UPDATE SET
                admin_channel_id = EXCLUDED.admin_channel_id,
                attendance_voice_channel_id = EXCLUDED.attendance_voice_channel_id,
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
    await audit_service.record_event(
        session,
        guild_id=guild_id,
        action_code="attendance_settings_update",
        target_id=guild_id,
    )
    await session.commit()
    return (
        int(previous_admin_channel_id)
        if previous_admin_channel_id is not None
        else None
    )


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
    await audit_service.record_event(
        session,
        guild_id=guild_id,
        action_code="alliance_mapping_create",
        target_id=int(alliance_id),
        alliance_id=int(alliance_id),
    )
    await session.commit()


async def delete_role_mapping(session: AsyncSession, *, guild_id: int, mapping_id: int) -> None:
    alliance_id = await session.scalar(
        text("""
            DELETE FROM guild_alliance_role_mappings
            WHERE guild_id = :guild_id AND mapping_id = :mapping_id
            RETURNING alliance_id
        """),
        {"guild_id": guild_id, "mapping_id": mapping_id},
    )
    if alliance_id is not None:
        await audit_service.record_event(
            session,
            guild_id=guild_id,
            action_code="alliance_mapping_delete",
            target_id=mapping_id,
            alliance_id=int(alliance_id),
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
    existing_assignment_id = await session.scalar(
        text("""
            SELECT assignment_id
            FROM guild_user_assignments
            WHERE guild_id = :guild_id
              AND discord_user_id = :discord_user_id
              AND scope_code = :scope_code
              AND alliance_id IS NOT DISTINCT FROM :alliance_id
            LIMIT 1
        """),
        {
            "guild_id": guild_id,
            "discord_user_id": discord_user_id,
            "scope_code": scope_code,
            "alliance_id": alliance_id,
        },
    )
    if existing_assignment_id is not None:
        await session.commit()
        return

    assignment_id = await session.scalar(
        text("""
            INSERT INTO guild_user_assignments (
                guild_id, discord_user_id, scope_code, alliance_id, updated_at
            ) VALUES (:guild_id, :discord_user_id, :scope_code, :alliance_id, NOW())
            ON CONFLICT DO NOTHING
            RETURNING assignment_id
        """),
        {
            "guild_id": guild_id,
            "discord_user_id": discord_user_id,
            "scope_code": scope_code,
            "alliance_id": alliance_id,
        },
    )
    if assignment_id is not None:
        await audit_service.record_event(
            session,
            guild_id=guild_id,
            action_code="assignment_create",
            target_id=int(assignment_id),
            alliance_id=alliance_id,
        )
    await session.commit()


async def repair_rounded_assignment_discord_ids(
    session: AsyncSession,
    *,
    guild_id: int,
    exact_discord_ids: set[int],
) -> bool:
    """Repair snowflakes rounded by a browser's unsafe JSON number parsing."""
    if not exact_discord_ids:
        return False
    assignments = (
        await session.execute(
            text("""
                SELECT assignment_id, discord_user_id, scope_code, alliance_id
                FROM guild_user_assignments
                WHERE guild_id = :guild_id
            """),
            {"guild_id": guild_id},
        )
    ).mappings().all()
    rounded_candidates: dict[float, list[int]] = {}
    for discord_user_id in exact_discord_ids:
        rounded_candidates.setdefault(float(discord_user_id), []).append(discord_user_id)

    repaired = False
    for row in assignments:
        stored_id = int(row["discord_user_id"])
        if stored_id in exact_discord_ids or stored_id <= 2**53:
            continue
        candidates = rounded_candidates.get(float(stored_id), [])
        if len(candidates) != 1:
            continue
        exact_id = candidates[0]
        updated_id = await session.scalar(
            text("""
                UPDATE guild_user_assignments AS current_assignment
                SET discord_user_id = :exact_id,
                    updated_at = NOW()
                WHERE current_assignment.assignment_id = :assignment_id
                  AND NOT EXISTS (
                      SELECT 1
                      FROM guild_user_assignments AS existing_assignment
                      WHERE existing_assignment.guild_id = :guild_id
                        AND existing_assignment.discord_user_id = :exact_id
                        AND existing_assignment.scope_code = :scope_code
                        AND existing_assignment.alliance_id
                            IS NOT DISTINCT FROM :alliance_id
                  )
                RETURNING current_assignment.assignment_id
            """),
            {
                "assignment_id": int(row["assignment_id"]),
                "guild_id": guild_id,
                "exact_id": exact_id,
                "scope_code": int(row["scope_code"]),
                "alliance_id": row["alliance_id"],
            },
        )
        repaired = repaired or updated_id is not None
    if repaired:
        await session.commit()
    return repaired


async def delete_assignment(session: AsyncSession, *, guild_id: int, assignment_id: int) -> None:
    deleted = (
        await session.execute(
            text("""
                DELETE FROM guild_user_assignments
                WHERE guild_id = :guild_id
                  AND assignment_id = :assignment_id
                RETURNING assignment_id, alliance_id
            """),
            {"guild_id": guild_id, "assignment_id": assignment_id},
        )
    ).mappings().one_or_none()
    if deleted is not None:
        await audit_service.record_event(
            session,
            guild_id=guild_id,
            action_code="assignment_delete",
            target_id=assignment_id,
            alliance_id=(
                int(deleted["alliance_id"])
                if deleted["alliance_id"] is not None
                else None
            ),
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
        "treasury_visibility_code": 2,
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
                treasury_visibility_code, user_access_code,
                updated_by_discord_user_id, updated_at
            ) VALUES (
                :guild_id, :alliance_id, :distribution_visibility_code,
                :treasury_visibility_code, :user_access_code,
                :updated_by_discord_user_id, NOW()
            )
            ON CONFLICT (guild_id, alliance_id) DO UPDATE SET
                distribution_visibility_code = EXCLUDED.distribution_visibility_code,
                treasury_visibility_code = EXCLUDED.treasury_visibility_code,
                user_access_code = EXCLUDED.user_access_code,
                updated_by_discord_user_id =
                    EXCLUDED.updated_by_discord_user_id,
                updated_at = NOW()
        """),
        {
            "guild_id": guild_id,
            "alliance_id": alliance_id,
            "distribution_visibility_code": distribution_visibility_code,
            "treasury_visibility_code": treasury_visibility_code,
            "user_access_code": user_access_code,
            "updated_by_discord_user_id": (
                audit_service.current_actor().discord_id
                if audit_service.current_actor()
                else None
            ),
        },
    )
    await audit_service.record_event(
        session,
        guild_id=guild_id,
        action_code="clan_policy_update",
        target_id=alliance_id,
        alliance_id=alliance_id,
        state_code=user_access_code,
    )
    await session.commit()
