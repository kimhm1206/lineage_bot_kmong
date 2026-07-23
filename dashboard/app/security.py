from __future__ import annotations

from fastapi import HTTPException, Request, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.config import get_settings


DEVELOPER_ENVIRONMENTS = {"local", "development", "test"}
ALLIANCE_OVERVIEW_ROLES = {"developer", "owner"}
ALLIANCE_OPERATION_ROLES = {"developer", "owner", "alliance_manager"}


def current_access_role(request: Request) -> str:
    """Resolve the web role without trusting client-provided headers or cookies."""
    role = str(getattr(request.state, "access_role", "") or "").strip().lower()
    if role:
        return role
    if get_settings().environment.strip().lower() in DEVELOPER_ENVIRONMENTS:
        return "developer"
    return "user"


def is_developer(request: Request) -> bool:
    return current_access_role(request) == "developer"


def current_discord_user_id(request: Request) -> int | None:
    for field_name in ("discord_user_id", "discord_id"):
        raw_value = getattr(request.state, field_name, None)
        try:
            user_id = int(raw_value)
        except (TypeError, ValueError):
            continue
        if user_id > 0:
            return user_id
    return None


async def can_select_alliances(
    request: Request,
    session: AsyncSession,
    guild_id: int | None,
) -> bool:
    if current_access_role(request) in ALLIANCE_OVERVIEW_ROLES:
        return True
    discord_user_id = current_discord_user_id(request)
    if guild_id is None or discord_user_id is None:
        return False
    owner_id = await session.scalar(
        text("SELECT owner_discord_id FROM guilds WHERE guild_id = :guild_id"),
        {"guild_id": guild_id},
    )
    return owner_id is not None and int(owner_id) == discord_user_id


async def current_user_alliance_id(
    request: Request,
    session: AsyncSession,
    guild_id: int | None,
) -> int | None:
    discord_user_id = current_discord_user_id(request)
    if guild_id is None or discord_user_id is None:
        return None
    alliance_id = await session.scalar(
        text("""
            SELECT u.alliance_id
            FROM users u
            JOIN guild_alliance_role_mappings m
              ON m.guild_id = :guild_id
             AND m.alliance_id = u.alliance_id
            WHERE u.discord_id = :discord_user_id
              AND u.is_active IS TRUE
              AND u.alliance_id IS NOT NULL
            ORDER BY u.updated_at DESC, u.user_id DESC
            LIMIT 1
        """),
        {
            "guild_id": guild_id,
            "discord_user_id": discord_user_id,
        },
    )
    if alliance_id is not None:
        return int(alliance_id)
    assigned_alliance_id = await session.scalar(
        text("""
            SELECT alliance_id
            FROM guild_user_assignments
            WHERE guild_id = :guild_id
              AND discord_user_id = :discord_user_id
              AND alliance_id IS NOT NULL
            ORDER BY scope_code, assignment_id
            LIMIT 1
        """),
        {
            "guild_id": guild_id,
            "discord_user_id": discord_user_id,
        },
    )
    return int(assigned_alliance_id) if assigned_alliance_id is not None else None


async def restrict_workspace_alliance(
    request: Request,
    session: AsyncSession,
    workspace: dict[str, object],
) -> bool:
    guild_id = workspace.get("guild_id")
    if await can_select_alliances(request, session, int(guild_id) if guild_id is not None else None):
        return True
    alliance_id = await current_user_alliance_id(
        request,
        session,
        int(guild_id) if guild_id is not None else None,
    )
    alliances = [
        row
        for row in list(workspace.get("alliances") or [])
        if int(row["alliance_id"]) == alliance_id
    ]
    workspace["alliances"] = alliances
    workspace["alliance_id"] = alliance_id if alliances else None
    workspace["selected_alliance"] = alliances[0] if alliances else None
    return False


async def require_alliance_access(
    request: Request,
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int,
) -> None:
    if await can_select_alliances(request, session, guild_id):
        return
    if await current_user_alliance_id(request, session, guild_id) != alliance_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="본인 혈맹의 정보만 확인할 수 있습니다.",
        )


def can_manage_alliance_operations(request: Request) -> bool:
    return current_access_role(request) in ALLIANCE_OPERATION_ROLES


def can_manage_alliance_treasury(request: Request) -> bool:
    return can_manage_alliance_operations(request)


def can_manage_clan_treasury(request: Request) -> bool:
    return current_access_role(request) in {"developer", "owner", "clan_manager", "clan_accountant"}


async def require_developer(request: Request) -> None:
    if not is_developer(request):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="개발자 전용 기능입니다.")
