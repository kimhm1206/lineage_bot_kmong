from __future__ import annotations

from fastapi import HTTPException, Request, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.config import get_settings
from dashboard.app.services import settings_store


DEVELOPER_ENVIRONMENTS = {"local", "development", "test"}
ALLIANCE_OVERVIEW_ROLES = {"developer", "owner"}
CLAN_ACCESS_DETAIL = "detail"
CLAN_ACCESS_OWN = "own"
CLAN_ACCESS_MANAGE = "manage"


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


def is_global_developer(request: Request) -> bool:
    return bool(getattr(request.state, "is_global_developer", False))


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


def allowed_guild_ids(request: Request) -> tuple[int, ...] | None:
    values = getattr(request.state, "allowed_guild_ids", None)
    if values is None:
        return None
    return tuple(int(value) for value in values)


def current_guild_id(request: Request) -> int | None:
    raw_value = getattr(request.state, "selected_guild_id", None)
    try:
        guild_id = int(raw_value)
    except (TypeError, ValueError):
        return None
    return guild_id if guild_id > 0 else None


def current_access_scopes(request: Request) -> frozenset[int]:
    values = getattr(request.state, "access_scopes", ())
    return frozenset(int(value) for value in values)


def has_assignment_scope(request: Request, *scope_codes: int) -> bool:
    return bool(current_access_scopes(request).intersection(scope_codes))


def require_selected_guild(request: Request, guild_id: int) -> None:
    allowed = allowed_guild_ids(request)
    selected = current_guild_id(request)
    if allowed is not None and guild_id not in allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="접근할 수 없는 서버입니다.",
        )
    if selected is not None and int(selected) != guild_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="선택한 서버의 설정만 변경할 수 있습니다.",
        )


async def can_select_alliances(
    request: Request,
    session: AsyncSession,
    guild_id: int | None,
) -> bool:
    if current_access_role(request) in {"developer", "owner"}:
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
    if is_global_developer(request):
        try:
            developer_view_alliance_id = int(
                getattr(request.state, "developer_view_alliance_id", None) or 0
            )
        except (TypeError, ValueError):
            developer_view_alliance_id = 0
        if developer_view_alliance_id > 0:
            return developer_view_alliance_id

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


def clan_visibility_allows(
    visibility_code: int,
    *,
    can_manage: bool,
    is_member: bool,
) -> bool:
    if can_manage:
        return True
    if visibility_code == 1:
        return False
    if visibility_code == 2:
        return is_member
    return False


def clan_user_access_mode(
    user_access_code: int,
    *,
    can_manage: bool,
) -> str:
    if can_manage:
        return CLAN_ACCESS_MANAGE
    return {
        2: CLAN_ACCESS_DETAIL,
        3: CLAN_ACCESS_OWN,
    }.get(user_access_code, CLAN_ACCESS_OWN)


async def current_internal_user_id(
    request: Request,
    session: AsyncSession,
) -> int | None:
    discord_user_id = current_discord_user_id(request)
    if discord_user_id is None:
        return None
    user_id = await session.scalar(
        text("""
            SELECT user_id
            FROM users
            WHERE discord_id = :discord_user_id
              AND is_active IS TRUE
            ORDER BY updated_at DESC, user_id DESC
            LIMIT 1
        """),
        {"discord_user_id": discord_user_id},
    )
    return int(user_id) if user_id is not None else None


async def require_clan_visibility(
    request: Request,
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int,
    resource: str,
) -> dict[str, object]:
    policy = await settings_store.get_policy(
        session,
        guild_id=guild_id,
        alliance_id=alliance_id,
    )
    selected_by_operator = await can_select_alliances(request, session, guild_id)
    own_alliance_id = await current_user_alliance_id(request, session, guild_id)
    is_member = own_alliance_id == alliance_id
    can_manage = can_manage_clan_treasury(request) and (
        selected_by_operator or is_member
    )
    visibility_key = (
        "distribution_visibility_code"
        if resource == "distribution"
        else "treasury_visibility_code"
    )
    if not clan_visibility_allows(
        int(policy[visibility_key]),
        can_manage=can_manage,
        is_member=is_member,
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="이 혈맹 정보의 공개 범위에 포함되지 않습니다.",
        )
    return {
        **policy,
        "can_manage": can_manage,
        "is_member": is_member,
        "access_mode": clan_user_access_mode(
            int(policy["user_access_code"]),
            can_manage=can_manage,
        ),
    }


def can_manage_alliance_operations(request: Request) -> bool:
    return (
        current_access_role(request) in {"developer", "owner"}
        or has_assignment_scope(request, 1)
    )


def can_manage_alliance_treasury(request: Request) -> bool:
    return can_manage_alliance_operations(request)


def can_manage_clan_treasury(request: Request) -> bool:
    return (
        current_access_role(request) in {"developer", "owner"}
        or has_assignment_scope(request, 2, 3)
    )


def can_manage_clan_configuration(request: Request) -> bool:
    return (
        current_access_role(request) in {"developer", "owner"}
        or has_assignment_scope(request, 2)
    )


def can_manage_notifications(request: Request) -> bool:
    return (
        can_manage_alliance_operations(request)
        or can_manage_clan_configuration(request)
    )


async def require_developer(request: Request) -> None:
    if not is_developer(request):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="개발자 전용 기능입니다.")
