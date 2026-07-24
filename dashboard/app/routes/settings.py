from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.config import BASE_DIR
from dashboard.app.database import get_session
from dashboard.app.security import (
    allowed_guild_ids,
    can_manage_alliance_operations,
    can_manage_clan_configuration,
    can_select_alliances,
    current_access_role,
    current_guild_id,
    current_user_alliance_id,
    require_alliance_access,
    require_developer,
    require_selected_guild,
)
from dashboard.app.services import bot_events, settings_store
from dashboard.app.services.discord_api import DiscordApiError, discord_api
from dashboard.app.ui.context import build_template_context


router = APIRouter(prefix="/settings", tags=["settings"])
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))

TEXT_CHANNEL_TYPES = {0, 5}
VOICE_CHANNEL_TYPES = {2, 13}
POLICY_LABELS = {
    "distribution_visibility_code": {1: "관리자만", 2: "혈맹원 전체"},
    "treasury_visibility_code": {1: "관리자만", 2: "혈맹원 전체"},
    "user_access_code": {2: "상세 조회", 3: "내 기록만"},
}


def _int_value(value: Any, *, minimum: int | None = None, maximum: int | None = None) -> int:
    parsed = int(str(value).strip())
    if minimum is not None and parsed < minimum:
        raise ValueError
    if maximum is not None and parsed > maximum:
        raise ValueError
    return parsed


def _optional_snowflake(value: Any) -> int | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        parsed = int(normalized)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _redirect(
    path: str,
    *,
    guild_id: int | None = None,
    alliance_id: int | None = None,
    notice: str = "",
    error: str = "",
) -> RedirectResponse:
    params: dict[str, str] = {}
    if alliance_id is not None:
        params["alliance_id"] = str(alliance_id)
    if notice:
        params["notice"] = notice
    if error:
        params["error"] = error
    query = f"?{urlencode(params)}" if params else ""
    return RedirectResponse(f"{path}{query}", status_code=303)


async def _guild_context(
    request: Request,
    session: AsyncSession,
) -> dict[str, Any]:
    stored_guilds = await settings_store.list_guilds(session)
    allowed = allowed_guild_ids(request)
    if current_access_role(request) != "developer" and allowed is not None:
        stored_guilds = [
            row for row in stored_guilds
            if int(row["guild_id"]) in allowed
        ]

    selected = current_guild_id(request)
    known_ids = {row["guild_id"] for row in stored_guilds}
    if selected not in known_ids:
        selected = next((row["guild_id"] for row in stored_guilds if row["is_enabled"]), None)
    if selected is None and stored_guilds:
        selected = stored_guilds[0]["guild_id"]

    guild_options = []
    for row in stored_guilds:
        guild_options.append(
            {
                **row,
                "name": row.get("guild_name") or f"서버 {row['guild_id']}",
                "icon": row.get("icon_hash"),
                "owner_id": row.get("owner_discord_id"),
                "synced": row.get("discord_synced_at") is not None,
            }
        )

    return {
        "guilds": guild_options,
        "guild_id": selected,
        "selected_guild": next((row for row in guild_options if row["guild_id"] == selected), None),
        "discord_configured": discord_api.configured,
        "discord_error": "" if discord_api.configured else "루트 .env에 Discord 봇 토큰을 설정해 주세요.",
    }


def _require_alliance_configuration(
    request: Request,
    guild_id: int | None,
) -> None:
    if guild_id is not None:
        require_selected_guild(request, guild_id)
    if not can_manage_alliance_operations(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="연합 운영 담당자만 설정할 수 있습니다.",
        )


async def _require_owner_configuration(
    request: Request,
    session: AsyncSession,
    guild_id: int,
) -> None:
    require_selected_guild(request, guild_id)
    if not await can_select_alliances(request, session, guild_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="서버 오너 또는 개발자만 설정할 수 있습니다.",
        )


async def _require_clan_configuration(
    request: Request,
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int,
) -> None:
    require_selected_guild(request, guild_id)
    if not can_manage_clan_configuration(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="각혈 관리자만 설정할 수 있습니다.",
        )
    await require_alliance_access(
        request,
        session,
        guild_id=guild_id,
        alliance_id=alliance_id,
    )


async def _discord_resources(guild_id: int | None, *resource_names: str) -> tuple[dict[str, Any], str]:
    resources: dict[str, Any] = {name: [] for name in resource_names}
    if guild_id is None or not discord_api.configured:
        return resources, ""
    try:
        values = await asyncio.gather(*(getattr(discord_api, name)(guild_id) for name in resource_names))
        resources.update(dict(zip(resource_names, values, strict=True)))
        return resources, ""
    except DiscordApiError as exc:
        return resources, str(exc)


def _role_alliance_map(mappings: list[dict[str, Any]]) -> dict[int, int]:
    return {
        int(mapping["role_id"]): int(mapping["alliance_id"])
        for mapping in mappings
        if mapping.get("role_id") is not None and mapping.get("alliance_id") is not None
    }


def _member_rows(
    members: list[dict[str, Any]],
    *,
    role_alliance_map: dict[int, int] | None = None,
) -> tuple[list[dict[str, Any]], dict[int, str]]:
    rows = []
    names: dict[int, str] = {}
    role_alliance_map = role_alliance_map or {}
    for member in members:
        user = member.get("user", {})
        if user.get("bot"):
            continue
        discord_id = int(user["id"])
        display_name = member.get("nick") or user.get("global_name") or user.get("username") or str(discord_id)
        alliance_ids = sorted(
            {
                role_alliance_map[int(role_id)]
                for role_id in member.get("roles", [])
                if int(role_id) in role_alliance_map
            }
        )
        rows.append(
            {
                "discord_id": discord_id,
                "display_name": display_name,
                "username": user.get("username", ""),
                "alliance_ids": alliance_ids,
            }
        )
        names[discord_id] = display_name
    rows.sort(key=lambda row: (row["display_name"].casefold(), row["discord_id"]))
    return rows, names


def _member_display_name(member: dict[str, Any]) -> str:
    user = member.get("user", {})
    return str(
        member.get("nick")
        or user.get("global_name")
        or user.get("username")
        or user.get("id")
        or ""
    ).strip()


def _display_name_value(value: Any) -> str | None:
    display_name = str(value or "").strip()
    if not display_name:
        return None
    return display_name[:100]


def _member_has_alliance_role(
    member: dict[str, Any],
    *,
    alliance_id: int,
    role_alliance_map: dict[int, int],
) -> bool:
    return any(
        role_alliance_map.get(int(role_id)) == alliance_id
        for role_id in member.get("roles", [])
    )


@router.get("/server")
async def server_settings(
    request: Request,
    guild_id: int | None = None,
    session: AsyncSession = Depends(get_session),
    _: None = Depends(require_developer),
):
    guild_data = await _guild_context(request, session)
    context = build_template_context(
        request,
        active_nav="developer.server",
        page_title="서버 기본 설정",
        page_description="설정을 적용할 Discord 서버를 등록하고 사용 여부를 관리합니다.",
        page_kicker="Server connection",
        page_badge="DEVELOPER",
    )
    context.update(guild_data)
    context.update({"notice": request.query_params.get("notice", ""), "error": request.query_params.get("error", "")})
    return templates.TemplateResponse(request, "pages/settings/server.html", context)


@router.post("/server")
async def save_server(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _: None = Depends(require_developer),
):
    form = await request.form()
    try:
        guild_id = _int_value(form.get("guild_id"), minimum=1)
        enabled = str(form.get("is_enabled", "true")).lower() == "true"
        guild_detail = await discord_api.guild(guild_id) if discord_api.configured and enabled else {}
        await settings_store.upsert_guild(
            session,
            guild_id,
            is_enabled=enabled,
            guild_name=guild_detail.get("name"),
            owner_discord_id=_optional_snowflake(guild_detail.get("owner_id")),
            icon_hash=guild_detail.get("icon"),
        )
        bot_result = await bot_events.publish_bot_event(
            session,
            "refresh_guild_registry",
            guild_id=guild_id,
        )
    except (ValueError, TypeError):
        return _redirect("/settings/server", error="올바른 서버 ID를 입력해 주세요.")
    except DiscordApiError as exc:
        return _redirect("/settings/server", guild_id=guild_id, error=str(exc))
    if not bot_result.applied:
        return _redirect(
            "/settings/server",
            guild_id=guild_id,
            error=(
                "서버 설정은 저장했지만 봇 반영을 확인하지 못했습니다. "
                f"{bot_result.message}"
            ),
        )
    return _redirect(
        "/settings/server",
        guild_id=guild_id,
        notice="서버 설정을 저장하고 봇에 반영했습니다.",
    )


@router.post("/server/select")
async def select_server_context(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _: None = Depends(require_developer),
):
    form = await request.form()
    try:
        guild_id = _int_value(form.get("guild_id"), minimum=1)
    except (ValueError, TypeError):
        return _redirect("/settings/server", error="변경할 서버를 선택해 주세요.")

    guilds = await settings_store.list_guilds(session)
    selected = next(
        (
            row
            for row in guilds
            if int(row["guild_id"]) == guild_id and bool(row["is_enabled"])
        ),
        None,
    )
    if selected is None:
        return _redirect("/settings/server", error="사용 중인 서버만 선택할 수 있습니다.")

    request.session["selected_guild_id"] = guild_id
    request.session["developer_view_mode"] = "developer"
    request.session.pop("developer_view_alliance_id", None)
    guild_name = str(selected.get("guild_name") or f"서버 {guild_id}")
    return _redirect(
        "/settings/server",
        notice=f"작업 서버를 {guild_name}(으)로 변경했습니다.",
    )


@router.get("/attendance")
async def attendance_settings(
    request: Request,
    guild_id: int | None = None,
    refresh: bool = False,
    session: AsyncSession = Depends(get_session),
):
    guild_data = await _guild_context(request, session)
    if guild_data["guild_id"] is not None:
        _require_alliance_configuration(request, int(guild_data["guild_id"]))
    if refresh and guild_data["guild_id"]:
        discord_api.clear_cache(f"channels:{guild_data['guild_id']}")
    selected = guild_data["selected_guild"] or {}
    resources, api_error = await _discord_resources(guild_data["guild_id"], "channels")
    channels = resources["channels"]
    context = build_template_context(
        request,
        active_nav="attendance.settings",
        page_title="출석 설정",
        page_description="출석 패널 채널과 음성 채널, 진행 시간을 한 곳에서 관리합니다.",
        page_kicker="Attendance configuration",
        page_badge="OWNER",
    )
    context.update(guild_data)
    context.update(
        {
            "text_channels": [row for row in channels if row.get("type") in TEXT_CHANNEL_TYPES],
            "voice_channels": [row for row in channels if row.get("type") in VOICE_CHANNEL_TYPES],
            "settings_values": selected,
            "discord_error": api_error or guild_data["discord_error"],
            "notice": request.query_params.get("notice", ""),
            "error": request.query_params.get("error", ""),
            "show_api_refresh": True,
            "api_refresh_label": "채널 목록 새로고침",
        }
    )
    return templates.TemplateResponse(request, "pages/settings/attendance.html", context)


@router.post("/attendance")
async def save_attendance(request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    try:
        guild_id = _int_value(form.get("guild_id"), minimum=1)
        _require_alliance_configuration(request, guild_id)
        admin_channel_id = _optional_snowflake(form.get("admin_channel_id"))
        voice_channel_id = _optional_snowflake(form.get("voice_channel_id"))
        log_channel_id = _optional_snowflake(form.get("log_channel_id"))
        timer = _int_value(form.get("timer"), minimum=1, maximum=86400)
        available_timer = _int_value(form.get("attendance_available_timer"), minimum=1, maximum=86400)

        if discord_api.configured:
            channels = await discord_api.channels(guild_id)
            by_id = {int(row["id"]): row for row in channels}
            if admin_channel_id and by_id.get(admin_channel_id, {}).get("type") not in TEXT_CHANNEL_TYPES:
                raise ValueError
            if log_channel_id and by_id.get(log_channel_id, {}).get("type") not in TEXT_CHANNEL_TYPES:
                raise ValueError
            if voice_channel_id and by_id.get(voice_channel_id, {}).get("type") not in VOICE_CHANNEL_TYPES:
                raise ValueError

        previous_admin_channel_id = await settings_store.save_attendance_settings(
            session,
            guild_id=guild_id,
            admin_channel_id=admin_channel_id,
            voice_channel_id=voice_channel_id,
            log_channel_id=log_channel_id,
            timer=timer,
            attendance_available_timer=available_timer,
        )
        bot_result = await bot_events.publish_bot_event(
            session,
            "refresh_admin_panel",
            guild_id=guild_id,
            data={
                "previous_admin_channel_id": previous_admin_channel_id,
            },
        )
    except (ValueError, TypeError):
        return _redirect("/settings/attendance", guild_id=_optional_snowflake(form.get("guild_id")), error="설정값을 확인해 주세요.")
    except DiscordApiError as exc:
        return _redirect("/settings/attendance", guild_id=guild_id, error=str(exc))
    if not bot_result.applied:
        return _redirect(
            "/settings/attendance",
            guild_id=guild_id,
            error=(
                "출석 설정은 저장했지만 봇 반영을 확인하지 못했습니다. "
                f"{bot_result.message}"
            ),
        )
    return _redirect(
        "/settings/attendance",
        guild_id=guild_id,
        notice="출석 설정을 저장하고 Discord 패널에 반영했습니다.",
    )


@router.get("/alliances")
async def alliance_settings(
    request: Request,
    guild_id: int | None = None,
    refresh: bool = False,
    session: AsyncSession = Depends(get_session),
):
    guild_data = await _guild_context(request, session)
    _require_alliance_configuration(
        request,
        int(guild_data["guild_id"]) if guild_data["guild_id"] is not None else None,
    )
    if refresh and guild_data["guild_id"]:
        discord_api.clear_cache(f"roles:{guild_data['guild_id']}")
    alliances = await settings_store.list_guild_alliances(session, guild_data["guild_id"]) if guild_data["guild_id"] else []
    mappings = await settings_store.list_role_mappings(session, guild_data["guild_id"]) if guild_data["guild_id"] else []
    resources, api_error = await _discord_resources(guild_data["guild_id"], "roles")
    mapped_role_ids = {row["role_id"] for row in mappings}
    context = build_template_context(
        request,
        active_nav="operations.alliances",
        page_title="혈맹과 역할 매핑",
        page_description="Discord 역할을 혈맹에 연결해 출석과 분배에서 소속을 판별합니다.",
        page_kicker="Clan role mapping",
        page_badge="ALLIANCE MANAGER",
    )
    context.update(guild_data)
    context.update(
        {
            "alliances": alliances,
            "mappings": mappings,
            "roles": [row for row in resources["roles"] if int(row["id"]) not in mapped_role_ids],
            "discord_error": api_error or guild_data["discord_error"],
            "notice": request.query_params.get("notice", ""),
            "error": request.query_params.get("error", ""),
            "show_api_refresh": True,
            "api_refresh_label": "역할 목록 새로고침",
        }
    )
    return templates.TemplateResponse(request, "pages/settings/alliances.html", context)


@router.post("/alliances")
async def save_alliance_mapping(request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    try:
        guild_id = _int_value(form.get("guild_id"), minimum=1)
        _require_alliance_configuration(request, guild_id)
        role_id = _int_value(form.get("role_id"), minimum=1)
        alliance_name = str(form.get("alliance_name", "")).strip()
        if not alliance_name or len(alliance_name) > 100:
            raise ValueError
        roles = await discord_api.roles(guild_id)
        role = next((row for row in roles if int(row["id"]) == role_id), None)
        if role is None:
            raise ValueError
        await settings_store.save_role_mapping(
            session,
            guild_id=guild_id,
            alliance_name=alliance_name,
            role_id=role_id,
            role_name=role["name"],
        )
    except (ValueError, TypeError):
        return _redirect("/settings/alliances", guild_id=_optional_snowflake(form.get("guild_id")), error="혈맹 이름과 역할을 확인해 주세요.")
    except DiscordApiError as exc:
        return _redirect("/settings/alliances", guild_id=guild_id, error=str(exc))
    return _redirect("/settings/alliances", guild_id=guild_id, notice="혈맹 역할을 연결했습니다.")


@router.post("/alliances/{mapping_id}/delete")
async def remove_alliance_mapping(mapping_id: int, request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    guild_id = _optional_snowflake(form.get("guild_id"))
    if guild_id is None:
        return _redirect("/settings/alliances", error="서버를 선택해 주세요.")
    _require_alliance_configuration(request, guild_id)
    await settings_store.delete_role_mapping(session, guild_id=guild_id, mapping_id=mapping_id)
    return _redirect("/settings/alliances", guild_id=guild_id, notice="역할 연결을 해제했습니다.")


@router.get("/managers")
async def manager_settings(
    request: Request,
    guild_id: int | None = None,
    refresh: bool = False,
    session: AsyncSession = Depends(get_session),
):
    guild_data = await _guild_context(request, session)
    if guild_data["guild_id"] is not None:
        await _require_owner_configuration(
            request,
            session,
            int(guild_data["guild_id"]),
        )
    if refresh and guild_data["guild_id"] is not None:
        discord_api.clear_members_cache(int(guild_data["guild_id"]))
    assignments = await settings_store.list_assignments(session, guild_data["guild_id"]) if guild_data["guild_id"] else []
    alliances = await settings_store.list_guild_alliances(session, guild_data["guild_id"]) if guild_data["guild_id"] else []
    mappings = await settings_store.list_role_mappings(session, guild_data["guild_id"]) if guild_data["guild_id"] else []
    resources, api_error = await _discord_resources(guild_data["guild_id"], "members")
    members, member_names = _member_rows(
        resources["members"],
        role_alliance_map=_role_alliance_map(mappings),
    )
    if guild_data["guild_id"] is not None:
        await settings_store.refresh_assignment_display_names(
            session,
            guild_id=int(guild_data["guild_id"]),
            names_by_discord_id=member_names,
        )
    for row in assignments:
        row["display_name"] = (
            member_names.get(row["discord_user_id"])
            or row.get("discord_display_name")
            or str(row["discord_user_id"])
        )
    alliance_managers = [
        row for row in assignments if row["scope_code"] == settings_store.SCOPE_ALLIANCE_MANAGER
    ]
    clan_manager_groups = []
    mapped_alliance_ids = {row["alliance_id"] for row in alliances}
    for alliance in alliances:
        clan_manager_groups.append(
            {
                **alliance,
                "managers": [
                    row
                    for row in assignments
                    if row["scope_code"] == settings_store.SCOPE_CLAN_MANAGER
                    and row["alliance_id"] == alliance["alliance_id"]
                ],
            }
        )
    orphan_clan_managers = [
        row
        for row in assignments
        if row["scope_code"] == settings_store.SCOPE_CLAN_MANAGER
        and row["alliance_id"] not in mapped_alliance_ids
    ]
    if orphan_clan_managers:
        clan_manager_groups.append(
            {
                "alliance_id": None,
                "alliance_name": "연결 해제된 혈맹",
                "managers": orphan_clan_managers,
            }
        )
    assigned_member_ids = {
        "alliance": [row["discord_user_id"] for row in alliance_managers],
        "clans": {
            str(group["alliance_id"]): [row["discord_user_id"] for row in group["managers"]]
            for group in clan_manager_groups
            if group["alliance_id"] is not None
        },
    }
    context = build_template_context(
        request,
        active_nav="operations.delegation",
        page_title="운영 담당자 설정",
        page_description="서버 구성원 중 연합 관리자와 각혈 관리자를 유저 단위로 지정합니다.",
        page_kicker="User assignments",
        page_badge="OWNER",
    )
    context.update(guild_data)
    context.update(
        {
            "members": members,
            "alliances": alliances,
            "alliance_managers": alliance_managers,
            "clan_manager_groups": clan_manager_groups,
            "assigned_member_ids": assigned_member_ids,
            "discord_error": api_error or guild_data["discord_error"],
            "notice": request.query_params.get("notice", ""),
            "error": request.query_params.get("error", ""),
            "show_api_refresh": True,
            "api_refresh_label": "서버 유저 목록 갱신",
        }
    )
    return templates.TemplateResponse(request, "pages/settings/managers.html", context)


@router.post("/managers")
async def save_manager(request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    try:
        guild_id = _int_value(form.get("guild_id"), minimum=1)
        await _require_owner_configuration(request, session, guild_id)
        discord_user_id = _int_value(form.get("discord_user_id"), minimum=1)
        discord_display_name = _display_name_value(form.get("discord_display_name"))
        scope_code = _int_value(form.get("scope_code"), minimum=1, maximum=2)
        alliance_id = (
            _optional_snowflake(form.get("alliance_id"))
            if scope_code == settings_store.SCOPE_CLAN_MANAGER
            else None
        )
        if scope_code == settings_store.SCOPE_CLAN_MANAGER and alliance_id is None:
            raise ValueError
        # The picker limits each clan to role-mapped members and additionally
        # includes existing alliance managers so they can hold both duties.
        # Do not repeat that live Discord role validation here: it can reject a
        # correct picker selection when Discord's member cache changes mid-flow.
        await settings_store.add_assignment(
            session,
            guild_id=guild_id,
            discord_user_id=discord_user_id,
            discord_display_name=discord_display_name,
            scope_code=scope_code,
            alliance_id=alliance_id,
        )
    except (ValueError, TypeError):
        return _redirect("/settings/managers", guild_id=_optional_snowflake(form.get("guild_id")), error="담당자와 권한 범위를 확인해 주세요.")
    except DiscordApiError as exc:
        return _redirect("/settings/managers", guild_id=guild_id, error=str(exc))
    return _redirect("/settings/managers", guild_id=guild_id, notice="운영 담당자를 지정했습니다.")


@router.post("/assignments/{assignment_id}/delete")
async def remove_assignment(assignment_id: int, request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    guild_id = _optional_snowflake(form.get("guild_id"))
    alliance_id = _optional_snowflake(form.get("alliance_id"))
    return_path = str(form.get("return_path", "/settings/managers"))
    if return_path not in {"/settings/managers", "/settings/clan"}:
        return_path = "/settings/managers"
    if guild_id is None:
        return _redirect(return_path, error="서버를 선택해 주세요.")
    if return_path == "/settings/managers":
        await _require_owner_configuration(request, session, guild_id)
    elif alliance_id is not None:
        await _require_clan_configuration(
            request,
            session,
            guild_id=guild_id,
            alliance_id=alliance_id,
        )
    await settings_store.delete_assignment(session, guild_id=guild_id, assignment_id=assignment_id)
    return _redirect(return_path, guild_id=guild_id, alliance_id=alliance_id, notice="담당자 지정을 해제했습니다.")


@router.get("/clan")
async def clan_settings(
    request: Request,
    guild_id: int | None = None,
    alliance_id: int | None = None,
    session: AsyncSession = Depends(get_session),
):
    guild_data = await _guild_context(request, session)
    alliances = await settings_store.list_guild_alliances(session, guild_data["guild_id"]) if guild_data["guild_id"] else []
    can_select_alliance = await can_select_alliances(request, session, guild_data["guild_id"])
    if not can_select_alliance:
        own_alliance_id = await current_user_alliance_id(request, session, guild_data["guild_id"])
        alliances = [row for row in alliances if row["alliance_id"] == own_alliance_id]
        alliance_id = own_alliance_id
    valid_alliance_ids = {row["alliance_id"] for row in alliances}
    if alliance_id not in valid_alliance_ids:
        alliance_id = alliances[0]["alliance_id"] if alliances else None
    if guild_data["guild_id"] is not None and alliance_id is not None:
        await _require_clan_configuration(
            request,
            session,
            guild_id=int(guild_data["guild_id"]),
            alliance_id=int(alliance_id),
        )
    elif not can_manage_clan_configuration(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="각혈 관리자만 설정할 수 있습니다.",
        )
    assignments = await settings_store.list_assignments(session, guild_data["guild_id"]) if guild_data["guild_id"] else []
    mappings = await settings_store.list_role_mappings(session, guild_data["guild_id"]) if guild_data["guild_id"] else []
    resources, api_error = await _discord_resources(guild_data["guild_id"], "members")
    members, member_names = _member_rows(
        resources["members"],
        role_alliance_map=_role_alliance_map(mappings),
    )
    accountants = [
        {
            **row,
            "display_name": member_names.get(row["discord_user_id"], str(row["discord_user_id"])),
        }
        for row in assignments
        if row["scope_code"] == settings_store.SCOPE_CLAN_ACCOUNTANT and row["alliance_id"] == alliance_id
    ]
    policy = await settings_store.get_policy(session, guild_id=guild_data["guild_id"], alliance_id=alliance_id) if guild_data["guild_id"] and alliance_id else {}
    context = build_template_context(
        request,
        active_nav="clan.staff",
        page_title="내 혈맹 권한 설정",
        page_description="담당 혈맹의 경리와 일반 유저 공개 범위를 관리합니다.",
        page_kicker="Clan manager settings",
        page_badge="각혈 관리자",
    )
    context.update(guild_data)
    context.update(
        {
            "members": members,
            "alliances": alliances,
            "alliance_id": alliance_id,
            "can_select_alliance": can_select_alliance,
            "selected_alliance": next(
                (row for row in alliances if row["alliance_id"] == alliance_id),
                None,
            ),
            "accountants": accountants,
            "accountant_ids": [row["discord_user_id"] for row in accountants],
            "policy": policy,
            "policy_labels": POLICY_LABELS,
            "discord_error": api_error or guild_data["discord_error"],
            "notice": request.query_params.get("notice", ""),
            "error": request.query_params.get("error", ""),
        }
    )
    return templates.TemplateResponse(request, "pages/settings/clan.html", context)


@router.post("/clan/accountants")
async def save_accountant(request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    try:
        guild_id = _int_value(form.get("guild_id"), minimum=1)
        alliance_id = _int_value(form.get("alliance_id"), minimum=1)
        discord_user_id = _int_value(form.get("discord_user_id"), minimum=1)
        discord_display_name: str | None = None
        await _require_clan_configuration(
            request,
            session,
            guild_id=guild_id,
            alliance_id=alliance_id,
        )
        if discord_api.configured:
            members = await discord_api.members(guild_id)
            selected_member = next(
                (
                    row
                    for row in members
                    if int(row.get("user", {}).get("id", 0)) == discord_user_id
                ),
                None,
            )
            if selected_member is None:
                raise ValueError
            mappings = await settings_store.list_role_mappings(session, guild_id)
            if not _member_has_alliance_role(
                selected_member,
                alliance_id=alliance_id,
                role_alliance_map=_role_alliance_map(mappings),
            ):
                raise ValueError
            discord_display_name = _member_display_name(selected_member)
        await settings_store.add_assignment(
            session,
            guild_id=guild_id,
            discord_user_id=discord_user_id,
            discord_display_name=discord_display_name,
            scope_code=settings_store.SCOPE_CLAN_ACCOUNTANT,
            alliance_id=alliance_id,
        )
    except (ValueError, TypeError):
        return _redirect(
            "/settings/clan",
            guild_id=_optional_snowflake(form.get("guild_id")),
            alliance_id=_optional_snowflake(form.get("alliance_id")),
            error="혈맹과 경리 유저를 확인해 주세요.",
        )
    except DiscordApiError as exc:
        return _redirect("/settings/clan", guild_id=guild_id, alliance_id=alliance_id, error=str(exc))
    return _redirect("/settings/clan", guild_id=guild_id, alliance_id=alliance_id, notice="혈맹 경리를 지정했습니다.")


@router.post("/clan/policy")
async def save_clan_policy(request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    try:
        guild_id = _int_value(form.get("guild_id"), minimum=1)
        alliance_id = _int_value(form.get("alliance_id"), minimum=1)
        await _require_clan_configuration(
            request,
            session,
            guild_id=guild_id,
            alliance_id=alliance_id,
        )
        distribution = _int_value(form.get("distribution_visibility_code"), minimum=1, maximum=2)
        treasury = _int_value(form.get("treasury_visibility_code"), minimum=1, maximum=2)
        access = _int_value(form.get("user_access_code"), minimum=2, maximum=3)
        await settings_store.save_policy(
            session,
            guild_id=guild_id,
            alliance_id=alliance_id,
            distribution_visibility_code=distribution,
            treasury_visibility_code=treasury,
            user_access_code=access,
        )
    except (ValueError, TypeError):
        return _redirect(
            "/settings/clan",
            guild_id=_optional_snowflake(form.get("guild_id")),
            alliance_id=_optional_snowflake(form.get("alliance_id")),
            error="공개 정책 값을 확인해 주세요.",
        )
    return _redirect("/settings/clan", guild_id=guild_id, alliance_id=alliance_id, notice="혈맹 공개 정책을 저장했습니다.")
