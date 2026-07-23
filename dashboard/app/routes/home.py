from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.config import BASE_DIR
from dashboard.app.database import get_session
from dashboard.app.security import (
    allowed_guild_ids,
    can_manage_alliance_operations,
    can_manage_clan_treasury,
    current_discord_user_id,
)
from dashboard.app.services import home_store, workspace_store
from dashboard.app.ui.context import build_template_context


router = APIRouter()
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))


@router.get("/")
async def index(
    request: Request,
    guild_id: int | None = None,
    session: AsyncSession = Depends(get_session),
):
    workspace = await workspace_store.resolve_workspace(
        session,
        guild_id,
        None,
        allowed_guild_ids(request),
    )
    selected_guild_id = workspace["guild_id"]
    overview = (
        await home_store.personal_overview(
            session,
            guild_id=int(selected_guild_id),
            discord_user_id=current_discord_user_id(request) or 0,
        )
        if selected_guild_id is not None
        else {"current_user": None, "cards": []}
    )
    guild_query = (
        f"?guild_id={selected_guild_id}"
        if selected_guild_id is not None
        else ""
    )
    workspace_cards = [
        {
            "id": "alliance-workspace",
            "tone": "alliance",
            "eyebrow": "Alliance workspace",
            "title": "연합 운영",
            "role": "연합 관리자",
            "description": "아이템 드랍 등록부터 혈맹별 1차 분배까지 담당하는 독립된 연합 업무 공간입니다.",
            "primary": "연합 대시보드",
            "href": f"/alliance/drops{guild_query}",
            "links": ["드랍 등록", "각혈 분배", "아이템 입찰", "연합 분배 설정"],
            "flow": ["드랍 등록", "연합 수수료", "혈맹별 분배"],
        },
        {
            "id": "clan-workspace",
            "tone": "clan",
            "eyebrow": "Clan workspace",
            "title": "내 혈맹 운영",
            "role": "각혈 관리자 · 경리",
            "description": "혈맹원 분배와 혈비를 처리하고 경리 지정 및 공개 정책을 관리하는 독립된 혈맹 업무 공간입니다.",
            "primary": "혈맹 대시보드",
            "href": f"/clan/settlements{guild_query}",
            "links": ["혈맹원 분배", "혈비 가계부", "혈맹 경리 관리", "정보 공개 설정"],
            "flow": ["혈맹 수령", "인원별 분배", "공개 정책"],
        },
    ]
    workspace_cards = [
        card
        for card in workspace_cards
        if (
            card["id"] == "alliance-workspace"
            and can_manage_alliance_operations(request)
        )
        or (
            card["id"] == "clan-workspace"
            and can_manage_clan_treasury(request)
        )
    ]
    common_modules = [
        {
            "icon": "calendar-check",
            "title": "출석 · 통계",
            "description": "회차별 출석과 인원·혈맹 통계",
            "href": f"/attendance/status{guild_query}",
        }
    ]
    if can_manage_alliance_operations(request) or can_manage_clan_treasury(request):
        common_modules.append(
            {
                "icon": "shield",
                "title": "서버 운영",
                "description": "혈맹, 권한, 알림과 작업 로그",
                "href": (
                    f"/settings/alliances{guild_query}"
                    if can_manage_alliance_operations(request)
                    else f"/settings/clan{guild_query}"
                ),
            }
        )
    context = build_template_context(
        request,
        active_nav="home.personal",
        page_title="업무 공간",
        page_description="개인 현황을 확인하고, 담당 업무에 따라 연합 운영 또는 내 혈맹 운영으로 이동합니다.",
        page_kicker="LOCAL PostgreSQL · testdb",
    )
    context.update(
        {
            **workspace,
            "personal_cards": overview["cards"],
            "dashboard_user": overview["current_user"],
            "workspaces": workspace_cards,
            "common_modules": common_modules,
        }
    )
    return templates.TemplateResponse(
        request,
        "pages/home/index.html",
        context,
    )
