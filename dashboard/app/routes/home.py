from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.config import BASE_DIR
from dashboard.app.database import get_session
from dashboard.app.security import (
    allowed_guild_ids,
    current_discord_user_id,
    current_guild_id,
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
        current_guild_id(request),
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
    context = build_template_context(
        request,
        active_nav="home.personal",
        page_title="홈",
        page_description="개인 현황과 주요 메뉴를 확인합니다.",
        page_kicker="LOCAL PostgreSQL · testdb",
    )
    context.update(
        {
            **workspace,
            "personal_cards": overview["cards"],
            "dashboard_user": overview["current_user"],
        }
    )
    return templates.TemplateResponse(
        request,
        "pages/home/index.html",
        context,
    )
