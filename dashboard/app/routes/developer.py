from __future__ import annotations

import asyncio
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.config import BASE_DIR, get_settings
from dashboard.app.database import get_session
from dashboard.app.security import require_developer
from dashboard.app.services import settings_store, system_store
from dashboard.app.services.discord_api import discord_api
from dashboard.app.ui.context import build_template_context


router = APIRouter(
    prefix="/developer",
    tags=["developer"],
    dependencies=[Depends(require_developer)],
)
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))


def _redirect(path: str, *, notice: str = "", error: str = "") -> RedirectResponse:
    params = {key: value for key, value in {"notice": notice, "error": error}.items() if value}
    query = f"?{urlencode(params)}" if params else ""
    return RedirectResponse(f"{path}{query}", status_code=303)


@router.get("/bot")
async def bot_integration(request: Request, session: AsyncSession = Depends(get_session)):
    app_settings = get_settings()
    guilds = await settings_store.list_guilds(session)
    context = build_template_context(
        request,
        active_nav="developer.bot",
        page_title="봇 연동",
        page_description="Discord Gateway를 실행하지 않고 REST API 조회 연결만 관리합니다.",
        page_kicker="Developer tools",
        page_badge="DEVELOPER",
    )
    context.update(
        {
            "guilds": guilds,
            "discord_configured": discord_api.configured,
            "api_base": app_settings.discord_api_base,
            "cache_ttl": app_settings.discord_cache_ttl_seconds,
            "notice": request.query_params.get("notice", ""),
            "error": request.query_params.get("error", ""),
        }
    )
    return templates.TemplateResponse(request, "pages/developer/bot.html", context)


@router.post("/bot/sync")
async def sync_bot_metadata(session: AsyncSession = Depends(get_session)):
    if not discord_api.configured:
        return _redirect("/developer/bot", error="Discord 봇 토큰이 설정되지 않았습니다.")

    guilds = await settings_store.list_guilds(session)
    if not guilds:
        return _redirect("/developer/bot", error="동기화할 서버가 없습니다.")

    for guild in guilds:
        discord_api.clear_cache(f"guild:{guild['guild_id']}")
    results = await asyncio.gather(
        *(discord_api.guild(guild["guild_id"]) for guild in guilds),
        return_exceptions=True,
    )
    metadata = []
    failures = 0
    for guild, result in zip(guilds, results, strict=True):
        if isinstance(result, Exception):
            failures += 1
            continue
        metadata.append(
            {
                "guild_id": guild["guild_id"],
                "guild_name": result.get("name") or f"서버 {guild['guild_id']}",
                "owner_discord_id": int(result["owner_id"]) if result.get("owner_id") else None,
                "icon_hash": result.get("icon"),
            }
        )
    if metadata:
        await settings_store.update_guild_metadata(session, metadata)

    if failures:
        return _redirect(
            "/developer/bot",
            notice=f"{len(metadata)}개 서버를 동기화했습니다.",
            error=f"{failures}개 서버는 봇 접근 권한을 확인해 주세요.",
        )
    return _redirect("/developer/bot", notice=f"{len(metadata)}개 서버 정보를 동기화했습니다.")


@router.get("/system")
async def system_diagnostics(request: Request, session: AsyncSession = Depends(get_session)):
    overview = await system_store.database_overview(session)
    schema_ok = not overview["missing_tables"] and not overview["unexpected_tables"]
    context = build_template_context(
        request,
        active_nav="developer.system",
        page_title="시스템 점검",
        page_description="로컬 테스트 DB 구조와 핵심 데이터 적재 상태를 확인합니다.",
        page_kicker="Developer tools",
        page_badge="DEVELOPER",
    )
    context.update({"overview": overview, "schema_ok": schema_ok})
    return templates.TemplateResponse(request, "pages/developer/system.html", context)
