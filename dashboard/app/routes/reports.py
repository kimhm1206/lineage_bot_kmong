from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.config import BASE_DIR
from dashboard.app.database import get_session
from dashboard.app.security import (
    allowed_guild_ids,
    can_manage_alliance_operations,
    current_discord_user_id,
    current_guild_id,
    require_selected_guild,
)
from dashboard.app.services import bot_events, report_service, workspace_store
from dashboard.app.services.discord_api import DiscordApiError, discord_api
from dashboard.app.ui.context import build_template_context


router = APIRouter(tags=["reports"])
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))


def _require_report_access(request: Request) -> None:
    if not can_manage_alliance_operations(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="알림을 관리할 권한이 없습니다.",
        )


async def _text_channels(guild_id: int) -> tuple[list[dict[str, Any]], str]:
    try:
        channels = await discord_api.channels(guild_id)
    except DiscordApiError as exc:
        return [], str(exc)
    return [
        {
            "id": int(channel["id"]),
            "name": str(channel.get("name") or channel["id"]),
        }
        for channel in channels
        if int(channel.get("type", -1)) in {0, 5}
    ], ""


@router.get("/operations/notifications")
async def notifications_page(
    request: Request,
    guild_id: int | None = None,
    session: AsyncSession = Depends(get_session),
):
    _require_report_access(request)
    workspace = await workspace_store.resolve_workspace(
        session,
        current_guild_id(request),
        None,
        allowed_guild_ids(request),
    )
    selected_guild_id = workspace["guild_id"]
    reports = (
        await report_service.list_reports(session, int(selected_guild_id))
        if selected_guild_id is not None
        else []
    )
    channels, channel_error = (
        await _text_channels(int(selected_guild_id))
        if selected_guild_id is not None
        else ([], "")
    )
    context = build_template_context(
        request,
        active_nav="operations.notifications",
        page_title="알림 관리",
        page_description="출석 통계 메시지를 구성하고 정해진 시각에 Discord로 발송합니다.",
        page_kicker="Scheduled reports",
        page_badge="ALLIANCE MANAGER",
    )
    context.update(workspace)
    context.update(
        {
            "reports": reports,
            "reports_json": [report["form_data"] for report in reports],
            "channels": channels,
            "channel_error": channel_error,
            "active_count": sum(1 for report in reports if report["status"] == "on"),
        }
    )
    return templates.TemplateResponse(
        request,
        "pages/operations/notifications.html",
        context,
    )


@router.post("/api/reports")
async def save_report(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    _require_report_access(request)
    form = await request.form()
    try:
        guild_id = int(str(form.get("guild_id") or ""))
        report_setting_id = int(str(form.get("report_setting_id") or "0")) or None
    except ValueError:
        return JSONResponse(
            {"ok": False, "message": "서버 또는 알림 ID를 확인해 주세요."},
            status_code=422,
        )
    if allowed_guild_ids(request) is not None and guild_id not in allowed_guild_ids(request):
        raise HTTPException(status_code=403, detail="접근할 수 없는 서버입니다.")
    require_selected_guild(request, guild_id)
    values = {key: value for key, value in form.items()}
    channels, channel_error = await _text_channels(guild_id)
    if channel_error:
        return JSONResponse({"ok": False, "message": channel_error}, status_code=502)
    channel_lookup = {int(row["id"]): row["name"] for row in channels}
    try:
        channel_id = int(str(values.get("channel_id") or ""))
    except ValueError:
        channel_id = 0
    if channel_id not in channel_lookup:
        return JSONResponse(
            {"ok": False, "message": "발송 가능한 Discord 채널을 선택해 주세요."},
            status_code=422,
        )
    values["channel_name"] = channel_lookup[channel_id]
    try:
        saved_id = await report_service.save_report(
            session,
            guild_id=guild_id,
            actor_discord_id=current_discord_user_id(request) or 0,
            form=values,
            report_setting_id=report_setting_id,
        )
        bot_result = await bot_events.publish_bot_event(
            session,
            "refresh_report_schedules",
            guild_id=guild_id,
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=422)
    if not bot_result.applied:
        return JSONResponse(
            {
                "ok": False,
                "persisted": True,
                "message": (
                    "알림 설정은 저장했지만 봇 스케줄 반영을 확인하지 못했습니다. "
                    f"{bot_result.message} 설정을 다시 저장하지 말고 봇 상태를 확인해 주세요."
                ),
                "report_setting_id": saved_id,
            },
            status_code=503,
        )
    return JSONResponse(
        {
            "ok": True,
            "message": "알림을 저장했습니다.",
            "report_setting_id": saved_id,
        }
    )


@router.post("/api/reports/preview")
async def preview_report(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    _require_report_access(request)
    form = await request.form()
    try:
        guild_id = int(str(form.get("guild_id") or ""))
    except ValueError:
        return JSONResponse({"ok": False, "message": "서버를 확인해 주세요."}, status_code=422)
    if allowed_guild_ids(request) is not None and guild_id not in allowed_guild_ids(request):
        raise HTTPException(status_code=403, detail="접근할 수 없는 서버입니다.")
    require_selected_guild(request, guild_id)
    workspace = await workspace_store.resolve_workspace(
        session,
        current_guild_id(request),
        None,
        allowed_guild_ids(request),
    )
    values = {key: value for key, value in form.items()}
    values["channel_name"] = "preview"
    try:
        preview = await report_service.preview_report(
            session,
            guild_id=guild_id,
            guild_name=str((workspace.get("selected_guild") or {}).get("name") or guild_id),
            form=values,
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=422)
    return JSONResponse({"ok": True, "preview": preview})


@router.post("/api/reports/{report_setting_id}/status")
async def report_status(
    request: Request,
    report_setting_id: int,
    session: AsyncSession = Depends(get_session),
):
    _require_report_access(request)
    form = await request.form()
    try:
        guild_id = int(str(form.get("guild_id") or ""))
    except ValueError:
        return JSONResponse({"ok": False, "message": "서버를 확인해 주세요."}, status_code=422)
    if allowed_guild_ids(request) is not None and guild_id not in allowed_guild_ids(request):
        raise HTTPException(status_code=403, detail="접근할 수 없는 서버입니다.")
    require_selected_guild(request, guild_id)
    target_status = str(form.get("status") or "")
    try:
        updated = await report_service.update_status(
            session,
            guild_id=guild_id,
            report_setting_id=report_setting_id,
            actor_discord_id=current_discord_user_id(request) or 0,
            status=target_status,
        )
        bot_result = await bot_events.publish_bot_event(
            session,
            "refresh_report_schedules",
            guild_id=guild_id,
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=422)
    if not updated:
        return JSONResponse({"ok": False, "message": "알림을 찾을 수 없습니다."}, status_code=404)
    if not bot_result.applied:
        return JSONResponse(
            {
                "ok": False,
                "persisted": True,
                "message": (
                    "알림 상태는 저장했지만 봇 스케줄 반영을 확인하지 못했습니다. "
                    f"{bot_result.message}"
                ),
            },
            status_code=503,
        )
    message = "알림을 삭제했습니다." if target_status == "delete" else "알림 상태를 변경했습니다."
    return JSONResponse({"ok": True, "message": message})
