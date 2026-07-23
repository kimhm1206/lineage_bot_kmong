from __future__ import annotations

import csv
import io
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.config import BASE_DIR
from dashboard.app.database import get_session
from dashboard.app.security import (
    can_manage_alliance_treasury,
    can_manage_clan_treasury,
    can_select_alliances,
    require_alliance_access,
    restrict_workspace_alliance,
)
from dashboard.app.services import workspace_store
from dashboard.app.ui.context import build_template_context


router = APIRouter(tags=["workspaces"])
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))

PageBuilder = Callable[..., Awaitable[dict[str, Any]]]
KST = timezone(timedelta(hours=9))


def _empty_page(message: str) -> dict[str, Any]:
    return {
        "summary_cards": [
            {"label": "조회 결과", "value": "0", "meta": message},
            {"label": "처리 대기", "value": "0", "meta": "표시할 데이터 없음"},
            {"label": "상태", "value": "준비", "meta": "설정을 먼저 확인해 주세요"},
        ],
        "columns": [],
        "rows": [],
        "pagination": {
            "page": 1,
            "page_size": workspace_store.PAGE_SIZE,
            "total": 0,
            "total_pages": 1,
            "pages": [1],
            "has_previous": False,
            "has_next": False,
        },
        "empty_message": message,
    }


def _optional_query_id(value: str | int | None) -> int | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        parsed = int(normalized)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


async def _render_workspace(
    request: Request,
    session: AsyncSession,
    *,
    active_nav: str,
    page_title: str,
    page_description: str,
    page_kicker: str,
    page_badge: str,
    builder: PageBuilder,
    guild_id: int | None,
    alliance_id: int | None,
    period: int | None,
    query: str,
    page: int,
    needs_alliance: bool = False,
    supports_period: bool = True,
    settings_href: str = "",
    settings_label: str = "",
    builder_kwargs: dict[str, Any] | None = None,
    treasury_form_action: str = "",
    can_edit_treasury: bool = False,
):
    workspace = await workspace_store.resolve_workspace(session, guild_id, alliance_id)
    can_select_alliance = await can_select_alliances(request, session, workspace["guild_id"])
    if needs_alliance:
        can_select_alliance = await restrict_workspace_alliance(request, session, workspace)
    selected_period = workspace_store.normalize_period(period)
    selected_guild_id = workspace["guild_id"]
    selected_alliance_id = workspace["alliance_id"]
    clean_query = query.strip()[:100]

    if selected_guild_id is None:
        page_data = _empty_page("등록된 서버가 없습니다.")
    elif needs_alliance and selected_alliance_id is None:
        page_data = _empty_page("역할과 연결된 혈맹이 없습니다.")
    else:
        call_args: dict[str, Any] = {
            "session": session,
            "guild_id": selected_guild_id,
            "query": clean_query,
            "page": max(page, 1),
        }
        if supports_period:
            call_args["period_days"] = selected_period
        if needs_alliance:
            call_args["alliance_id"] = selected_alliance_id
        if builder_kwargs:
            call_args.update(builder_kwargs)
        page_data = await builder(**call_args)

    context = build_template_context(
        request,
        active_nav=active_nav,
        page_title=page_title,
        page_description=page_description,
        page_kicker=page_kicker,
        page_badge=page_badge,
    )
    context.update(workspace)
    context.update(page_data)
    context.update(
        {
            "query": clean_query,
            "period": selected_period,
            "period_options": workspace_store.filter_options(workspace_store.PERIOD_OPTIONS, selected_period),
            "supports_period": supports_period,
            "needs_alliance": needs_alliance,
            "can_select_alliance": can_select_alliance,
            "settings_href": settings_href,
            "settings_label": settings_label,
            "result_label": f"총 {page_data['pagination']['total']:,}건",
            "notice": request.query_params.get("notice", ""),
            "error": request.query_params.get("error", ""),
            "treasury_form": {
                "action": treasury_form_action,
                "distribution_action": (
                    "/alliance/treasury/distributions"
                    if page_data.get("treasury_scope_code") == 1
                    else "/clan/treasury/distributions"
                ),
                "can_edit": can_edit_treasury,
                "occurred_at_value": datetime.now(KST).strftime("%Y-%m-%dT%H:%M"),
            } if (
                treasury_form_action
                and selected_guild_id is not None
                and "treasury_scope_code" in page_data
            ) else None,
        }
    )
    return templates.TemplateResponse(request, "pages/workspace/index.html", context)


async def _attendance_context(
    request: Request,
    session: AsyncSession,
    *,
    active_nav: str,
    page_title: str,
    page_description: str,
    guild_id: int | None,
    alliance_id: int | None,
    period: int | None,
    query: str,
    clan_scoped: bool = False,
) -> tuple[dict[str, Any], dict[str, Any], int, str]:
    workspace = await workspace_store.resolve_workspace(session, guild_id, alliance_id)
    can_select_alliance = await can_select_alliances(request, session, workspace["guild_id"])
    if clan_scoped:
        can_select_alliance = await restrict_workspace_alliance(request, session, workspace)
    selected_period = workspace_store.normalize_period(period)
    clean_query = query.strip()[:100]
    context = build_template_context(
        request,
        active_nav=active_nav,
        page_title=page_title,
        page_description=page_description,
        page_kicker="Attendance",
        page_badge="USER",
    )
    context.update(workspace)
    context["can_select_alliance"] = can_select_alliance
    context.update(
        {
            "query": clean_query,
            "period": selected_period,
            "period_options": workspace_store.filter_options(
                workspace_store.PERIOD_OPTIONS,
                selected_period,
            ),
        }
    )
    return context, workspace, selected_period, clean_query


def _treasury_redirect(
    path: str,
    *,
    guild_id: int | None,
    alliance_id: int | None = None,
    notice: str = "",
    error: str = "",
) -> RedirectResponse:
    params: dict[str, str] = {}
    if guild_id is not None:
        params["guild_id"] = str(guild_id)
    if alliance_id is not None:
        params["alliance_id"] = str(alliance_id)
    if notice:
        params["notice"] = notice
    if error:
        params["error"] = error
    return RedirectResponse(f"{path}?{urlencode(params)}", status_code=303)


def _required_positive_int(value: Any) -> int:
    parsed = int(str(value or "").replace(",", "").strip())
    if parsed <= 0:
        raise ValueError
    return parsed


def _entry_timestamp(value: Any) -> int:
    raw = str(value or "").strip()
    parsed = datetime.fromisoformat(raw) if raw else datetime.now(KST)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=KST)
    return int(parsed.timestamp())


async def _save_treasury_entry(
    request: Request,
    session: AsyncSession,
    *,
    path: str,
    account_scope_code: int,
    can_edit: bool,
) -> RedirectResponse:
    if not can_edit:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="가계부 작성 권한이 없습니다.")

    form = await request.form()
    guild_id: int | None = None
    alliance_id: int | None = None
    try:
        guild_id = _required_positive_int(form.get("guild_id"))
        if account_scope_code == 2:
            alliance_id = _required_positive_int(form.get("alliance_id"))
            await require_alliance_access(
                request,
                session,
                guild_id=guild_id,
                alliance_id=alliance_id,
            )
        direction = int(str(form.get("direction") or ""))
        category_id = _required_positive_int(form.get("treasury_category_id"))
        amount_adena = _required_positive_int(form.get("amount_adena"))
        occurred_at = _entry_timestamp(form.get("occurred_at"))
        await workspace_store.record_treasury_entry(
            session,
            guild_id=guild_id,
            alliance_id=alliance_id,
            account_scope_code=account_scope_code,
            treasury_category_id=category_id,
            direction=direction,
            amount_adena=amount_adena,
            occurred_at=occurred_at,
            memo=str(form.get("memo") or ""),
        )
    except (TypeError, ValueError) as exc:
        message = str(exc).strip() or "입출금 정보를 다시 확인해 주세요."
        return _treasury_redirect(path, guild_id=guild_id, alliance_id=alliance_id, error=message)
    return _treasury_redirect(path, guild_id=guild_id, alliance_id=alliance_id, notice="가계부 내역을 기록했습니다.")


async def _save_treasury_distribution(
    request: Request,
    session: AsyncSession,
    *,
    path: str,
    account_scope_code: int,
    can_edit: bool,
) -> RedirectResponse:
    if not can_edit:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="공금 분배 권한이 없습니다.")

    form = await request.form()
    guild_id: int | None = None
    alliance_id: int | None = None
    try:
        guild_id = _required_positive_int(form.get("guild_id"))
        if account_scope_code == 2:
            alliance_id = _required_positive_int(form.get("alliance_id"))
            await require_alliance_access(
                request,
                session,
                guild_id=guild_id,
                alliance_id=alliance_id,
            )
        requested_amount = _required_positive_int(form.get("requested_amount"))
        excluded_discord_ids = [
            _required_positive_int(value)
            for value in form.getlist("excluded_discord_ids")
            if str(value or "").strip()
        ]
        await workspace_store.create_treasury_distribution(
            session,
            guild_id=guild_id,
            alliance_id=alliance_id,
            account_scope_code=account_scope_code,
            requested_amount=requested_amount,
            excluded_discord_ids=excluded_discord_ids,
            memo=str(form.get("memo") or ""),
        )
    except (TypeError, ValueError) as exc:
        message = str(exc).strip() or "분배 정보를 다시 확인해 주세요."
        return _treasury_redirect(path, guild_id=guild_id, alliance_id=alliance_id, error=message)
    return _treasury_redirect(
        path,
        guild_id=guild_id,
        alliance_id=alliance_id,
        notice="공금 분배를 생성했습니다.",
    )


async def _update_treasury_distribution_status(
    request: Request,
    session: AsyncSession,
    *,
    distribution_id: int,
    user_id: int | None,
) -> RedirectResponse:
    scope = await workspace_store.treasury_distribution_scope(session, distribution_id)
    if scope is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="공금 분배 기록을 찾을 수 없습니다.")
    account_scope_code = int(scope["account_scope_code"] or 0)
    can_edit = (
        can_manage_alliance_treasury(request)
        if account_scope_code == 1
        else can_manage_clan_treasury(request)
    )
    if not can_edit:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="공금 분배 권한이 없습니다.")
    if account_scope_code == 2:
        await require_alliance_access(
            request,
            session,
            guild_id=int(scope["guild_id"]),
            alliance_id=int(scope["alliance_id"]),
        )

    form = await request.form()
    path = "/alliance/treasury" if account_scope_code == 1 else "/clan/treasury"
    try:
        status_code = int(str(form.get("status_code") or ""))
        changed = await workspace_store.set_treasury_distribution_recipient_status(
            session,
            treasury_distribution_id=distribution_id,
            user_id=user_id,
            status_code=status_code,
        )
    except (TypeError, ValueError) as exc:
        return _treasury_redirect(
            path,
            guild_id=int(scope["guild_id"] or 0),
            alliance_id=int(scope["alliance_id"]) if scope["alliance_id"] is not None else None,
            error=str(exc).strip() or "지급 상태를 변경하지 못했습니다.",
        )
    label = "지급 완료" if status_code == 1 else "미지급"
    return _treasury_redirect(
        path,
        guild_id=int(scope["guild_id"] or 0),
        alliance_id=int(scope["alliance_id"]) if scope["alliance_id"] is not None else None,
        notice=f"{changed:,}건을 {label} 상태로 변경했습니다.",
    )


@router.get("/_legacy/alliance/drops", include_in_schema=False)
async def alliance_drops(
    request: Request, guild_id: int | None = None, period: int | None = None,
    q: str = "", page: int = 1, session: AsyncSession = Depends(get_session),
):
    return await _render_workspace(
        request, session,
        active_nav="alliance.drops",
        page_title="드랍 등록 내역",
        page_description="출석 회차와 연결된 아이템 드랍 및 정산 진행 상태를 확인합니다.",
        page_kicker="Alliance operations",
        page_badge="ALLIANCE MANAGER",
        builder=workspace_store.alliance_drops_page,
        guild_id=guild_id, alliance_id=None, period=period, query=q, page=page,
        settings_href="/alliance/items", settings_label="아이템 관리",
    )


@router.get("/_legacy/alliance/settlements", include_in_schema=False)
async def alliance_settlements(
    request: Request, guild_id: int | None = None, period: int | None = None,
    q: str = "", page: int = 1, session: AsyncSession = Depends(get_session),
):
    return await _render_workspace(
        request, session,
        active_nav="alliance.settlement",
        page_title="각혈 분배",
        page_description="드랍별 혈맹 분배금과 1차 정산 상태를 한눈에 확인합니다.",
        page_kicker="Alliance settlement",
        page_badge="ALLIANCE MANAGER",
        builder=workspace_store.alliance_settlements_page,
        guild_id=guild_id, alliance_id=None, period=period, query=q, page=page,
        settings_href="/alliance/settings", settings_label="분배 설정",
    )


@router.get("/alliance/treasury")
async def alliance_treasury(
    request: Request, guild_id: int | None = None, period: int | None = None,
    q: str = "", page: int = 1, session: AsyncSession = Depends(get_session),
):
    can_edit = can_manage_alliance_treasury(request)
    return await _render_workspace(
        request, session,
        active_nav="alliance.treasury",
        page_title="연합비 가계부",
        page_description="연합 전체의 입출금과 거래 후 잔액을 시간 순서대로 확인합니다.",
        page_kicker="Alliance treasury",
        page_badge="ALLIANCE MANAGER",
        builder=workspace_store.treasury_page,
        guild_id=guild_id, alliance_id=None, period=period, query=q, page=page,
        builder_kwargs={
            "alliance_id": None,
            "account_scope_code": 1,
            "include_distribution_users": can_edit,
        },
        treasury_form_action="/alliance/treasury/entries",
        can_edit_treasury=can_edit,
    )


@router.post("/alliance/treasury/entries")
async def create_alliance_treasury_entry(
    request: Request, session: AsyncSession = Depends(get_session),
):
    return await _save_treasury_entry(
        request,
        session,
        path="/alliance/treasury",
        account_scope_code=1,
        can_edit=can_manage_alliance_treasury(request),
    )


@router.post("/alliance/treasury/distributions")
async def create_alliance_treasury_distribution(
    request: Request, session: AsyncSession = Depends(get_session),
):
    return await _save_treasury_distribution(
        request,
        session,
        path="/alliance/treasury",
        account_scope_code=1,
        can_edit=can_manage_alliance_treasury(request),
    )


@router.get("/_legacy/alliance/bidding", include_in_schema=False)
async def alliance_bidding(
    request: Request, guild_id: int | None = None, q: str = "", page: int = 1,
    session: AsyncSession = Depends(get_session),
):
    params = {"guild_id": guild_id, "q": q}
    query_string = urlencode({key: value for key, value in params.items() if value not in (None, "")})
    return RedirectResponse(
        f"/alliance/bidding{'?' + query_string if query_string else ''}",
        status_code=302,
    )


@router.get("/_legacy/alliance/items", include_in_schema=False)
async def alliance_items(
    request: Request, guild_id: int | None = None, q: str = "", page: int = 1,
    session: AsyncSession = Depends(get_session),
):
    return await _render_workspace(
        request, session,
        active_nav="alliance.items",
        page_title="아이템 관리",
        page_description="드랍 등록에 사용하는 아이템과 기본 원화 시세를 확인합니다.",
        page_kicker="Item catalog",
        page_badge="ALLIANCE MANAGER",
        builder=workspace_store.items_page,
        guild_id=guild_id, alliance_id=None, period=None, query=q, page=page,
        supports_period=False,
    )


@router.get("/_legacy/alliance/settings", include_in_schema=False)
async def alliance_settings(
    request: Request, guild_id: int | None = None, q: str = "", page: int = 1,
    session: AsyncSession = Depends(get_session),
):
    return await _render_workspace(
        request, session,
        active_nav="alliance.settings",
        page_title="연합 분배 설정",
        page_description="혈맹별 1차 분배 전에 적용되는 연합 수수료 규칙을 확인합니다.",
        page_kicker="Alliance fee rules",
        page_badge="OWNER",
        builder=workspace_store.fee_rules_page,
        guild_id=guild_id, alliance_id=None, period=None, query=q, page=page,
        supports_period=False,
        builder_kwargs={"alliance_id": None, "scope_code": 1},
    )


@router.get("/_legacy/clan/settlements", include_in_schema=False)
async def clan_settlements(
    request: Request, guild_id: int | None = None, alliance_id: int | None = None,
    period: int | None = None, q: str = "", page: int = 1,
    session: AsyncSession = Depends(get_session),
):
    return await _render_workspace(
        request, session,
        active_nav="clan.settlement",
        page_title="혈맹원 분배",
        page_description="혈맹원과 내부 수수료를 같은 분배 대상으로 조회합니다.",
        page_kicker="Clan settlement",
        page_badge="CLAN MANAGER",
        builder=workspace_store.clan_settlements_page,
        guild_id=guild_id, alliance_id=alliance_id, period=period, query=q, page=page,
        needs_alliance=True,
        settings_href="/clan/settings", settings_label="혈맹 분배 설정",
    )


@router.get("/clan/treasury")
async def clan_treasury(
    request: Request, guild_id: int | None = None, alliance_id: int | None = None,
    period: int | None = None, q: str = "", page: int = 1,
    session: AsyncSession = Depends(get_session),
):
    can_edit = can_manage_clan_treasury(request)
    return await _render_workspace(
        request, session,
        active_nav="clan.treasury",
        page_title="혈비 가계부",
        page_description="혈비 입출금과 거래 후 잔액을 시간 순서대로 확인합니다.",
        page_kicker="Clan treasury",
        page_badge="CLAN MANAGER",
        builder=workspace_store.treasury_page,
        guild_id=guild_id, alliance_id=alliance_id, period=period, query=q, page=page,
        needs_alliance=True,
        builder_kwargs={
            "account_scope_code": 2,
            "include_distribution_users": can_edit,
        },
        treasury_form_action="/clan/treasury/entries",
        can_edit_treasury=can_edit,
    )


@router.post("/clan/treasury/entries")
async def create_clan_treasury_entry(
    request: Request, session: AsyncSession = Depends(get_session),
):
    return await _save_treasury_entry(
        request,
        session,
        path="/clan/treasury",
        account_scope_code=2,
        can_edit=can_manage_clan_treasury(request),
    )


@router.post("/clan/treasury/distributions")
async def create_clan_treasury_distribution(
    request: Request, session: AsyncSession = Depends(get_session),
):
    return await _save_treasury_distribution(
        request,
        session,
        path="/clan/treasury",
        account_scope_code=2,
        can_edit=can_manage_clan_treasury(request),
    )


@router.post("/treasury/distributions/{distribution_id}/recipients")
async def update_all_treasury_distribution_recipients(
    request: Request,
    distribution_id: int,
    session: AsyncSession = Depends(get_session),
):
    return await _update_treasury_distribution_status(
        request,
        session,
        distribution_id=distribution_id,
        user_id=None,
    )


@router.post("/treasury/distributions/{distribution_id}/recipients/{user_id}")
async def update_treasury_distribution_recipient(
    request: Request,
    distribution_id: int,
    user_id: int,
    session: AsyncSession = Depends(get_session),
):
    return await _update_treasury_distribution_status(
        request,
        session,
        distribution_id=distribution_id,
        user_id=user_id,
    )


@router.get("/clan/forfeits")
async def clan_forfeits(
    request: Request, guild_id: int | None = None, alliance_id: int | None = None,
    period: int | None = None, q: str = "", page: int = 1,
    session: AsyncSession = Depends(get_session),
):
    return await _render_workspace(
        request, session,
        active_nav="clan.forfeits",
        page_title="귀속 관리",
        page_description="기한 내 수령하지 않아 혈비로 귀속된 분배 기록을 조회합니다.",
        page_kicker="Member forfeitures",
        page_badge="CLAN MANAGER",
        builder=workspace_store.forfeits_page,
        guild_id=guild_id, alliance_id=alliance_id, period=period, query=q, page=page,
        needs_alliance=True,
    )


@router.get("/_legacy/clan/settings", include_in_schema=False)
async def clan_settings(
    request: Request, guild_id: int | None = None, alliance_id: int | None = None,
    q: str = "", page: int = 1, session: AsyncSession = Depends(get_session),
):
    return await _render_workspace(
        request, session,
        active_nav="clan.settings",
        page_title="혈맹 분배 설정",
        page_description="혈비와 기타 내부 수수료 규칙을 혈맹 단위로 확인합니다.",
        page_kicker="Clan fee rules",
        page_badge="CLAN MANAGER",
        builder=workspace_store.fee_rules_page,
        guild_id=guild_id, alliance_id=alliance_id, period=None, query=q, page=page,
        needs_alliance=True, supports_period=False,
        builder_kwargs={"scope_code": 2},
    )


@router.get("/attendance/status")
async def attendance_status(
    request: Request, guild_id: int | None = None, period: int | None = None,
    q: str = "", page: int = 1, session: AsyncSession = Depends(get_session),
):
    context, workspace, selected_period, clean_query = await _attendance_context(
        request,
        session,
        active_nav="attendance.status",
        page_title="출석 현황",
        page_description="회차별 시작 시각과 참여 인원을 최근 기록부터 확인합니다.",
        guild_id=guild_id,
        alliance_id=None,
        period=period,
        query=q,
    )
    if workspace["guild_id"] is None:
        page_data = {
            "summary_cards": [],
            "sessions": [],
            "pagination": _empty_page("등록된 서버가 없습니다.")["pagination"],
        }
    else:
        page_data = await workspace_store.attendance_sessions_page(
            session,
            guild_id=workspace["guild_id"],
            period_days=selected_period,
            query=clean_query,
            page=max(page, 1),
        )
    context.update(page_data)
    return templates.TemplateResponse(request, "pages/attendance/status.html", context)


@router.get("/attendance/statistics")
async def attendance_statistics(
    request: Request, guild_id: int | None = None, period: int | None = None,
    alliance_id: str | None = None, q: str = "", page: int = 1,
    session: AsyncSession = Depends(get_session),
):
    context, workspace, selected_period, clean_query = await _attendance_context(
        request,
        session,
        active_nav="attendance.stats",
        page_title="출석 통계",
        page_description="기간 내 유저별 출석 횟수와 마지막 참여 시각을 비교합니다.",
        guild_id=guild_id,
        alliance_id=None,
        period=period,
        query=q,
    )
    valid_alliance_ids = {int(row["alliance_id"]) for row in workspace["alliances"]}
    requested_alliance_id = _optional_query_id(alliance_id)
    filter_alliance_id = requested_alliance_id if requested_alliance_id in valid_alliance_ids else None
    if workspace["guild_id"] is None:
        page_data = {
            "summary_cards": [], "user_rankings": [], "daily_stats": [],
            "alliance_stats": [], "hour_stats": [],
            "pagination": _empty_page("등록된 서버가 없습니다.")["pagination"],
        }
    else:
        page_data = await workspace_store.attendance_statistics_page(
            session,
            guild_id=workspace["guild_id"],
            period_days=selected_period,
            query=clean_query,
            page=max(page, 1),
            alliance_id=filter_alliance_id,
        )
    context.update(page_data)
    context["filter_alliance_id"] = filter_alliance_id
    return templates.TemplateResponse(request, "pages/attendance/statistics.html", context)


@router.get("/attendance/statistics/export")
async def attendance_statistics_export(
    request: Request, guild_id: int | None = None, period: int | None = None,
    alliance_id: str | None = None, q: str = "",
    session: AsyncSession = Depends(get_session),
):
    _context, workspace, selected_period, clean_query = await _attendance_context(
        request,
        session,
        active_nav="attendance.stats",
        page_title="출석 통계",
        page_description="출석 통계 CSV",
        guild_id=guild_id,
        alliance_id=None,
        period=period,
        query=q,
    )
    if workspace["guild_id"] is None:
        raise HTTPException(status_code=404, detail="등록된 서버가 없습니다.")
    valid_alliance_ids = {int(row["alliance_id"]) for row in workspace["alliances"]}
    requested_alliance_id = _optional_query_id(alliance_id)
    filter_alliance_id = requested_alliance_id if requested_alliance_id in valid_alliance_ids else None
    rows = await workspace_store.attendance_statistics_export_rows(
        session,
        guild_id=workspace["guild_id"],
        period_days=selected_period,
        query=clean_query,
        alliance_id=filter_alliance_id,
    )
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["순위", "닉네임", "혈맹", "출석 횟수", "첫 출석", "마지막 출석"])
    for index, row in enumerate(rows, start=1):
        writer.writerow(
            [
                index,
                row["user_name"],
                row["alliance_name"],
                row["attendance_count"],
                row["first_attendance"],
                row["last_attendance"],
            ]
        )
    return Response(
        content="\ufeff" + output.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=attendance-statistics.csv"},
    )


@router.get("/attendance/clan")
async def clan_attendance(
    request: Request, guild_id: int | None = None, alliance_id: int | None = None,
    period: int | None = None, q: str = "", page: int = 1,
    session: AsyncSession = Depends(get_session),
):
    context, workspace, selected_period, clean_query = await _attendance_context(
        request,
        session,
        active_nav="attendance.alliance",
        page_title="내 혈맹 통계",
        page_description="혈맹원을 기준으로 출석 횟수와 기간 참여율을 확인합니다.",
        guild_id=guild_id,
        alliance_id=alliance_id,
        period=period,
        query=q,
        clan_scoped=True,
    )
    if workspace["guild_id"] is None or workspace["alliance_id"] is None:
        page_data = {
            "summary_cards": [], "user_rankings": [], "hour_stats": [],
            "weekday_stats": [], "daily_rows": [], "weekly_rankings": [],
            "monthly_rankings": [],
            "pagination": _empty_page("역할과 연결된 혈맹이 없습니다.")["pagination"],
        }
    else:
        page_data = await workspace_store.clan_attendance_page(
            session,
            guild_id=workspace["guild_id"],
            alliance_id=workspace["alliance_id"],
            period_days=selected_period,
            query=clean_query,
            page=max(page, 1),
        )
    context.update(page_data)
    return templates.TemplateResponse(request, "pages/attendance/clan.html", context)


@router.get("/operations/notifications")
async def operation_notifications(
    request: Request, guild_id: int | None = None, q: str = "", page: int = 1,
    session: AsyncSession = Depends(get_session),
):
    return await _render_workspace(
        request, session,
        active_nav="operations.notifications",
        page_title="알림 관리",
        page_description="등록된 통계 알림의 채널, 스케줄과 다음 발송 시각을 확인합니다.",
        page_kicker="Scheduled reports",
        page_badge="OWNER",
        builder=workspace_store.reports_page,
        guild_id=guild_id, alliance_id=None, period=None, query=q, page=page,
        supports_period=False,
    )


@router.get("/operations/audit")
async def operation_audit(
    request: Request, guild_id: int | None = None, period: int | None = None,
    q: str = "", page: int = 1, session: AsyncSession = Depends(get_session),
):
    return await _render_workspace(
        request, session,
        active_nav="operations.audit",
        page_title="작업 로그",
        page_description="출석, 아이템, 드랍, 입찰과 혈비 작업 이력을 시간 순서대로 확인합니다.",
        page_kicker="Operation audit",
        page_badge="OWNER",
        builder=workspace_store.audit_page,
        guild_id=guild_id, alliance_id=None, period=period, query=q, page=page,
    )
