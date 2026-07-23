from __future__ import annotations

from collections.abc import Awaitable
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.config import BASE_DIR
from dashboard.app.database import get_session
from dashboard.app.security import (
    can_manage_alliance_operations,
    can_select_alliances,
    current_user_alliance_id,
    require_alliance_access,
    restrict_workspace_alliance,
)
from dashboard.app.services import operations_store, settlement_service, workspace_store
from dashboard.app.ui.context import build_template_context


router = APIRouter(tags=["operations"])
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))


def _int(value: Any, *, required: bool = True) -> int | None:
    normalized = str(value or "").strip()
    if not normalized and not required:
        return None
    try:
        parsed = int(normalized)
    except (TypeError, ValueError):
        raise settlement_service.SettlementError("필수 선택값을 확인해 주세요.") from None
    if parsed <= 0:
        raise settlement_service.SettlementError("필수 선택값을 확인해 주세요.")
    return parsed


def _bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "on", "yes"}


def _status(value: Any) -> int:
    try:
        status = int(str(value).strip())
    except (TypeError, ValueError):
        raise settlement_service.SettlementError("정산 상태값을 확인해 주세요.") from None
    if status not in {0, 1, 2}:
        raise settlement_service.SettlementError("정산 상태값을 확인해 주세요.")
    return status


async def _result(session: AsyncSession, operation: Awaitable[settlement_service.OperationResult]) -> JSONResponse:
    try:
        result = await operation
        await session.commit()
        return JSONResponse(
            {
                "ok": True,
                "message": result.message,
                "affected_ids": list(result.affected_ids),
            }
        )
    except settlement_service.SettlementError as exc:
        await session.rollback()
        return JSONResponse({"ok": False, "message": str(exc)}, status_code=422)
    except Exception:
        await session.rollback()
        raise


async def _context(
    request: Request,
    session: AsyncSession,
    *,
    guild_id: int | None,
    alliance_id: int | None,
    active_nav: str,
    page_title: str,
    page_description: str,
    page_badge: str,
    clan_scoped: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    workspace = await workspace_store.resolve_workspace(session, guild_id, alliance_id)
    can_select = await can_select_alliances(request, session, workspace["guild_id"])
    if clan_scoped:
        can_select = await restrict_workspace_alliance(request, session, workspace)
    context = build_template_context(
        request,
        active_nav=active_nav,
        page_title=page_title,
        page_description=page_description,
        page_kicker="Operations workspace",
        page_badge=page_badge,
    )
    context.update(workspace)
    context["can_select_alliance"] = can_select
    return context, workspace


def _query(value: str) -> str:
    return value.strip()[:100]


async def _require_bid_management(
    request: Request,
    session: AsyncSession,
    guild_id: int,
) -> None:
    if await can_select_alliances(request, session, guild_id):
        return
    if not can_manage_alliance_operations(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="입찰 기록을 관리할 권한이 없습니다.",
        )


@router.get("/alliance/drops")
async def drops_page(
    request: Request,
    guild_id: int | None = None,
    period: int | None = None,
    q: str = "",
    status: str = "all",
    page: int = 1,
    session: AsyncSession = Depends(get_session),
):
    context, workspace = await _context(
        request,
        session,
        guild_id=guild_id,
        alliance_id=None,
        active_nav="alliance.drops",
        page_title="드랍 등록",
        page_description="출석 회차와 아이템을 연결하고 판매 완료 후 분배를 시작합니다.",
        page_badge="ALLIANCE MANAGER",
    )
    selected_period = workspace_store.normalize_period(period)
    page_data = (
        await operations_store.drop_management_page(
            session,
            guild_id=int(workspace["guild_id"]),
            period_days=selected_period,
            query=_query(q),
            status=status,
            page=max(page, 1),
        )
        if workspace["guild_id"] is not None
        else {
            "rows": [], "pagination": operations_store._pagination(0, 1),
            "summary_cards": [], "attendance_options": [], "item_options": [],
            "alliance_options": [], "buyer_users": [], "selected_status": "all",
        }
    )
    context.update(page_data)
    context.update(
        {
            "period": selected_period,
            "period_options": workspace_store.filter_options(workspace_store.PERIOD_OPTIONS, selected_period),
            "query": _query(q),
        }
    )
    return templates.TemplateResponse(request, "pages/operations/drops.html", context)


@router.get("/alliance/settlements")
async def alliance_settlements_page(
    request: Request,
    guild_id: int | None = None,
    period: int | None = None,
    q: str = "",
    session: AsyncSession = Depends(get_session),
):
    context, workspace = await _context(
        request,
        session,
        guild_id=guild_id,
        alliance_id=None,
        active_nav="alliance.settlement",
        page_title="각혈 분배",
        page_description="혈맹과 연합 수수료를 같은 정산 대상으로 관리합니다.",
        page_badge="ALLIANCE MANAGER",
    )
    selected_period = workspace_store.normalize_period(period)
    page_data = (
        await operations_store.alliance_settlement_entities(
            session,
            guild_id=int(workspace["guild_id"]),
            period_days=selected_period,
            query="",
        )
        if workspace["guild_id"] is not None
        else {"entities": [], "summary_cards": []}
    )
    context.update(page_data)
    context.update(
        {
            "period": selected_period,
            "period_options": workspace_store.filter_options(workspace_store.PERIOD_OPTIONS, selected_period),
            "query": _query(q),
            "settlement_level": "alliance",
        }
    )
    return templates.TemplateResponse(request, "pages/operations/settlements.html", context)


@router.get("/clan/settlements")
async def clan_settlements_page(
    request: Request,
    guild_id: int | None = None,
    alliance_id: int | None = None,
    period: int | None = None,
    q: str = "",
    session: AsyncSession = Depends(get_session),
):
    context, workspace = await _context(
        request,
        session,
        guild_id=guild_id,
        alliance_id=alliance_id,
        active_nav="clan.settlement",
        page_title="혈맹원 분배",
        page_description="혈맹원과 내부 수수료를 동일한 카드에서 정산합니다.",
        page_badge="CLAN MANAGER",
        clan_scoped=True,
    )
    selected_period = workspace_store.normalize_period(period)
    page_data = (
        await operations_store.clan_settlement_entities(
            session,
            guild_id=int(workspace["guild_id"]),
            alliance_id=int(workspace["alliance_id"]),
            period_days=selected_period,
            query="",
        )
        if workspace["guild_id"] is not None and workspace["alliance_id"] is not None
        else {"entities": [], "summary_cards": []}
    )
    context.update(page_data)
    context.update(
        {
            "period": selected_period,
            "period_options": workspace_store.filter_options(workspace_store.PERIOD_OPTIONS, selected_period),
            "query": _query(q),
            "settlement_level": "clan",
        }
    )
    return templates.TemplateResponse(request, "pages/operations/settlements.html", context)


@router.get("/alliance/items")
async def items_page(
    request: Request,
    guild_id: int | None = None,
    q: str = "",
    session: AsyncSession = Depends(get_session),
):
    context, workspace = await _context(
        request,
        session,
        guild_id=guild_id,
        alliance_id=None,
        active_nav="alliance.items",
        page_title="아이템 관리",
        page_description="드랍 등록에 사용할 아이템과 기본 원화 시세를 관리합니다.",
        page_badge="ALLIANCE MANAGER",
    )
    page_data = (
        await operations_store.item_management_page(session, guild_id=int(workspace["guild_id"]), query="")
        if workspace["guild_id"] is not None
        else {"items": [], "summary_cards": []}
    )
    context.update(page_data)
    context["query"] = _query(q)
    return templates.TemplateResponse(request, "pages/operations/items.html", context)


@router.get("/alliance/settings")
async def alliance_fee_page(
    request: Request,
    guild_id: int | None = None,
    q: str = "",
    session: AsyncSession = Depends(get_session),
):
    context, workspace = await _context(
        request,
        session,
        guild_id=guild_id,
        alliance_id=None,
        active_nav="alliance.settings",
        page_title="연합 분배 설정",
        page_description="판매금에서 먼저 차감할 연합 수수료 규칙을 관리합니다.",
        page_badge="OWNER",
    )
    page_data = (
        await operations_store.fee_management_page(
            session, guild_id=int(workspace["guild_id"]), alliance_id=None, scope_code=1, query=""
        )
        if workspace["guild_id"] is not None
        else {"fee_rules": []}
    )
    context.update(page_data)
    context.update({"query": _query(q), "scope_code": 1})
    return templates.TemplateResponse(request, "pages/operations/fees.html", context)


@router.get("/clan/settings")
async def clan_fee_page(
    request: Request,
    guild_id: int | None = None,
    alliance_id: int | None = None,
    q: str = "",
    session: AsyncSession = Depends(get_session),
):
    context, workspace = await _context(
        request,
        session,
        guild_id=guild_id,
        alliance_id=alliance_id,
        active_nav="clan.settings",
        page_title="혈맹 분배 설정",
        page_description="혈맹 분배금에서 차감할 혈비와 내부 수수료를 관리합니다.",
        page_badge="CLAN MANAGER",
        clan_scoped=True,
    )
    page_data = (
        await operations_store.fee_management_page(
            session,
            guild_id=int(workspace["guild_id"]),
            alliance_id=int(workspace["alliance_id"]),
            scope_code=2,
            query="",
        )
        if workspace["guild_id"] is not None and workspace["alliance_id"] is not None
        else {"fee_rules": []}
    )
    context.update(page_data)
    context.update({"query": _query(q), "scope_code": 2})
    return templates.TemplateResponse(request, "pages/operations/fees.html", context)


@router.get("/alliance/bidding")
async def bidding_page(
    request: Request,
    guild_id: int | None = None,
    q: str = "",
    session: AsyncSession = Depends(get_session),
):
    context, workspace = await _context(
        request,
        session,
        guild_id=guild_id,
        alliance_id=None,
        active_nav="alliance.bidding",
        page_title="아이템 입찰",
        page_description="혈맹별 아이템 구매 횟수와 날짜별 구매 기록을 관리합니다.",
        page_badge="ALLIANCE MANAGER",
    )
    can_select = bool(context["can_select_alliance"])
    visible_alliance_id = (
        None
        if can_select
        else await current_user_alliance_id(request, session, workspace["guild_id"])
    )
    page_data = (
        await operations_store.bid_management_page(
            session,
            guild_id=int(workspace["guild_id"]),
            query="",
            visible_alliance_id=visible_alliance_id,
        )
        if workspace["guild_id"] is not None
        else {"item_rows": [], "alliances": [], "summary_cards": []}
    )
    context.update(page_data)
    context.update(
        {
            "query": _query(q),
            "can_manage_bidding": can_select or can_manage_alliance_operations(request),
        }
    )
    return templates.TemplateResponse(request, "pages/operations/bidding.html", context)


@router.get("/my/distributions")
async def personal_distribution_page(
    request: Request,
    guild_id: int | None = None,
    user_id: int | None = None,
    period: int | None = None,
    session: AsyncSession = Depends(get_session),
):
    context, workspace = await _context(
        request,
        session,
        guild_id=guild_id,
        alliance_id=None,
        active_nav="home.distributions",
        page_title="내 분배금",
        page_description="받아야 할 분배금과 수령·귀속 상태를 확인합니다.",
        page_badge="USER",
    )
    selected_period = workspace_store.normalize_period(period)
    page_data = (
        await operations_store.personal_distribution_page(
            session,
            guild_id=int(workspace["guild_id"]),
            user_id=user_id,
            period_days=selected_period,
        )
        if workspace["guild_id"] is not None
        else {"users": [], "user_id": None, "selected_user": None, "details": [], "summary_cards": []}
    )
    context.update(page_data)
    context.update(
        {
            "period": selected_period,
            "period_options": workspace_store.filter_options(workspace_store.PERIOD_OPTIONS, selected_period),
        }
    )
    return templates.TemplateResponse(request, "pages/operations/personal.html", context)


@router.post("/api/drops")
async def create_drop(request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    return await _result(
        session,
        settlement_service.create_drop(
            session,
            guild_id=int(_int(form.get("guild_id"))),
            attendance_id=int(_int(form.get("attendance_id"))),
            item_id=int(_int(form.get("item_id"))),
            excluded_alliance_ids=form.getlist("excluded_alliance_ids"),
        ),
    )


@router.post("/api/drops/{drop_id}")
async def update_drop(drop_id: int, request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    return await _result(
        session,
        settlement_service.update_drop(
            session,
            drop_id=drop_id,
            guild_id=int(_int(form.get("guild_id"))),
            attendance_id=int(_int(form.get("attendance_id"))),
            item_id=int(_int(form.get("item_id"))),
            excluded_alliance_ids=form.getlist("excluded_alliance_ids"),
        ),
    )


@router.post("/api/drops/{drop_id}/delete")
async def delete_drop(drop_id: int, request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    return await _result(
        session,
        settlement_service.delete_drop(session, drop_id=drop_id, guild_id=int(_int(form.get("guild_id")))),
    )


@router.post("/api/drops/{drop_id}/sale")
async def complete_sale(drop_id: int, request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    buyer_user_id = _int(form.get("buyer_user_id"), required=False)
    return await _result(
        session,
        settlement_service.complete_sale(
            session,
            drop_id=drop_id,
            guild_id=int(_int(form.get("guild_id"))),
            buyer_alliance_id=int(_int(form.get("buyer_alliance_id"))),
            buyer_user_id=buyer_user_id,
            adena_market_rate=int(_int(form.get("adena_market_rate"))),
        ),
    )


@router.post("/api/drops/{drop_id}/reopen")
async def reopen_sale(drop_id: int, request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    return await _result(
        session,
        settlement_service.reopen_sale(session, drop_id=drop_id, guild_id=int(_int(form.get("guild_id")))),
    )


@router.post("/api/payouts/{payout_object_id}/status")
async def set_payout_status(payout_object_id: int, request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    return await _result(
        session,
        settlement_service.set_payout_status(
            session,
            payout_object_id=payout_object_id,
            status_code=_status(form.get("status_code")),
        ),
    )


@router.post("/api/payout-groups/{group_type}/{target_id}/status")
async def set_payout_group_status(
    group_type: str,
    target_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    form = await request.form()
    return await _result(
        session,
        settlement_service.set_payout_group_status(
            session,
            guild_id=int(_int(form.get("guild_id"))),
            group_type=group_type,
            target_id=target_id,
            alliance_id=_int(form.get("alliance_id"), required=False),
            status_code=_status(form.get("status_code")),
        ),
    )


@router.post("/api/items")
async def create_item(request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    return await _result(
        session,
        settlement_service.create_item(
            session,
            guild_id=int(_int(form.get("guild_id"))),
            item_name=str(form.get("item_name") or ""),
            default_price=form.get("default_price") or 0,
        ),
    )


@router.post("/api/items/{item_id}")
async def update_item(item_id: int, request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    return await _result(
        session,
        settlement_service.update_item(
            session,
            guild_id=int(_int(form.get("guild_id"))),
            item_id=item_id,
            item_name=str(form.get("item_name") or ""),
            default_price=form.get("default_price") or 0,
        ),
    )


@router.post("/api/items/{item_id}/delete")
async def delete_item(item_id: int, request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    return await _result(
        session,
        settlement_service.delete_item(
            session,
            guild_id=int(_int(form.get("guild_id"))),
            item_id=item_id,
        ),
    )


@router.post("/api/fee-rules")
async def create_fee_rule(request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    return await _result(
        session,
        settlement_service.create_fee_rule(
            session,
            guild_id=int(_int(form.get("guild_id"))),
            alliance_id=_int(form.get("alliance_id"), required=False),
            scope_code=int(str(form.get("scope_code") or "1")),
            rule_name=str(form.get("rule_name") or ""),
            percent=form.get("percent"),
        ),
    )


@router.post("/api/fee-rules/{fee_rule_id}")
async def update_fee_rule(fee_rule_id: int, request: Request, session: AsyncSession = Depends(get_session)):
    form = await request.form()
    return await _result(
        session,
        settlement_service.update_fee_rule(
            session,
            guild_id=int(_int(form.get("guild_id"))),
            fee_rule_id=fee_rule_id,
            rule_name=str(form.get("rule_name") or ""),
            percent=form.get("percent"),
            is_active=_bool(form.get("is_active")),
        ),
    )


@router.post("/api/items/{item_id}/alliances/{alliance_id}/purchase")
async def record_bid_purchase(
    item_id: int,
    alliance_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    form = await request.form()
    guild_id = int(_int(form.get("guild_id")))
    await _require_bid_management(request, session, guild_id)
    await require_alliance_access(
        request,
        session,
        guild_id=guild_id,
        alliance_id=alliance_id,
    )
    return await _result(
        session,
        settlement_service.record_bid_purchase(
            session,
            guild_id=guild_id,
            item_id=item_id,
            alliance_id=alliance_id,
        ),
    )


@router.get("/api/bid-purchases/items/{item_id}")
async def bid_purchase_history(
    item_id: int,
    request: Request,
    guild_id: int,
    session: AsyncSession = Depends(get_session),
):
    can_select = await can_select_alliances(request, session, guild_id)
    visible_alliance_id = (
        None
        if can_select
        else await current_user_alliance_id(request, session, guild_id)
    )
    if visible_alliance_id is None and not can_select:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="본인 혈맹의 구매 기록만 확인할 수 있습니다.",
        )
    return {
        "ok": True,
        **await operations_store.bid_item_purchase_history(
            session,
            guild_id=guild_id,
            item_id=item_id,
            visible_alliance_id=visible_alliance_id,
        ),
    }


@router.get("/api/clan-settlement-history")
async def clan_settlement_history(
    request: Request,
    guild_id: int,
    alliance_id: int,
    period: int | None = None,
    q: str = "",
    history_status: str = "all",
    page: int = 1,
    session: AsyncSession = Depends(get_session),
):
    await require_alliance_access(
        request,
        session,
        guild_id=guild_id,
        alliance_id=alliance_id,
    )
    selected_status = history_status if history_status in {"all", "complete", "forfeited"} else "all"
    return {
        "ok": True,
        **await operations_store.clan_settlement_history_page(
            session,
            guild_id=guild_id,
            alliance_id=alliance_id,
            period_days=workspace_store.normalize_period(period),
            query=_query(q),
            status_filter=selected_status,
            page=max(page, 1),
        ),
    }
