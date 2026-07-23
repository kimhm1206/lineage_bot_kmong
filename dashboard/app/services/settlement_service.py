from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession


PPM_BASE = 1_000_000
STATUS_PENDING = 0
STATUS_COMPLETE = 1
STATUS_FORFEITED = 2
OBJECT_ALLIANCE = 1
OBJECT_MEMBER = 2
OBJECT_FEE = 3
FIXED_ALLIANCE_FEE = "alliance_fee"
FIXED_CLAN_FUND = "clan_fund"
FIXED_FEE_NAMES = {
    FIXED_ALLIANCE_FEE: "연합 수수료",
    FIXED_CLAN_FUND: "혈비",
}


class SettlementError(ValueError):
    pass


@dataclass(frozen=True)
class OperationResult:
    message: str
    affected_ids: tuple[int, ...] = ()


def _now() -> int:
    return int(time.time())


def _clean_name(value: Any, *, label: str, max_length: int = 100) -> str:
    normalized = " ".join(str(value or "").strip().split())
    if not normalized:
        raise SettlementError(f"{label}을(를) 입력해 주세요.")
    if len(normalized) > max_length:
        raise SettlementError(f"{label}은(는) {max_length}자 이내로 입력해 주세요.")
    return normalized


def _positive_int(value: Any, *, label: str, allow_zero: bool = False) -> int:
    try:
        parsed = int(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        raise SettlementError(f"{label} 값을 확인해 주세요.") from None
    minimum = 0 if allow_zero else 1
    if parsed < minimum:
        raise SettlementError(f"{label}은(는) {minimum:,} 이상이어야 합니다.")
    return parsed


def _rate_ppm(value: Any) -> int:
    try:
        percent = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        raise SettlementError("수수료율을 확인해 주세요.") from None
    if percent < Decimal(0) or percent > Decimal(100):
        raise SettlementError("수수료율은 0%부터 100% 사이여야 합니다.")
    return int(percent * Decimal(10_000))


def _id_list(values: Iterable[Any]) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    for value in values:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in seen:
            seen.add(parsed)
            result.append(parsed)
    return result


async def _audit(
    session: AsyncSession,
    *,
    guild_id: int,
    action_code: str,
    target_id: int | None,
    attendance_id: int | None = None,
    user_id: int | None = None,
    item_id: int | None = None,
    alliance_id: int | None = None,
    state_code: int | None = None,
    amount_value: int | None = None,
) -> None:
    action_type_id = await session.scalar(
        text("SELECT action_type_id FROM audit_action_types WHERE action_code = :code"),
        {"code": action_code},
    )
    if action_type_id is None:
        return
    audit_event_id = await session.scalar(
        text("""
            INSERT INTO audit_events (
                guild_id, actor_id, actor_role, action_type_id, target_id, occurred_at
            ) VALUES (:guild_id, NULL, 4, :action_type_id, :target_id, :now)
            RETURNING audit_event_id
        """),
        {
            "guild_id": guild_id,
            "action_type_id": int(action_type_id),
            "target_id": target_id,
            "now": _now(),
        },
    )
    await session.execute(
        text("""
            INSERT INTO audit_event_contexts (
                audit_event_id, attendance_id, user_id, loot_event_id,
                item_id, alliance_id, state_code, amount_value
            ) VALUES (
                :audit_event_id, :attendance_id, :user_id, :loot_event_id,
                :item_id, :alliance_id, :state_code, :amount_value
            )
        """),
        {
            "audit_event_id": int(audit_event_id),
            "attendance_id": attendance_id,
            "user_id": user_id,
            "loot_event_id": target_id if action_code.startswith(("loot_", "sale_")) else None,
            "item_id": item_id,
            "alliance_id": alliance_id,
            "state_code": state_code,
            "amount_value": amount_value,
        },
    )


async def _catalog_version(session: AsyncSession, item_id: int, guild_id: int) -> int:
    item = (
        await session.execute(
            text("""
                SELECT item_id, item_name
                FROM items
                WHERE item_id = :item_id
                  AND guild_id = :guild_id
                  AND status_code = 1
            """),
            {"item_id": item_id, "guild_id": guild_id},
        )
    ).mappings().one_or_none()
    if item is None:
        raise SettlementError("사용 가능한 아이템을 선택해 주세요.")
    version = (
        await session.execute(
            text("""
                SELECT item_version_id, item_name
                FROM catalog_item_versions
                WHERE item_id = :item_id
                ORDER BY valid_from DESC, item_version_id DESC
                LIMIT 1
            """),
            {"item_id": item_id},
        )
    ).mappings().one_or_none()
    if version is not None and version["item_name"] == item["item_name"]:
        return int(version["item_version_id"])
    return int(
        await session.scalar(
            text("""
                INSERT INTO catalog_item_versions (item_id, item_name, valid_from)
                VALUES (:item_id, :item_name, :valid_from)
                RETURNING item_version_id
            """),
            {"item_id": item_id, "item_name": item["item_name"], "valid_from": _now()},
        )
    )


async def _drop_item_snapshot(session: AsyncSession, item_id: int, guild_id: int) -> tuple[int, int]:
    item_version_id = await _catalog_version(session, item_id, guild_id)
    default_price = await session.scalar(
        text("""
            SELECT default_price
            FROM items
            WHERE item_id = :item_id
              AND guild_id = :guild_id
              AND status_code = 1
        """),
        {"item_id": item_id, "guild_id": guild_id},
    )
    cash_price_krw = int(default_price or 0)
    if cash_price_krw <= 0:
        raise SettlementError("아이템의 기본 원화 시세를 먼저 설정해 주세요.")
    return item_version_id, cash_price_krw


async def _attendance_epoch(session: AsyncSession, attendance_id: int, guild_id: int) -> int:
    value = await session.scalar(
        text("""
            SELECT EXTRACT(EPOCH FROM started_at::timestamp)::BIGINT
            FROM attendance_sessions
            WHERE attendance_id = :attendance_id AND guild_id = :guild_id
        """),
        {"attendance_id": attendance_id, "guild_id": guild_id},
    )
    if value is None:
        raise SettlementError("선택한 출석 회차를 찾을 수 없습니다.")
    return int(value)


async def _replace_drop_snapshot(
    session: AsyncSession,
    *,
    drop_id: int,
    attendance_id: int,
    excluded_alliance_ids: Iterable[Any],
) -> None:
    await session.execute(
        text("DELETE FROM settlement_drop_participants WHERE drop_id = :drop_id"),
        {"drop_id": drop_id},
    )
    await session.execute(
        text("""
            INSERT INTO settlement_drop_participants (drop_id, user_id, alliance_id)
            SELECT :drop_id, e.user_id, u.alliance_id
            FROM attendance_entries e
            JOIN users u ON u.user_id = e.user_id
            WHERE e.attendance_id = :attendance_id
        """),
        {"drop_id": drop_id, "attendance_id": attendance_id},
    )
    participant_count = int(
        await session.scalar(
            text("SELECT COUNT(*) FROM settlement_drop_participants WHERE drop_id = :drop_id"),
            {"drop_id": drop_id},
        )
        or 0
    )
    if participant_count == 0:
        raise SettlementError("출석 참여자가 없는 회차는 드랍에 연결할 수 없습니다.")

    await session.execute(
        text("DELETE FROM settlement_drop_excluded_alliances WHERE drop_id = :drop_id"),
        {"drop_id": drop_id},
    )
    excluded = _id_list(excluded_alliance_ids)
    if excluded:
        statement = text("""
            INSERT INTO settlement_drop_excluded_alliances (drop_id, alliance_id)
            SELECT :drop_id, alliance_id
            FROM alliances
            WHERE alliance_id IN :alliance_ids AND is_active IS TRUE
        """).bindparams(bindparam("alliance_ids", expanding=True))
        await session.execute(statement, {"drop_id": drop_id, "alliance_ids": excluded})


async def create_drop(
    session: AsyncSession,
    *,
    guild_id: int,
    attendance_id: int,
    item_id: int,
    excluded_alliance_ids: Iterable[Any] = (),
) -> OperationResult:
    occurred_at = await _attendance_epoch(session, attendance_id, guild_id)
    item_version_id, cash_price_krw = await _drop_item_snapshot(session, item_id, guild_id)
    drop_id = int(
        await session.scalar(
            text("""
                INSERT INTO settlement_drops (
                    guild_id, attendance_id, item_version_id, cash_price_krw,
                    adena_market_rate, gross_adena, occurred_at, created_by_user_id
                ) VALUES (
                    :guild_id, :attendance_id, :item_version_id, :cash_price_krw,
                    1, 0, :occurred_at, NULL
                ) RETURNING drop_id
            """),
            {
                "guild_id": guild_id,
                "attendance_id": attendance_id,
                "item_version_id": item_version_id,
                "cash_price_krw": cash_price_krw,
                "occurred_at": occurred_at,
            },
        )
    )
    await _replace_drop_snapshot(
        session,
        drop_id=drop_id,
        attendance_id=attendance_id,
        excluded_alliance_ids=excluded_alliance_ids,
    )
    now = _now()
    await session.execute(
        text("""
            INSERT INTO settlement_drop_sales (
                drop_id, status_code, buyer_alliance_id, buyer_user_id,
                completed_at, completed_by_user_id, created_at, updated_at
            ) VALUES (:drop_id, 0, NULL, NULL, NULL, NULL, :now, :now)
        """),
        {"drop_id": drop_id, "now": now},
    )
    await _audit(
        session,
        guild_id=guild_id,
        action_code="loot_create",
        target_id=drop_id,
        attendance_id=attendance_id,
        item_id=item_id,
    )
    return OperationResult("드랍을 판매 대기 목록에 등록했습니다.", (drop_id,))


async def update_drop(
    session: AsyncSession,
    *,
    drop_id: int,
    guild_id: int,
    attendance_id: int,
    item_id: int,
    excluded_alliance_ids: Iterable[Any] = (),
) -> OperationResult:
    drop = (
        await session.execute(
            text("""
                SELECT d.drop_id, s.status_code
                FROM settlement_drops d
                JOIN settlement_drop_sales s ON s.drop_id = d.drop_id
                WHERE d.drop_id = :drop_id AND d.guild_id = :guild_id
                FOR UPDATE
            """),
            {"drop_id": drop_id, "guild_id": guild_id},
        )
    ).mappings().one_or_none()
    if drop is None:
        raise SettlementError("드랍 기록을 찾을 수 없습니다.")
    if int(drop["status_code"]) != 0:
        raise SettlementError("판매 완료된 드랍은 판매를 취소한 뒤 수정해 주세요.")
    occurred_at = await _attendance_epoch(session, attendance_id, guild_id)
    item_version_id, cash_price_krw = await _drop_item_snapshot(session, item_id, guild_id)
    await session.execute(
        text("DELETE FROM settlement_payout_objects WHERE drop_id = :drop_id"),
        {"drop_id": drop_id},
    )
    await session.execute(
        text("""
            UPDATE settlement_drops
            SET attendance_id = :attendance_id,
                item_version_id = :item_version_id,
                cash_price_krw = :cash_price_krw,
                adena_market_rate = 1,
                gross_adena = 0,
                occurred_at = :occurred_at
            WHERE drop_id = :drop_id
        """),
        {
            "drop_id": drop_id,
            "attendance_id": attendance_id,
            "item_version_id": item_version_id,
            "cash_price_krw": cash_price_krw,
            "occurred_at": occurred_at,
        },
    )
    await _replace_drop_snapshot(
        session,
        drop_id=drop_id,
        attendance_id=attendance_id,
        excluded_alliance_ids=excluded_alliance_ids,
    )
    await _audit(
        session,
        guild_id=guild_id,
        action_code="loot_update",
        target_id=drop_id,
        attendance_id=attendance_id,
        item_id=item_id,
    )
    return OperationResult("드랍 정보를 수정했습니다.", (drop_id,))


async def delete_drop(session: AsyncSession, *, drop_id: int, guild_id: int) -> OperationResult:
    non_pending = int(
        await session.scalar(
            text("""
                SELECT COUNT(*)
                FROM settlement_payout_objects po
                JOIN settlement_drops d ON d.drop_id = po.drop_id
                WHERE po.drop_id = :drop_id AND d.guild_id = :guild_id
                  AND po.status_code <> 0
            """),
            {"drop_id": drop_id, "guild_id": guild_id},
        )
        or 0
    )
    if non_pending:
        raise SettlementError("이미 정산된 내역이 있어 드랍을 삭제할 수 없습니다.")
    deleted = await session.scalar(
        text("""
            DELETE FROM settlement_drops
            WHERE drop_id = :drop_id AND guild_id = :guild_id
            RETURNING attendance_id
        """),
        {"drop_id": drop_id, "guild_id": guild_id},
    )
    if deleted is None:
        raise SettlementError("드랍 기록을 찾을 수 없습니다.")
    await _audit(
        session,
        guild_id=guild_id,
        action_code="loot_delete",
        target_id=drop_id,
        attendance_id=int(deleted),
    )
    return OperationResult("드랍 기록을 삭제했습니다.", (drop_id,))


async def _latest_fee_rules(
    session: AsyncSession,
    *,
    guild_id: int,
    scope_code: int,
    alliance_id: int | None,
) -> list[dict[str, Any]]:
    alliance_clause = "r.alliance_id IS NULL" if scope_code == 1 else "r.alliance_id = :alliance_id"
    rows = (
        await session.execute(
            text(f"""
                SELECT r.fee_rule_id, r.fixed_code,
                       latest.fee_rule_version_id,
                       latest.rule_name, latest.rate_ppm
                FROM settlement_fee_rules r
                JOIN LATERAL (
                    SELECT v.fee_rule_version_id, v.rule_name, v.rate_ppm
                    FROM settlement_fee_rule_versions v
                    WHERE v.fee_rule_id = r.fee_rule_id
                    ORDER BY v.valid_from DESC, v.fee_rule_version_id DESC
                    LIMIT 1
                ) latest ON TRUE
                WHERE r.guild_id = :guild_id
                  AND r.scope_code = :scope_code
                  AND r.is_active IS TRUE
                  AND {alliance_clause}
                ORDER BY r.fee_rule_id
            """),
            {"guild_id": guild_id, "scope_code": scope_code, "alliance_id": alliance_id},
        )
    ).mappings().all()
    result = [dict(row) for row in rows]
    if sum(int(row["rate_ppm"]) for row in result) > PPM_BASE:
        raise SettlementError("활성 수수료 합계가 100%를 초과합니다. 수수료 설정을 확인해 주세요.")
    return result


async def _build_alliance_payouts(session: AsyncSession, *, drop_id: int) -> None:
    drop = (
        await session.execute(
            text("""
                SELECT d.guild_id, d.gross_adena, s.buyer_alliance_id
                FROM settlement_drops d
                JOIN settlement_drop_sales s ON s.drop_id = d.drop_id
                WHERE d.drop_id = :drop_id AND s.status_code = 1
            """),
            {"drop_id": drop_id},
        )
    ).mappings().one_or_none()
    if drop is None:
        raise SettlementError("판매 완료 상태를 확인할 수 없습니다.")
    rules = await _latest_fee_rules(
        session,
        guild_id=int(drop["guild_id"]),
        scope_code=1,
        alliance_id=None,
    )
    gross = int(drop["gross_adena"])
    total_fee = 0
    for rule in rules:
        amount = gross * int(rule["rate_ppm"]) // PPM_BASE
        total_fee += amount
        if amount <= 0:
            continue
        await session.execute(
            text("""
                INSERT INTO settlement_payout_objects (
                    drop_id, parent_payout_object_id, object_code,
                    recipient_alliance_id, recipient_user_id, fee_rule_version_id,
                    amount_adena, status_code, completed_at, completed_by_user_id
                ) VALUES (
                    :drop_id, NULL, 3, NULL, NULL, :version_id,
                    :amount, 0, NULL, NULL
                )
            """),
            {"drop_id": drop_id, "version_id": rule["fee_rule_version_id"], "amount": amount},
        )
    distributable = max(gross - total_fee, 0)
    groups = (
        await session.execute(
            text("""
                SELECT p.alliance_id, COUNT(*)::BIGINT AS member_count
                FROM settlement_drop_participants p
                WHERE p.drop_id = :drop_id
                  AND p.alliance_id IS NOT NULL
                  AND p.alliance_id <> :buyer_alliance_id
                  AND NOT EXISTS (
                      SELECT 1
                      FROM settlement_drop_excluded_alliances excluded
                      WHERE excluded.drop_id = p.drop_id
                        AND excluded.alliance_id = p.alliance_id
                  )
                GROUP BY p.alliance_id
                ORDER BY p.alliance_id
            """),
            {"drop_id": drop_id, "buyer_alliance_id": int(drop["buyer_alliance_id"])},
        )
    ).mappings().all()
    eligible_count = sum(int(row["member_count"]) for row in groups)
    if eligible_count == 0:
        raise SettlementError("구매·제외 혈맹을 빼면 분배할 참여자가 없습니다.")
    per_member = distributable // eligible_count
    for group in groups:
        amount = per_member * int(group["member_count"])
        await session.execute(
            text("""
                INSERT INTO settlement_payout_objects (
                    drop_id, parent_payout_object_id, object_code,
                    recipient_alliance_id, recipient_user_id, fee_rule_version_id,
                    amount_adena, status_code, completed_at, completed_by_user_id
                ) VALUES (
                    :drop_id, NULL, 1, :alliance_id, NULL, NULL,
                    :amount, 0, NULL, NULL
                )
            """),
            {"drop_id": drop_id, "alliance_id": group["alliance_id"], "amount": amount},
        )


async def complete_sale(
    session: AsyncSession,
    *,
    drop_id: int,
    guild_id: int,
    buyer_alliance_id: int,
    buyer_user_id: int | None,
    adena_market_rate: int,
) -> OperationResult:
    adena_market_rate = _positive_int(adena_market_rate, label="아데나 시세")
    sale = (
        await session.execute(
            text("""
                SELECT s.status_code, d.cash_price_krw, i.default_price
                FROM settlement_drop_sales s
                JOIN settlement_drops d ON d.drop_id = s.drop_id
                JOIN catalog_item_versions v ON v.item_version_id = d.item_version_id
                JOIN items i ON i.item_id = v.item_id
                WHERE s.drop_id = :drop_id AND d.guild_id = :guild_id
                FOR UPDATE
            """),
            {"drop_id": drop_id, "guild_id": guild_id},
        )
    ).mappings().one_or_none()
    if sale is None:
        raise SettlementError("판매할 드랍 기록을 찾을 수 없습니다.")
    cash_price_krw = int(sale["cash_price_krw"] or sale["default_price"] or 0)
    if cash_price_krw <= 0:
        raise SettlementError("저장된 원화 시세가 없습니다. 아이템 시세를 설정한 뒤 드랍 정보를 다시 저장해 주세요.")
    buyer_exists = await session.scalar(
        text("""
            SELECT 1
            FROM guild_alliance_role_mappings
            WHERE guild_id = :guild_id AND alliance_id = :alliance_id
            LIMIT 1
        """),
        {"guild_id": guild_id, "alliance_id": buyer_alliance_id},
    )
    if buyer_exists is None:
        raise SettlementError("이 서버에 역할 매핑된 구매 혈맹을 선택해 주세요.")
    if buyer_user_id is not None:
        valid_buyer = await session.scalar(
            text("""
                SELECT 1 FROM users
                WHERE user_id = :user_id AND alliance_id = :alliance_id AND is_active IS TRUE
            """),
            {"user_id": buyer_user_id, "alliance_id": buyer_alliance_id},
        )
        if valid_buyer is None:
            raise SettlementError("구매자는 구매 혈맹의 활성 유저여야 합니다.")
    non_pending = int(
        await session.scalar(
            text("SELECT COUNT(*) FROM settlement_payout_objects WHERE drop_id = :drop_id AND status_code <> 0"),
            {"drop_id": drop_id},
        )
        or 0
    )
    if non_pending:
        raise SettlementError("이미 정산이 시작된 판매 정보는 변경할 수 없습니다.")
    await session.execute(
        text("DELETE FROM settlement_payout_objects WHERE drop_id = :drop_id"),
        {"drop_id": drop_id},
    )
    gross_adena = cash_price_krw * 10_000 // adena_market_rate
    if gross_adena <= 0:
        raise SettlementError("계산된 판매 아데나가 0입니다. 가격과 시세를 확인해 주세요.")
    now = _now()
    await session.execute(
        text("""
            UPDATE settlement_drops
            SET cash_price_krw = :cash_price,
                adena_market_rate = :market_rate,
                gross_adena = :gross_adena
            WHERE drop_id = :drop_id
        """),
        {
            "drop_id": drop_id,
            "cash_price": cash_price_krw,
            "market_rate": adena_market_rate,
            "gross_adena": gross_adena,
        },
    )
    await session.execute(
        text("""
            UPDATE settlement_drop_sales
            SET status_code = 1,
                buyer_alliance_id = :buyer_alliance_id,
                buyer_user_id = :buyer_user_id,
                completed_at = :now,
                completed_by_user_id = NULL,
                updated_at = :now
            WHERE drop_id = :drop_id
        """),
        {
            "drop_id": drop_id,
            "buyer_alliance_id": buyer_alliance_id,
            "buyer_user_id": buyer_user_id,
            "now": now,
        },
    )
    await _build_alliance_payouts(session, drop_id=drop_id)
    await _audit(
        session,
        guild_id=guild_id,
        action_code="sale_complete" if int(sale["status_code"]) == 0 else "sale_update",
        target_id=drop_id,
        alliance_id=buyer_alliance_id,
        amount_value=gross_adena,
    )
    return OperationResult("판매를 완료하고 혈맹별 분배금을 계산했습니다.", (drop_id,))


async def reopen_sale(session: AsyncSession, *, drop_id: int, guild_id: int) -> OperationResult:
    non_pending = int(
        await session.scalar(
            text("""
                SELECT COUNT(*)
                FROM settlement_payout_objects po
                JOIN settlement_drops d ON d.drop_id = po.drop_id
                WHERE po.drop_id = :drop_id AND d.guild_id = :guild_id
                  AND po.status_code <> 0
            """),
            {"drop_id": drop_id, "guild_id": guild_id},
        )
        or 0
    )
    if non_pending:
        raise SettlementError("정산이 시작되어 판매 상태를 되돌릴 수 없습니다.")
    await session.execute(
        text("DELETE FROM settlement_payout_objects WHERE drop_id = :drop_id"),
        {"drop_id": drop_id},
    )
    updated = await session.execute(
        text("""
            UPDATE settlement_drop_sales s
            SET status_code = 0, buyer_alliance_id = NULL, buyer_user_id = NULL,
                completed_at = NULL, completed_by_user_id = NULL, updated_at = :now
            FROM settlement_drops d
            WHERE s.drop_id = d.drop_id AND s.drop_id = :drop_id AND d.guild_id = :guild_id
        """),
        {"drop_id": drop_id, "guild_id": guild_id, "now": _now()},
    )
    if updated.rowcount == 0:
        raise SettlementError("판매 기록을 찾을 수 없습니다.")
    await _audit(session, guild_id=guild_id, action_code="sale_reopen", target_id=drop_id)
    return OperationResult("판매 대기 상태로 되돌렸습니다.", (drop_id,))


async def _build_clan_children(
    session: AsyncSession,
    *,
    parent_payout_object_id: int,
) -> None:
    parent = (
        await session.execute(
            text("""
                SELECT po.drop_id, po.recipient_alliance_id, po.amount_adena, d.guild_id
                FROM settlement_payout_objects po
                JOIN settlement_drops d ON d.drop_id = po.drop_id
                WHERE po.payout_object_id = :payout_id AND po.object_code = 1
            """),
            {"payout_id": parent_payout_object_id},
        )
    ).mappings().one()
    child_count = int(
        await session.scalar(
            text("SELECT COUNT(*) FROM settlement_payout_objects WHERE parent_payout_object_id = :parent_id"),
            {"parent_id": parent_payout_object_id},
        )
        or 0
    )
    if child_count:
        return
    rules = await _latest_fee_rules(
        session,
        guild_id=int(parent["guild_id"]),
        scope_code=2,
        alliance_id=int(parent["recipient_alliance_id"]),
    )
    parent_amount = int(parent["amount_adena"])
    total_fee = 0
    for rule in rules:
        amount = parent_amount * int(rule["rate_ppm"]) // PPM_BASE
        total_fee += amount
        if amount <= 0:
            continue
        await session.execute(
            text("""
                INSERT INTO settlement_payout_objects (
                    drop_id, parent_payout_object_id, object_code,
                    recipient_alliance_id, recipient_user_id, fee_rule_version_id,
                    amount_adena, status_code, completed_at, completed_by_user_id
                ) VALUES (
                    :drop_id, :parent_id, 3, NULL, NULL, :version_id,
                    :amount, 0, NULL, NULL
                )
            """),
            {
                "drop_id": parent["drop_id"],
                "parent_id": parent_payout_object_id,
                "version_id": rule["fee_rule_version_id"],
                "amount": amount,
            },
        )
    members = list(
        (
            await session.execute(
                text("""
                    SELECT user_id
                    FROM settlement_drop_participants
                    WHERE drop_id = :drop_id AND alliance_id = :alliance_id
                    ORDER BY user_id
                """),
                {"drop_id": parent["drop_id"], "alliance_id": parent["recipient_alliance_id"]},
            )
        ).scalars()
    )
    if not members:
        raise SettlementError("해당 혈맹의 출석 참여자를 찾을 수 없습니다.")
    per_member = max(parent_amount - total_fee, 0) // len(members)
    for user_id in members:
        await session.execute(
            text("""
                INSERT INTO settlement_payout_objects (
                    drop_id, parent_payout_object_id, object_code,
                    recipient_alliance_id, recipient_user_id, fee_rule_version_id,
                    amount_adena, status_code, completed_at, completed_by_user_id
                ) VALUES (
                    :drop_id, :parent_id, 2, NULL, :user_id, NULL,
                    :amount, 0, NULL, NULL
                )
            """),
            {
                "drop_id": parent["drop_id"],
                "parent_id": parent_payout_object_id,
                "user_id": int(user_id),
                "amount": per_member,
            },
        )


async def _treasury_credit(
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int | None,
    scope_code: int,
    source_type_id: int,
    source_id: int,
    amount: int,
    category_name: str,
    memo: str,
) -> None:
    if amount <= 0:
        return
    account = (
        await session.execute(
            text("""
                SELECT treasury_account_id, current_balance
                FROM treasury_accounts
                WHERE guild_id = :guild_id
                  AND account_scope_code = :scope_code
                  AND alliance_id IS NOT DISTINCT FROM :alliance_id
                FOR UPDATE
            """),
            {"guild_id": guild_id, "scope_code": scope_code, "alliance_id": alliance_id},
        )
    ).mappings().one_or_none()
    if account is None:
        account = (
            await session.execute(
                text("""
                    INSERT INTO treasury_accounts (
                        guild_id, alliance_id, account_scope_code, current_balance, updated_at
                    ) VALUES (:guild_id, :alliance_id, :scope_code, 0, :now)
                    RETURNING treasury_account_id, current_balance
                """),
                {"guild_id": guild_id, "alliance_id": alliance_id, "scope_code": scope_code, "now": _now()},
            )
        ).mappings().one()
    existing = await session.scalar(
        text("""
            SELECT 1 FROM treasury_entries
            WHERE treasury_account_id = :account_id
              AND source_type_id = :source_type_id
              AND source_id = :source_id
        """),
        {
            "account_id": account["treasury_account_id"],
            "source_type_id": source_type_id,
            "source_id": source_id,
        },
    )
    if existing:
        return
    category_id = await session.scalar(
        text("""
            SELECT treasury_category_id
            FROM treasury_categories
            WHERE guild_id = :guild_id AND account_scope_code = :scope_code
              AND direction = 1 AND category_name = :category_name
        """),
        {"guild_id": guild_id, "scope_code": scope_code, "category_name": category_name},
    )
    if category_id is None:
        category_id = await session.scalar(
            text("""
                INSERT INTO treasury_categories (
                    guild_id, account_scope_code, direction, category_name, is_active
                ) VALUES (:guild_id, :scope_code, 1, :category_name, TRUE)
                ON CONFLICT (guild_id, account_scope_code, direction, category_name)
                DO UPDATE SET is_active = TRUE
                RETURNING treasury_category_id
            """),
            {"guild_id": guild_id, "scope_code": scope_code, "category_name": category_name},
        )
    balance_after = int(account["current_balance"]) + amount
    now = _now()
    await session.execute(
        text("""
            INSERT INTO treasury_entries (
                treasury_account_id, treasury_category_id, direction, amount_adena,
                balance_after, source_type_id, source_id, memo, occurred_at,
                created_at, created_by_user_id, reversal_of_entry_id
            ) VALUES (
                :account_id, :category_id, 1, :amount, :balance_after,
                :source_type_id, :source_id, :memo, :now, :now, NULL, NULL
            )
        """),
        {
            "account_id": account["treasury_account_id"],
            "category_id": category_id,
            "amount": amount,
            "balance_after": balance_after,
            "source_type_id": source_type_id,
            "source_id": source_id,
            "memo": memo,
            "now": now,
        },
    )
    await session.execute(
        text("""
            UPDATE treasury_accounts
            SET current_balance = :balance_after, updated_at = :now
            WHERE treasury_account_id = :account_id
        """),
        {
            "account_id": account["treasury_account_id"],
            "balance_after": balance_after,
            "now": now,
        },
    )


async def set_payout_status(
    session: AsyncSession,
    *,
    payout_object_id: int,
    status_code: int,
) -> OperationResult:
    if status_code not in {STATUS_PENDING, STATUS_COMPLETE, STATUS_FORFEITED}:
        raise SettlementError("지원하지 않는 정산 상태입니다.")
    row = (
        await session.execute(
            text("""
                SELECT po.*, d.guild_id, v.item_name,
                       parent.recipient_alliance_id AS parent_alliance_id,
                       fr.scope_code, fr.alliance_id AS fee_alliance_id,
                       fr.fixed_code,
                       fv.rule_name,
                       COALESCE(recipient.game_nickname, recipient.discord_nickname) AS recipient_name
                FROM settlement_payout_objects po
                JOIN settlement_drops d ON d.drop_id = po.drop_id
                JOIN catalog_item_versions v ON v.item_version_id = d.item_version_id
                LEFT JOIN settlement_payout_objects parent
                  ON parent.payout_object_id = po.parent_payout_object_id
                LEFT JOIN users recipient ON recipient.user_id = po.recipient_user_id
                LEFT JOIN settlement_fee_rule_versions fv
                  ON fv.fee_rule_version_id = po.fee_rule_version_id
                LEFT JOIN settlement_fee_rules fr ON fr.fee_rule_id = fv.fee_rule_id
                WHERE po.payout_object_id = :payout_id
                FOR UPDATE OF po
            """),
            {"payout_id": payout_object_id},
        )
    ).mappings().one_or_none()
    if row is None:
        raise SettlementError("정산 대상을 찾을 수 없습니다.")
    object_code = int(row["object_code"])
    current_status = int(row["status_code"])
    if current_status == status_code:
        return OperationResult("이미 같은 상태로 처리되어 있습니다.", (payout_object_id,))
    if status_code == STATUS_FORFEITED and object_code != OBJECT_MEMBER:
        raise SettlementError("귀속은 혈맹원 분배금에만 사용할 수 있습니다.")
    if status_code == STATUS_COMPLETE and object_code == OBJECT_ALLIANCE:
        await _build_clan_children(session, parent_payout_object_id=payout_object_id)
    if status_code == STATUS_PENDING and object_code == OBJECT_ALLIANCE:
        child_status_count = int(
            await session.scalar(
                text("""
                    SELECT COUNT(*) FROM settlement_payout_objects
                    WHERE parent_payout_object_id = :parent_id AND status_code <> 0
                """),
                {"parent_id": payout_object_id},
            )
            or 0
        )
        if child_status_count:
            raise SettlementError("혈맹원 또는 수수료 정산이 시작되어 완료를 취소할 수 없습니다.")
        await session.execute(
            text("DELETE FROM settlement_payout_objects WHERE parent_payout_object_id = :parent_id"),
            {"parent_id": payout_object_id},
        )
    if status_code == STATUS_PENDING and current_status != STATUS_PENDING and object_code in {OBJECT_MEMBER, OBJECT_FEE}:
        ledger_exists = await session.scalar(
            text("""
                SELECT 1 FROM treasury_entries
                WHERE source_id = :source_id AND source_type_id IN (2, 3, 4)
                LIMIT 1
            """),
            {"source_id": payout_object_id},
        )
        if ledger_exists:
            raise SettlementError("가계부에 반영된 정산은 가계부에서 취소 기록을 남겨야 합니다.")
    completed_at = None if status_code == STATUS_PENDING else _now()
    await session.execute(
        text("""
            UPDATE settlement_payout_objects
            SET status_code = :status_code,
                completed_at = :completed_at,
                completed_by_user_id = NULL
            WHERE payout_object_id = :payout_id
        """),
        {
            "payout_id": payout_object_id,
            "status_code": status_code,
            "completed_at": completed_at,
        },
    )
    if status_code == STATUS_COMPLETE and object_code == OBJECT_FEE:
        scope_code = int(row["scope_code"] or 1)
        fixed_code = str(row["fixed_code"] or "")
        should_credit = (
            scope_code == 1 and fixed_code == FIXED_ALLIANCE_FEE
        ) or (
            scope_code == 2 and fixed_code == FIXED_CLAN_FUND
        )
        if should_credit:
            alliance_id = (
                int(row["parent_alliance_id"] or row["fee_alliance_id"])
                if scope_code == 2
                else None
            )
            await _treasury_credit(
                session,
                guild_id=int(row["guild_id"]),
                alliance_id=alliance_id,
                scope_code=scope_code,
                source_type_id=4 if scope_code == 1 else 3,
                source_id=payout_object_id,
                amount=int(row["amount_adena"]),
                category_name=(
                    FIXED_FEE_NAMES[FIXED_ALLIANCE_FEE]
                    if scope_code == 1
                    else FIXED_FEE_NAMES[FIXED_CLAN_FUND]
                ),
                memo=f"{row['rule_name']} · {row['item_name']}",
            )
    if status_code == STATUS_FORFEITED and object_code == OBJECT_MEMBER:
        alliance_id = int(row["parent_alliance_id"])
        await _treasury_credit(
            session,
            guild_id=int(row["guild_id"]),
            alliance_id=alliance_id,
            scope_code=2,
            source_type_id=2,
            source_id=payout_object_id,
            amount=int(row["amount_adena"]),
            category_name="귀속 혈비",
            memo=f"미수령 귀속 · {row['recipient_name'] or '알 수 없는 유저'} · {row['item_name']}",
        )
    await _audit(
        session,
        guild_id=int(row["guild_id"]),
        action_code="payout_status",
        target_id=payout_object_id,
        user_id=int(row["recipient_user_id"]) if row["recipient_user_id"] is not None else None,
        alliance_id=(
            int(row["recipient_alliance_id"])
            if row["recipient_alliance_id"] is not None
            else int(row["parent_alliance_id"])
            if row["parent_alliance_id"] is not None
            else None
        ),
        state_code=status_code,
        amount_value=int(row["amount_adena"]),
    )
    labels = {0: "미완료", 1: "완료", 2: "귀속"}
    return OperationResult(f"정산 상태를 {labels[status_code]}로 변경했습니다.", (payout_object_id,))


async def payout_access_scope(
    session: AsyncSession,
    *,
    payout_object_id: int,
) -> dict[str, int] | None:
    row = (
        await session.execute(
            text("""
                SELECT d.guild_id,
                       CASE
                           WHEN payout.object_code = 1
                             OR (
                                 payout.object_code = 3
                                 AND rule.scope_code = 1
                             )
                           THEN 1
                           ELSE 2
                       END AS scope_code,
                       COALESCE(
                           payout.recipient_alliance_id,
                           parent.recipient_alliance_id,
                           rule.alliance_id
                       ) AS alliance_id
                FROM settlement_payout_objects payout
                JOIN settlement_drops d ON d.drop_id = payout.drop_id
                LEFT JOIN settlement_payout_objects parent
                  ON parent.payout_object_id =
                     payout.parent_payout_object_id
                LEFT JOIN settlement_fee_rule_versions version
                  ON version.fee_rule_version_id =
                     payout.fee_rule_version_id
                LEFT JOIN settlement_fee_rules rule
                  ON rule.fee_rule_id = version.fee_rule_id
                WHERE payout.payout_object_id = :payout_object_id
            """),
            {"payout_object_id": payout_object_id},
        )
    ).mappings().one_or_none()
    if row is None:
        return None
    return {
        "guild_id": int(row["guild_id"]),
        "scope_code": int(row["scope_code"]),
        "alliance_id": (
            int(row["alliance_id"])
            if row["alliance_id"] is not None
            else 0
        ),
    }


async def treasury_recipient_access_scope(
    session: AsyncSession,
    *,
    recipient_id: int,
) -> dict[str, int] | None:
    row = (
        await session.execute(
            text("""
                SELECT account.guild_id,
                       account.account_scope_code AS scope_code,
                       COALESCE(
                           account.alliance_id,
                           recipient.alliance_id
                       ) AS alliance_id
                FROM treasury_distribution_recipients recipient
                JOIN treasury_distributions distribution
                  ON distribution.treasury_distribution_id =
                     recipient.treasury_distribution_id
                JOIN treasury_accounts account
                  ON account.treasury_account_id =
                     distribution.treasury_account_id
                WHERE recipient.treasury_distribution_recipient_id =
                      :recipient_id
            """),
            {"recipient_id": recipient_id},
        )
    ).mappings().one_or_none()
    if row is None:
        return None
    return {
        "guild_id": int(row["guild_id"]),
        "scope_code": int(row["scope_code"]),
        "alliance_id": (
            int(row["alliance_id"])
            if row["alliance_id"] is not None
            else 0
        ),
    }


async def fee_rule_access_scope(
    session: AsyncSession,
    *,
    guild_id: int,
    fee_rule_id: int,
) -> dict[str, int] | None:
    row = (
        await session.execute(
            text("""
                SELECT scope_code, alliance_id
                FROM settlement_fee_rules
                WHERE guild_id = :guild_id
                  AND fee_rule_id = :fee_rule_id
            """),
            {"guild_id": guild_id, "fee_rule_id": fee_rule_id},
        )
    ).mappings().one_or_none()
    if row is None:
        return None
    return {
        "scope_code": int(row["scope_code"]),
        "alliance_id": (
            int(row["alliance_id"])
            if row["alliance_id"] is not None
            else 0
        ),
    }


async def set_treasury_distribution_recipient_status(
    session: AsyncSession,
    *,
    recipient_id: int,
    status_code: int,
) -> OperationResult:
    if status_code not in {STATUS_COMPLETE, STATUS_FORFEITED}:
        raise SettlementError("공금 분배는 완료 또는 귀속 처리만 가능합니다.")
    row = (
        await session.execute(
            text("""
                SELECT r.treasury_distribution_recipient_id,
                       r.user_id, r.alliance_id, r.status_code,
                       d.per_recipient_amount, d.memo,
                       account.guild_id, account.account_scope_code,
                       account.alliance_id AS account_alliance_id,
                       COALESCE(
                           recipient.game_nickname,
                           recipient.discord_nickname
                       ) AS recipient_name
                FROM treasury_distribution_recipients r
                JOIN treasury_distributions d
                  ON d.treasury_distribution_id =
                     r.treasury_distribution_id
                JOIN treasury_accounts account
                  ON account.treasury_account_id = d.treasury_account_id
                LEFT JOIN users recipient ON recipient.user_id = r.user_id
                WHERE r.treasury_distribution_recipient_id = :recipient_id
                FOR UPDATE OF r
            """),
            {"recipient_id": recipient_id},
        )
    ).mappings().one_or_none()
    if row is None:
        raise SettlementError("공금 분배 대상을 찾을 수 없습니다.")
    current_status = int(row["status_code"])
    if current_status == status_code:
        return OperationResult(
            "이미 같은 상태로 처리되어 있습니다.",
            (recipient_id,),
        )
    if current_status != STATUS_PENDING:
        raise SettlementError("이미 처리된 공금 분배는 이 화면에서 변경할 수 없습니다.")
    if status_code == STATUS_FORFEITED:
        if int(row["account_scope_code"]) != 2 or row["user_id"] is None:
            raise SettlementError("귀속은 혈맹원 대상 혈비 분배에만 사용할 수 있습니다.")
        source_type_id = await session.scalar(
            text("""
                SELECT source_type_id
                FROM treasury_source_types
                WHERE source_code = 'treasury_distribution_forfeiture'
            """)
        )
        if source_type_id is None:
            raise SettlementError("공금 귀속 원본 유형을 찾을 수 없습니다.")
        await _treasury_credit(
            session,
            guild_id=int(row["guild_id"]),
            alliance_id=int(row["account_alliance_id"]),
            scope_code=2,
            source_type_id=int(source_type_id),
            source_id=recipient_id,
            amount=int(row["per_recipient_amount"]),
            category_name="귀속 혈비",
            memo=(
                f"혈비 잔액 분배 귀속 · "
                f"{row['recipient_name'] or '알 수 없는 유저'}"
            ),
        )
    elif (
        status_code == STATUS_COMPLETE
        and int(row["account_scope_code"]) == 1
        and row["alliance_id"] is not None
    ):
        source_type_id = await session.scalar(
            text("""
                SELECT source_type_id
                FROM treasury_source_types
                WHERE source_code = 'alliance_distribution_receipt'
            """)
        )
        if source_type_id is None:
            raise SettlementError("연합비 분배 수령 원본 유형을 찾을 수 없습니다.")
        await _treasury_credit(
            session,
            guild_id=int(row["guild_id"]),
            alliance_id=int(row["alliance_id"]),
            scope_code=2,
            source_type_id=int(source_type_id),
            source_id=recipient_id,
            amount=int(row["per_recipient_amount"]),
            category_name="연합비 분배 수령",
            memo="연합비 잔액 분배 수령",
        )
    await session.execute(
        text("""
            UPDATE treasury_distribution_recipients
            SET status_code = :status_code,
                completed_at = :completed_at
            WHERE treasury_distribution_recipient_id = :recipient_id
        """),
        {
            "recipient_id": recipient_id,
            "status_code": status_code,
            "completed_at": _now(),
        },
    )
    await _audit(
        session,
        guild_id=int(row["guild_id"]),
        action_code="payout_status",
        target_id=recipient_id,
        user_id=int(row["user_id"]) if row["user_id"] is not None else None,
        alliance_id=(
            int(row["alliance_id"])
            if row["alliance_id"] is not None
            else int(row["account_alliance_id"])
            if row["account_alliance_id"] is not None
            else None
        ),
        state_code=status_code,
        amount_value=int(row["per_recipient_amount"]),
    )
    label = "완료" if status_code == STATUS_COMPLETE else "귀속"
    return OperationResult(
        f"공금 분배를 {label} 처리했습니다.",
        (recipient_id,),
    )


async def set_payout_group_status(
    session: AsyncSession,
    *,
    guild_id: int,
    group_type: str,
    target_id: int,
    alliance_id: int | None,
    status_code: int,
) -> OperationResult:
    treasury_sql: str | None = None
    if group_type == "alliance":
        sql = """
            SELECT po.payout_object_id
            FROM settlement_payout_objects po
            JOIN settlement_drops d ON d.drop_id = po.drop_id
            JOIN settlement_drop_sales s ON s.drop_id = d.drop_id AND s.status_code = 1
            WHERE d.guild_id = :guild_id AND po.object_code = 1
              AND po.recipient_alliance_id = :target_id AND po.status_code = 0
            ORDER BY po.payout_object_id
        """
        if status_code == 1:
            treasury_sql = """
                SELECT r.treasury_distribution_recipient_id
                FROM treasury_distribution_recipients r
                JOIN treasury_distributions distribution
                  ON distribution.treasury_distribution_id =
                     r.treasury_distribution_id
                JOIN treasury_accounts account
                  ON account.treasury_account_id =
                     distribution.treasury_account_id
                WHERE account.guild_id = :guild_id
                  AND account.account_scope_code = 1
                  AND r.alliance_id = :target_id
                  AND r.status_code = 0
                ORDER BY r.treasury_distribution_recipient_id
            """
    elif group_type == "member":
        sql = """
            SELECT po.payout_object_id
            FROM settlement_payout_objects po
            JOIN settlement_drops d ON d.drop_id = po.drop_id
            LEFT JOIN settlement_payout_objects parent ON parent.payout_object_id = po.parent_payout_object_id
            WHERE d.guild_id = :guild_id AND po.object_code = 2
              AND po.recipient_user_id = :target_id
              AND parent.recipient_alliance_id = :alliance_id
              AND po.status_code = 0
            ORDER BY po.payout_object_id
        """
        treasury_sql = """
            SELECT r.treasury_distribution_recipient_id
            FROM treasury_distribution_recipients r
            JOIN treasury_distributions distribution
              ON distribution.treasury_distribution_id =
                 r.treasury_distribution_id
            JOIN treasury_accounts account
              ON account.treasury_account_id =
                 distribution.treasury_account_id
            WHERE account.guild_id = :guild_id
              AND account.account_scope_code = 2
              AND account.alliance_id = :alliance_id
              AND r.user_id = :target_id
              AND r.status_code = 0
            ORDER BY r.treasury_distribution_recipient_id
        """
    elif group_type == "fee":
        fee_scope_filter = (
            "fr.scope_code = 1"
            if alliance_id is None
            else "fr.scope_code = 2 AND parent.recipient_alliance_id = :alliance_id"
        )
        sql = f"""
            SELECT po.payout_object_id
            FROM settlement_payout_objects po
            JOIN settlement_drops d ON d.drop_id = po.drop_id
            JOIN settlement_fee_rule_versions fv ON fv.fee_rule_version_id = po.fee_rule_version_id
            JOIN settlement_fee_rules fr ON fr.fee_rule_id = fv.fee_rule_id
            LEFT JOIN settlement_payout_objects parent ON parent.payout_object_id = po.parent_payout_object_id
            WHERE d.guild_id = :guild_id AND po.object_code = 3
              AND fr.fee_rule_id = :target_id AND po.status_code = 0
              AND {fee_scope_filter}
            ORDER BY po.payout_object_id
        """
    else:
        raise SettlementError("지원하지 않는 일괄 처리 대상입니다.")
    payout_ids = [
        int(value)
        for value in (
            await session.execute(
                text(sql),
                {
                    "guild_id": guild_id,
                    "target_id": target_id,
                    "alliance_id": alliance_id,
                },
            )
        ).scalars()
    ]
    treasury_recipient_ids = (
        [
            int(value)
            for value in (
                await session.execute(
                    text(treasury_sql),
                    {
                        "guild_id": guild_id,
                        "target_id": target_id,
                        "alliance_id": alliance_id,
                    },
                )
            ).scalars()
        ]
        if treasury_sql
        else []
    )
    if not payout_ids and not treasury_recipient_ids:
        raise SettlementError("처리할 미완료 내역이 없습니다.")
    for payout_id in payout_ids:
        await set_payout_status(session, payout_object_id=payout_id, status_code=status_code)
    for recipient_id in treasury_recipient_ids:
        await set_treasury_distribution_recipient_status(
            session,
            recipient_id=recipient_id,
            status_code=status_code,
        )
    total_count = len(payout_ids) + len(treasury_recipient_ids)
    return OperationResult(
        f"{total_count:,}건을 한 번에 처리했습니다.",
        tuple(payout_ids + treasury_recipient_ids),
    )


async def create_item(
    session: AsyncSession, *, guild_id: int, item_name: str, default_price: Any
) -> OperationResult:
    name = _clean_name(item_name, label="아이템 이름")
    price = _positive_int(default_price, label="기본 원화 시세", allow_zero=True)
    existing = (
        await session.execute(
            text("""
                SELECT item_id, status_code
                FROM items
                WHERE guild_id = :guild_id
                  AND LOWER(item_name) = LOWER(:name)
                FOR UPDATE
            """),
            {"guild_id": guild_id, "name": name},
        )
    ).mappings().one_or_none()
    if existing is not None and int(existing["status_code"]) == 1:
        raise SettlementError("같은 이름의 아이템이 이미 있습니다.")
    if existing is not None:
        item_id = int(existing["item_id"])
        await session.execute(
            text("""
                UPDATE items
                SET item_name = :name,
                    default_price = :price,
                    status_code = 1,
                    updated_at = NOW()
                WHERE item_id = :item_id
                  AND guild_id = :guild_id
            """),
            {"guild_id": guild_id, "item_id": item_id, "name": name, "price": price},
        )
        await _catalog_version(session, item_id, guild_id)
        await _audit(
            session,
            guild_id=guild_id,
            action_code="item_create",
            target_id=item_id,
            item_id=item_id,
            amount_value=price,
        )
        return OperationResult("아이템을 다시 사용하도록 복구했습니다.", (item_id,))
    item_id = int(
        await session.scalar(
            text("""
                INSERT INTO items (guild_id, item_name, default_price, updated_at)
                VALUES (:guild_id, :name, :price, NOW())
                RETURNING item_id
            """),
            {"guild_id": guild_id, "name": name, "price": price},
        )
    )
    await _catalog_version(session, item_id, guild_id)
    await _audit(session, guild_id=guild_id, action_code="item_create", target_id=item_id, item_id=item_id, amount_value=price)
    return OperationResult("아이템을 추가했습니다.", (item_id,))


async def update_item(
    session: AsyncSession,
    *,
    guild_id: int,
    item_id: int,
    item_name: str,
    default_price: Any,
) -> OperationResult:
    name = _clean_name(item_name, label="아이템 이름")
    price = _positive_int(default_price, label="기본 원화 시세", allow_zero=True)
    duplicate = await session.scalar(
        text("""
            SELECT 1 FROM items
            WHERE guild_id = :guild_id AND item_id <> :item_id
              AND LOWER(item_name) = LOWER(:name)
        """),
        {"guild_id": guild_id, "item_id": item_id, "name": name},
    )
    if duplicate:
        raise SettlementError("같은 이름의 아이템이 이미 있습니다.")
    updated = await session.execute(
        text("""
            UPDATE items
            SET item_name = :name, default_price = :price, updated_at = NOW()
            WHERE item_id = :item_id
              AND guild_id = :guild_id
              AND status_code = 1
        """),
        {"guild_id": guild_id, "item_id": item_id, "name": name, "price": price},
    )
    if updated.rowcount == 0:
        raise SettlementError("아이템을 찾을 수 없습니다.")
    await _catalog_version(session, item_id, guild_id)
    await _audit(session, guild_id=guild_id, action_code="item_update", target_id=item_id, item_id=item_id, amount_value=price)
    return OperationResult("아이템 정보를 수정했습니다.", (item_id,))


async def delete_item(session: AsyncSession, *, guild_id: int, item_id: int) -> OperationResult:
    updated = await session.execute(
        text("""
            UPDATE items
            SET status_code = 0,
                updated_at = NOW()
            WHERE item_id = :item_id
              AND guild_id = :guild_id
              AND status_code = 1
        """),
        {"item_id": item_id, "guild_id": guild_id},
    )
    if updated.rowcount == 0:
        raise SettlementError("아이템을 찾을 수 없습니다.")
    await _audit(session, guild_id=guild_id, action_code="item_delete", target_id=item_id, item_id=item_id)
    return OperationResult("아이템 목록에서 삭제했습니다.", (item_id,))


async def create_fee_rule(
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int | None,
    scope_code: int,
    rule_name: str,
    percent: Any,
) -> OperationResult:
    name = _clean_name(rule_name, label="수수료 이름", max_length=60)
    if name in FIXED_FEE_NAMES.values():
        raise SettlementError(f"{name} 항목은 기본 규칙에서 비율만 수정할 수 있습니다.")
    ppm = _rate_ppm(percent)
    if scope_code == 1:
        alliance_id = None
    elif alliance_id is None:
        raise SettlementError("혈맹 수수료에는 혈맹 선택이 필요합니다.")
    existing_total = int(
        await session.scalar(
            text("""
                SELECT COALESCE(SUM(latest.rate_ppm), 0)
                FROM settlement_fee_rules r
                JOIN LATERAL (
                    SELECT rate_ppm FROM settlement_fee_rule_versions v
                    WHERE v.fee_rule_id = r.fee_rule_id
                    ORDER BY valid_from DESC, fee_rule_version_id DESC LIMIT 1
                ) latest ON TRUE
                WHERE r.guild_id = :guild_id AND r.scope_code = :scope_code
                  AND r.alliance_id IS NOT DISTINCT FROM :alliance_id
                  AND r.is_active IS TRUE
            """),
            {"guild_id": guild_id, "scope_code": scope_code, "alliance_id": alliance_id},
        )
        or 0
    )
    if existing_total + ppm > PPM_BASE:
        raise SettlementError("활성 수수료 합계는 100%를 넘을 수 없습니다.")
    fee_rule_id = int(
        await session.scalar(
            text("""
                INSERT INTO settlement_fee_rules (
                    guild_id, alliance_id, scope_code, is_active
                ) VALUES (:guild_id, :alliance_id, :scope_code, TRUE)
                RETURNING fee_rule_id
            """),
            {"guild_id": guild_id, "alliance_id": alliance_id, "scope_code": scope_code},
        )
    )
    await session.execute(
        text("""
            INSERT INTO settlement_fee_rule_versions (
                fee_rule_id, rule_name, rate_ppm, valid_from
            ) VALUES (:fee_rule_id, :name, :ppm, :now)
        """),
        {"fee_rule_id": fee_rule_id, "name": name, "ppm": ppm, "now": _now()},
    )
    await recalculate_open_settlements(session, guild_id=guild_id, scope_code=scope_code, alliance_id=alliance_id)
    return OperationResult("수수료 규칙을 추가했습니다.", (fee_rule_id,))


async def update_fee_rule(
    session: AsyncSession,
    *,
    guild_id: int,
    fee_rule_id: int,
    rule_name: str,
    percent: Any,
    is_active: bool,
) -> OperationResult:
    name = _clean_name(rule_name, label="수수료 이름", max_length=60)
    ppm = _rate_ppm(percent)
    rule = (
        await session.execute(
            text("""
                SELECT fee_rule_id, scope_code, alliance_id, fixed_code
                FROM settlement_fee_rules
                WHERE fee_rule_id = :fee_rule_id AND guild_id = :guild_id
                FOR UPDATE
            """),
            {"fee_rule_id": fee_rule_id, "guild_id": guild_id},
        )
    ).mappings().one_or_none()
    if rule is None:
        raise SettlementError("수수료 규칙을 찾을 수 없습니다.")
    fixed_code = str(rule["fixed_code"] or "")
    if fixed_code:
        name = FIXED_FEE_NAMES[fixed_code]
        is_active = True
    if is_active:
        other_total = int(
            await session.scalar(
                text("""
                    SELECT COALESCE(SUM(latest.rate_ppm), 0)
                    FROM settlement_fee_rules r
                    JOIN LATERAL (
                        SELECT rate_ppm FROM settlement_fee_rule_versions v
                        WHERE v.fee_rule_id = r.fee_rule_id
                        ORDER BY valid_from DESC, fee_rule_version_id DESC LIMIT 1
                    ) latest ON TRUE
                    WHERE r.guild_id = :guild_id AND r.scope_code = :scope_code
                      AND r.alliance_id IS NOT DISTINCT FROM :alliance_id
                      AND r.fee_rule_id <> :fee_rule_id AND r.is_active IS TRUE
                """),
                {
                    "guild_id": guild_id,
                    "scope_code": rule["scope_code"],
                    "alliance_id": rule["alliance_id"],
                    "fee_rule_id": fee_rule_id,
                },
            )
            or 0
        )
        if other_total + ppm > PPM_BASE:
            raise SettlementError("활성 수수료 합계는 100%를 넘을 수 없습니다.")
    await session.execute(
        text("UPDATE settlement_fee_rules SET is_active = :is_active WHERE fee_rule_id = :fee_rule_id"),
        {"fee_rule_id": fee_rule_id, "is_active": is_active},
    )
    await session.execute(
        text("""
            INSERT INTO settlement_fee_rule_versions (fee_rule_id, rule_name, rate_ppm, valid_from)
            VALUES (:fee_rule_id, :name, :ppm, :now)
        """),
        {"fee_rule_id": fee_rule_id, "name": name, "ppm": ppm, "now": _now()},
    )
    await recalculate_open_settlements(
        session,
        guild_id=guild_id,
        scope_code=int(rule["scope_code"]),
        alliance_id=int(rule["alliance_id"]) if rule["alliance_id"] is not None else None,
    )
    return OperationResult("수수료 규칙을 수정했습니다.", (fee_rule_id,))


async def recalculate_open_settlements(
    session: AsyncSession,
    *,
    guild_id: int,
    scope_code: int,
    alliance_id: int | None,
) -> None:
    if scope_code == 1:
        drop_ids = [
            int(value)
            for value in (
                await session.execute(
                    text("""
                        SELECT d.drop_id
                        FROM settlement_drops d
                        JOIN settlement_drop_sales s ON s.drop_id = d.drop_id AND s.status_code = 1
                        WHERE d.guild_id = :guild_id
                          AND NOT EXISTS (
                              SELECT 1 FROM settlement_payout_objects po
                              WHERE po.drop_id = d.drop_id AND po.status_code <> 0
                          )
                    """),
                    {"guild_id": guild_id},
                )
            ).scalars()
        ]
        for drop_id in drop_ids:
            await session.execute(text("DELETE FROM settlement_payout_objects WHERE drop_id = :drop_id"), {"drop_id": drop_id})
            await _build_alliance_payouts(session, drop_id=drop_id)
        return
    parent_ids = [
        int(value)
        for value in (
            await session.execute(
                text("""
                    SELECT parent.payout_object_id
                    FROM settlement_payout_objects parent
                    JOIN settlement_drops d ON d.drop_id = parent.drop_id
                    WHERE d.guild_id = :guild_id
                      AND parent.object_code = 1
                      AND parent.recipient_alliance_id = :alliance_id
                      AND parent.status_code = 1
                      AND NOT EXISTS (
                          SELECT 1 FROM settlement_payout_objects child
                          WHERE child.parent_payout_object_id = parent.payout_object_id
                            AND child.status_code <> 0
                      )
                """),
                {"guild_id": guild_id, "alliance_id": alliance_id},
            )
        ).scalars()
    ]
    for parent_id in parent_ids:
        await session.execute(
            text("DELETE FROM settlement_payout_objects WHERE parent_payout_object_id = :parent_id"),
            {"parent_id": parent_id},
        )
        await _build_clan_children(session, parent_payout_object_id=parent_id)


async def normalize_all_open_settlements(session: AsyncSession) -> None:
    await session.execute(
        text("""
            DELETE FROM settlement_payout_objects child
            USING settlement_payout_objects parent
            WHERE child.parent_payout_object_id = parent.payout_object_id
              AND parent.object_code = 1
              AND parent.status_code = 0
              AND NOT EXISTS (
                  SELECT 1
                  FROM settlement_payout_objects sibling
                  WHERE sibling.parent_payout_object_id = parent.payout_object_id
                    AND sibling.status_code <> 0
              )
        """)
    )
    guild_ids = [
        int(value)
        for value in (
            await session.execute(text("SELECT guild_id FROM guilds ORDER BY guild_id"))
        ).scalars()
    ]
    for guild_id in guild_ids:
        await recalculate_open_settlements(
            session,
            guild_id=guild_id,
            scope_code=1,
            alliance_id=None,
        )
    mappings = (
        await session.execute(
            text("""
                SELECT DISTINCT guild_id, alliance_id
                FROM guild_alliance_role_mappings
                ORDER BY guild_id, alliance_id
            """)
        )
    ).mappings().all()
    for mapping in mappings:
        await recalculate_open_settlements(
            session,
            guild_id=int(mapping["guild_id"]),
            scope_code=2,
            alliance_id=int(mapping["alliance_id"]),
        )
    await session.execute(
        text("""
            DELETE FROM settlement_fee_rules rule
            WHERE rule.fixed_code IS NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM settlement_fee_rule_versions version
                  JOIN settlement_payout_objects payout
                    ON payout.fee_rule_version_id = version.fee_rule_version_id
                  WHERE version.fee_rule_id = rule.fee_rule_id
              )
              AND EXISTS (
                  SELECT 1
                  FROM settlement_fee_rule_versions latest
                  WHERE latest.fee_rule_version_id = (
                      SELECT candidate.fee_rule_version_id
                      FROM settlement_fee_rule_versions candidate
                      WHERE candidate.fee_rule_id = rule.fee_rule_id
                      ORDER BY candidate.valid_from DESC,
                               candidate.fee_rule_version_id DESC
                      LIMIT 1
                  )
                    AND (
                        (rule.scope_code = 1 AND latest.rule_name = '연합 수수료')
                        OR (rule.scope_code = 2 AND latest.rule_name = '혈비')
                    )
              )
        """)
    )


async def record_bid_purchase(
    session: AsyncSession,
    *,
    guild_id: int,
    item_id: int,
    alliance_id: int,
) -> OperationResult:
    item = (
        await session.execute(
            text("""
                SELECT item_id
                FROM items
                WHERE item_id = :item_id
                  AND guild_id = :guild_id
                  AND status_code = 1
                FOR UPDATE
            """),
            {"item_id": item_id, "guild_id": guild_id},
        )
    ).mappings().one_or_none()
    if item is None:
        raise SettlementError("아이템 관리에서 해당 아이템을 찾을 수 없습니다.")
    mapped = await session.scalar(
        text("""
            SELECT 1
            FROM guild_alliance_role_mappings
            WHERE guild_id = :guild_id AND alliance_id = :alliance_id
        """),
        {"guild_id": guild_id, "alliance_id": alliance_id},
    )
    if mapped is None:
        raise SettlementError("역할 매핑된 혈맹만 구매 기록을 추가할 수 있습니다.")
    purchase_no = int(
        await session.scalar(
            text("""
                SELECT COALESCE(MAX(cycle_no), 0) + 1
                FROM bid_item_results
                WHERE guild_id = :guild_id
                  AND item_id = :item_id
                  AND alliance_id = :alliance_id
            """),
            {
                "guild_id": guild_id,
                "item_id": item_id,
                "alliance_id": alliance_id,
            },
        )
        or 1
    )
    result_id = int(
        await session.scalar(
            text("""
                INSERT INTO bid_item_results (
                    guild_id, item_id, alliance_id, cycle_no,
                    selected_by_discord_id, selected_at, memo, updated_at
                ) VALUES (
                    :guild_id, :item_id, :alliance_id, :purchase_no,
                    NULL, TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS'), NULL, NOW()
                )
                RETURNING result_id
            """),
            {
                "guild_id": guild_id,
                "item_id": item_id,
                "alliance_id": alliance_id,
                "purchase_no": purchase_no,
            },
        )
    )
    await _audit(
        session,
        guild_id=guild_id,
        action_code="bid_status",
        target_id=result_id,
        alliance_id=alliance_id,
        state_code=1,
    )
    return OperationResult("구매 횟수를 1회 추가했습니다.", (item_id, alliance_id))
