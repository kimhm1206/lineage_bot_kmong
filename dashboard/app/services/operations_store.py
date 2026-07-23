from __future__ import annotations

import math
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


PAGE_SIZE = 12
STATUS_LABELS = {0: "미완료", 1: "완료", 2: "귀속"}
STATUS_TONES = {0: "pending", 1: "complete", 2: "forfeited"}


def _money(value: Any) -> str:
    return f"{int(value or 0):,}"


def _percent(value: Any) -> str:
    return f"{int(value or 0) / 10_000:g}%"


def _pagination(total: int, page: int, page_size: int = PAGE_SIZE) -> dict[str, Any]:
    total_pages = max(math.ceil(total / page_size), 1)
    page = min(max(page, 1), total_pages)
    start = max(1, min(page - 2, total_pages - 4))
    end = min(total_pages, start + 4)
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "pages": list(range(start, end + 1)),
        "has_previous": page > 1,
        "has_next": page < total_pages,
        "offset": (page - 1) * page_size,
    }


async def drop_management_page(
    session: AsyncSession,
    *,
    guild_id: int,
    period_days: int,
    query: str,
    status: str,
    page: int,
) -> dict[str, Any]:
    status = status if status in {"all", "pending", "sold"} else "all"
    period_clause = "" if period_days == 0 else "AND d.occurred_at >= EXTRACT(EPOCH FROM NOW() - (:period_days * INTERVAL '1 day'))::BIGINT"
    status_clause = {"all": "", "pending": "AND s.status_code = 0", "sold": "AND s.status_code = 1"}[status]
    search_clause = "AND (v.item_name ILIKE :query OR CAST(d.attendance_id AS TEXT) ILIKE :query)" if query else ""
    params = {
        "guild_id": guild_id,
        "period_days": period_days,
        "query": f"%{query}%",
    }
    base_sql = """
        FROM settlement_drops d
        JOIN catalog_item_versions v ON v.item_version_id = d.item_version_id
        JOIN items i ON i.item_id = v.item_id
        JOIN settlement_drop_sales s ON s.drop_id = d.drop_id
        LEFT JOIN alliances buyer_a ON buyer_a.alliance_id = s.buyer_alliance_id
        LEFT JOIN users buyer_u ON buyer_u.user_id = s.buyer_user_id
    """
    where_sql = f"WHERE d.guild_id = :guild_id {period_clause} {status_clause} {search_clause}"
    total = int(await session.scalar(text(f"SELECT COUNT(*) {base_sql} {where_sql}"), params) or 0)
    pagination = _pagination(total, page)
    rows = [
        dict(row)
        for row in (
            await session.execute(
                text(f"""
                    SELECT d.drop_id, d.attendance_id, d.cash_price_krw,
                           d.adena_market_rate, d.gross_adena, d.occurred_at,
                           v.item_name, i.item_id, i.default_price, s.status_code AS sale_status,
                           s.buyer_alliance_id, s.buyer_user_id,
                           COALESCE(buyer_a.display_name, buyer_a.alliance_name) AS buyer_alliance_name,
                           COALESCE(buyer_u.game_nickname, buyer_u.discord_nickname) AS buyer_user_name,
                           TO_CHAR(TO_TIMESTAMP(d.occurred_at), 'YYYY-MM-DD HH24:MI') AS occurred_at_label,
                           COALESCE(participants.participant_count, 0) AS participant_count,
                           COALESCE(participants.alliance_count, 0) AS alliance_count,
                           COALESCE(payouts.pending_count, 0) AS pending_count,
                           COALESCE(payouts.processed_count, 0) AS processed_count
                    {base_sql}
                    LEFT JOIN LATERAL (
                        SELECT COUNT(*) AS participant_count,
                               COUNT(DISTINCT p.alliance_id)
                                   FILTER (WHERE p.alliance_id IS NOT NULL) AS alliance_count
                        FROM settlement_drop_participants p
                        WHERE p.drop_id = d.drop_id
                    ) participants ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT COUNT(*) FILTER (WHERE po.status_code = 0) AS pending_count,
                               COUNT(*) FILTER (WHERE po.status_code <> 0) AS processed_count
                        FROM settlement_payout_objects po
                        WHERE po.drop_id = d.drop_id
                    ) payouts ON TRUE
                    {where_sql}
                    ORDER BY d.occurred_at DESC, d.drop_id DESC
                    LIMIT :limit OFFSET :offset
                """),
                {**params, "limit": pagination["page_size"], "offset": pagination["offset"]},
            )
        ).mappings().all()
    ]
    drop_ids = [int(row["drop_id"]) for row in rows]
    distribution_by_drop: dict[int, list[dict[str, Any]]] = {drop_id: [] for drop_id in drop_ids}
    excluded_by_drop: dict[int, list[int]] = {drop_id: [] for drop_id in drop_ids}
    if drop_ids:
        details = (
            await session.execute(
                text("""
                    SELECT p.drop_id, p.alliance_id,
                           COALESCE(a.display_name, a.alliance_name, '미분류') AS alliance_name,
                           COUNT(*) AS member_count,
                           EXISTS (
                               SELECT 1 FROM settlement_drop_excluded_alliances x
                               WHERE x.drop_id = p.drop_id AND x.alliance_id = p.alliance_id
                           ) AS is_excluded
                    FROM settlement_drop_participants p
                    LEFT JOIN alliances a ON a.alliance_id = p.alliance_id
                    WHERE p.drop_id = ANY(:drop_ids)
                    GROUP BY p.drop_id, p.alliance_id, a.display_name, a.alliance_name, a.sort_order
                    ORDER BY p.drop_id DESC, COALESCE(a.sort_order, 2147483647), alliance_name
                """),
                {"drop_ids": drop_ids},
            )
        ).mappings().all()
        for detail in details:
            drop_id = int(detail["drop_id"])
            entry = dict(detail)
            entry["member_label"] = f"{int(detail['member_count']):,}명"
            distribution_by_drop[drop_id].append(entry)
            if detail["is_excluded"] and detail["alliance_id"] is not None:
                excluded_by_drop[drop_id].append(int(detail["alliance_id"]))
    for row in rows:
        for key in (
            "drop_id",
            "attendance_id",
            "item_id",
            "default_price",
            "cash_price_krw",
            "adena_market_rate",
            "gross_adena",
            "sale_status",
            "participant_count",
            "alliance_count",
            "pending_count",
            "processed_count",
        ):
            row[key] = int(row[key] or 0)
        if row["sale_status"] == 0 and row["cash_price_krw"] <= 0:
            row["cash_price_krw"] = row["default_price"]
        for key in ("buyer_alliance_id", "buyer_user_id"):
            row[key] = int(row[key]) if row[key] is not None else None
        row["sale_label"] = "판매 완료" if int(row["sale_status"]) == 1 else "판매 대기"
        row["sale_tone"] = "complete" if int(row["sale_status"]) == 1 else "pending"
        row["cash_label"] = f"{_money(row['cash_price_krw'])}원" if int(row["cash_price_krw"] or 0) else "미입력"
        row["gross_label"] = _money(row["gross_adena"]) if int(row["gross_adena"] or 0) else "판매 전"
        row["participants"] = distribution_by_drop[int(row["drop_id"])]
        row["excluded_alliance_ids"] = excluded_by_drop[int(row["drop_id"])]
        row["editor_data"] = {
            "drop_id": row["drop_id"],
            "attendance_id": row["attendance_id"],
            "item_id": row["item_id"],
            "item_name": row["item_name"],
            "cash_price_krw": row["cash_price_krw"],
            "adena_market_rate": row["adena_market_rate"],
            "buyer_alliance_id": row["buyer_alliance_id"],
            "buyer_user_id": row["buyer_user_id"],
            "excluded_alliance_ids": row["excluded_alliance_ids"],
        }

    overview = (
        await session.execute(
            text(f"""
                SELECT COUNT(*) AS drop_count,
                       COUNT(*) FILTER (WHERE s.status_code = 0) AS pending_sales,
                       COUNT(*) FILTER (WHERE s.status_code = 1) AS sold_count,
                       COALESCE(SUM(d.gross_adena) FILTER (WHERE s.status_code = 1), 0) AS sold_adena
                FROM settlement_drops d
                JOIN settlement_drop_sales s ON s.drop_id = d.drop_id
                WHERE d.guild_id = :guild_id {period_clause}
            """),
            params,
        )
    ).mappings().one()
    attendance_options = [
        dict(row)
        for row in (
            await session.execute(
                text("""
                    SELECT s.attendance_id,
                           TO_CHAR(s.started_at::timestamp, 'YYYY-MM-DD HH24:MI') AS started_at_label,
                           COUNT(e.user_id) AS participant_count
                    FROM attendance_sessions s
                    LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
                    WHERE s.guild_id = :guild_id
                    GROUP BY s.attendance_id, s.started_at
                    ORDER BY s.started_at::timestamp DESC
                """),
                {"guild_id": guild_id},
            )
        ).mappings().all()
    ]
    item_options = [
        dict(row)
        for row in (
            await session.execute(
                text("""
                    SELECT item_id, item_name, default_price
                    FROM items
                    WHERE guild_id = :guild_id
                      AND status_code = 1
                    ORDER BY item_name
                """),
                {"guild_id": guild_id},
            )
        ).mappings().all()
    ]
    for item in item_options:
        item["item_id"] = int(item["item_id"])
        item["default_price"] = int(item["default_price"] or 0)
        item["default_price_label"] = f"{_money(item['default_price'])}원" if item["default_price"] else "시세 미설정"
    alliance_options = [
        dict(row)
        for row in (
            await session.execute(
                text("""
                    SELECT DISTINCT a.alliance_id,
                           COALESCE(a.display_name, a.alliance_name) AS alliance_name,
                           COALESCE(a.sort_order, 2147483647) AS sort_order
                    FROM guild_alliance_role_mappings m
                    JOIN alliances a ON a.alliance_id = m.alliance_id
                    WHERE m.guild_id = :guild_id AND a.is_active IS TRUE
                    ORDER BY sort_order, alliance_name
                """),
                {"guild_id": guild_id},
            )
        ).mappings().all()
    ]
    buyer_users = [
        dict(row)
        for row in (
            await session.execute(
                text("""
                    SELECT DISTINCT u.user_id, u.discord_id, u.alliance_id,
                           COALESCE(u.game_nickname, u.discord_nickname) AS display_name,
                           u.discord_nickname AS username
                    FROM users u
                    JOIN attendance_entries e ON e.user_id = u.user_id
                    JOIN attendance_sessions s ON s.attendance_id = e.attendance_id
                    WHERE s.guild_id = :guild_id AND u.is_active IS TRUE
                      AND u.alliance_id IS NOT NULL
                    ORDER BY display_name
                """),
                {"guild_id": guild_id},
            )
        ).mappings().all()
    ]
    return {
        "rows": rows,
        "pagination": pagination,
        "selected_status": status,
        "summary_cards": [
            {"label": "등록 드랍", "value": f"{int(overview['drop_count']):,}", "meta": "선택 기간"},
            {"label": "판매 대기", "value": f"{int(overview['pending_sales']):,}", "meta": "구매 혈맹·아데나 시세 확정 전"},
            {"label": "판매 완료", "value": f"{int(overview['sold_count']):,}", "meta": "분배 계산 생성"},
            {"label": "판매 아데나", "value": _money(overview["sold_adena"]), "meta": "수수료 차감 전"},
        ],
        "attendance_options": attendance_options,
        "item_options": item_options,
        "alliance_options": alliance_options,
        "buyer_users": buyer_users,
    }


def _entity_from_row(row: dict[str, Any], *, entity_type: str) -> dict[str, Any]:
    status = int(row["status_code"])
    return {
        "payout_object_id": int(row["payout_object_id"]),
        "item_name": row["item_name"],
        "attendance_id": int(row["attendance_id"]),
        "occurred_at_label": row["occurred_at_label"],
        "amount": int(row["amount_adena"]),
        "amount_label": _money(row["amount_adena"]),
        "status_code": status,
        "status_label": STATUS_LABELS[status],
        "status_tone": STATUS_TONES[status],
        "entity_type": entity_type,
    }


def _finish_entity(entity: dict[str, Any]) -> None:
    details = entity["details"]
    entity["total_amount"] = sum(row["amount"] for row in details)
    entity["pending_amount"] = sum(row["amount"] for row in details if row["status_code"] == 0)
    entity["complete_amount"] = sum(row["amount"] for row in details if row["status_code"] == 1)
    entity["forfeited_amount"] = sum(row["amount"] for row in details if row["status_code"] == 2)
    entity["pending_count"] = sum(1 for row in details if row["status_code"] == 0)
    entity["complete_count"] = sum(1 for row in details if row["status_code"] == 1)
    entity["forfeited_count"] = sum(1 for row in details if row["status_code"] == 2)
    entity["total_count"] = len(details)
    entity["total_label"] = _money(entity["total_amount"])
    entity["pending_label"] = _money(entity["pending_amount"])
    entity["state"] = "complete" if entity["pending_count"] == 0 else "pending"


async def _active_fee_entities(
    session: AsyncSession,
    *,
    guild_id: int,
    scope_code: int,
    alliance_id: int | None,
    eyebrow: str,
) -> dict[str, dict[str, Any]]:
    alliance_clause = (
        "AND r.alliance_id IS NULL"
        if alliance_id is None
        else "AND r.alliance_id = :alliance_id"
    )
    rows = (
        await session.execute(
            text(f"""
                SELECT r.fee_rule_id, latest.rule_name
                FROM settlement_fee_rules r
                JOIN LATERAL (
                    SELECT v.rule_name
                    FROM settlement_fee_rule_versions v
                    WHERE v.fee_rule_id = r.fee_rule_id
                    ORDER BY v.valid_from DESC, v.fee_rule_version_id DESC
                    LIMIT 1
                ) latest ON TRUE
                WHERE r.guild_id = :guild_id
                  AND r.scope_code = :scope_code
                  AND r.is_active IS TRUE
                  {alliance_clause}
                ORDER BY latest.rule_name, r.fee_rule_id
            """),
            {
                "guild_id": guild_id,
                "scope_code": scope_code,
                "alliance_id": alliance_id,
            },
        )
    ).mappings().all()
    return {
        f"fee:{int(row['fee_rule_id'])}": {
            "key": f"fee:{int(row['fee_rule_id'])}",
            "entity_type": "fee",
            "target_id": int(row["fee_rule_id"]),
            "name": row["rule_name"] or eyebrow,
            "eyebrow": eyebrow,
            "details": [],
        }
        for row in rows
    }


async def alliance_settlement_entities(
    session: AsyncSession,
    *,
    guild_id: int,
    period_days: int,
    query: str,
) -> dict[str, Any]:
    period_clause = "" if period_days == 0 else "AND d.occurred_at >= EXTRACT(EPOCH FROM NOW() - (:period_days * INTERVAL '1 day'))::BIGINT"
    mapped_alliances = (
        await session.execute(
            text("""
                SELECT DISTINCT a.alliance_id,
                       COALESCE(a.display_name, a.alliance_name) AS alliance_name
                FROM guild_alliance_role_mappings mapping
                JOIN alliances a ON a.alliance_id = mapping.alliance_id
                WHERE mapping.guild_id = :guild_id
                  AND a.is_active IS TRUE
                ORDER BY alliance_name
            """),
            {"guild_id": guild_id},
        )
    ).mappings().all()
    fee_entities = await _active_fee_entities(
        session,
        guild_id=guild_id,
        scope_code=1,
        alliance_id=None,
        eyebrow="연합 수수료",
    )
    rows = [
        dict(row)
        for row in (
            await session.execute(
                text(f"""
                    SELECT po.payout_object_id, po.object_code, po.recipient_alliance_id,
                           po.fee_rule_version_id, po.amount_adena, po.status_code,
                           d.attendance_id, v.item_name,
                           TO_CHAR(TO_TIMESTAMP(d.occurred_at), 'MM/DD HH24:MI') AS occurred_at_label,
                           COALESCE(a.display_name, a.alliance_name) AS alliance_name,
                           fr.fee_rule_id, fv.rule_name
                    FROM settlement_payout_objects po
                    JOIN settlement_drops d ON d.drop_id = po.drop_id
                    JOIN settlement_drop_sales s ON s.drop_id = d.drop_id AND s.status_code = 1
                    JOIN catalog_item_versions v ON v.item_version_id = d.item_version_id
                    LEFT JOIN alliances a ON a.alliance_id = po.recipient_alliance_id
                    LEFT JOIN settlement_fee_rule_versions fv ON fv.fee_rule_version_id = po.fee_rule_version_id
                    LEFT JOIN settlement_fee_rules fr ON fr.fee_rule_id = fv.fee_rule_id
                    WHERE d.guild_id = :guild_id {period_clause}
                      AND (po.object_code = 1 OR (po.object_code = 3 AND po.parent_payout_object_id IS NULL))
                      AND po.status_code = 0
                    ORDER BY d.occurred_at DESC, po.payout_object_id DESC
                """),
                {"guild_id": guild_id, "period_days": period_days},
            )
        ).mappings().all()
    ]
    entities: dict[str, dict[str, Any]] = dict(fee_entities)
    entities.update({
        f"alliance:{int(alliance['alliance_id'])}": {
            "key": f"alliance:{int(alliance['alliance_id'])}",
            "entity_type": "alliance",
            "target_id": int(alliance["alliance_id"]),
            "name": alliance["alliance_name"] or "미분류",
            "eyebrow": "혈맹 분배",
            "details": [],
        }
        for alliance in mapped_alliances
    })
    for row in rows:
        if int(row["object_code"]) == 1:
            key = f"alliance:{int(row['recipient_alliance_id'])}"
            entity = entities.setdefault(
                key,
                {
                    "key": key,
                    "entity_type": "alliance",
                    "target_id": int(row["recipient_alliance_id"]),
                    "name": row["alliance_name"] or "미분류",
                    "eyebrow": "혈맹 분배",
                    "details": [],
                },
            )
            detail_type = "alliance"
        else:
            key = f"fee:{int(row['fee_rule_id'])}"
            entity = entities.setdefault(
                key,
                {
                    "key": key,
                    "entity_type": "fee",
                    "target_id": int(row["fee_rule_id"]),
                    "name": row["rule_name"] or "연합 수수료",
                    "eyebrow": "연합 수수료",
                    "details": [],
                },
            )
            detail_type = "fee"
        entity["details"].append(_entity_from_row(row, entity_type=detail_type))
    entity_rows = list(entities.values())
    for entity in entity_rows:
        _finish_entity(entity)
        entity["state"] = "pending" if entity["pending_count"] else "idle"
    if query:
        lowered = query.casefold()
        entity_rows = [
            entity
            for entity in entity_rows
            if lowered in entity["name"].casefold()
            or any(lowered in detail["item_name"].casefold() for detail in entity["details"])
        ]
    entity_rows.sort(key=lambda item: (item["entity_type"] != "fee", item["state"] == "idle", item["name"]))
    pending_total = sum(entity["pending_amount"] for entity in entity_rows)
    return {
        "entities": entity_rows,
        "summary_cards": [
            {"label": "미분배 아데나", "value": _money(pending_total), "meta": "완료 전 금액"},
        ],
    }


async def alliance_settlement_history_page(
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int,
    page: int,
) -> dict[str, Any]:
    params = {
        "guild_id": guild_id,
        "alliance_id": alliance_id,
    }
    alliance_name = await session.scalar(
        text("""
            SELECT COALESCE(a.display_name, a.alliance_name)
            FROM alliances a
            JOIN guild_alliance_role_mappings mapping
              ON mapping.alliance_id = a.alliance_id
             AND mapping.guild_id = :guild_id
            WHERE a.alliance_id = :alliance_id
            LIMIT 1
        """),
        params,
    )
    if alliance_name is None:
        return {
            "alliance_name": "",
            "history": [],
            "pagination": _pagination(0, 1, 30),
            "summary": {"complete_count": 0, "total_amount_label": "0"},
        }
    total_row = (
        await session.execute(
            text("""
                SELECT COUNT(*) AS complete_count,
                       COALESCE(SUM(po.amount_adena), 0) AS total_amount
                FROM settlement_payout_objects po
                JOIN settlement_drops d ON d.drop_id = po.drop_id
                JOIN settlement_drop_sales sale
                  ON sale.drop_id = d.drop_id
                 AND sale.status_code = 1
                WHERE d.guild_id = :guild_id
                  AND po.object_code = 1
                  AND po.recipient_alliance_id = :alliance_id
                  AND po.status_code = 1
            """),
            params,
        )
    ).mappings().one()
    total = int(total_row["complete_count"] or 0)
    pagination = _pagination(total, page, 30)
    rows = [
        dict(row)
        for row in (
            await session.execute(
                text("""
                    SELECT po.payout_object_id, po.amount_adena,
                           d.attendance_id, item.item_name,
                           TO_CHAR(
                               TO_TIMESTAMP(d.occurred_at),
                               'YYYY-MM-DD HH24:MI'
                           ) AS occurred_at_label,
                           TO_CHAR(
                               TO_TIMESTAMP(po.completed_at),
                               'YYYY-MM-DD HH24:MI'
                           ) AS completed_at_label,
                           COUNT(child.payout_object_id) FILTER (
                               WHERE child.status_code <> 0
                           ) AS started_child_count
                    FROM settlement_payout_objects po
                    JOIN settlement_drops d ON d.drop_id = po.drop_id
                    JOIN settlement_drop_sales sale
                      ON sale.drop_id = d.drop_id
                     AND sale.status_code = 1
                    JOIN catalog_item_versions item
                      ON item.item_version_id = d.item_version_id
                    LEFT JOIN settlement_payout_objects child
                      ON child.parent_payout_object_id = po.payout_object_id
                    WHERE d.guild_id = :guild_id
                      AND po.object_code = 1
                      AND po.recipient_alliance_id = :alliance_id
                      AND po.status_code = 1
                    GROUP BY po.payout_object_id, d.attendance_id,
                             d.occurred_at, item.item_name
                    ORDER BY po.completed_at DESC, po.payout_object_id DESC
                    LIMIT :limit OFFSET :offset
                """),
                {
                    **params,
                    "limit": pagination["page_size"],
                    "offset": pagination["offset"],
                },
            )
        ).mappings().all()
    ]
    for row in rows:
        row["payout_object_id"] = int(row["payout_object_id"])
        row["attendance_id"] = int(row["attendance_id"])
        row["amount_adena"] = int(row["amount_adena"])
        row["amount_label"] = _money(row["amount_adena"])
        row["started_child_count"] = int(row["started_child_count"] or 0)
        row["can_cancel"] = row["started_child_count"] == 0
        row["progress_label"] = (
            "완료 취소 가능"
            if row["can_cancel"]
            else "혈맹 분배 진행됨"
        )
    return {
        "alliance_name": str(alliance_name),
        "history": rows,
        "pagination": pagination,
        "summary": {
            "complete_count": total,
            "total_amount_label": _money(total_row["total_amount"]),
        },
    }


async def clan_settlement_entities(
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int,
    period_days: int,
    query: str,
) -> dict[str, Any]:
    period_clause = "" if period_days == 0 else "AND d.occurred_at >= EXTRACT(EPOCH FROM NOW() - (:period_days * INTERVAL '1 day'))::BIGINT"
    entities = await _active_fee_entities(
        session,
        guild_id=guild_id,
        scope_code=2,
        alliance_id=alliance_id,
        eyebrow="내부 수수료",
    )
    rows = [
        dict(row)
        for row in (
            await session.execute(
                text(f"""
                    SELECT po.payout_object_id, po.object_code, po.recipient_user_id,
                           po.fee_rule_version_id, po.amount_adena, po.status_code,
                           d.attendance_id, v.item_name,
                           TO_CHAR(TO_TIMESTAMP(d.occurred_at), 'MM/DD HH24:MI') AS occurred_at_label,
                           COALESCE(u.game_nickname, u.discord_nickname) AS user_name,
                           fr.fee_rule_id, fv.rule_name
                    FROM settlement_payout_objects po
                    JOIN settlement_drops d ON d.drop_id = po.drop_id
                    JOIN settlement_drop_sales s ON s.drop_id = d.drop_id AND s.status_code = 1
                    JOIN catalog_item_versions v ON v.item_version_id = d.item_version_id
                    JOIN settlement_payout_objects parent ON parent.payout_object_id = po.parent_payout_object_id
                    LEFT JOIN users u ON u.user_id = po.recipient_user_id
                    LEFT JOIN settlement_fee_rule_versions fv ON fv.fee_rule_version_id = po.fee_rule_version_id
                    LEFT JOIN settlement_fee_rules fr ON fr.fee_rule_id = fv.fee_rule_id
                    WHERE d.guild_id = :guild_id {period_clause}
                      AND parent.recipient_alliance_id = :alliance_id
                      AND po.object_code IN (2, 3)
                      AND po.status_code = 0
                    ORDER BY d.occurred_at DESC, po.payout_object_id DESC
                """),
                {"guild_id": guild_id, "alliance_id": alliance_id, "period_days": period_days},
            )
        ).mappings().all()
    ]
    for row in rows:
        if int(row["object_code"]) == 2:
            key = f"member:{int(row['recipient_user_id'])}"
            entity = entities.setdefault(
                key,
                {
                    "key": key,
                    "entity_type": "member",
                    "target_id": int(row["recipient_user_id"]),
                    "name": row["user_name"] or "알 수 없는 유저",
                    "eyebrow": "혈맹원",
                    "details": [],
                },
            )
            detail_type = "member"
        else:
            key = f"fee:{int(row['fee_rule_id'])}"
            entity = entities.setdefault(
                key,
                {
                    "key": key,
                    "entity_type": "fee",
                    "target_id": int(row["fee_rule_id"]),
                    "name": row["rule_name"] or "내부 수수료",
                    "eyebrow": "내부 수수료",
                    "details": [],
                },
            )
            detail_type = "fee"
        entity["details"].append(_entity_from_row(row, entity_type=detail_type))
    entity_rows = list(entities.values())
    for entity in entity_rows:
        _finish_entity(entity)
        entity["state"] = "pending" if entity["pending_count"] else "idle"
    if query:
        lowered = query.casefold()
        entity_rows = [
            entity
            for entity in entity_rows
            if lowered in entity["name"].casefold()
            or any(lowered in detail["item_name"].casefold() for detail in entity["details"])
        ]
    entity_rows.sort(key=lambda item: (item["entity_type"] != "fee", item["name"]))
    return {
        "entities": entity_rows,
        "summary_cards": [
            {"label": "미분배 아데나", "value": _money(sum(e["pending_amount"] for e in entity_rows)), "meta": "현재 지급할 금액"},
        ],
    }


async def clan_settlement_history_page(
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int,
    period_days: int,
    query: str,
    status_filter: str,
    page: int,
) -> dict[str, Any]:
    period_clause = (
        ""
        if period_days == 0
        else "AND d.occurred_at >= EXTRACT(EPOCH FROM NOW() - (:period_days * INTERVAL '1 day'))::BIGINT"
    )
    status_clause = {
        "complete": "AND po.status_code = 1",
        "forfeited": "AND po.status_code = 2",
    }.get(status_filter, "AND po.status_code IN (1, 2)")
    search_clause = """
        AND (
            COALESCE(u.game_nickname, u.discord_nickname, fv.rule_name, '') ILIKE :query
            OR v.item_name ILIKE :query
            OR CAST(d.attendance_id AS TEXT) ILIKE :query
        )
    """ if query else ""
    params = {
        "guild_id": guild_id,
        "alliance_id": alliance_id,
        "period_days": period_days,
        "query": f"%{query}%",
    }
    from_sql = f"""
        FROM settlement_payout_objects po
        JOIN settlement_drops d ON d.drop_id = po.drop_id
        JOIN settlement_drop_sales s ON s.drop_id = d.drop_id AND s.status_code = 1
        JOIN catalog_item_versions v ON v.item_version_id = d.item_version_id
        JOIN settlement_payout_objects parent ON parent.payout_object_id = po.parent_payout_object_id
        LEFT JOIN users u ON u.user_id = po.recipient_user_id
        LEFT JOIN settlement_fee_rule_versions fv ON fv.fee_rule_version_id = po.fee_rule_version_id
        WHERE d.guild_id = :guild_id
          AND parent.recipient_alliance_id = :alliance_id
          AND po.object_code IN (2, 3)
          {period_clause}
          {status_clause}
          {search_clause}
    """
    total = int(
        await session.scalar(
            text(f"SELECT COUNT(*) {from_sql}"),
            params,
        )
        or 0
    )
    pagination = _pagination(total, page, 40)
    rows = [
        dict(row)
        for row in (
            await session.execute(
                text(f"""
                    SELECT po.payout_object_id, po.object_code, po.amount_adena,
                           po.status_code, d.attendance_id, v.item_name,
                           COALESCE(
                               u.game_nickname,
                               u.discord_nickname,
                               fv.rule_name,
                               '알 수 없는 대상'
                           ) AS target_name,
                           TO_CHAR(TO_TIMESTAMP(d.occurred_at), 'MM/DD HH24:MI') AS occurred_at_label,
                           TO_CHAR(TO_TIMESTAMP(po.completed_at), 'YYYY-MM-DD HH24:MI') AS completed_at_label
                    {from_sql}
                    ORDER BY po.completed_at DESC, po.payout_object_id DESC
                    LIMIT :limit OFFSET :offset
                """),
                {
                    **params,
                    "limit": pagination["page_size"],
                    "offset": pagination["offset"],
                },
            )
        ).mappings().all()
    ]
    for row in rows:
        status_code = int(row["status_code"])
        row["payout_object_id"] = int(row["payout_object_id"])
        row["attendance_id"] = int(row["attendance_id"])
        row["amount_adena"] = int(row["amount_adena"])
        row["amount_label"] = _money(row["amount_adena"])
        row["status_label"] = STATUS_LABELS[status_code]
        row["status_tone"] = STATUS_TONES[status_code]
        row["target_type"] = "혈맹원" if int(row["object_code"]) == 2 else "내부 수수료"
    summary = (
        await session.execute(
            text(f"""
                SELECT
                    COUNT(*) FILTER (WHERE po.status_code = 1) AS complete_count,
                    COUNT(*) FILTER (WHERE po.status_code = 2) AS forfeited_count,
                    COALESCE(SUM(po.amount_adena) FILTER (WHERE po.status_code IN (1, 2)), 0) AS total_amount
                FROM settlement_payout_objects po
                JOIN settlement_drops d ON d.drop_id = po.drop_id
                JOIN settlement_drop_sales s ON s.drop_id = d.drop_id AND s.status_code = 1
                JOIN settlement_payout_objects parent ON parent.payout_object_id = po.parent_payout_object_id
                WHERE d.guild_id = :guild_id
                  AND parent.recipient_alliance_id = :alliance_id
                  AND po.object_code IN (2, 3)
                  AND po.status_code IN (1, 2)
                  {period_clause}
            """),
            params,
        )
    ).mappings().one()
    return {
        "history": rows,
        "pagination": pagination,
        "summary": {
            "complete_count": int(summary["complete_count"] or 0),
            "forfeited_count": int(summary["forfeited_count"] or 0),
            "total_amount_label": _money(summary["total_amount"]),
        },
    }


async def item_management_page(session: AsyncSession, *, guild_id: int, query: str) -> dict[str, Any]:
    search = "AND item_name ILIKE :query" if query else ""
    rows = [
        dict(row)
        for row in (
            await session.execute(
                text(f"""
                    SELECT item_id, guild_id, item_name, default_price,
                           TO_CHAR(updated_at, 'YYYY-MM-DD HH24:MI') AS updated_at_label
                    FROM items
                    WHERE guild_id = :guild_id
                      AND status_code = 1 {search}
                    ORDER BY item_name
                """),
                {"guild_id": guild_id, "query": f"%{query}%"},
            )
        ).mappings().all()
    ]
    for row in rows:
        row["item_id"] = int(row["item_id"])
        row["guild_id"] = int(row["guild_id"]) if row["guild_id"] is not None else None
        row["default_price"] = int(row["default_price"]) if row["default_price"] is not None else None
        row["price_label"] = f"{_money(row['default_price'])}원" if row["default_price"] is not None else "미설정"
    return {
        "items": rows,
    }


async def fee_management_page(
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int | None,
    scope_code: int,
    query: str,
) -> dict[str, Any]:
    alliance_clause = "r.alliance_id IS NULL" if scope_code == 1 else "r.alliance_id = :alliance_id"
    search = "AND latest.rule_name ILIKE :query" if query else ""
    rows = [
        dict(row)
        for row in (
            await session.execute(
                text(f"""
                    SELECT r.fee_rule_id, r.is_active, r.fixed_code,
                           latest.rule_name, latest.rate_ppm
                    FROM settlement_fee_rules r
                    JOIN LATERAL (
                        SELECT v.rule_name, v.rate_ppm
                        FROM settlement_fee_rule_versions v
                        WHERE v.fee_rule_id = r.fee_rule_id
                        ORDER BY v.valid_from DESC, v.fee_rule_version_id DESC LIMIT 1
                    ) latest ON TRUE
                    WHERE r.guild_id = :guild_id AND r.scope_code = :scope_code
                      AND {alliance_clause} {search}
                    ORDER BY r.is_active DESC, latest.rule_name
                """),
                {
                    "guild_id": guild_id,
                    "alliance_id": alliance_id,
                    "scope_code": scope_code,
                    "query": f"%{query}%",
                },
            )
        ).mappings().all()
    ]
    for row in rows:
        row["fee_rule_id"] = int(row["fee_rule_id"])
        row["rate_ppm"] = int(row["rate_ppm"] or 0)
        row["rate_label"] = _percent(row["rate_ppm"])
        row["is_fixed"] = bool(row["fixed_code"])
    return {"fee_rules": rows}


async def bid_management_page(
    session: AsyncSession,
    *,
    guild_id: int,
    query: str,
    visible_alliance_id: int | None = None,
) -> dict[str, Any]:
    search = "AND i.item_name ILIKE :query" if query else ""
    alliance_filter = "AND a.alliance_id = :visible_alliance_id" if visible_alliance_id is not None else ""
    params = {
        "guild_id": guild_id,
        "query": f"%{query}%",
        "visible_alliance_id": visible_alliance_id,
    }
    alliances = [
        dict(row)
        for row in (
            await session.execute(
                text(f"""
                    SELECT DISTINCT a.alliance_id,
                           COALESCE(a.display_name, a.alliance_name) AS alliance_name,
                           COALESCE(a.sort_order, 2147483647) AS sort_order
                    FROM guild_alliance_role_mappings m
                    JOIN alliances a ON a.alliance_id = m.alliance_id
                    WHERE m.guild_id = :guild_id AND a.is_active IS TRUE
                      {alliance_filter}
                    ORDER BY sort_order, alliance_name
                """),
                params,
            )
        ).mappings().all()
    ]
    items = [
        dict(row)
        for row in (
            await session.execute(
                text(f"""
                    SELECT i.item_id, i.item_name
                    FROM items i
                    WHERE i.guild_id = :guild_id
                      AND i.status_code = 1
                      {search}
                    ORDER BY i.item_name
                """),
                params,
            )
        ).mappings().all()
    ]
    count_filter = "AND r.alliance_id = :visible_alliance_id" if visible_alliance_id is not None else ""
    count_rows = (
        await session.execute(
            text(f"""
                SELECT r.item_id, r.alliance_id, COUNT(*) AS purchase_count
                FROM bid_item_results r
                WHERE r.guild_id = :guild_id
                  {count_filter}
                GROUP BY r.item_id, r.alliance_id
            """),
            params,
        )
    ).mappings().all()
    counts = {
        (int(row["item_id"]), int(row["alliance_id"])): int(row["purchase_count"])
        for row in count_rows
    }
    for alliance in alliances:
        alliance["alliance_id"] = int(alliance["alliance_id"])
    item_rows = []
    for item in items:
        item_id = int(item["item_id"])
        item_rows.append(
            {
                "item_id": item_id,
                "item_name": item["item_name"],
                "alliance_counts": [
                    {
                        "alliance_id": int(alliance["alliance_id"]),
                        "alliance_name": alliance["alliance_name"],
                        "purchase_count": counts.get(
                            (item_id, int(alliance["alliance_id"])),
                            0,
                        ),
                    }
                    for alliance in alliances
                ],
            }
        )
    return {
        "item_rows": item_rows,
        "alliances": alliances,
        "summary_cards": [
            {"label": "아이템", "value": f"{len(item_rows):,}", "meta": "아이템 관리 기준"},
            {"label": "혈맹", "value": f"{len(alliances):,}", "meta": "역할 매핑 기준"},
        ],
    }


async def bid_item_purchase_history(
    session: AsyncSession,
    *,
    guild_id: int,
    item_id: int,
    visible_alliance_id: int | None = None,
) -> dict[str, Any]:
    item = (
        await session.execute(
            text("""
                SELECT item_id, item_name
                FROM items
                WHERE guild_id = :guild_id
                  AND item_id = :item_id
                  AND status_code = 1
            """),
            {"guild_id": guild_id, "item_id": item_id},
        )
    ).mappings().one_or_none()
    if item is None:
        return {"item_name": "", "history": []}
    alliance_filter = (
        "AND r.alliance_id = :visible_alliance_id"
        if visible_alliance_id is not None
        else ""
    )
    rows = [
        dict(row)
        for row in (
            await session.execute(
                text(f"""
                    SELECT r.result_id, r.item_id,
                           COALESCE(a.display_name, a.alliance_name) AS alliance_name,
                           TO_CHAR(
                               TO_TIMESTAMP(r.selected_at, 'YYYY-MM-DD HH24:MI:SS'),
                               'YYYY-MM-DD HH24:MI'
                           ) AS purchased_at,
                           TO_CHAR(
                               TO_TIMESTAMP(r.selected_at, 'YYYY-MM-DD HH24:MI:SS'),
                               'MM월 DD일 HH24:MI'
                           ) AS purchased_at_short
                    FROM bid_item_results r
                    JOIN alliances a ON a.alliance_id = r.alliance_id
                    WHERE r.guild_id = :guild_id
                      AND r.item_id = :item_id
                      {alliance_filter}
                    ORDER BY r.selected_at DESC, r.result_id DESC
                """),
                {
                    "guild_id": guild_id,
                    "item_id": item_id,
                    "visible_alliance_id": visible_alliance_id,
                },
            )
        ).mappings().all()
    ]
    for row in rows:
        row["result_id"] = int(row["result_id"])
        row["item_id"] = int(row["item_id"])
    return {
        "item_name": item["item_name"],
        "history": rows,
    }


async def personal_distribution_page(
    session: AsyncSession,
    *,
    guild_id: int,
    user_id: int | None,
    period_days: int,
) -> dict[str, Any]:
    users = [
        dict(row)
        for row in (
            await session.execute(
                text("""
                    SELECT DISTINCT u.user_id,
                           COALESCE(u.game_nickname, u.discord_nickname) AS user_name,
                           u.alliance_id,
                           COALESCE(a.display_name, a.alliance_name, '미분류') AS alliance_name
                    FROM users u
                    JOIN attendance_entries e ON e.user_id = u.user_id
                    JOIN attendance_sessions s ON s.attendance_id = e.attendance_id
                    LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
                    WHERE s.guild_id = :guild_id AND u.is_active IS TRUE
                    ORDER BY user_name
                """),
                {"guild_id": guild_id},
            )
        ).mappings().all()
    ]
    valid_ids = {int(row["user_id"]) for row in users}
    if user_id not in valid_ids:
        user_id = int(users[0]["user_id"]) if users else None
    selected_user = next((row for row in users if int(row["user_id"]) == user_id), None)
    if user_id is None:
        return {"users": users, "user_id": None, "selected_user": None, "details": [], "summary_cards": []}
    period_clause = "" if period_days == 0 else "AND d.occurred_at >= EXTRACT(EPOCH FROM NOW() - (:period_days * INTERVAL '1 day'))::BIGINT"
    details = [
        dict(row)
        for row in (
            await session.execute(
                text(f"""
                    SELECT po.payout_object_id, po.amount_adena, po.status_code,
                           d.attendance_id, v.item_name,
                           TO_CHAR(TO_TIMESTAMP(d.occurred_at), 'YYYY-MM-DD HH24:MI') AS occurred_at_label
                    FROM settlement_payout_objects po
                    JOIN settlement_drops d ON d.drop_id = po.drop_id
                    JOIN settlement_drop_sales s ON s.drop_id = d.drop_id AND s.status_code = 1
                    JOIN catalog_item_versions v ON v.item_version_id = d.item_version_id
                    WHERE d.guild_id = :guild_id AND po.object_code = 2
                      AND po.recipient_user_id = :user_id {period_clause}
                    ORDER BY d.occurred_at DESC, po.payout_object_id DESC
                """),
                {"guild_id": guild_id, "user_id": user_id, "period_days": period_days},
            )
        ).mappings().all()
    ]
    for row in details:
        status = int(row["status_code"])
        row["amount_label"] = _money(row["amount_adena"])
        row["status_label"] = STATUS_LABELS[status]
        row["status_tone"] = STATUS_TONES[status]
    pending = sum(int(row["amount_adena"]) for row in details if int(row["status_code"]) == 0)
    complete = sum(int(row["amount_adena"]) for row in details if int(row["status_code"]) == 1)
    forfeited = sum(int(row["amount_adena"]) for row in details if int(row["status_code"]) == 2)
    return {
        "users": users,
        "user_id": user_id,
        "selected_user": selected_user,
        "details": details,
        "summary_cards": [
            {"label": "기간 내 총 분배금", "value": _money(pending + complete + forfeited), "meta": f"{len(details):,}건"},
            {"label": "미수령", "value": _money(pending), "meta": f"{sum(1 for r in details if int(r['status_code']) == 0):,}건"},
            {"label": "수령 완료", "value": _money(complete), "meta": f"{sum(1 for r in details if int(r['status_code']) == 1):,}건"},
            {"label": "혈비 귀속", "value": _money(forfeited), "meta": f"{sum(1 for r in details if int(r['status_code']) == 2):,}건"},
        ],
    }
