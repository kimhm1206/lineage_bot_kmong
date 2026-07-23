from __future__ import annotations

import math
import time
from collections.abc import Sequence
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.services import settings_store


PAGE_SIZE = 20
ATTENDANCE_PAGE_SIZE = 10
PERIOD_OPTIONS = (
    {"value": 7, "label": "최근 7일"},
    {"value": 30, "label": "최근 한 달"},
    {"value": 0, "label": "전체 기간"},
)

STATUS_LABELS = {0: "미완료", 1: "완료", 2: "귀속"}
STATUS_TONES = {0: "warning", 1: "success", 2: "muted"}

ACTION_LABELS = {
    "attendance_add": "출석 인원 추가",
    "attendance_delete": "출석 인원 삭제",
    "item_create": "아이템 등록",
    "item_update": "아이템 수정",
    "item_delete": "아이템 삭제",
    "loot_create": "드랍 등록",
    "loot_update": "드랍 수정",
    "loot_delete": "드랍 삭제",
    "sale_complete": "판매 완료",
    "sale_update": "판매 정보 수정",
    "sale_reopen": "판매 대기로 변경",
    "bid_item": "입찰 아이템 등록",
    "bid_item_delete": "입찰 아이템 삭제",
    "bid_status": "입찰 구매 기록 추가",
    "payout_status": "분배 상태 변경",
    "treasury_deposit": "가계부 입금",
    "treasury_withdrawal": "가계부 출금",
    "treasury_reversal": "가계부 기록 취소",
}

ROLE_LABELS = {
    1: "Developer",
    2: "Owner",
    3: "Alliance manager",
    4: "Clan manager",
    5: "Clan accountant",
}


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _money(value: Any) -> str:
    return f"{_int(value):,}"


def _percent(rate_ppm: Any) -> str:
    rate = _int(rate_ppm) / 10_000
    return f"{rate:g}%"


def _period_clause(column: str, period_days: int, *, unix: bool) -> str:
    if period_days <= 0:
        return ""
    if unix:
        return f" AND {column} >= EXTRACT(EPOCH FROM NOW() - (:period_days * INTERVAL '1 day'))::BIGINT"
    return (
        f" AND {column} >= TO_CHAR("
        "(NOW() AT TIME ZONE 'Asia/Seoul') - (:period_days * INTERVAL '1 day'), "
        "'YYYY-MM-DD HH24:MI:SS')"
    )


def _pagination(page: int, total: int, page_size: int = PAGE_SIZE) -> dict[str, Any]:
    total_pages = max(1, math.ceil(total / page_size))
    current = min(max(page, 1), total_pages)
    start = max(1, current - 2)
    end = min(total_pages, start + 4)
    start = max(1, end - 4)
    return {
        "page": current,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "pages": list(range(start, end + 1)),
        "has_previous": current > 1,
        "has_next": current < total_pages,
    }


async def _fetch_page(
    session: AsyncSession,
    *,
    count_sql: str,
    rows_sql: str,
    params: dict[str, Any],
    page: int,
    page_size: int = PAGE_SIZE,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    total = _int(await session.scalar(text(count_sql), params))
    pagination = _pagination(page, total, page_size)
    query_params = {
        **params,
        "limit": page_size,
        "offset": (pagination["page"] - 1) * page_size,
    }
    rows = (await session.execute(text(rows_sql), query_params)).mappings().all()
    return [dict(row) for row in rows], pagination


async def resolve_workspace(
    session: AsyncSession,
    requested_guild_id: int | None,
    requested_alliance_id: int | None,
    allowed_guild_ids: Sequence[int] | None = None,
) -> dict[str, Any]:
    guild_rows = await settings_store.list_guilds(session)
    if allowed_guild_ids is not None:
        allowed = {int(value) for value in allowed_guild_ids}
        guild_rows = [row for row in guild_rows if int(row["guild_id"]) in allowed]
    enabled = [row for row in guild_rows if row["is_enabled"]]
    visible_guilds = enabled or guild_rows
    known_guild_ids = {row["guild_id"] for row in visible_guilds}
    guild_id = requested_guild_id if requested_guild_id in known_guild_ids else None
    if guild_id is None and visible_guilds:
        guild_id = visible_guilds[0]["guild_id"]

    guilds = [
        {
            **row,
            "name": row.get("guild_name") or f"서버 {row['guild_id']}",
        }
        for row in visible_guilds
    ]
    alliances = await settings_store.list_guild_alliances(session, guild_id) if guild_id else []
    known_alliance_ids = {row["alliance_id"] for row in alliances}
    alliance_id = requested_alliance_id if requested_alliance_id in known_alliance_ids else None
    if alliance_id is None and alliances:
        alliance_id = alliances[0]["alliance_id"]

    return {
        "guilds": guilds,
        "guild_id": guild_id,
        "selected_guild": next((row for row in guilds if row["guild_id"] == guild_id), None),
        "alliances": alliances,
        "alliance_id": alliance_id,
        "selected_alliance": next((row for row in alliances if row["alliance_id"] == alliance_id), None),
    }


async def attendance_sessions_page(
    session: AsyncSession,
    *,
    guild_id: int,
    period_days: int,
    query: str,
    page: int,
    date_from: str = "",
    date_to: str = "",
) -> dict[str, Any]:
    period = _period_clause("s.started_at", period_days, unix=False)
    date_filter = ""
    if date_from:
        date_filter += " AND s.started_at::timestamp >= CAST(:date_from AS VARCHAR)::DATE"
    if date_to:
        date_filter += " AND s.started_at::timestamp < CAST(:date_to AS VARCHAR)::DATE + INTERVAL '1 day'"
    search = """
        AND (
            CAST(s.attendance_id AS TEXT) ILIKE :query
            OR COALESCE(starter.discord_nickname, '') ILIKE :query
            OR EXISTS (
                SELECT 1
                FROM attendance_entries search_entry
                JOIN users search_user ON search_user.user_id = search_entry.user_id
                LEFT JOIN alliances search_alliance ON search_alliance.alliance_id = search_user.alliance_id
                WHERE search_entry.attendance_id = s.attendance_id
                  AND (
                      search_user.discord_nickname ILIKE :query
                      OR COALESCE(search_alliance.display_name, search_alliance.alliance_name, '') ILIKE :query
                  )
            )
        )
    """ if query else ""
    params = {
        "guild_id": guild_id,
        "period_days": period_days,
        "query": f"%{query}%",
        "date_from": date_from or None,
        "date_to": date_to or None,
    }
    count_from_sql = f"""
        FROM attendance_sessions s
        LEFT JOIN users starter ON starter.discord_id = s.started_by_discord_id
        WHERE s.guild_id = :guild_id {period} {date_filter} {search}
    """
    rows, pagination = await _fetch_page(
        session,
        count_sql=f"SELECT COUNT(*) {count_from_sql}",
        rows_sql=f"""
            SELECT s.attendance_id,
                   TO_CHAR(s.started_at::timestamp, 'YYYY-MM-DD HH24:MI') AS started_at_label,
                   COALESCE(starter.discord_nickname, CAST(s.started_by_discord_id AS TEXT), '-') AS started_by,
                   COUNT(e.user_id) AS participant_count,
                   COUNT(DISTINCT au.alliance_id) FILTER (WHERE au.alliance_id IS NOT NULL) AS alliance_count
            FROM attendance_sessions s
            LEFT JOIN users starter ON starter.discord_id = s.started_by_discord_id
            LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
            LEFT JOIN users au ON au.user_id = e.user_id
            WHERE s.guild_id = :guild_id {period} {date_filter} {search}
            GROUP BY s.attendance_id, s.started_at, starter.discord_nickname, s.started_by_discord_id
            ORDER BY s.started_at::timestamp DESC
            LIMIT :limit OFFSET :offset
        """,
        params=params,
        page=page,
        page_size=ATTENDANCE_PAGE_SIZE,
    )

    session_map = {int(row["attendance_id"]): row for row in rows}
    for row in rows:
        row["alliances"] = []
    if session_map:
        detail_params: dict[str, Any] = {}
        placeholders: list[str] = []
        for index, attendance_id in enumerate(session_map):
            key = f"attendance_id_{index}"
            detail_params[key] = attendance_id
            placeholders.append(f":{key}")
        detail_rows = (await session.execute(text(f"""
            SELECT e.attendance_id, u.user_id, u.discord_id,
                   COALESCE(u.game_nickname, u.discord_nickname) AS discord_nickname,
                   a.alliance_id,
                   COALESCE(a.display_name, a.alliance_name, '미분류') AS alliance_name,
                   COALESCE(a.sort_order, 2147483647) AS alliance_sort
            FROM attendance_entries e
            JOIN users u ON u.user_id = e.user_id
            LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
            WHERE e.attendance_id IN ({', '.join(placeholders)})
            ORDER BY e.attendance_id DESC, alliance_sort, alliance_name, u.discord_nickname
        """), detail_params)).mappings().all()
        alliance_maps: dict[int, dict[int | None, dict[str, Any]]] = {
            attendance_id: {} for attendance_id in session_map
        }
        for detail in detail_rows:
            attendance_id = int(detail["attendance_id"])
            alliance_id = int(detail["alliance_id"]) if detail["alliance_id"] is not None else None
            alliance = alliance_maps[attendance_id].setdefault(
                alliance_id,
                {
                    "alliance_id": alliance_id,
                    "alliance_name": str(detail["alliance_name"]),
                    "members": [],
                    "count": 0,
                },
            )
            alliance["members"].append(
                {
                    "user_id": int(detail["user_id"]),
                    "discord_id": str(detail["discord_id"]),
                    "discord_nickname": str(detail["discord_nickname"]),
                }
            )
            alliance["count"] += 1
        for attendance_id, alliances in alliance_maps.items():
            session_map[attendance_id]["alliances"] = list(alliances.values())

    for row in rows:
        row["attendance_label"] = f"#{row['attendance_id']}"
        row["participant_label"] = f"{_int(row['participant_count']):,}명"
        row["alliance_label"] = f"{_int(row['alliance_count']):,}개 혈맹"
    return {
        "sessions": rows,
        "pagination": pagination,
    }


async def attendance_member_options(
    session: AsyncSession,
    *,
    guild_id: int,
) -> list[dict[str, Any]]:
    rows = (
        await session.execute(
            text("""
                SELECT DISTINCT
                       u.user_id,
                       u.discord_id,
                       COALESCE(u.game_nickname, u.discord_nickname) AS display_name,
                       u.discord_nickname,
                       u.alliance_id,
                       COALESCE(a.display_name, a.alliance_name, '미분류') AS alliance_name,
                       COALESCE(a.sort_order, 2147483647) AS alliance_sort
                FROM users u
                LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
                WHERE u.is_active IS TRUE
                  AND (
                      EXISTS (
                          SELECT 1
                          FROM guild_alliance_role_mappings mapping
                          WHERE mapping.guild_id = :guild_id
                            AND mapping.alliance_id = u.alliance_id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM attendance_entries entry
                          JOIN attendance_sessions attendance
                            ON attendance.attendance_id = entry.attendance_id
                          WHERE entry.user_id = u.user_id
                            AND attendance.guild_id = :guild_id
                      )
                  )
                ORDER BY alliance_sort, alliance_name, display_name, u.user_id
            """),
            {"guild_id": guild_id},
        )
    ).mappings().all()
    return [
        {
            "user_id": int(row["user_id"]),
            "discord_id": str(row["discord_id"]),
            "display_name": str(row["display_name"]),
            "username": str(row["alliance_name"]),
            "alliance_id": int(row["alliance_id"]) if row["alliance_id"] is not None else None,
        }
        for row in rows
    ]


async def attendance_session_detail(
    session: AsyncSession,
    *,
    guild_id: int,
    attendance_id: int,
) -> dict[str, Any] | None:
    attendance = (
        await session.execute(
            text("""
                SELECT s.attendance_id,
                       TO_CHAR(s.started_at::timestamp, 'YYYY-MM-DD HH24:MI') AS started_at_label,
                       COALESCE(starter.discord_nickname, CAST(s.started_by_discord_id AS TEXT), '-') AS started_by
                FROM attendance_sessions s
                LEFT JOIN users starter ON starter.discord_id = s.started_by_discord_id
                WHERE s.guild_id = :guild_id
                  AND s.attendance_id = :attendance_id
            """),
            {"guild_id": guild_id, "attendance_id": attendance_id},
        )
    ).mappings().one_or_none()
    if attendance is None:
        return None

    member_rows = (
        await session.execute(
            text("""
                SELECT u.user_id, u.discord_id,
                       COALESCE(u.game_nickname, u.discord_nickname) AS discord_nickname,
                       a.alliance_id,
                       COALESCE(a.display_name, a.alliance_name, '미분류') AS alliance_name,
                       COALESCE(a.sort_order, 2147483647) AS alliance_sort
                FROM attendance_entries e
                JOIN users u ON u.user_id = e.user_id
                LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
                WHERE e.attendance_id = :attendance_id
                ORDER BY alliance_sort, alliance_name, discord_nickname, u.user_id
            """),
            {"attendance_id": attendance_id},
        )
    ).mappings().all()
    alliances: dict[int | None, dict[str, Any]] = {}
    for member in member_rows:
        alliance_id = int(member["alliance_id"]) if member["alliance_id"] is not None else None
        alliance = alliances.setdefault(
            alliance_id,
            {
                "alliance_id": alliance_id,
                "alliance_name": str(member["alliance_name"]),
                "members": [],
                "count": 0,
            },
        )
        alliance["members"].append(
            {
                "user_id": int(member["user_id"]),
                "discord_id": str(member["discord_id"]),
                "discord_nickname": str(member["discord_nickname"]),
            }
        )
        alliance["count"] += 1

    result = dict(attendance)
    result["participant_count"] = len(member_rows)
    result["participant_label"] = f"{len(member_rows):,}명"
    result["alliances"] = list(alliances.values())
    return result


async def attendance_statistics_page(
    session: AsyncSession,
    *,
    guild_id: int,
    period_days: int,
    query: str,
    page: int,
    alliance_id: int | None = None,
) -> dict[str, Any]:
    period = _period_clause("s.started_at", period_days, unix=False)
    search = " AND COALESCE(u.game_nickname, u.discord_nickname) ILIKE :query" if query else ""
    alliance_filter = " AND u.alliance_id = :alliance_id" if alliance_id is not None else ""
    params = {
        "guild_id": guild_id,
        "period_days": period_days,
        "query": f"%{query}%",
        "alliance_id": alliance_id,
    }
    from_sql = f"""
        FROM attendance_entries e
        JOIN attendance_sessions s ON s.attendance_id = e.attendance_id
        JOIN users u ON u.user_id = e.user_id
        LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
        WHERE s.guild_id = :guild_id {period} {alliance_filter} {search}
    """
    count_sql = f"SELECT COUNT(*) FROM (SELECT u.user_id {from_sql} GROUP BY u.user_id) ranked"
    rows, pagination = await _fetch_page(
        session,
        count_sql=count_sql,
        rows_sql=f"""
            SELECT u.user_id,
                   COALESCE(u.game_nickname, u.discord_nickname) AS user_name,
                   COALESCE(a.display_name, a.alliance_name, '미분류') AS alliance_name,
                   COUNT(*) AS attendance_count,
                   TO_CHAR(MAX(s.started_at::timestamp), 'YYYY-MM-DD HH24:MI') AS last_attendance
            {from_sql}
            GROUP BY u.user_id, u.game_nickname, u.discord_nickname, a.display_name, a.alliance_name
            ORDER BY attendance_count DESC, user_name
            LIMIT :limit OFFSET :offset
        """,
        params=params,
        page=page,
    )
    max_count = _int(await session.scalar(text(f"""
        SELECT COALESCE(MAX(attendance_count), 0)
        FROM (
            SELECT COUNT(*) AS attendance_count
            {from_sql}
            GROUP BY u.user_id
        ) ranked
    """), params))
    for index, row in enumerate(rows, start=(pagination["page"] - 1) * PAGE_SIZE + 1):
        row["rank"] = f"{index}위"
        row["attendance_label"] = f"{_int(row['attendance_count']):,}회"
        row["share"] = round((_int(row["attendance_count"]) / max_count * 100), 1) if max_count else 0

    session_count = _int(await session.scalar(text(f"""
        SELECT COUNT(*) FROM attendance_sessions s
        WHERE s.guild_id = :guild_id {period}
    """), params))
    overview = (await session.execute(text(f"""
        SELECT COUNT(*) AS attendance_count,
               COUNT(DISTINCT e.user_id) AS unique_user_count
        {from_sql}
    """), params)).mappings().one()
    attendance_count = _int(overview["attendance_count"])

    daily_stats = [dict(row) for row in (await session.execute(text(f"""
        SELECT DATE(s.started_at::timestamp)::TEXT AS attendance_date,
               COUNT(DISTINCT s.attendance_id) AS session_count,
               COUNT(*) AS attendance_count,
               COUNT(DISTINCT e.user_id) AS unique_user_count
        {from_sql}
        GROUP BY DATE(s.started_at::timestamp)
        ORDER BY DATE(s.started_at::timestamp) DESC
        LIMIT 31
    """), params)).mappings().all()]
    alliance_stats = [dict(row) for row in (await session.execute(text(f"""
        SELECT COALESCE(a.display_name, a.alliance_name, '미분류') AS alliance_name,
               COUNT(DISTINCT s.attendance_id) AS session_count,
               COUNT(*) AS attendance_count,
               COUNT(DISTINCT e.user_id) AS unique_user_count
        {from_sql}
        GROUP BY a.alliance_id, a.display_name, a.alliance_name, a.sort_order
        ORDER BY attendance_count DESC, COALESCE(a.sort_order, 2147483647), alliance_name
    """), params)).mappings().all()]
    hour_stats = [dict(row) for row in (await session.execute(text(f"""
        SELECT EXTRACT(HOUR FROM s.started_at::timestamp)::INTEGER AS hour,
               COUNT(DISTINCT s.attendance_id) AS session_count,
               COUNT(*) AS attendance_count,
               ROUND(COUNT(*)::NUMERIC / NULLIF(COUNT(DISTINCT s.attendance_id), 0), 1) AS average_count
        {from_sql}
        GROUP BY EXTRACT(HOUR FROM s.started_at::timestamp)
        ORDER BY attendance_count DESC, hour
        LIMIT 12
    """), params)).mappings().all()]
    for row in hour_stats:
        row["hour_label"] = f"{_int(row['hour']):02d}:00"
        row["average_label"] = f"평균 {float(row['average_count'] or 0):.1f}명"
    return {
        "summary_cards": [
            {"label": "기간 내 회차", "value": f"{session_count:,}", "meta": "최근 한 달 기본"},
            {"label": "누적 출석", "value": f"{attendance_count:,}", "meta": "필터 조건 기준"},
            {"label": "참여 인원", "value": f"{_int(overview['unique_user_count']):,}", "meta": "고유 유저"},
            {"label": "평균 참여", "value": f"{(attendance_count / session_count):.1f}명" if session_count else "0명", "meta": "회차당 참여"},
        ],
        "user_rankings": rows,
        "daily_stats": daily_stats,
        "alliance_stats": alliance_stats,
        "hour_stats": hour_stats,
        "pagination": pagination,
    }


async def attendance_statistics_export_rows(
    session: AsyncSession,
    *,
    guild_id: int,
    period_days: int,
    query: str,
    alliance_id: int | None = None,
) -> list[dict[str, Any]]:
    period = _period_clause("s.started_at", period_days, unix=False)
    search = " AND COALESCE(u.game_nickname, u.discord_nickname) ILIKE :query" if query else ""
    alliance_filter = " AND u.alliance_id = :alliance_id" if alliance_id is not None else ""
    params = {
        "guild_id": guild_id,
        "period_days": period_days,
        "query": f"%{query}%",
        "alliance_id": alliance_id,
    }
    rows = (await session.execute(text(f"""
        SELECT COALESCE(u.game_nickname, u.discord_nickname) AS user_name,
               COALESCE(a.display_name, a.alliance_name, '미분류') AS alliance_name,
               COUNT(*) AS attendance_count,
               TO_CHAR(MIN(s.started_at::timestamp), 'YYYY-MM-DD HH24:MI') AS first_attendance,
               TO_CHAR(MAX(s.started_at::timestamp), 'YYYY-MM-DD HH24:MI') AS last_attendance
        FROM attendance_entries e
        JOIN attendance_sessions s ON s.attendance_id = e.attendance_id
        JOIN users u ON u.user_id = e.user_id
        LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
        WHERE s.guild_id = :guild_id {period} {alliance_filter} {search}
        GROUP BY u.user_id, u.game_nickname, u.discord_nickname, a.display_name, a.alliance_name
        ORDER BY attendance_count DESC, user_name
    """), params)).mappings().all()
    return [dict(row) for row in rows]


async def clan_attendance_page(
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int,
    period_days: int,
    query: str,
    page: int,
) -> dict[str, Any]:
    period = _period_clause("s.started_at", period_days, unix=False)
    search = " AND COALESCE(u.game_nickname, u.discord_nickname) ILIKE :query" if query else ""
    params = {
        "guild_id": guild_id,
        "alliance_id": alliance_id,
        "period_days": period_days,
        "query": f"%{query}%",
    }
    from_sql = f"""
        FROM users u
        LEFT JOIN attendance_entries e ON e.user_id = u.user_id
        LEFT JOIN attendance_sessions s ON s.attendance_id = e.attendance_id AND s.guild_id = :guild_id {period}
        WHERE u.alliance_id = :alliance_id AND u.is_active IS TRUE {search}
    """
    rows, pagination = await _fetch_page(
        session,
        count_sql=f"SELECT COUNT(*) FROM users u WHERE u.alliance_id = :alliance_id AND u.is_active IS TRUE {search}",
        rows_sql=f"""
            SELECT u.user_id,
                   COALESCE(u.game_nickname, u.discord_nickname) AS user_name,
                   COUNT(s.attendance_id) AS attendance_count,
                   TO_CHAR(MAX(s.started_at::timestamp), 'YYYY-MM-DD HH24:MI') AS last_attendance
            {from_sql}
            GROUP BY u.user_id, u.game_nickname, u.discord_nickname
            ORDER BY attendance_count DESC, user_name
            LIMIT :limit OFFSET :offset
        """,
        params=params,
        page=page,
    )
    total_sessions = _int(await session.scalar(text(f"""
        SELECT COUNT(*) FROM attendance_sessions s
        WHERE s.guild_id = :guild_id {period}
    """), params))
    for index, row in enumerate(rows, start=(pagination["page"] - 1) * PAGE_SIZE + 1):
        count = _int(row["attendance_count"])
        row["rank"] = f"{index}위"
        row["attendance_label"] = f"{count:,}회"
        row["rate_label"] = f"{(count / total_sessions * 100):.1f}%" if total_sessions else "0%"
        row["rate"] = round(count / total_sessions * 100, 1) if total_sessions else 0
        row["last_attendance"] = row["last_attendance"] or "기록 없음"

    overview = (await session.execute(text(f"""
        SELECT COUNT(DISTINCT s.attendance_id) AS alliance_session_count,
               COUNT(s.attendance_id) AS attendance_count,
               COUNT(DISTINCT u.user_id) FILTER (WHERE s.attendance_id IS NOT NULL) AS participating_users
        {from_sql}
    """), params)).mappings().one()
    alliance_session_count = _int(overview["alliance_session_count"])
    attendance_count = _int(overview["attendance_count"])

    hour_stats = [dict(row) for row in (await session.execute(text(f"""
        SELECT EXTRACT(HOUR FROM s.started_at::timestamp)::INTEGER AS hour,
               COUNT(DISTINCT s.attendance_id) AS session_count,
               COUNT(*) AS attendance_count,
               ROUND(COUNT(*)::NUMERIC / NULLIF(COUNT(DISTINCT s.attendance_id), 0), 1) AS average_count
        FROM attendance_entries e
        JOIN attendance_sessions s ON s.attendance_id = e.attendance_id
        JOIN users u ON u.user_id = e.user_id
        WHERE s.guild_id = :guild_id AND u.alliance_id = :alliance_id {period}
        GROUP BY EXTRACT(HOUR FROM s.started_at::timestamp)
        ORDER BY attendance_count DESC, hour
        LIMIT 10
    """), params)).mappings().all()]
    for row in hour_stats:
        row["label"] = f"{_int(row['hour']):02d}:00"

    weekday_labels = {1: "월", 2: "화", 3: "수", 4: "목", 5: "금", 6: "토", 7: "일"}
    weekday_stats = [dict(row) for row in (await session.execute(text(f"""
        SELECT EXTRACT(ISODOW FROM s.started_at::timestamp)::INTEGER AS weekday,
               COUNT(DISTINCT s.attendance_id) AS session_count,
               COUNT(*) AS attendance_count,
               ROUND(COUNT(*)::NUMERIC / NULLIF(COUNT(DISTINCT s.attendance_id), 0), 1) AS average_count
        FROM attendance_entries e
        JOIN attendance_sessions s ON s.attendance_id = e.attendance_id
        JOIN users u ON u.user_id = e.user_id
        WHERE s.guild_id = :guild_id AND u.alliance_id = :alliance_id {period}
        GROUP BY EXTRACT(ISODOW FROM s.started_at::timestamp)
        ORDER BY weekday
    """), params)).mappings().all()]
    for row in weekday_stats:
        row["label"] = weekday_labels.get(_int(row["weekday"]), "-")

    daily_rows = [dict(row) for row in (await session.execute(text(f"""
        SELECT DATE(s.started_at::timestamp)::TEXT AS attendance_date,
               COUNT(DISTINCT s.attendance_id) AS session_count,
               COUNT(*) AS attendance_count,
               COUNT(DISTINCT e.user_id) AS unique_user_count
        FROM attendance_entries e
        JOIN attendance_sessions s ON s.attendance_id = e.attendance_id
        JOIN users u ON u.user_id = e.user_id
        WHERE s.guild_id = :guild_id AND u.alliance_id = :alliance_id {period}
        GROUP BY DATE(s.started_at::timestamp)
        ORDER BY DATE(s.started_at::timestamp) DESC
        LIMIT 31
    """), params)).mappings().all()]

    async def current_rankings(date_clause: str) -> list[dict[str, Any]]:
        ranking_rows = [dict(row) for row in (await session.execute(text(f"""
            SELECT COALESCE(u.game_nickname, u.discord_nickname) AS user_name,
                   COUNT(*) AS attendance_count
            FROM attendance_entries e
            JOIN attendance_sessions s ON s.attendance_id = e.attendance_id
            JOIN users u ON u.user_id = e.user_id
            WHERE s.guild_id = :guild_id
              AND u.alliance_id = :alliance_id
              AND {date_clause}
            GROUP BY u.user_id, u.game_nickname, u.discord_nickname
            ORDER BY attendance_count DESC, user_name
            LIMIT 10
        """), params)).mappings().all()]
        for index, ranking in enumerate(ranking_rows, start=1):
            ranking["rank"] = index
        return ranking_rows

    weekly_rankings = await current_rankings(
        "s.started_at::timestamp >= DATE_TRUNC('week', NOW() AT TIME ZONE 'Asia/Seoul')"
    )
    monthly_rankings = await current_rankings(
        "s.started_at::timestamp >= DATE_TRUNC('month', NOW() AT TIME ZONE 'Asia/Seoul')"
    )
    return {
        "summary_cards": [
            {"label": "혈맹원", "value": f"{pagination['total']:,}", "meta": "활성 유저"},
            {"label": "혈맹 참여 회차", "value": f"{alliance_session_count:,}", "meta": f"전체 {total_sessions:,}회"},
            {"label": "참여 회차율", "value": f"{(alliance_session_count / total_sessions * 100):.1f}%" if total_sessions else "0%", "meta": "서버 전체 회차 기준"},
            {"label": "평균 인원", "value": f"{(attendance_count / alliance_session_count):.1f}명" if alliance_session_count else "0명", "meta": f"누적 {attendance_count:,}명"},
        ],
        "user_rankings": rows,
        "hour_stats": hour_stats,
        "weekday_stats": weekday_stats,
        "daily_rows": daily_rows,
        "weekly_rankings": weekly_rankings,
        "monthly_rankings": monthly_rankings,
        "pagination": pagination,
    }


async def alliance_drops_page(
    session: AsyncSession,
    *, guild_id: int, period_days: int, query: str, page: int
) -> dict[str, Any]:
    period = _period_clause("d.occurred_at", period_days, unix=True)
    search = " AND v.item_name ILIKE :query" if query else ""
    params = {"guild_id": guild_id, "period_days": period_days, "query": f"%{query}%"}
    count_from_sql = f"""
        FROM settlement_drops d
        JOIN catalog_item_versions v ON v.item_version_id = d.item_version_id
        WHERE d.guild_id = :guild_id {period} {search}
    """
    rows, pagination = await _fetch_page(
        session,
        count_sql=f"SELECT COUNT(*) {count_from_sql}",
        rows_sql=f"""
            SELECT d.drop_id, v.item_name, d.attendance_id, d.gross_adena, d.cash_price_krw,
                   TO_CHAR(TO_TIMESTAMP(d.occurred_at), 'YYYY-MM-DD HH24:MI') AS occurred_at_label,
                   COUNT(DISTINCT p.user_id) AS participant_count,
                   COUNT(DISTINCT p.alliance_id) FILTER (WHERE p.alliance_id IS NOT NULL) AS alliance_count,
                   COUNT(po.payout_object_id) FILTER (WHERE po.status_code = 0) AS pending_count
            FROM settlement_drops d
            JOIN catalog_item_versions v ON v.item_version_id = d.item_version_id
            LEFT JOIN settlement_drop_participants p ON p.drop_id = d.drop_id
            LEFT JOIN settlement_payout_objects po ON po.drop_id = d.drop_id AND po.object_code = 1
            WHERE d.guild_id = :guild_id {period} {search}
            GROUP BY d.drop_id, v.item_name
            ORDER BY d.occurred_at DESC, d.drop_id DESC
            LIMIT :limit OFFSET :offset
        """,
        params=params,
        page=page,
    )
    for row in rows:
        row["attendance_label"] = f"#{row['attendance_id']}"
        row["gross_adena_label"] = _money(row["gross_adena"])
        row["participant_label"] = f"{_int(row['participant_count']):,}명"
        row["state"] = "정산 중" if _int(row["pending_count"]) else "완료"
        row["state_tone"] = "warning" if _int(row["pending_count"]) else "success"
    totals = (await session.execute(text(f"""
        SELECT COUNT(*) AS drop_count, COALESCE(SUM(d.gross_adena), 0) AS gross_total,
               COALESCE(SUM(d.cash_price_krw), 0) AS cash_total
        {count_from_sql}
    """), params)).mappings().one()
    return {
        "summary_cards": [
            {"label": "드랍", "value": f"{_int(totals['drop_count']):,}건", "meta": "선택 기간"},
            {"label": "총 판매 아데나", "value": _money(totals["gross_total"]), "meta": "수수료 차감 전"},
            {"label": "현금 기록", "value": f"{_money(totals['cash_total'])}원", "meta": "등록 원화"},
        ],
        "columns": [
            {"key": "item_name", "label": "아이템", "emphasis": True},
            {"key": "attendance_label", "label": "출석"},
            {"key": "occurred_at_label", "label": "드랍 시각"},
            {"key": "participant_label", "label": "참여"},
            {"key": "gross_adena_label", "label": "판매 아데나", "numeric": True},
            {"key": "state", "label": "정산", "status_key": "state_tone"},
        ],
        "rows": rows,
        "pagination": pagination,
    }


async def alliance_settlements_page(
    session: AsyncSession,
    *, guild_id: int, period_days: int, query: str, page: int
) -> dict[str, Any]:
    period = _period_clause("d.occurred_at", period_days, unix=True)
    search = " AND (v.item_name ILIKE :query OR COALESCE(a.display_name, a.alliance_name, '') ILIKE :query)" if query else ""
    params = {"guild_id": guild_id, "period_days": period_days, "query": f"%{query}%"}
    from_sql = f"""
        FROM settlement_payout_objects po
        JOIN settlement_drops d ON d.drop_id = po.drop_id
        JOIN catalog_item_versions v ON v.item_version_id = d.item_version_id
        LEFT JOIN alliances a ON a.alliance_id = po.recipient_alliance_id
        WHERE d.guild_id = :guild_id AND po.object_code = 1 {period} {search}
    """
    rows, pagination = await _fetch_page(
        session,
        count_sql=f"SELECT COUNT(*) {from_sql}",
        rows_sql=f"""
            SELECT po.payout_object_id, v.item_name,
                   COALESCE(a.display_name, a.alliance_name, '미분류') AS alliance_name,
                   po.amount_adena, po.status_code,
                   TO_CHAR(TO_TIMESTAMP(d.occurred_at), 'YYYY-MM-DD HH24:MI') AS occurred_at_label
            {from_sql}
            ORDER BY d.occurred_at DESC, po.payout_object_id DESC
            LIMIT :limit OFFSET :offset
        """,
        params=params,
        page=page,
    )
    for row in rows:
        row["amount_label"] = _money(row["amount_adena"])
        row["state"] = STATUS_LABELS.get(_int(row["status_code"]), "확인 필요")
        row["state_tone"] = STATUS_TONES.get(_int(row["status_code"]), "muted")
    totals = (await session.execute(text(f"""
        SELECT COALESCE(SUM(po.amount_adena), 0) AS total,
               COALESCE(SUM(po.amount_adena) FILTER (WHERE po.status_code = 0), 0) AS pending,
               COUNT(*) FILTER (WHERE po.status_code = 1) AS completed_count
        {from_sql}
    """), params)).mappings().one()
    return {
        "summary_cards": [
            {"label": "혈맹 분배금", "value": _money(totals["total"]), "meta": "전체 대상"},
            {"label": "미분배", "value": _money(totals["pending"]), "meta": "완료 전"},
            {"label": "완료", "value": f"{_int(totals['completed_count']):,}건", "meta": "혈맹별 지급"},
        ],
        "columns": [
            {"key": "item_name", "label": "아이템", "emphasis": True},
            {"key": "alliance_name", "label": "혈맹"},
            {"key": "occurred_at_label", "label": "발생 시각"},
            {"key": "amount_label", "label": "분배 아데나", "numeric": True},
            {"key": "state", "label": "상태", "status_key": "state_tone"},
        ],
        "rows": rows,
        "pagination": pagination,
    }


async def items_page(
    session: AsyncSession,
    *, guild_id: int, query: str, page: int
) -> dict[str, Any]:
    search = " AND i.item_name ILIKE :query" if query else ""
    params = {"guild_id": guild_id, "query": f"%{query}%"}
    scope = "WHERE i.guild_id = :guild_id AND i.status_code = 1"
    rows, pagination = await _fetch_page(
        session,
        count_sql=f"SELECT COUNT(*) FROM items i {scope} {search}",
        rows_sql=f"""
            SELECT i.item_id, i.item_name, i.default_price,
                   TO_CHAR(i.updated_at, 'YYYY-MM-DD HH24:MI') AS updated_at_label
            FROM items i {scope} {search}
            ORDER BY i.item_name
            LIMIT :limit OFFSET :offset
        """,
        params=params,
        page=page,
    )
    for row in rows:
        row["price_label"] = f"{_money(row['default_price'])}원" if row["default_price"] is not None else "미설정"
    return {
        "summary_cards": [],
        "columns": [
            {"key": "item_name", "label": "아이템", "emphasis": True},
            {"key": "price_label", "label": "기본 원화 시세", "numeric": True},
            {"key": "updated_at_label", "label": "수정 시각"},
        ],
        "rows": rows,
        "pagination": pagination,
    }


async def fee_rules_page(
    session: AsyncSession,
    *, guild_id: int, alliance_id: int | None, scope_code: int, query: str, page: int
) -> dict[str, Any]:
    alliance_filter = " AND r.alliance_id = :alliance_id" if scope_code == 2 else " AND r.alliance_id IS NULL"
    search = " AND latest.rule_name ILIKE :query" if query else ""
    params = {"guild_id": guild_id, "alliance_id": alliance_id, "scope_code": scope_code, "query": f"%{query}%"}
    from_sql = f"""
        FROM settlement_fee_rules r
        JOIN LATERAL (
            SELECT v.rule_name, v.rate_ppm, v.valid_from
            FROM settlement_fee_rule_versions v
            WHERE v.fee_rule_id = r.fee_rule_id
            ORDER BY v.valid_from DESC, v.fee_rule_version_id DESC
            LIMIT 1
        ) latest ON TRUE
        WHERE r.guild_id = :guild_id AND r.scope_code = :scope_code {alliance_filter} {search}
    """
    rows, pagination = await _fetch_page(
        session,
        count_sql=f"SELECT COUNT(*) {from_sql}",
        rows_sql=f"""
            SELECT r.fee_rule_id, latest.rule_name, latest.rate_ppm, r.is_active,
                   TO_CHAR(TO_TIMESTAMP(latest.valid_from), 'YYYY-MM-DD HH24:MI') AS valid_from_label
            {from_sql}
            ORDER BY r.is_active DESC, latest.rule_name
            LIMIT :limit OFFSET :offset
        """,
        params=params,
        page=page,
    )
    for row in rows:
        row["rate_label"] = _percent(row["rate_ppm"])
        row["state"] = "사용" if row["is_active"] else "중지"
        row["state_tone"] = "success" if row["is_active"] else "muted"
    total_rate = sum(_int(row["rate_ppm"]) for row in rows if row["is_active"])
    return {
        "summary_cards": [
            {"label": "수수료 규칙", "value": f"{pagination['total']:,}개", "meta": "현재 범위"},
            {"label": "활성 규칙", "value": f"{sum(1 for r in rows if r['is_active']):,}개", "meta": "현재 페이지"},
            {"label": "합산 비율", "value": _percent(total_rate), "meta": "현재 페이지 활성 규칙"},
        ],
        "columns": [
            {"key": "rule_name", "label": "수수료", "emphasis": True},
            {"key": "rate_label", "label": "비율"},
            {"key": "valid_from_label", "label": "적용 시각"},
            {"key": "state", "label": "상태", "status_key": "state_tone"},
        ],
        "rows": rows,
        "pagination": pagination,
    }


async def clan_settlements_page(
    session: AsyncSession,
    *, guild_id: int, alliance_id: int, period_days: int, query: str, page: int
) -> dict[str, Any]:
    period = _period_clause("d.occurred_at", period_days, unix=True)
    search = " AND (v.item_name ILIKE :query OR COALESCE(u.game_nickname, u.discord_nickname, fv.rule_name, '') ILIKE :query)" if query else ""
    params = {"guild_id": guild_id, "alliance_id": alliance_id, "period_days": period_days, "query": f"%{query}%"}
    from_sql = f"""
        FROM settlement_payout_objects po
        JOIN settlement_drops d ON d.drop_id = po.drop_id
        JOIN catalog_item_versions v ON v.item_version_id = d.item_version_id
        LEFT JOIN users u ON u.user_id = po.recipient_user_id
        LEFT JOIN settlement_fee_rule_versions fv ON fv.fee_rule_version_id = po.fee_rule_version_id
        LEFT JOIN settlement_fee_rules fr ON fr.fee_rule_id = fv.fee_rule_id
        LEFT JOIN settlement_payout_objects parent_po ON parent_po.payout_object_id = po.parent_payout_object_id
        WHERE d.guild_id = :guild_id
          AND po.object_code IN (2, 3)
          AND COALESCE(parent_po.recipient_alliance_id, fr.alliance_id, u.alliance_id) = :alliance_id
          {period} {search}
    """
    rows, pagination = await _fetch_page(
        session,
        count_sql=f"SELECT COUNT(*) {from_sql}",
        rows_sql=f"""
            SELECT po.payout_object_id, v.item_name,
                   CASE WHEN po.object_code = 3 THEN fv.rule_name
                        ELSE COALESCE(u.game_nickname, u.discord_nickname, '알 수 없음') END AS recipient_name,
                   CASE WHEN po.object_code = 3 THEN '수수료' ELSE '혈맹원' END AS object_type,
                   po.amount_adena, po.status_code,
                   TO_CHAR(TO_TIMESTAMP(d.occurred_at), 'YYYY-MM-DD HH24:MI') AS occurred_at_label
            {from_sql}
            ORDER BY d.occurred_at DESC, po.payout_object_id DESC
            LIMIT :limit OFFSET :offset
        """,
        params=params,
        page=page,
    )
    for row in rows:
        row["amount_label"] = _money(row["amount_adena"])
        row["state"] = STATUS_LABELS.get(_int(row["status_code"]), "확인 필요")
        row["state_tone"] = STATUS_TONES.get(_int(row["status_code"]), "muted")
    totals = (await session.execute(text(f"""
        SELECT COALESCE(SUM(po.amount_adena), 0) AS total,
               COALESCE(SUM(po.amount_adena) FILTER (WHERE po.status_code = 0), 0) AS pending,
               COUNT(*) FILTER (WHERE po.status_code = 2) AS forfeited_count
        {from_sql}
    """), params)).mappings().one()
    return {
        "summary_cards": [
            {"label": "분배 대상", "value": _money(totals["total"]), "meta": "혈맹원 및 수수료"},
            {"label": "미분배", "value": _money(totals["pending"]), "meta": "정산 대기"},
            {"label": "귀속", "value": f"{_int(totals['forfeited_count']):,}건", "meta": "혈비 전환"},
        ],
        "columns": [
            {"key": "recipient_name", "label": "대상", "emphasis": True},
            {"key": "object_type", "label": "구분"},
            {"key": "item_name", "label": "아이템"},
            {"key": "occurred_at_label", "label": "발생 시각"},
            {"key": "amount_label", "label": "분배 아데나", "numeric": True},
            {"key": "state", "label": "상태", "status_key": "state_tone"},
        ],
        "rows": rows,
        "pagination": pagination,
    }


async def treasury_page(
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int | None,
    account_scope_code: int,
    period_days: int,
    query: str,
    page: int,
    include_distribution_users: bool = False,
    ledger_date: str = "",
    direction_filter: int = 0,
    category_filter_active: bool = False,
    category_ids: Sequence[int] | None = None,
) -> dict[str, Any]:
    if account_scope_code not in {1, 2}:
        raise ValueError("지원하지 않는 가계부 범위입니다.")
    if account_scope_code == 2 and alliance_id is None:
        raise ValueError("혈맹 가계부에는 혈맹 선택이 필요합니다.")

    period = _period_clause("e.occurred_at", period_days, unix=True)
    search = " AND (COALESCE(c.category_name, '') ILIKE :query OR COALESCE(e.memo, '') ILIKE :query)" if query else ""
    date_filter = (
        " AND DATE(TO_TIMESTAMP(e.occurred_at) AT TIME ZONE 'Asia/Seoul') = TO_DATE(:ledger_date, 'YYYY-MM-DD')"
        if ledger_date
        else ""
    )
    normalized_direction = direction_filter if direction_filter in {-1, 1} else 0
    direction_clause = " AND e.direction = :direction_filter" if normalized_direction else ""
    normalized_category_ids = sorted(
        {
            _int(category_id)
            for category_id in (category_ids or [])
            if _int(category_id) > 0
        }
    )
    category_params = {
        f"category_id_{index}": category_id
        for index, category_id in enumerate(normalized_category_ids)
    }
    category_clause = (
        " AND e.treasury_category_id IN ("
        + ", ".join(f":{name}" for name in category_params)
        + ")"
        if category_params
        else (" AND FALSE" if category_filter_active else "")
    )
    scope_filter = (
        "a.account_scope_code = 1 AND a.alliance_id IS NULL"
        if account_scope_code == 1
        else "a.account_scope_code = 2 AND a.alliance_id = :alliance_id"
    )
    params = {
        "guild_id": guild_id,
        "alliance_id": alliance_id,
        "account_scope_code": account_scope_code,
        "period_days": period_days,
        "query": f"%{query}%",
        "ledger_date": ledger_date,
        "direction_filter": normalized_direction,
        **category_params,
    }
    from_sql = f"""
        FROM treasury_entries e
        JOIN treasury_accounts a ON a.treasury_account_id = e.treasury_account_id
        LEFT JOIN treasury_categories c ON c.treasury_category_id = e.treasury_category_id
        LEFT JOIN treasury_source_types st ON st.source_type_id = e.source_type_id
        WHERE a.guild_id = :guild_id AND {scope_filter}
              {period} {search} {date_filter} {direction_clause} {category_clause}
    """
    rows, pagination = await _fetch_page(
        session,
        count_sql=f"SELECT COUNT(*) {from_sql}",
        rows_sql=f"""
            SELECT e.treasury_entry_id, e.direction,
                   COALESCE(c.category_name, st.source_code, '기타') AS category_name,
                   e.amount_adena, e.balance_after, COALESCE(e.memo, '-') AS memo,
                   TO_CHAR(TO_TIMESTAMP(e.occurred_at) AT TIME ZONE 'Asia/Seoul', 'YYYY-MM-DD HH24:MI') AS occurred_at_label
            {from_sql}
            ORDER BY e.occurred_at DESC, e.treasury_entry_id DESC
            LIMIT :limit OFFSET :offset
        """,
        params=params,
        page=page,
    )
    for row in rows:
        is_income = _int(row["direction"]) == 1
        row["direction_label"] = "입금" if is_income else "출금"
        row["direction_tone"] = "success" if is_income else "warning"
        row["amount_label"] = ("+" if is_income else "-") + _money(row["amount_adena"])
        row["balance_label"] = _money(row["balance_after"])
    account = (await session.execute(text(f"""
        SELECT treasury_account_id, current_balance FROM treasury_accounts a
        WHERE a.guild_id = :guild_id AND {scope_filter}
    """), params)).mappings().first()
    totals = (await session.execute(text(f"""
        SELECT COALESCE(SUM(e.amount_adena) FILTER (WHERE e.direction = 1), 0) AS income,
               COALESCE(SUM(e.amount_adena) FILTER (WHERE e.direction = -1), 0) AS expense
        {from_sql}
    """), params)).mappings().one()
    categories = await list_treasury_categories(session, guild_id, account_scope_code)
    account_label = "연합비" if account_scope_code == 1 else "혈비"
    eligible_users: list[dict[str, Any]] = []
    eligible_alliances: list[dict[str, Any]] = []
    if include_distribution_users:
        if account_scope_code == 1:
            eligible_alliances = [
                {
                    "alliance_id": _int(row["alliance_id"]),
                    "display_name": str(row["alliance_name"]),
                    "member_count": _int(row["member_count"]),
                }
                for row in (
                    await session.execute(
                        text("""
                            SELECT a.alliance_id,
                                   COALESCE(a.display_name, a.alliance_name) AS alliance_name,
                                   COUNT(DISTINCT u.user_id) FILTER (
                                       WHERE u.is_active IS TRUE
                                   ) AS member_count
                            FROM guild_alliance_role_mappings mapping
                            JOIN alliances a
                              ON a.alliance_id = mapping.alliance_id
                            LEFT JOIN users u
                              ON u.alliance_id = a.alliance_id
                            WHERE mapping.guild_id = :guild_id
                              AND a.is_active IS TRUE
                            GROUP BY a.alliance_id
                            ORDER BY COALESCE(a.sort_order, 2147483647),
                                     alliance_name, a.alliance_id
                        """),
                        params,
                    )
                ).mappings().all()
            ]
        else:
            eligible_users = [
                {
                    **dict(row),
                    "user_id": _int(row["user_id"]),
                    "discord_id": _int(row["discord_id"]),
                    "display_name": str(row["display_name"]),
                    "username": str(row["alliance_name"]),
                }
                for row in (
                    await session.execute(
                        text("""
                            SELECT u.user_id, u.discord_id,
                                   COALESCE(u.game_nickname, u.discord_nickname) AS display_name,
                                   COALESCE(a.display_name, a.alliance_name, '미분류') AS alliance_name
                            FROM users u
                            LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
                            WHERE u.is_active IS TRUE
                              AND u.alliance_id = :alliance_id
                              AND EXISTS (
                                  SELECT 1
                                  FROM attendance_entries ae
                                  JOIN attendance_sessions ats
                                    ON ats.attendance_id = ae.attendance_id
                                  WHERE ae.user_id = u.user_id
                                    AND ats.guild_id = :guild_id
                              )
                            ORDER BY display_name, u.user_id
                        """),
                        params,
                    )
                ).mappings().all()
            ]
    distribution_rows: list[dict[str, Any]] = []
    if account:
        distribution_rows = [
            dict(row)
            for row in (
                await session.execute(
                    text("""
                        SELECT d.treasury_distribution_id, d.requested_amount,
                               d.per_recipient_amount, d.distributed_amount,
                               d.recipient_count, COALESCE(d.memo, '') AS memo,
                               TO_CHAR(
                                   TO_TIMESTAMP(d.created_at) AT TIME ZONE 'Asia/Seoul',
                                   'YYYY-MM-DD HH24:MI'
                               ) AS created_at_label,
                               COUNT(*) FILTER (WHERE r.status_code = 0) AS pending_count,
                               COUNT(*) FILTER (WHERE r.status_code = 1) AS completed_count
                        FROM treasury_distributions d
                        JOIN treasury_distribution_recipients r
                          ON r.treasury_distribution_id = d.treasury_distribution_id
                        WHERE d.treasury_account_id = :account_id
                        GROUP BY d.treasury_distribution_id
                        ORDER BY d.created_at DESC, d.treasury_distribution_id DESC
                        LIMIT 12
                    """),
                    {"account_id": account["treasury_account_id"]},
                )
            ).mappings().all()
        ]
    distribution_map = {
        _int(row["treasury_distribution_id"]): row for row in distribution_rows
    }
    for row in distribution_rows:
        row["recipients"] = []
        row["distributed_amount_label"] = _money(row["distributed_amount"])
        row["per_recipient_amount_label"] = _money(row["per_recipient_amount"])
        row["is_complete"] = _int(row["pending_count"]) == 0
    if distribution_map:
        distribution_ids = list(distribution_map)
        recipient_rows = (
            await session.execute(
                text("""
                    SELECT r.treasury_distribution_id,
                           r.treasury_distribution_recipient_id,
                           r.user_id, r.alliance_id, r.status_code,
                           COALESCE(
                               u.game_nickname,
                               u.discord_nickname,
                               target_alliance.display_name,
                               target_alliance.alliance_name,
                               '알 수 없는 대상'
                           ) AS recipient_name,
                           COALESCE(
                               target_alliance.display_name,
                               target_alliance.alliance_name,
                               user_alliance.display_name,
                               user_alliance.alliance_name,
                               '미분류'
                           ) AS alliance_name
                    FROM treasury_distribution_recipients r
                    LEFT JOIN users u ON u.user_id = r.user_id
                    LEFT JOIN alliances user_alliance
                      ON user_alliance.alliance_id = u.alliance_id
                    LEFT JOIN alliances target_alliance
                      ON target_alliance.alliance_id = r.alliance_id
                    WHERE r.treasury_distribution_id = ANY(:distribution_ids)
                    ORDER BY r.status_code, alliance_name, recipient_name,
                             r.treasury_distribution_recipient_id
                """),
                {"distribution_ids": distribution_ids},
            )
        ).mappings().all()
        for recipient in recipient_rows:
            distribution_map[_int(recipient["treasury_distribution_id"])]["recipients"].append(
                {
                    **dict(recipient),
                    "treasury_distribution_recipient_id": _int(
                        recipient["treasury_distribution_recipient_id"]
                    ),
                    "user_id": (
                        _int(recipient["user_id"])
                        if recipient["user_id"] is not None
                        else None
                    ),
                    "alliance_id": (
                        _int(recipient["alliance_id"])
                        if recipient["alliance_id"] is not None
                        else None
                    ),
                    "status_code": _int(recipient["status_code"]),
                    "status_label": {
                        0: "미지급",
                        1: "지급 완료",
                        2: "혈비 귀속",
                    }.get(_int(recipient["status_code"]), "확인 필요"),
                    "status_tone": {
                        0: "warning",
                        1: "success",
                        2: "muted",
                    }.get(_int(recipient["status_code"]), "muted"),
                }
            )
    pending_distribution_amount = 0
    if account:
        pending_distribution_amount = _int(
            await session.scalar(
                text("""
                    SELECT COALESCE(SUM(d.per_recipient_amount), 0)
                    FROM treasury_distribution_recipients r
                    JOIN treasury_distributions d
                      ON d.treasury_distribution_id = r.treasury_distribution_id
                    WHERE d.treasury_account_id = :account_id
                      AND r.status_code = 0
                """),
                {"account_id": account["treasury_account_id"]},
            )
        )
    return {
        "summary_cards": [
            {"label": f"현재 {account_label}", "value": _money(account["current_balance"] if account else 0), "meta": "가계부 잔액"},
            {"label": "기간 입금", "value": _money(totals["income"]), "meta": "선택 기간"},
            {"label": "기간 출금", "value": _money(totals["expense"]), "meta": "선택 기간"},
            {"label": "지급 대기", "value": _money(pending_distribution_amount), "meta": "공금 분배 미지급액"},
        ],
        "columns": [
            {"key": "occurred_at_label", "label": "시각", "filter_kind": "date"},
            {"key": "direction_label", "label": "구분", "status_key": "direction_tone", "filter_kind": "direction"},
            {"key": "category_name", "label": "항목", "emphasis": True, "filter_kind": "category"},
            {"key": "memo", "label": "내용"},
            {"key": "amount_label", "label": "금액", "numeric": True},
            {"key": "balance_label", "label": "잔액", "numeric": True},
        ],
        "rows": rows,
        "pagination": pagination,
        "treasury_categories": categories,
        "treasury_scope_code": account_scope_code,
        "treasury_balance": _int(account["current_balance"]) if account else 0,
        "treasury_account_label": account_label,
        "treasury_distribution_users": eligible_users,
        "treasury_distribution_alliances": eligible_alliances,
        "treasury_distributions": distribution_rows,
        "treasury_filters": {
            "ledger_date": ledger_date,
            "direction": normalized_direction,
            "category_ids": normalized_category_ids,
            "category_active": category_filter_active,
            "has_filters": bool(
                ledger_date
                or normalized_direction
                or category_filter_active
            ),
        },
        "treasury_filter_categories": [
            category
            for direction_categories in categories.values()
            for category in direction_categories
        ],
    }


async def list_treasury_categories(
    session: AsyncSession,
    guild_id: int,
    account_scope_code: int,
) -> dict[int, list[dict[str, Any]]]:
    rows = (await session.execute(
        text("""
            SELECT treasury_category_id, direction, category_name
            FROM treasury_categories
            WHERE guild_id = :guild_id
              AND account_scope_code = :account_scope_code
              AND is_active = TRUE
              AND direction IN (-1, 1)
            ORDER BY direction DESC, category_name, treasury_category_id
        """),
        {"guild_id": guild_id, "account_scope_code": account_scope_code},
    )).mappings().all()
    categories: dict[int, list[dict[str, Any]]] = {1: [], -1: []}
    for row in rows:
        categories[_int(row["direction"])].append(dict(row))
    return categories


async def record_treasury_entry(
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int | None,
    account_scope_code: int,
    treasury_category_id: int,
    direction: int,
    amount_adena: int,
    occurred_at: int,
    memo: str,
    created_by_user_id: int | None = None,
) -> int:
    if account_scope_code not in {1, 2}:
        raise ValueError("지원하지 않는 가계부 범위입니다.")
    if account_scope_code == 1:
        alliance_id = None
    elif alliance_id is None:
        raise ValueError("혈맹을 선택해 주세요.")
    if direction not in {-1, 1}:
        raise ValueError("입금 또는 출금을 선택해 주세요.")
    if amount_adena <= 0:
        raise ValueError("금액은 1 아데나 이상 입력해 주세요.")
    if occurred_at <= 0:
        raise ValueError("거래 시각을 확인해 주세요.")

    clean_memo = memo.strip()[:500]
    try:
        if account_scope_code == 2:
            alliance_exists = await session.scalar(
                text("""
                    SELECT 1
                    FROM guild_alliance_role_mappings
                    WHERE guild_id = :guild_id AND alliance_id = :alliance_id
                    LIMIT 1
                """),
                {"guild_id": guild_id, "alliance_id": alliance_id},
            )
            if not alliance_exists:
                raise ValueError("선택한 서버의 혈맹을 확인해 주세요.")

        category = (await session.execute(
            text("""
                SELECT treasury_category_id, direction
                FROM treasury_categories
                WHERE treasury_category_id = :treasury_category_id
                  AND guild_id = :guild_id
                  AND account_scope_code = :account_scope_code
                  AND is_active = TRUE
            """),
            {
                "treasury_category_id": treasury_category_id,
                "guild_id": guild_id,
                "account_scope_code": account_scope_code,
            },
        )).mappings().first()
        if category is None or _int(category["direction"]) != direction:
            raise ValueError("입출금 구분과 항목을 다시 확인해 주세요.")

        now = int(time.time())
        if account_scope_code == 1:
            await session.execute(
                text("""
                    INSERT INTO treasury_accounts (
                        guild_id, alliance_id, account_scope_code, current_balance, updated_at
                    ) VALUES (:guild_id, NULL, 1, 0, :updated_at)
                    ON CONFLICT (guild_id) WHERE account_scope_code = 1 DO NOTHING
                """),
                {"guild_id": guild_id, "updated_at": now},
            )
            account_filter = "account_scope_code = 1 AND alliance_id IS NULL"
        else:
            await session.execute(
                text("""
                    INSERT INTO treasury_accounts (
                        guild_id, alliance_id, account_scope_code, current_balance, updated_at
                    ) VALUES (:guild_id, :alliance_id, 2, 0, :updated_at)
                    ON CONFLICT (guild_id, alliance_id) WHERE account_scope_code = 2 DO NOTHING
                """),
                {"guild_id": guild_id, "alliance_id": alliance_id, "updated_at": now},
            )
            account_filter = "account_scope_code = 2 AND alliance_id = :alliance_id"

        account = (await session.execute(
            text(f"""
                SELECT treasury_account_id, current_balance
                FROM treasury_accounts
                WHERE guild_id = :guild_id AND {account_filter}
                FOR UPDATE
            """),
            {"guild_id": guild_id, "alliance_id": alliance_id},
        )).mappings().one()
        balance_after = _int(account["current_balance"]) + (direction * amount_adena)
        source_type_id = await session.scalar(
            text("SELECT source_type_id FROM treasury_source_types WHERE source_code = 'manual'")
        )
        if source_type_id is None:
            raise RuntimeError("수동 가계부 원본 유형이 없습니다.")

        treasury_entry_id = await session.scalar(
            text("""
                INSERT INTO treasury_entries (
                    treasury_account_id, treasury_category_id, direction,
                    amount_adena, balance_after, source_type_id, source_id,
                    memo, occurred_at, created_at, created_by_user_id,
                    reversal_of_entry_id
                ) VALUES (
                    :treasury_account_id, :treasury_category_id, :direction,
                    :amount_adena, :balance_after, :source_type_id, NULL,
                    :memo, :occurred_at, :created_at, :created_by_user_id,
                    NULL
                )
                RETURNING treasury_entry_id
            """),
            {
                "treasury_account_id": account["treasury_account_id"],
                "treasury_category_id": treasury_category_id,
                "direction": direction,
                "amount_adena": amount_adena,
                "balance_after": balance_after,
                "source_type_id": source_type_id,
                "memo": clean_memo or None,
                "occurred_at": occurred_at,
                "created_at": now,
                "created_by_user_id": created_by_user_id,
            },
        )
        await session.execute(
            text("""
                UPDATE treasury_accounts
                SET current_balance = :current_balance, updated_at = :updated_at
                WHERE treasury_account_id = :treasury_account_id
            """),
            {
                "current_balance": balance_after,
                "updated_at": now,
                "treasury_account_id": account["treasury_account_id"],
            },
        )
        await session.commit()
        return _int(treasury_entry_id)
    except Exception:
        await session.rollback()
        raise


async def create_treasury_distribution(
    session: AsyncSession,
    *,
    guild_id: int,
    alliance_id: int | None,
    account_scope_code: int,
    requested_amount: int,
    excluded_recipient_ids: Sequence[int],
    memo: str,
    created_by_user_id: int | None = None,
) -> int:
    if account_scope_code not in {1, 2}:
        raise ValueError("지원하지 않는 가계부 범위입니다.")
    if account_scope_code == 1:
        alliance_id = None
    elif alliance_id is None:
        raise ValueError("혈맹을 선택해 주세요.")
    if requested_amount <= 0:
        raise ValueError("분배 금액은 1 아데나 이상 입력해 주세요.")

    try:
        scope_filter = (
            "account_scope_code = 1 AND alliance_id IS NULL"
            if account_scope_code == 1
            else "account_scope_code = 2 AND alliance_id = :alliance_id"
        )
        params = {"guild_id": guild_id, "alliance_id": alliance_id}
        account = (
            await session.execute(
                text(f"""
                    SELECT treasury_account_id, current_balance
                    FROM treasury_accounts
                    WHERE guild_id = :guild_id AND {scope_filter}
                    FOR UPDATE
                """),
                params,
            )
        ).mappings().one_or_none()
        if account is None:
            raise ValueError("분배할 가계부 계정을 찾을 수 없습니다.")
        if requested_amount > _int(account["current_balance"]):
            raise ValueError("분배 금액이 현재 가계부 잔액보다 큽니다.")

        excluded_ids = sorted(
            {int(value) for value in excluded_recipient_ids if int(value) > 0}
        )
        if account_scope_code == 1:
            exclusion_filter = (
                "AND NOT (a.alliance_id = ANY(:excluded_ids))"
                if excluded_ids
                else ""
            )
            recipient_rows = (
                await session.execute(
                    text(f"""
                        SELECT DISTINCT a.alliance_id
                        FROM guild_alliance_role_mappings mapping
                        JOIN alliances a
                          ON a.alliance_id = mapping.alliance_id
                        WHERE mapping.guild_id = :guild_id
                          AND a.is_active IS TRUE
                          {exclusion_filter}
                        ORDER BY a.alliance_id
                    """),
                    {**params, "excluded_ids": excluded_ids},
                )
            ).scalars().all()
        else:
            exclusion_filter = (
                "AND NOT (u.discord_id = ANY(:excluded_ids))"
                if excluded_ids
                else ""
            )
            recipient_rows = (
                await session.execute(
                    text(f"""
                        SELECT u.user_id
                        FROM users u
                        WHERE u.is_active IS TRUE
                          AND u.alliance_id = :alliance_id
                          {exclusion_filter}
                          AND EXISTS (
                              SELECT 1
                              FROM attendance_entries ae
                              JOIN attendance_sessions ats
                                ON ats.attendance_id = ae.attendance_id
                              WHERE ae.user_id = u.user_id
                                AND ats.guild_id = :guild_id
                          )
                        ORDER BY u.user_id
                    """),
                    {**params, "excluded_ids": excluded_ids},
                )
            ).scalars().all()
        recipient_ids = [int(value) for value in recipient_rows]
        if not recipient_ids:
            target_label = "혈맹" if account_scope_code == 1 else "혈맹원"
            raise ValueError(f"분배 대상 {target_label}이 없습니다. 제외 설정을 확인해 주세요.")
        per_recipient_amount = requested_amount // len(recipient_ids)
        if per_recipient_amount <= 0:
            raise ValueError("분배 금액이 대상 인원보다 작습니다.")
        distributed_amount = per_recipient_amount * len(recipient_ids)
        now = int(time.time())
        distribution_id = await session.scalar(
            text("""
                INSERT INTO treasury_distributions (
                    treasury_account_id, requested_amount, per_recipient_amount,
                    distributed_amount, recipient_count, memo, created_at,
                    created_by_user_id
                ) VALUES (
                    :account_id, :requested_amount, :per_recipient_amount,
                    :distributed_amount, :recipient_count, :memo, :created_at,
                    :created_by_user_id
                )
                RETURNING treasury_distribution_id
            """),
            {
                "account_id": account["treasury_account_id"],
                "requested_amount": requested_amount,
                "per_recipient_amount": per_recipient_amount,
                "distributed_amount": distributed_amount,
                "recipient_count": len(recipient_ids),
                "memo": memo.strip()[:500] or None,
                "created_at": now,
                "created_by_user_id": created_by_user_id,
            },
        )
        await session.execute(
            text(f"""
                INSERT INTO treasury_distribution_recipients (
                    treasury_distribution_id,
                    {"alliance_id" if account_scope_code == 1 else "user_id"},
                    status_code, completed_at
                )
                SELECT :distribution_id, UNNEST(CAST(:recipient_ids AS BIGINT[])), 0, NULL
            """),
            {"distribution_id": distribution_id, "recipient_ids": recipient_ids},
        )
        category_name = "연합비 분배" if account_scope_code == 1 else "혈비 분배"
        category_id = await session.scalar(
            text("""
                INSERT INTO treasury_categories (
                    guild_id, account_scope_code, direction, category_name, is_active
                ) VALUES (:guild_id, :scope_code, -1, :category_name, TRUE)
                ON CONFLICT (guild_id, account_scope_code, direction, category_name)
                DO UPDATE SET is_active = TRUE
                RETURNING treasury_category_id
            """),
            {
                "guild_id": guild_id,
                "scope_code": account_scope_code,
                "category_name": category_name,
            },
        )
        source_type_id = await session.scalar(
            text("""
                SELECT source_type_id
                FROM treasury_source_types
                WHERE source_code = 'treasury_distribution'
            """)
        )
        if source_type_id is None:
            raise RuntimeError("공금 분배 원본 유형이 없습니다.")
        balance_after = _int(account["current_balance"]) - distributed_amount
        await session.execute(
            text("""
                INSERT INTO treasury_entries (
                    treasury_account_id, treasury_category_id, direction,
                    amount_adena, balance_after, source_type_id, source_id,
                    memo, occurred_at, created_at, created_by_user_id,
                    reversal_of_entry_id
                ) VALUES (
                    :account_id, :category_id, -1, :amount,
                    :balance_after, :source_type_id, :distribution_id,
                    :memo, :now, :now, :created_by_user_id, NULL
                )
            """),
            {
                "account_id": account["treasury_account_id"],
                "category_id": category_id,
                "amount": distributed_amount,
                "balance_after": balance_after,
                "source_type_id": source_type_id,
                "distribution_id": distribution_id,
                "memo": memo.strip()[:500] or (
                    f"{category_name} · {len(recipient_ids)}"
                    f"{'개 혈맹' if account_scope_code == 1 else '명'}"
                ),
                "now": now,
                "created_by_user_id": created_by_user_id,
            },
        )
        await session.execute(
            text("""
                UPDATE treasury_accounts
                SET current_balance = :balance_after, updated_at = :updated_at
                WHERE treasury_account_id = :account_id
            """),
            {
                "balance_after": balance_after,
                "updated_at": now,
                "account_id": account["treasury_account_id"],
            },
        )
        await session.commit()
        return _int(distribution_id)
    except Exception:
        await session.rollback()
        raise


async def treasury_distribution_scope(
    session: AsyncSession,
    treasury_distribution_id: int,
) -> dict[str, int | None] | None:
    row = (
        await session.execute(
            text("""
                SELECT d.treasury_distribution_id, a.guild_id, a.alliance_id,
                       a.account_scope_code
                FROM treasury_distributions d
                JOIN treasury_accounts a
                  ON a.treasury_account_id = d.treasury_account_id
                WHERE d.treasury_distribution_id = :distribution_id
            """),
            {"distribution_id": treasury_distribution_id},
        )
    ).mappings().one_or_none()
    return dict(row) if row else None


async def set_treasury_distribution_recipient_status(
    session: AsyncSession,
    *,
    treasury_distribution_id: int,
    user_id: int | None,
    status_code: int,
) -> int:
    if status_code not in {0, 1}:
        raise ValueError("지원하지 않는 지급 상태입니다.")
    completed_at = int(time.time()) if status_code == 1 else None
    user_filter = "" if user_id is None else "AND user_id = :user_id"
    result = await session.execute(
        text(f"""
            UPDATE treasury_distribution_recipients
            SET status_code = :status_code, completed_at = :completed_at
            WHERE treasury_distribution_id = :distribution_id
              {user_filter}
              AND status_code <> :status_code
        """),
        {
            "distribution_id": treasury_distribution_id,
            "user_id": user_id,
            "status_code": status_code,
            "completed_at": completed_at,
        },
    )
    await session.commit()
    return int(result.rowcount or 0)


async def forfeits_page(
    session: AsyncSession,
    *, guild_id: int, alliance_id: int, period_days: int, query: str, page: int
) -> dict[str, Any]:
    period = _period_clause("COALESCE(po.completed_at, d.occurred_at)", period_days, unix=True)
    search = " AND (v.item_name ILIKE :query OR COALESCE(u.game_nickname, u.discord_nickname, '') ILIKE :query)" if query else ""
    params = {"guild_id": guild_id, "alliance_id": alliance_id, "period_days": period_days, "query": f"%{query}%"}
    from_sql = f"""
        FROM settlement_payout_objects po
        JOIN settlement_drops d ON d.drop_id = po.drop_id
        JOIN catalog_item_versions v ON v.item_version_id = d.item_version_id
        JOIN users u ON u.user_id = po.recipient_user_id
        LEFT JOIN settlement_payout_objects parent_po ON parent_po.payout_object_id = po.parent_payout_object_id
        WHERE d.guild_id = :guild_id
          AND COALESCE(parent_po.recipient_alliance_id, u.alliance_id) = :alliance_id
          AND po.object_code = 2 AND po.status_code = 2 {period} {search}
    """
    rows, pagination = await _fetch_page(
        session,
        count_sql=f"SELECT COUNT(*) {from_sql}",
        rows_sql=f"""
            SELECT po.payout_object_id, COALESCE(u.game_nickname, u.discord_nickname) AS user_name,
                   v.item_name, po.amount_adena,
                   TO_CHAR(TO_TIMESTAMP(COALESCE(po.completed_at, d.occurred_at)), 'YYYY-MM-DD HH24:MI') AS completed_at_label
            {from_sql}
            ORDER BY COALESCE(po.completed_at, d.occurred_at) DESC, po.payout_object_id DESC
            LIMIT :limit OFFSET :offset
        """,
        params=params,
        page=page,
    )
    for row in rows:
        row["amount_label"] = _money(row["amount_adena"])
        row["state"] = "혈비 귀속"
        row["state_tone"] = "muted"
    total_amount = _int(await session.scalar(text(f"SELECT COALESCE(SUM(po.amount_adena), 0) {from_sql}"), params))
    return {
        "summary_cards": [
            {"label": "귀속 혈비", "value": _money(total_amount), "meta": "선택 기간"},
            {"label": "귀속 건수", "value": f"{pagination['total']:,}건", "meta": "미수령 전환"},
            {"label": "대상 인원", "value": f"{len({row['user_name'] for row in rows}):,}명", "meta": "현재 페이지"},
        ],
        "columns": [
            {"key": "user_name", "label": "유저", "emphasis": True},
            {"key": "item_name", "label": "아이템"},
            {"key": "amount_label", "label": "귀속 아데나", "numeric": True},
            {"key": "completed_at_label", "label": "귀속 시각"},
            {"key": "state", "label": "상태", "status_key": "state_tone"},
        ],
        "rows": rows,
        "pagination": pagination,
    }


async def reports_page(
    session: AsyncSession,
    *, guild_id: int, query: str, page: int
) -> dict[str, Any]:
    search = " AND (COALESCE(r.report_name, '') ILIKE :query OR r.channel_name ILIKE :query)" if query else ""
    params = {"guild_id": guild_id, "query": f"%{query}%"}
    from_sql = f"FROM scheduled_report_settings r WHERE r.guild_id = :guild_id AND r.status <> 'delete' {search}"
    rows, pagination = await _fetch_page(
        session,
        count_sql=f"SELECT COUNT(*) {from_sql}",
        rows_sql=f"""
            SELECT r.report_setting_id, COALESCE(r.report_name, '이름 없는 알림') AS report_name,
                   r.frequency, r.period_type, r.run_time, r.channel_name, r.status,
                   COALESCE(r.last_sent_at, '-') AS last_sent_at,
                   COALESCE(r.next_run_at, '-') AS next_run_at
            {from_sql}
            ORDER BY r.status = 'on' DESC, r.run_time, r.report_setting_id
            LIMIT :limit OFFSET :offset
        """,
        params=params,
        page=page,
    )
    frequency_labels = {"daily": "매일", "weekly": "매주", "monthly": "매월"}
    period_labels = {"today": "오늘", "recent_7_days": "최근 7일", "recent_30_days": "최근 한 달", "all": "전체"}
    for row in rows:
        row["schedule_label"] = f"{frequency_labels.get(row['frequency'], row['frequency'])} {row['run_time']}"
        row["period_label"] = period_labels.get(row["period_type"], row["period_type"])
        row["state"] = "사용" if row["status"] == "on" else "중지"
        row["state_tone"] = "success" if row["status"] == "on" else "muted"
    return {
        "summary_cards": [
            {"label": "등록 알림", "value": f"{pagination['total']:,}개", "meta": "삭제 제외"},
            {"label": "사용 중", "value": f"{sum(1 for r in rows if r['status'] == 'on'):,}개", "meta": "현재 페이지"},
            {"label": "발송 채널", "value": f"{len({r['channel_name'] for r in rows}):,}개", "meta": "현재 페이지"},
        ],
        "columns": [
            {"key": "report_name", "label": "알림", "emphasis": True},
            {"key": "schedule_label", "label": "스케줄"},
            {"key": "period_label", "label": "조회 기간"},
            {"key": "channel_name", "label": "채널"},
            {"key": "next_run_at", "label": "다음 발송"},
            {"key": "state", "label": "상태", "status_key": "state_tone"},
        ],
        "rows": rows,
        "pagination": pagination,
    }


async def audit_page(
    session: AsyncSession,
    *, guild_id: int, period_days: int, query: str, page: int
) -> dict[str, Any]:
    period = _period_clause("e.occurred_at", period_days, unix=True)
    search = " AND (COALESCE(actor.fallback_name, u.discord_nickname, CAST(actor.discord_id AS TEXT), '') ILIKE :query OR at.action_code ILIKE :query)" if query else ""
    params = {"guild_id": guild_id, "period_days": period_days, "query": f"%{query}%"}
    from_sql = f"""
        FROM audit_events e
        JOIN audit_action_types at ON at.action_type_id = e.action_type_id
        JOIN audit_entity_types et ON et.entity_type_id = at.entity_type_id
        LEFT JOIN audit_actors actor ON actor.actor_id = e.actor_id
        LEFT JOIN users u ON u.user_id = actor.user_id
        WHERE e.guild_id = :guild_id {period} {search}
    """
    rows, pagination = await _fetch_page(
        session,
        count_sql=f"SELECT COUNT(*) {from_sql}",
        rows_sql=f"""
            SELECT e.audit_event_id, at.action_code, et.entity_code, e.target_id, e.actor_role,
                   COALESCE(actor.fallback_name, u.discord_nickname, CAST(actor.discord_id AS TEXT), '시스템') AS actor_name,
                   TO_CHAR(TO_TIMESTAMP(e.occurred_at), 'YYYY-MM-DD HH24:MI:SS') AS occurred_at_label
            {from_sql}
            ORDER BY e.occurred_at DESC, e.audit_event_id DESC
            LIMIT :limit OFFSET :offset
        """,
        params=params,
        page=page,
    )
    for row in rows:
        row["action_label"] = ACTION_LABELS.get(row["action_code"], row["action_code"])
        row["target_label"] = f"{row['entity_code']} #{row['target_id']}" if row["target_id"] is not None else row["entity_code"]
        row["role_label"] = ROLE_LABELS.get(_int(row["actor_role"]), "User")
    action_count = len({row["action_code"] for row in rows})
    return {
        "summary_cards": [
            {"label": "작업 기록", "value": f"{pagination['total']:,}건", "meta": "선택 기간"},
            {"label": "작업 종류", "value": f"{action_count:,}개", "meta": "현재 페이지"},
            {"label": "표시 범위", "value": "운영 작업", "meta": "상태 일괄 완료 제외"},
        ],
        "columns": [
            {"key": "occurred_at_label", "label": "시각"},
            {"key": "actor_name", "label": "작업자", "emphasis": True},
            {"key": "role_label", "label": "권한"},
            {"key": "action_label", "label": "작업"},
            {"key": "target_label", "label": "대상"},
        ],
        "rows": rows,
        "pagination": pagination,
    }


def normalize_period(value: int | None) -> int:
    return value if value in {0, 7, 30} else 30


def filter_options(options: Sequence[dict[str, Any]], value: int) -> list[dict[str, Any]]:
    return [{**option, "selected": option["value"] == value} for option in options]
