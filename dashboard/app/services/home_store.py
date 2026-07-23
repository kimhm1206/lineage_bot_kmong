from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def _money(value: Any) -> str:
    return f"{int(value or 0):,}"


async def current_user(
    session: AsyncSession,
    *,
    guild_id: int,
    discord_user_id: int,
) -> dict[str, Any] | None:
    row = (
        await session.execute(
            text("""
                SELECT u.user_id, u.discord_id,
                       COALESCE(u.game_nickname, u.discord_nickname) AS user_name,
                       u.alliance_id,
                       COALESCE(a.display_name, a.alliance_name, '미분류') AS alliance_name
                FROM users u
                LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
                WHERE u.discord_id = :discord_user_id
                  AND u.is_active IS TRUE
                  AND EXISTS (
                      SELECT 1
                      FROM attendance_entries entry
                      JOIN attendance_sessions attendance
                        ON attendance.attendance_id = entry.attendance_id
                      WHERE entry.user_id = u.user_id
                        AND attendance.guild_id = :guild_id
                  )
                ORDER BY u.updated_at DESC, u.user_id DESC
                LIMIT 1
            """),
            {"guild_id": guild_id, "discord_user_id": discord_user_id},
        )
    ).mappings().one_or_none()
    return dict(row) if row else None


async def personal_overview(
    session: AsyncSession,
    *,
    guild_id: int,
    discord_user_id: int,
) -> dict[str, Any]:
    user = await current_user(
        session,
        guild_id=guild_id,
        discord_user_id=discord_user_id,
    )
    if user is None:
        return {
            "current_user": None,
            "cards": [
                {"label": "내 미수령 분배금", "value": "-", "meta": "출석 유저 연결 필요"},
                {"label": "수동 귀속", "value": "-", "meta": "출석 유저 연결 필요"},
                {"label": "최근 참여 드랍", "value": "-", "meta": "최근 한 달"},
                {"label": "마지막 출석", "value": "-", "meta": "기록 없음"},
            ],
        }
    user_id = int(user["user_id"])
    payout = (
        await session.execute(
            text("""
                WITH personal_payouts AS (
                    SELECT payout.amount_adena, payout.status_code,
                           drop_row.occurred_at
                    FROM settlement_payout_objects payout
                    JOIN settlement_drops drop_row
                      ON drop_row.drop_id = payout.drop_id
                    JOIN settlement_drop_sales sale
                      ON sale.drop_id = drop_row.drop_id
                     AND sale.status_code = 1
                    WHERE drop_row.guild_id = :guild_id
                      AND payout.object_code = 2
                      AND payout.recipient_user_id = :user_id

                    UNION ALL

                    SELECT distribution.per_recipient_amount,
                           recipient.status_code,
                           distribution.created_at
                    FROM treasury_distribution_recipients recipient
                    JOIN treasury_distributions distribution
                      ON distribution.treasury_distribution_id =
                         recipient.treasury_distribution_id
                    JOIN treasury_accounts account
                      ON account.treasury_account_id =
                         distribution.treasury_account_id
                    WHERE account.guild_id = :guild_id
                      AND account.account_scope_code = 2
                      AND recipient.user_id = :user_id
                )
                SELECT
                    COALESCE(SUM(amount_adena) FILTER (WHERE status_code = 0), 0)
                        AS pending_amount,
                    COUNT(*) FILTER (WHERE status_code = 0) AS pending_count,
                    COALESCE(SUM(amount_adena) FILTER (WHERE status_code = 2), 0)
                        AS forfeited_amount,
                    COUNT(*) FILTER (WHERE status_code = 2) AS forfeited_count
                FROM personal_payouts
            """),
            {"guild_id": guild_id, "user_id": user_id},
        )
    ).mappings().one()
    recent_drop_count = int(
        await session.scalar(
            text("""
                SELECT COUNT(DISTINCT participant.drop_id)
                FROM settlement_drop_participants participant
                JOIN settlement_drops drop_row
                  ON drop_row.drop_id = participant.drop_id
                WHERE participant.user_id = :user_id
                  AND drop_row.guild_id = :guild_id
                  AND drop_row.occurred_at >= EXTRACT(
                      EPOCH FROM NOW() - INTERVAL '30 days'
                  )::BIGINT
            """),
            {"guild_id": guild_id, "user_id": user_id},
        )
        or 0
    )
    last_attendance = await session.scalar(
        text("""
            SELECT TO_CHAR(
                       MAX(attendance.started_at::timestamp),
                       'YYYY-MM-DD HH24:MI'
                   )
            FROM attendance_entries entry
            JOIN attendance_sessions attendance
              ON attendance.attendance_id = entry.attendance_id
            WHERE entry.user_id = :user_id
              AND attendance.guild_id = :guild_id
        """),
        {"guild_id": guild_id, "user_id": user_id},
    )
    return {
        "current_user": user,
        "cards": [
            {
                "label": "내 미수령 분배금",
                "value": _money(payout["pending_amount"]),
                "meta": f"{int(payout['pending_count'] or 0):,}건 · 아데나",
            },
            {
                "label": "수동 귀속",
                "value": _money(payout["forfeited_amount"]),
                "meta": f"{int(payout['forfeited_count'] or 0):,}건 · 기간 제한 없음",
            },
            {
                "label": "최근 참여 드랍",
                "value": f"{recent_drop_count:,}건",
                "meta": "최근 한 달 기준",
            },
            {
                "label": "마지막 출석",
                "value": str(last_attendance or "-"),
                "meta": user["alliance_name"],
            },
        ],
    }
