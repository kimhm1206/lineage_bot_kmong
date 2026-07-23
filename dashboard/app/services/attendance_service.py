from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.services import settlement_service


class AttendanceEditError(ValueError):
    pass


def _user_ids(values: Iterable[object]) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    for value in values:
        try:
            user_id = int(value)
        except (TypeError, ValueError):
            continue
        if user_id > 0 and user_id not in seen:
            seen.add(user_id)
            result.append(user_id)
    return result


async def _lock_attendance(
    session: AsyncSession,
    *,
    guild_id: int,
    attendance_id: int,
) -> None:
    exists = await session.scalar(
        text("""
            SELECT 1
            FROM attendance_sessions
            WHERE attendance_id = :attendance_id
              AND guild_id = :guild_id
            FOR UPDATE
        """),
        {"attendance_id": attendance_id, "guild_id": guild_id},
    )
    if exists is None:
        raise AttendanceEditError("선택한 출석 회차를 찾을 수 없습니다.")


async def _editable_drop_ids(
    session: AsyncSession,
    *,
    guild_id: int,
    attendance_id: int,
) -> list[tuple[int, int]]:
    rows = (
        await session.execute(
            text("""
                SELECT d.drop_id, sale.status_code,
                       EXISTS (
                           SELECT 1
                           FROM settlement_payout_objects payout
                           WHERE payout.drop_id = d.drop_id
                             AND payout.status_code <> 0
                       ) AS settlement_started
                FROM settlement_drops d
                JOIN settlement_drop_sales sale ON sale.drop_id = d.drop_id
                WHERE d.guild_id = :guild_id
                  AND d.attendance_id = :attendance_id
                ORDER BY d.drop_id
                FOR UPDATE OF d, sale
            """),
            {"guild_id": guild_id, "attendance_id": attendance_id},
        )
    ).mappings().all()
    if any(bool(row["settlement_started"]) for row in rows):
        raise AttendanceEditError(
            "이미 분배가 진행된 드랍과 연결된 출석은 인원을 수정할 수 없습니다."
        )
    return [(int(row["drop_id"]), int(row["status_code"])) for row in rows]


async def _sync_drop_participants(
    session: AsyncSession,
    *,
    attendance_id: int,
    drops: list[tuple[int, int]],
) -> None:
    participant_count = int(
        await session.scalar(
            text("SELECT COUNT(*) FROM attendance_entries WHERE attendance_id = :attendance_id"),
            {"attendance_id": attendance_id},
        )
        or 0
    )
    if drops and participant_count == 0:
        raise AttendanceEditError("드랍과 연결된 출석 회차는 참여자를 모두 삭제할 수 없습니다.")

    for drop_id, sale_status in drops:
        await session.execute(
            text("DELETE FROM settlement_drop_participants WHERE drop_id = :drop_id"),
            {"drop_id": drop_id},
        )
        await session.execute(
            text("""
                INSERT INTO settlement_drop_participants (drop_id, user_id, alliance_id)
                SELECT :drop_id, entry.user_id, users.alliance_id
                FROM attendance_entries entry
                JOIN users ON users.user_id = entry.user_id
                WHERE entry.attendance_id = :attendance_id
            """),
            {"drop_id": drop_id, "attendance_id": attendance_id},
        )
        if sale_status == 1:
            await session.execute(
                text("DELETE FROM settlement_payout_objects WHERE drop_id = :drop_id"),
                {"drop_id": drop_id},
            )
            try:
                await settlement_service._build_alliance_payouts(session, drop_id=drop_id)
            except settlement_service.SettlementError as exc:
                raise AttendanceEditError(str(exc)) from exc


async def add_members(
    session: AsyncSession,
    *,
    guild_id: int,
    attendance_id: int,
    user_ids: Iterable[object],
) -> int:
    requested = _user_ids(user_ids)
    if not requested:
        raise AttendanceEditError("추가할 유저를 선택해 주세요.")

    await _lock_attendance(
        session,
        guild_id=guild_id,
        attendance_id=attendance_id,
    )
    drops = await _editable_drop_ids(
        session,
        guild_id=guild_id,
        attendance_id=attendance_id,
    )
    eligible_statement = text("""
        SELECT u.user_id
        FROM users u
        WHERE u.user_id IN :user_ids
          AND u.is_active IS TRUE
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
    """).bindparams(bindparam("user_ids", expanding=True))
    eligible_ids = {
        int(value)
        for value in (
            await session.execute(
                eligible_statement,
                {"user_ids": requested, "guild_id": guild_id},
            )
        ).scalars()
    }
    if eligible_ids != set(requested):
        raise AttendanceEditError("현재 서버에서 확인할 수 없는 유저가 포함되어 있습니다.")

    inserted_ids: list[int] = []
    for user_id in requested:
        inserted = await session.scalar(
            text("""
                INSERT INTO attendance_entries (attendance_id, user_id)
                VALUES (:attendance_id, :user_id)
                ON CONFLICT (attendance_id, user_id) DO NOTHING
                RETURNING user_id
            """),
            {"attendance_id": attendance_id, "user_id": user_id},
        )
        if inserted is not None:
            inserted_ids.append(int(inserted))
    if not inserted_ids:
        raise AttendanceEditError("선택한 유저는 이미 이 회차에 출석되어 있습니다.")

    await _sync_drop_participants(
        session,
        attendance_id=attendance_id,
        drops=drops,
    )
    for user_id in inserted_ids:
        await settlement_service._audit(
            session,
            guild_id=guild_id,
            action_code="attendance_add",
            target_id=attendance_id,
            attendance_id=attendance_id,
            user_id=user_id,
        )
    return len(inserted_ids)


async def remove_member(
    session: AsyncSession,
    *,
    guild_id: int,
    attendance_id: int,
    user_id: int,
) -> None:
    await _lock_attendance(
        session,
        guild_id=guild_id,
        attendance_id=attendance_id,
    )
    drops = await _editable_drop_ids(
        session,
        guild_id=guild_id,
        attendance_id=attendance_id,
    )
    removed = await session.scalar(
        text("""
            DELETE FROM attendance_entries
            WHERE attendance_id = :attendance_id
              AND user_id = :user_id
            RETURNING user_id
        """),
        {"attendance_id": attendance_id, "user_id": user_id},
    )
    if removed is None:
        raise AttendanceEditError("이 회차에서 삭제할 출석 유저를 찾을 수 없습니다.")

    await _sync_drop_participants(
        session,
        attendance_id=attendance_id,
        drops=drops,
    )
    await settlement_service._audit(
        session,
        guild_id=guild_id,
        action_code="attendance_delete",
        target_id=attendance_id,
        attendance_id=attendance_id,
        user_id=user_id,
    )
