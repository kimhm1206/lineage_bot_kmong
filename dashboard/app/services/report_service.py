from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.identifiers import snowflake_text
from dashboard.app.services import audit_service


KST = timezone(timedelta(hours=9))
FREQUENCY_DAYS = {"daily": 1, "every_3_days": 3, "weekly": 7}
FREQUENCY_LABELS = {
    "daily": "매일",
    "every_3_days": "3일마다",
    "weekly": "매주",
    "monthly": "매월",
}
PERIOD_LABELS = {
    "today": "오늘",
    "yesterday": "어제",
    "recent_3_days": "최근 3일",
    "recent_7_days": "최근 7일",
    "this_week": "이번 주",
    "this_month": "이번 달",
}
GROUP_LABELS = {"alliance": "혈맹별", "none": "전체"}
TARGET_LABELS = {"user": "유저", "alliance": "혈맹"}
METRIC_LABELS = {"attendance_count": "출석 횟수", "unique_user_count": "참여 인원"}
OUTPUT_LABELS = {"grouped_ranking": "그룹별 랭킹", "ranking": "단일 랭킹"}
ALLOWED_FREQUENCIES = set(FREQUENCY_LABELS)
ALLOWED_PERIODS = set(PERIOD_LABELS)
ALLOWED_GROUPS = set(GROUP_LABELS)
ALLOWED_TARGETS = set(TARGET_LABELS)
ALLOWED_METRICS = set(METRIC_LABELS)
ALLOWED_OUTPUTS = set(OUTPUT_LABELS)


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _parse_run_time(value: str) -> time:
    hour_text, separator, minute_text = str(value or "").partition(":")
    if not separator:
        raise ValueError("발송 시간을 선택해 주세요.")
    try:
        hour = int(hour_text)
        minute = int(minute_text)
    except ValueError:
        raise ValueError("발송 시간을 확인해 주세요.") from None
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise ValueError("발송 시간은 00:00부터 23:59 사이여야 합니다.")
    return time(hour, minute)


def _format_run_time(value: str) -> str:
    clock = _parse_run_time(value)
    return f"{clock.hour:02d}시 {clock.minute:02d}분"


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    normalized = str(value).strip().replace("T", " ")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=KST)
    return parsed.astimezone(KST)


def _format_datetime(value: datetime) -> str:
    return value.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")


def next_run_from(now: datetime, run_time: str, frequency: str) -> datetime:
    clock = _parse_run_time(run_time)
    candidate = datetime.combine(now.date(), clock, tzinfo=KST)
    if frequency == "monthly":
        while candidate <= now:
            month = candidate.month + 1
            year = candidate.year
            if month > 12:
                year += 1
                month = 1
            if month == 12:
                last_day = 31
            else:
                last_day = (date(year, month + 1, 1) - timedelta(days=1)).day
            candidate = candidate.replace(
                year=year,
                month=month,
                day=min(candidate.day, last_day),
            )
        return candidate
    step = timedelta(days=FREQUENCY_DAYS.get(frequency, 1))
    while candidate <= now:
        candidate += step
    return candidate


def _period_bounds(period: str, now: datetime) -> tuple[datetime, datetime]:
    today = now.date()
    if period == "yesterday":
        start_day = end_day = today - timedelta(days=1)
    elif period == "recent_3_days":
        start_day, end_day = today - timedelta(days=2), today
    elif period == "recent_7_days":
        start_day, end_day = today - timedelta(days=6), today
    elif period == "this_week":
        start_day, end_day = today - timedelta(days=today.weekday()), today
    elif period == "this_month":
        start_day, end_day = today.replace(day=1), today
    else:
        start_day = end_day = today
    return (
        datetime.combine(start_day, time.min, tzinfo=KST),
        datetime.combine(end_day, time(23, 59, 59), tzinfo=KST),
    )


def _normalize_form(form: dict[str, Any]) -> dict[str, Any]:
    frequency = str(form.get("frequency") or "")
    period = str(form.get("period_type") or "")
    group_by = str(form.get("group_by") or "")
    rank_target = str(form.get("rank_target") or "")
    metric = str(form.get("metric") or "")
    output = str(form.get("output") or "")
    if frequency not in ALLOWED_FREQUENCIES:
        raise ValueError("발송 주기를 선택해 주세요.")
    if period not in ALLOWED_PERIODS:
        raise ValueError("조회 기간을 선택해 주세요.")
    if group_by not in ALLOWED_GROUPS:
        raise ValueError("묶음 기준을 선택해 주세요.")
    if rank_target not in ALLOWED_TARGETS:
        raise ValueError("랭킹 대상을 선택해 주세요.")
    if metric not in ALLOWED_METRICS:
        raise ValueError("집계값을 선택해 주세요.")
    if output not in ALLOWED_OUTPUTS:
        raise ValueError("출력 방식을 선택해 주세요.")
    run_time = str(form.get("run_time") or "")
    _parse_run_time(run_time)
    report_name = str(form.get("report_name") or "").strip()[:100]
    title = str(form.get("title") or "").strip()[:200]
    if not report_name:
        raise ValueError("알림 이름을 입력해 주세요.")
    if not title:
        raise ValueError("Discord 메시지 제목을 입력해 주세요.")
    try:
        channel_id = int(str(form.get("channel_id") or ""))
        limit = int(str(form.get("limit") or ""))
    except ValueError:
        raise ValueError("채널과 표시 개수를 확인해 주세요.") from None
    if channel_id <= 0:
        raise ValueError("발송 채널을 선택해 주세요.")
    if not 1 <= limit <= 30:
        raise ValueError("표시 개수는 1개부터 30개까지 가능합니다.")
    channel_name = str(form.get("channel_name") or "").strip()[:100]
    if not channel_name:
        raise ValueError("선택한 채널 정보를 확인해 주세요.")
    return {
        "report_name": report_name,
        "frequency": frequency,
        "period_type": period,
        "group_by": group_by,
        "rank_target": rank_target,
        "metric": metric,
        "output": output,
        "run_time": run_time,
        "channel_id": channel_id,
        "channel_name": channel_name,
        "limit": limit,
        "title": title,
        "group_header": str(form.get("group_header") or "{group_name}").strip()[:200]
        or "{group_name}",
        "row_template": str(form.get("row_template") or "{rank}. {label} - {value}회").strip()[:300]
        or "{rank}. {label} - {value}회",
        "empty_text": str(form.get("empty_text") or "출석 기록 없음").strip()[:200]
        or "출석 기록 없음",
    }


async def list_reports(session: AsyncSession, guild_id: int) -> list[dict[str, Any]]:
    rows = (
        await session.execute(
            text("""
                SELECT report_setting_id, report_name, frequency, period_type,
                       subject_type, result_type, run_time, channel_id,
                       channel_name, schedule_json, query_json, render_json,
                       status, last_sent_at, next_run_at, updated_at
                FROM scheduled_report_settings
                WHERE guild_id = :guild_id AND status <> 'delete'
                ORDER BY status = 'on' DESC, run_time, report_setting_id
            """),
            {"guild_id": guild_id},
        )
    ).mappings().all()
    reports: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        schedule = _json_dict(row["schedule_json"])
        query = _json_dict(row["query_json"])
        render = _json_dict(row["render_json"])
        frequency = str(schedule.get("type") or row["frequency"])
        period = str(query.get("period") or row["period_type"])
        group_by = str(query.get("group_by") or "alliance")
        rank_target = str(query.get("rank_target") or row["subject_type"])
        metric = str(query.get("metric") or "attendance_count")
        output = str(render.get("output") or row["result_type"])
        run_time = str(schedule.get("time") or row["run_time"])
        report = {
            **row,
            "report_setting_id": int(row["report_setting_id"]),
            "channel_id": snowflake_text(row["channel_id"]),
            "frequency": frequency,
            "period_type": period,
            "group_by": group_by,
            "rank_target": rank_target,
            "metric": metric,
            "output": output,
            "run_time": run_time,
            "limit": int(query.get("limit") or 10),
            "title": str(render.get("title") or row["report_name"] or "통계 알림"),
            "group_header": str(render.get("group_header") or "{group_name}"),
            "row_template": str(render.get("row") or "{rank}. {label} - {value}회"),
            "empty_text": str(render.get("empty") or "출석 기록 없음"),
            "frequency_label": FREQUENCY_LABELS.get(frequency, frequency),
            "period_label": PERIOD_LABELS.get(period, period),
            "target_label": TARGET_LABELS.get(rank_target, rank_target),
            "schedule_label": f"{FREQUENCY_LABELS.get(frequency, frequency)} {_format_run_time(run_time)}",
            "status_label": "사용 중" if row["status"] == "on" else "중지",
            "status_tone": "success" if row["status"] == "on" else "muted",
        }
        report["form_data"] = {
            key: report[key]
            for key in (
                "report_setting_id",
                "report_name",
                "channel_id",
                "frequency",
                "run_time",
                "period_type",
                "group_by",
                "rank_target",
                "metric",
                "limit",
                "output",
                "title",
                "group_header",
                "row_template",
                "empty_text",
            )
        }
        reports.append(report)
    return reports


async def save_report(
    session: AsyncSession,
    *,
    guild_id: int,
    actor_discord_id: int,
    form: dict[str, Any],
    report_setting_id: int | None,
) -> int:
    config = _normalize_form(form)
    schedule = {
        "type": config["frequency"],
        "time": config["run_time"],
        "timezone": "Asia/Seoul",
    }
    query = {
        "dataset": "attendance",
        "period": config["period_type"],
        "group_by": config["group_by"],
        "rank_target": config["rank_target"],
        "metric": config["metric"],
        "limit": config["limit"],
    }
    render = {
        "output": config["output"],
        "title": config["title"],
        "group_header": config["group_header"],
        "row": config["row_template"],
        "empty": config["empty_text"],
    }
    next_run_at = _format_datetime(
        next_run_from(datetime.now(KST), config["run_time"], config["frequency"])
    )
    params = {
        **config,
        "guild_id": guild_id,
        "actor_discord_id": actor_discord_id,
        "schedule_json": json.dumps(schedule, ensure_ascii=False),
        "query_json": json.dumps(query, ensure_ascii=False),
        "render_json": json.dumps(render, ensure_ascii=False),
        "next_run_at": next_run_at,
        "report_setting_id": report_setting_id,
    }
    is_update = bool(report_setting_id)
    if report_setting_id:
        saved_id = await session.scalar(
            text("""
                UPDATE scheduled_report_settings
                SET updated_by_discord_id = :actor_discord_id,
                    report_name = :report_name, frequency = :frequency,
                    period_type = :period_type, subject_type = :rank_target,
                    result_type = :output, run_time = :run_time,
                    channel_id = :channel_id, channel_name = :channel_name,
                    schedule_json = CAST(:schedule_json AS JSONB),
                    query_json = CAST(:query_json AS JSONB),
                    render_json = CAST(:render_json AS JSONB),
                    next_run_at = :next_run_at, updated_at = NOW()
                WHERE report_setting_id = :report_setting_id
                  AND guild_id = :guild_id AND status <> 'delete'
                RETURNING report_setting_id
            """),
            params,
        )
        if saved_id is None:
            raise ValueError("수정할 알림을 찾을 수 없습니다.")
    else:
        saved_id = await session.scalar(
            text("""
                INSERT INTO scheduled_report_settings (
                    guild_id, created_by_discord_id, updated_by_discord_id,
                    report_name, frequency, period_type, subject_type,
                    result_type, run_time, channel_id, channel_name,
                    schedule_json, query_json, render_json, status,
                    next_run_at, updated_at
                ) VALUES (
                    :guild_id, :actor_discord_id, :actor_discord_id,
                    :report_name, :frequency, :period_type, :rank_target,
                    :output, :run_time, :channel_id, :channel_name,
                    CAST(:schedule_json AS JSONB), CAST(:query_json AS JSONB),
                    CAST(:render_json AS JSONB), 'on', :next_run_at, NOW()
                )
                RETURNING report_setting_id
            """),
            params,
        )
    await audit_service.record_event(
        session,
        guild_id=guild_id,
        action_code="report_update" if is_update else "report_create",
        target_id=int(saved_id),
    )
    await session.commit()
    return int(saved_id)


async def update_status(
    session: AsyncSession,
    *,
    guild_id: int,
    report_setting_id: int,
    actor_discord_id: int,
    status: str,
) -> bool:
    if status not in {"on", "off", "delete"}:
        raise ValueError("알림 상태를 확인해 주세요.")
    next_run_at = None
    if status == "on":
        row = (
            await session.execute(
                text("""
                    SELECT frequency, run_time
                    FROM scheduled_report_settings
                    WHERE guild_id = :guild_id
                      AND report_setting_id = :report_setting_id
                      AND status <> 'delete'
                """),
                {"guild_id": guild_id, "report_setting_id": report_setting_id},
            )
        ).mappings().one_or_none()
        if row is None:
            return False
        next_run_at = _format_datetime(
            next_run_from(datetime.now(KST), str(row["run_time"]), str(row["frequency"]))
        )
    result = await session.execute(
        text("""
            UPDATE scheduled_report_settings
            SET status = :status, next_run_at = :next_run_at,
                updated_by_discord_id = :actor_discord_id, updated_at = NOW()
            WHERE guild_id = :guild_id
              AND report_setting_id = :report_setting_id
              AND status <> 'delete'
        """),
        {
            "guild_id": guild_id,
            "report_setting_id": report_setting_id,
            "actor_discord_id": actor_discord_id,
            "status": status,
            "next_run_at": next_run_at,
        },
    )
    if result.rowcount:
        await audit_service.record_event(
            session,
            guild_id=guild_id,
            action_code="report_status",
            target_id=report_setting_id,
            state_code={"off": 0, "on": 1, "delete": 2}[status],
        )
    await session.commit()
    return bool(result.rowcount)


async def _ranking_rows(
    session: AsyncSession,
    *,
    guild_id: int,
    query: dict[str, Any],
    start_at: datetime,
    end_at: datetime,
) -> list[dict[str, Any]]:
    rank_target = str(query.get("rank_target") or "user")
    group_by = str(query.get("group_by") or "alliance")
    metric = str(query.get("metric") or "attendance_count")
    limit = max(1, min(int(query.get("limit") or 10), 30))
    if rank_target == "alliance":
        value_sql = (
            "COUNT(DISTINCT entry.user_id)"
            if metric == "unique_user_count"
            else "COUNT(*)"
        )
    else:
        value_sql = "COUNT(DISTINCT entry.attendance_id)"
    params = {
        "guild_id": guild_id,
        "start_at": start_at.astimezone(KST).replace(tzinfo=None),
        "end_at": end_at.astimezone(KST).replace(tzinfo=None),
    }
    if rank_target == "alliance":
        rows = (
            await session.execute(
                text(f"""
                    SELECT '전체' AS group_name,
                           COALESCE(alliance.display_name, alliance.alliance_name, '미분류') AS label,
                           {value_sql} AS value
                    FROM attendance_entries entry
                    JOIN attendance_sessions attendance
                      ON attendance.attendance_id = entry.attendance_id
                    JOIN users user_row ON user_row.user_id = entry.user_id
                    LEFT JOIN alliances alliance
                      ON alliance.alliance_id = user_row.alliance_id
                    WHERE attendance.guild_id = :guild_id
                      AND attendance.started_at::timestamp BETWEEN :start_at AND :end_at
                    GROUP BY COALESCE(alliance.display_name, alliance.alliance_name, '미분류')
                    ORDER BY value DESC, label
                """),
                params,
            )
        ).mappings().all()
    else:
        group_sql = (
            "COALESCE(alliance.display_name, alliance.alliance_name, '미분류')"
            if group_by == "alliance"
            else "'전체'"
        )
        rows = (
            await session.execute(
                text(f"""
                    SELECT {group_sql} AS group_name,
                           COALESCE(user_row.game_nickname, user_row.discord_nickname) AS label,
                           {value_sql} AS value
                    FROM attendance_entries entry
                    JOIN attendance_sessions attendance
                      ON attendance.attendance_id = entry.attendance_id
                    JOIN users user_row ON user_row.user_id = entry.user_id
                    LEFT JOIN alliances alliance
                      ON alliance.alliance_id = user_row.alliance_id
                    WHERE attendance.guild_id = :guild_id
                      AND attendance.started_at::timestamp BETWEEN :start_at AND :end_at
                    GROUP BY {group_sql}, user_row.user_id,
                             user_row.game_nickname, user_row.discord_nickname
                    ORDER BY group_name, value DESC, label
                """),
                params,
            )
        ).mappings().all()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for source in rows:
        row = dict(source)
        grouped[str(row["group_name"] or "전체")].append(row)
    output: list[dict[str, Any]] = []
    for group_name, group_rows in grouped.items():
        for rank, row in enumerate(group_rows[:limit], start=1):
            output.append(
                {
                    "group_name": group_name,
                    "rank": rank,
                    "label": str(row["label"] or "-"),
                    "value": int(row["value"] or 0),
                }
            )
    return output


def _safe_template(template: str, **values: Any) -> str:
    try:
        return template.format(**values)
    except (KeyError, IndexError, ValueError):
        return template


def _clip(value: str, length: int) -> str:
    return value if len(value) <= length else value[: length - 1] + "…"


def message_chunks(message: str, limit: int = 1900) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    in_code_block = False

    def rendered_length(lines: list[str], close_code_block: bool) -> int:
        suffix = ["```"] if close_code_block else []
        return len("\n".join([*lines, *suffix]))

    def flush() -> None:
        nonlocal current
        if not current:
            return
        output = [*current, *(["```"] if in_code_block else [])]
        chunks.append("\n".join(output).strip())
        current = ["```"] if in_code_block else []

    for source_line in message.splitlines():
        line = _clip(source_line, max(limit - 16, 1))
        if current and rendered_length([*current, line], in_code_block) > limit:
            flush()
        current.append(line)
        if source_line.strip() == "```":
            in_code_block = not in_code_block
    flush()
    return [chunk for chunk in chunks if chunk]


async def build_message(
    session: AsyncSession,
    *,
    guild_id: int,
    guild_name: str,
    schedule: dict[str, Any],
    query: dict[str, Any],
    render: dict[str, Any],
    now: datetime | None = None,
) -> str:
    now = now or datetime.now(KST)
    start_at, end_at = _period_bounds(str(query.get("period") or "today"), now)
    rows = await _ranking_rows(
        session,
        guild_id=guild_id,
        query=query,
        start_at=start_at,
        end_at=end_at,
    )
    title = str(render.get("title") or "통계 알림")
    group_template = str(render.get("group_header") or "{group_name}")
    row_template = str(render.get("row") or "{rank}. {label} - {value}회")
    lines = [
        f"**{title}**",
        f"서버: {guild_name}",
        f"기간: {start_at.strftime('%Y-%m-%d %H:%M')} ~ {end_at.strftime('%Y-%m-%d %H:%M')}",
        "",
    ]
    if not rows:
        lines.append(str(render.get("empty") or "출석 기록 없음"))
        return "\n".join(lines)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["group_name"]].append(row)
    output = str(render.get("output") or "grouped_ranking")
    if output == "ranking" or str(query.get("group_by")) == "none":
        lines.append("```")
        lines.extend(
            _safe_template(
                row_template,
                rank=row["rank"],
                label=_clip(row["label"], 28),
                value=row["value"],
                group_name=_clip(row["group_name"], 18),
            )
            for row in rows
        )
        lines.append("```")
    else:
        for group_name, group_rows in grouped.items():
            lines.append(_safe_template(group_template, group_name=group_name))
            lines.append("```")
            lines.extend(
                _safe_template(
                    row_template,
                    rank=row["rank"],
                    label=_clip(row["label"], 28),
                    value=row["value"],
                    group_name=_clip(group_name, 18),
                )
                for row in group_rows
            )
            lines.append("```")
            lines.append("")
    return "\n".join(lines).strip()


async def preview_report(
    session: AsyncSession,
    *,
    guild_id: int,
    guild_name: str,
    form: dict[str, Any],
) -> str:
    config = _normalize_form(form)
    return await build_message(
        session,
        guild_id=guild_id,
        guild_name=guild_name,
        schedule={"type": config["frequency"], "time": config["run_time"]},
        query={
            "period": config["period_type"],
            "group_by": config["group_by"],
            "rank_target": config["rank_target"],
            "metric": config["metric"],
            "limit": config["limit"],
        },
        render={
            "output": config["output"],
            "title": config["title"],
            "group_header": config["group_header"],
            "row": config["row_template"],
            "empty": config["empty_text"],
        },
    )
