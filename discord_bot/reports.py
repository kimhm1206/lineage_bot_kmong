from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, time, timedelta, timezone
from typing import Any

import discord
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

from discord_bot.storage import database


KST = timezone(timedelta(hours=9))
RETRY_DELAY = timedelta(minutes=10)
SCHEDULE_REFRESH_SECONDS = max(
    15,
    int(os.getenv("REPORT_SCHEDULE_REFRESH_SECONDS", "60")),
)
FREQUENCY_DAYS = {
    "daily": 1,
    "every_3_days": 3,
    "weekly": 7,
}
PERIOD_LABELS = {
    "today": "오늘",
    "recent_7_days": "최근 일주일",
    "yesterday": "전날",
    "recent_3_days": "최근 3일",
    "this_week": "이번 주",
    "this_month": "이번 달",
}
FREQUENCY_LABELS = {
    "daily": "매일",
    "every_3_days": "3일마다",
    "weekly": "일주일마다",
    "monthly": "매월",
}


def start_report_scheduler(bot: discord.Bot) -> None:
    scheduler = getattr(bot, "report_scheduler", None)
    if scheduler is None:
        scheduler = AsyncIOScheduler(
            timezone=KST,
            job_defaults={
                "coalesce": True,
                "max_instances": 1,
                "misfire_grace_time": 300,
            },
        )
        scheduler.start()
        bot.report_scheduler = scheduler

    if not hasattr(bot, "report_scheduler_reload_lock"):
        bot.report_scheduler_reload_lock = asyncio.Lock()

    existing_task = getattr(bot, "report_scheduler_reload_task", None)
    if isinstance(existing_task, asyncio.Task) and not existing_task.done():
        return
    bot.report_scheduler_reload_task = asyncio.create_task(
        _watch_report_schedules(bot)
    )


async def _watch_report_schedules(bot: discord.Bot) -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            await reload_report_schedules(bot)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[report-scheduler] refresh failed: {exc}")
        await asyncio.sleep(SCHEDULE_REFRESH_SECONDS)


async def reload_report_schedules(bot: discord.Bot) -> dict[str, int]:
    await bot.wait_until_ready()
    lock = getattr(bot, "report_scheduler_reload_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        bot.report_scheduler_reload_lock = lock

    async with lock:
        scheduler = _get_scheduler(bot)
        scheduler.remove_all_jobs()
        reports = await asyncio.to_thread(database.get_active_scheduled_reports)
        connected_guild_ids = _connected_guild_ids(bot)
        scheduled_count = 0
        for report in reports:
            if int(report["guild_id"]) not in connected_guild_ids:
                continue
            did_schedule = await _schedule_report(bot, report)
            scheduled_count += 1 if did_schedule else 0
        print(f"[report-scheduler] reloaded {scheduled_count} active reports")
        return {"scheduled_count": scheduled_count}


async def _schedule_report(bot: discord.Bot, report: dict[str, Any]) -> bool:
    scheduler = _get_scheduler(bot)
    report_setting_id = int(report["report_setting_id"])
    now = datetime.now(KST)
    next_run_at = _parse_datetime(report.get("next_run_at"))
    scheduled_at = next_run_at
    if next_run_at is None:
        schedule = _report_schedule(report)
        next_run_at = _next_run_from_now(
            now,
            str(schedule.get("time") or report["run_time"] or "00:00"),
            str(schedule.get("type") or report["frequency"] or "daily"),
        )
        scheduled_at = next_run_at
        await asyncio.to_thread(
            database.update_scheduled_report_next_run,
            report_setting_id,
            _format_datetime(next_run_at),
        )
    elif next_run_at <= now:
        next_run_at = now + timedelta(seconds=1)

    scheduler.add_job(
        _run_report_job,
        trigger=DateTrigger(run_date=next_run_at, timezone=KST),
        id=_job_id(report_setting_id),
        replace_existing=True,
        args=[bot, report_setting_id, _format_datetime(scheduled_at or next_run_at)],
    )
    return True


async def _run_report_job(
    bot: discord.Bot,
    report_setting_id: int,
    scheduled_run_at: str,
) -> None:
    report = await asyncio.to_thread(
        database.get_active_scheduled_report,
        report_setting_id,
    )
    if report is None:
        return

    guild_id = int(report["guild_id"])
    if guild_id not in _connected_guild_ids(bot):
        print(
            "[report-scheduler] skipped unsupported guild "
            f"report={report_setting_id} guild_id={guild_id}"
        )
        return

    now = datetime.now(KST)
    scheduled_at = _parse_datetime(scheduled_run_at) or now
    try:
        channel = await _resolve_channel(bot, int(report["channel_id"]))
        message = await _build_report_message(bot, report, now)
        await _send_chunked(channel, message)
    except Exception as exc:
        retry_at = now + RETRY_DELAY
        await asyncio.to_thread(
            database.update_scheduled_report_next_run,
            report_setting_id,
            _format_datetime(retry_at),
        )
        report["next_run_at"] = _format_datetime(retry_at)
        await _schedule_report(bot, report)
        print(f"[report-scheduler] send failed report={report_setting_id}: {exc}")
        return

    following_run = _next_run_from_now(
        now,
        str(_report_schedule(report).get("time") or report["run_time"] or "00:00"),
        str(_report_schedule(report).get("type") or report["frequency"] or "daily"),
    )
    await asyncio.to_thread(
        database.mark_scheduled_report_sent,
        report_setting_id,
        _format_datetime(now),
        _format_datetime(following_run),
    )
    report["next_run_at"] = _format_datetime(following_run)
    await _schedule_report(bot, report)


def _get_scheduler(bot: discord.Bot) -> AsyncIOScheduler:
    scheduler = getattr(bot, "report_scheduler", None)
    if scheduler is None:
        raise RuntimeError("report scheduler is not started")
    return scheduler


def _job_id(report_setting_id: int) -> str:
    return f"report:{report_setting_id}"


def _connected_guild_ids(bot: discord.Bot) -> set[int]:
    return {int(guild.id) for guild in bot.guilds}


async def _resolve_channel(bot: discord.Bot, channel_id: int) -> Any:
    channel = bot.get_channel(channel_id)
    if channel is None:
        channel = await bot.fetch_channel(channel_id)
    if not hasattr(channel, "send"):
        raise RuntimeError(f"메시지를 보낼 수 없는 채널입니다. channel_id={channel_id}")
    return channel


async def _build_report_message(
    bot: discord.Bot,
    report: dict[str, Any],
    now: datetime,
) -> str:
    guild_id = int(report["guild_id"])
    guild = bot.get_guild(guild_id)
    guild_name = guild.name if guild is not None else str(guild_id)
    schedule = _report_schedule(report)
    query = _report_query(report)
    render = _report_render(report)
    start_at, end_at = _period_bounds(str(query.get("period") or "today"), now)
    rows = await asyncio.to_thread(
        database.get_report_attendance_ranking,
        guild_id,
        _format_datetime(start_at),
        _format_datetime(end_at),
        group_by=str(query.get("group_by") or "alliance"),
        rank_target=str(query.get("rank_target") or "user"),
        metric=str(query.get("metric") or "attendance_count"),
        limit=int(query.get("limit") or 10),
    )
    return _format_report_message(rows, schedule, query, render, guild_name, start_at, end_at)


def _format_report_message(
    rows: list[dict[str, Any]],
    schedule: dict[str, Any],
    query: dict[str, Any],
    render: dict[str, Any],
    guild_name: str,
    start_at: datetime,
    end_at: datetime,
) -> str:
    title = str(render.get("title") or "통계 알림")
    group_template = str(render.get("group_header") or "{group_name}")
    row_template = str(render.get("row") or "{rank}. {label} - {value}회")
    empty_text = str(render.get("empty") or "출석 기록 없음")
    output = str(render.get("output") or "grouped_ranking")
    lines = [
        f"**{title}**",
        f"서버: {guild_name}",
        (
            f"기간: {_format_datetime(start_at)} ~ "
            f"{_format_datetime(end_at)}"
        ),
        (
            f"설정: {PERIOD_LABELS.get(str(query.get('period')), str(query.get('period')))} · "
            f"{FREQUENCY_LABELS.get(str(schedule.get('type')), str(schedule.get('type')))} "
            f"{_format_run_time(str(schedule.get('time') or '00:00'))}"
        ),
        "",
    ]
    if not rows:
        lines.append(empty_text)
        return "\n".join(lines)

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("group_name") or "전체"), []).append(row)

    if output == "ranking" or str(query.get("group_by")) == "none":
        lines.append("```")
        lines.extend(_format_report_rows(rows, row_template))
        lines.append("```")
        return "\n".join(lines)

    for group_name, group_rows in grouped.items():
        lines.append(_safe_template(group_template, group_name=group_name))
        lines.append("```")
        lines.extend(_format_report_rows(group_rows, row_template))
        lines.append("```")
        lines.append("")
    return "\n".join(lines).strip()


def _format_report_rows(rows: list[dict[str, Any]], row_template: str) -> list[str]:
    lines = []
    for index, row in enumerate(rows, start=1):
        lines.append(
            _safe_template(
                row_template,
                rank=int(row.get("rank") or index),
                label=_clip(str(row.get("label") or "-"), 28),
                value=int(row.get("value") or 0),
                group_name=_clip(str(row.get("group_name") or "전체"), 18),
            )
        )
    return lines


def _safe_template(template: str, **values: Any) -> str:
    try:
        return template.format(**values)
    except (KeyError, IndexError, ValueError):
        return str(template)


async def _send_chunked(channel: Any, content: str) -> None:
    max_length = 1900
    if len(content) <= max_length:
        await channel.send(content)
        return

    chunk_lines: list[str] = []
    chunk_length = 0
    for line in content.splitlines():
        line_length = len(line) + 1
        if chunk_lines and chunk_length + line_length > max_length:
            await channel.send("\n".join(chunk_lines))
            chunk_lines = []
            chunk_length = 0
        chunk_lines.append(line)
        chunk_length += line_length
    if chunk_lines:
        await channel.send("\n".join(chunk_lines))


def _period_bounds(period_type: str, now: datetime) -> tuple[datetime, datetime]:
    today = now.date()
    if period_type == "yesterday":
        day = today - timedelta(days=1)
        return _day_start(day), _day_end(day)
    if period_type == "recent_3_days":
        return _day_start(today - timedelta(days=2)), _day_end(today)
    if period_type == "recent_7_days":
        return _day_start(today - timedelta(days=6)), _day_end(today)
    if period_type == "this_week":
        return _day_start(today - timedelta(days=today.weekday())), _day_end(today)
    if period_type == "this_month":
        return _day_start(today.replace(day=1)), _day_end(today)
    return _day_start(today), _day_end(today)


def _next_run_from_now(now: datetime, run_time: str, frequency: str) -> datetime:
    run_clock = _parse_run_time(run_time)
    candidate = datetime.combine(now.date(), run_clock, tzinfo=KST)
    if frequency == "monthly":
        while candidate <= now:
            candidate = _add_month(candidate)
        return candidate
    interval = timedelta(days=FREQUENCY_DAYS.get(frequency, 1))
    while candidate <= now:
        candidate += interval
    return candidate


def _add_month(value: datetime) -> datetime:
    month = value.month + 1
    year = value.year
    if month > 12:
        month = 1
        year += 1
    day = min(value.day, _last_day_of_month(year, month))
    return value.replace(year=year, month=month, day=day)


def _last_day_of_month(year: int, month: int) -> int:
    if month == 12:
        return 31
    next_month = datetime(year, month + 1, 1, tzinfo=KST)
    return int((next_month - timedelta(days=1)).day)


def _parse_run_time(value: str) -> time:
    hour_text, _, minute_text = (value or "00:00").partition(":")
    return time(hour=int(hour_text or 0), minute=int(minute_text or 0))


def _format_run_time(value: str) -> str:
    hour_text, _, minute_text = (value or "00:00").partition(":")
    hour = int(hour_text or 0)
    minute = int(minute_text or 0)
    if minute == 0:
        return f"{hour:02d}시"
    return f"{hour:02d}시 {minute:02d}분"


def _report_schedule(report: dict[str, Any]) -> dict[str, Any]:
    value = report.get("schedule_json")
    parsed = _json_dict(value)
    if parsed is not None:
        return parsed
    return {
        "type": str(report.get("frequency") or "daily"),
        "time": str(report.get("run_time") or "00:00"),
        "timezone": "Asia/Seoul",
    }


def _report_query(report: dict[str, Any]) -> dict[str, Any]:
    value = report.get("query_json")
    parsed = _json_dict(value)
    if parsed is not None:
        return parsed
    subject_type = str(report.get("subject_type") or "user")
    return {
        "dataset": "attendance",
        "period": str(report.get("period_type") or "today"),
        "group_by": "alliance" if subject_type == "alliance" else "none",
        "rank_target": subject_type,
        "metric": "attendance_count",
        "limit": 10,
    }


def _report_render(report: dict[str, Any]) -> dict[str, Any]:
    value = report.get("render_json")
    parsed = _json_dict(value)
    if parsed is not None:
        return parsed
    return {
        "output": "grouped_ranking",
        "title": str(report.get("report_name") or "통계 알림"),
        "group_header": "{group_name}",
        "row": "{rank}. {label} - {value}회",
        "empty": "출석 기록 없음",
    }


def _json_dict(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            return parsed
    return None


def _parse_datetime(value: object) -> datetime | None:
    if not value:
        return None
    text = str(value)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z"):
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=KST)
            return parsed.astimezone(KST)
        except ValueError:
            continue
    return None


def _format_datetime(value: datetime) -> str:
    return value.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")


def _day_start(value: Any) -> datetime:
    return datetime.combine(value, time.min, tzinfo=KST)


def _day_end(value: Any) -> datetime:
    return datetime.combine(value, time(23, 59, 59), tzinfo=KST)


def _clip(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 1)] + "…"
