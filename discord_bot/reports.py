from __future__ import annotations

import asyncio
from datetime import datetime, time, timedelta, timezone
from typing import Any

import discord
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

from common import database


KST = timezone(timedelta(hours=9))
RETRY_DELAY = timedelta(minutes=10)
FREQUENCY_DAYS = {
    "daily": 1,
    "every_3_days": 3,
    "weekly": 7,
}
PERIOD_LABELS = {
    "recent_7_days": "최근 일주일",
    "yesterday": "전날",
    "recent_3_days": "최근 3일",
}
SUBJECT_LABELS = {
    "user": "유저별",
    "alliance": "혈맹별",
}
RESULT_LABELS = {
    "ranking": "순위",
    "all": "전체",
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
    bot.report_scheduler_reload_task = asyncio.create_task(reload_report_schedules(bot))


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
        next_run_at = _next_run_from_now(
            now,
            str(report["run_time"] or "00:00"),
            str(report["frequency"]),
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
        str(report["run_time"] or "00:00"),
        str(report["frequency"]),
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
    start_at, end_at = _period_bounds(str(report["period_type"]), now)
    subject_type = str(report["subject_type"])
    result_type = str(report["result_type"])

    if subject_type == "alliance":
        rows = await asyncio.to_thread(
            database.get_alliance_attendance_stats,
            guild_id,
            _format_datetime(start_at),
            _format_datetime(end_at),
            None,
        )
        table = _format_alliance_rows(rows, result_type)
        total_count = len(rows)
    else:
        fetch_limit = 30 if result_type == "all" else 20
        rows = await asyncio.to_thread(
            database.get_user_attendance_stats,
            guild_id,
            _format_datetime(start_at),
            _format_datetime(end_at),
            None,
            None,
            fetch_limit,
        )
        table = _format_user_rows(rows, result_type)
        total_count = len(rows)

    title_suffix = "랭킹" if result_type == "ranking" else "전체"
    lines = [
        f"**통계 알림 - {SUBJECT_LABELS.get(subject_type, subject_type)} {title_suffix}**",
        f"서버: {guild_name}",
        (
            f"기간: {_format_datetime(start_at)} ~ "
            f"{_format_datetime(end_at)}"
        ),
        (
            f"설정: {PERIOD_LABELS.get(str(report['period_type']), str(report['period_type']))} · "
            f"{RESULT_LABELS.get(result_type, result_type)} · "
            f"#{report['channel_name']}"
        ),
        f"표시: {total_count}건",
        "",
        "```text",
        table,
        "```",
    ]
    if result_type == "all" and total_count >= 30 and subject_type == "user":
        lines.append("Discord 메시지 길이를 고려해 최대 30명까지 표시합니다.")
    return "\n".join(lines)


def _format_user_rows(rows: list[dict[str, Any]], result_type: str) -> str:
    if not rows:
        return "조건에 맞는 출석 기록이 없습니다."
    limit = 30 if result_type == "all" else 20
    lines = ["순위 | 닉네임 | 혈맹 | 출석", "--- | --- | --- | ---"]
    for index, row in enumerate(rows[:limit], start=1):
        nickname = _clip(str(row.get("discord_nickname") or "-"), 18)
        alliance = _clip(str(row.get("alliance_name") or "미분류"), 10)
        count = int(row.get("attendance_count") or 0)
        lines.append(f"{index} | {nickname} | {alliance} | {count}회")
    return "\n".join(lines)


def _format_alliance_rows(rows: list[dict[str, Any]], result_type: str) -> str:
    if not rows:
        return "조건에 맞는 혈맹 기록이 없습니다."
    limit = 30 if result_type == "all" else 20
    lines = ["순위 | 혈맹 | 회차 | 누적 | 인원", "--- | --- | --- | --- | ---"]
    for index, row in enumerate(rows[:limit], start=1):
        alliance = _clip(str(row.get("alliance_name") or "미분류"), 14)
        session_count = int(row.get("session_count") or 0)
        attendance_count = int(row.get("attendance_count") or 0)
        user_count = int(row.get("unique_user_count") or 0)
        lines.append(
            f"{index} | {alliance} | {session_count} | {attendance_count} | {user_count}"
        )
    return "\n".join(lines)


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
    return _day_start(today - timedelta(days=6)), _day_end(today)


def _next_run_from_now(now: datetime, run_time: str, frequency: str) -> datetime:
    run_clock = _parse_run_time(run_time)
    candidate = datetime.combine(now.date(), run_clock, tzinfo=KST)
    interval = timedelta(days=FREQUENCY_DAYS.get(frequency, 1))
    while candidate <= now:
        candidate += interval
    return candidate


def _parse_run_time(value: str) -> time:
    hour_text, _, minute_text = (value or "00:00").partition(":")
    return time(hour=int(hour_text or 0), minute=int(minute_text or 0))


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
