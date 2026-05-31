from __future__ import annotations

import asyncio
from typing import Any

import discord

from common import database
from discord_bot.reports import reload_report_schedules
from discord_bot.utils.attendance import (
    persist_attendance_snapshot,
    send_attendance_summary,
    send_ranker_attendance_ids,
    start_attendance,
    stop_attendance,
)
from discord_bot.utils.panel import clear_old_admin_panel, rebuild_admin_panel


POLL_INTERVAL_SECONDS = 2
COMMAND_BATCH_SIZE = 5


def start_command_queue_worker(bot: discord.Bot) -> None:
    existing_task = getattr(bot, "command_queue_task", None)
    if isinstance(existing_task, asyncio.Task) and not existing_task.done():
        return
    bot.command_queue_task = asyncio.create_task(_poll_command_queue(bot))


async def _poll_command_queue(bot: discord.Bot) -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            rows = await asyncio.to_thread(
                database.claim_bot_commands,
                COMMAND_BATCH_SIZE,
            )
            for row in rows:
                await _process_command(bot, row)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[command-queue] poll failed: {exc}")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def _process_command(bot: discord.Bot, row: dict[str, Any]) -> None:
    command_id = int(row["command_id"])
    command_type = str(row["command_type"])
    try:
        if command_type == "start_attendance":
            result = await _process_start_attendance(bot, row)
        elif command_type == "stop_attendance":
            result = await _process_stop_attendance(bot, row)
        elif command_type == "refresh_admin_panel":
            result = await _process_refresh_admin_panel(bot, row)
        elif command_type == "refresh_report_schedules":
            result = await reload_report_schedules(bot)
        else:
            raise RuntimeError(f"Unsupported command_type: {command_type}")
    except Exception as exc:
        await asyncio.to_thread(database.fail_bot_command, command_id, str(exc))
        return

    await asyncio.to_thread(database.complete_bot_command, command_id, result)


async def _process_start_attendance(
    bot: discord.Bot,
    row: dict[str, Any],
) -> dict[str, Any]:
    guild = _get_guild(bot, row)
    member = await _resolve_member(guild, row.get("requested_by_discord_id"))
    if member is None:
        raise RuntimeError("요청자를 Discord 서버에서 찾을 수 없습니다.")

    ok, message = await start_attendance(bot, guild, member)
    if not ok:
        raise RuntimeError(message)
    return {"message": message}


async def _process_stop_attendance(
    bot: discord.Bot,
    row: dict[str, Any],
) -> dict[str, Any]:
    guild = _get_guild(bot, row)
    member = await _resolve_member(guild, row.get("requested_by_discord_id"))
    payload = row.get("payload_json") or {}
    if not isinstance(payload, dict):
        payload = {}
    should_save = _payload_bool(payload.get("save_attendance"), default=True)
    result = await stop_attendance(
        bot,
        guild,
        stopped_by=member,
        reason="manual",
    )
    if not result["ok"]:
        raise RuntimeError(str(result["message"]))

    snapshot = result["snapshot"]
    stopped_by_mention = member.mention if member is not None else "웹"
    if not should_save:
        await send_attendance_summary(
            bot,
            guild,
            snapshot=snapshot,
            reason="manual",
            stopped_by_mention=stopped_by_mention,
            save_status="기록 저장 X",
        )
        return {
            "message": str(result["message"]),
            "attendance_id": None,
            "participant_count": int(result.get("participant_count") or 0),
            "saved": False,
        }

    attendance_id = await persist_attendance_snapshot(bot, guild, snapshot)
    await send_attendance_summary(
        bot,
        guild,
        snapshot=snapshot,
        reason="manual",
        stopped_by_mention=stopped_by_mention,
        save_status=f"DB 저장 완료: #{attendance_id}",
    )
    await send_ranker_attendance_ids(guild, snapshot)
    return {
        "message": str(result["message"]),
        "attendance_id": attendance_id,
        "participant_count": int(result.get("participant_count") or 0),
        "saved": True,
    }


async def _process_refresh_admin_panel(
    bot: discord.Bot,
    row: dict[str, Any],
) -> dict[str, Any]:
    guild = _get_guild(bot, row)
    payload = row.get("payload_json") or {}
    previous_admin_channel_id = _optional_int(payload.get("previous_admin_channel_id"))
    current_settings = database.get_settings(guild.id)
    if (
        previous_admin_channel_id is not None
        and previous_admin_channel_id != current_settings.admin_channel_id
    ):
        await clear_old_admin_panel(bot, guild, previous_admin_channel_id)
    message = await rebuild_admin_panel(bot, guild.id)
    return {
        "message": "settings refreshed",
        "panel_message_id": message.id if message is not None else None,
    }


def _get_guild(bot: discord.Bot, row: dict[str, Any]) -> discord.Guild:
    guild_id = row.get("guild_id")
    if guild_id is None:
        raise RuntimeError("guild_id가 없는 명령입니다.")
    guild = bot.get_guild(int(guild_id))
    if guild is None:
        raise RuntimeError(f"봇이 서버에 접속해 있지 않습니다. guild_id={guild_id}")
    return guild


async def _resolve_member(
    guild: discord.Guild,
    discord_id: object,
) -> discord.Member | None:
    if discord_id is None:
        return None
    member_id = int(discord_id)
    member = guild.get_member(member_id)
    if member is not None:
        return member
    try:
        return await guild.fetch_member(member_id)
    except (discord.Forbidden, discord.HTTPException, discord.NotFound):
        return None


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _payload_bool(value: object, *, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off"}
    return bool(value)
