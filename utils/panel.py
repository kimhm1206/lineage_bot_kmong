from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import discord

from db import GuildSettings, get_settings


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
UI_STATE_PATH = DATA_DIR / "ui_state.json"
KST = timezone(timedelta(hours=9))
VERSION_LABEL = "Ver1.3"


def build_admin_panel_embed(
    bot: discord.Bot,
    guild: discord.Guild,
    settings: GuildSettings,
) -> discord.Embed:
    embed = discord.Embed(
        title="출석 패널",
        description="출석 시스템 설정과 버튼을 여기에서 관리합니다.",
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="출석 채널",
        value=_format_channel(guild, settings.admin_channel_id),
        inline=False,
    )
    embed.add_field(
        name="출석 음성채널",
        value=_format_channel(guild, settings.attendance_voice_channel_id),
        inline=False,
    )
    embed.add_field(
        name="타이머",
        value=_format_timer(settings.timer),
        inline=False,
    )
    embed.set_footer(text=_build_runtime_footer(bot))
    return embed


def build_attendance_embed(
    guild: discord.Guild,
    voice_channel: discord.abc.GuildChannel,
    seconds: int,
) -> discord.Embed:
    minutes = seconds // 60
    embed = discord.Embed(
        title="출석 진행 중",
        description="아래 버튼을 누르거나 `/출석` 명령어로 참여할 수 있습니다.",
        color=discord.Color.green(),
    )
    embed.add_field(name="대상 음성채널", value=voice_channel.mention, inline=False)
    embed.add_field(name="타이머", value=_format_timer(seconds), inline=False)
    if minutes > 0:
        embed.set_footer(text=f"약 {minutes}분 동안 출석이 진행됩니다.")
    else:
        embed.set_footer(text="초 단위 출석이 진행됩니다.")
    return embed


async def rebuild_admin_panel(bot: discord.Bot, guild_id: int) -> discord.Message | None:
    from views.admin_panel import AdminPanelView

    guild = bot.get_guild(guild_id)
    if guild is None:
        return None

    settings = get_settings(guild_id)
    if settings.admin_channel_id is None:
        _clear_panel_state(bot, guild_id)
        return None

    channel = guild.get_channel(settings.admin_channel_id)
    if not isinstance(channel, discord.TextChannel):
        _clear_panel_state(bot, guild_id)
        return None

    await delete_bot_messages(channel, bot.user.id if bot.user else 0)

    embed = build_admin_panel_embed(bot, guild, settings)
    view = AdminPanelView(bot, guild_id)
    message = await channel.send(embed=embed, view=view)

    _set_panel_state(bot, guild_id, channel.id, message.id)
    return message


async def update_admin_panel(bot: discord.Bot, guild_id: int) -> discord.Message | None:
    from views.admin_panel import AdminPanelView

    guild = bot.get_guild(guild_id)
    if guild is None:
        return None

    settings = get_settings(guild_id)
    if settings.admin_channel_id is None:
        _clear_panel_state(bot, guild_id)
        return None

    channel = guild.get_channel(settings.admin_channel_id)
    if not isinstance(channel, discord.TextChannel):
        _clear_panel_state(bot, guild_id)
        return None

    panel_state = _get_panel_state(bot, guild_id)
    panel_message_id = panel_state.get("message_id")
    panel_channel_id = panel_state.get("channel_id")

    if panel_message_id is None or panel_channel_id != channel.id:
        return await rebuild_admin_panel(bot, guild_id)

    try:
        message = await channel.fetch_message(panel_message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return await rebuild_admin_panel(bot, guild_id)

    embed = build_admin_panel_embed(bot, guild, settings)
    view = AdminPanelView(bot, guild_id)
    await message.edit(embed=embed, view=view)
    return message


async def clear_old_admin_panel(
    bot: discord.Bot, guild: discord.Guild, channel_id: int | None
) -> None:
    if channel_id is None:
        return

    channel = guild.get_channel(channel_id)
    if isinstance(channel, discord.TextChannel):
        await delete_bot_messages(channel, bot.user.id if bot.user else 0)


async def delete_bot_messages(
    channel: discord.TextChannel, bot_user_id: int, limit: int = 50
) -> None:
    async for message in channel.history(limit=limit):
        if message.author.id != bot_user_id:
            continue
        try:
            await message.delete()
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            continue


async def delete_attendance_message(
    bot: discord.Bot,
    guild_id: int,
    *,
    channel_id: int | None = None,
    message_id: int | None = None,
) -> None:
    if channel_id is None or message_id is None:
        state = get_attendance_state(bot, guild_id)
        channel_id = state.get("channel_id")
        message_id = state.get("message_id")

    if not isinstance(channel_id, int) or not isinstance(message_id, int):
        return

    guild = bot.get_guild(guild_id)
    if guild is None:
        return

    channel = guild.get_channel(channel_id)
    if channel is None or not hasattr(channel, "fetch_message"):
        return

    try:
        message = await channel.fetch_message(message_id)
        await message.delete()
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return


def _format_channel(guild: discord.Guild, channel_id: int | None) -> str:
    if channel_id is None:
        return "미설정"

    channel = guild.get_channel(channel_id)
    if channel is None:
        return "미설정"

    return channel.mention


def _format_timer(seconds: int | None) -> str:
    if seconds is None:
        return "미설정"
    if seconds % 60 == 0:
        return f"{seconds}초 ({seconds // 60}분)"
    return f"{seconds}초"


def _get_panel_state(bot: discord.Bot, guild_id: int) -> dict[str, int | None]:
    panel_state_by_guild = getattr(bot, "panel_state_by_guild", None)
    if panel_state_by_guild is None:
        panel_state_by_guild = {}
        bot.panel_state_by_guild = panel_state_by_guild

    state = panel_state_by_guild.get(guild_id)
    if state is None:
        persisted = _load_persisted_panel_state(guild_id)
        state = {
            "channel_id": persisted.get("channel_id"),
            "message_id": persisted.get("message_id"),
            "thread_id": persisted.get("thread_id"),
        }
        panel_state_by_guild[guild_id] = state
    return state


def _set_panel_state(
    bot: discord.Bot, guild_id: int, channel_id: int, message_id: int
) -> None:
    state = _get_panel_state(bot, guild_id)
    previous_message_id = state.get("message_id")
    state["channel_id"] = channel_id
    state["message_id"] = message_id
    if previous_message_id != message_id:
        state["thread_id"] = None
    _save_persisted_panel_state(guild_id, state)


def get_saved_log_thread_id(bot: discord.Bot, guild_id: int) -> int | None:
    state = _get_panel_state(bot, guild_id)
    thread_id = state.get("thread_id")
    return thread_id if isinstance(thread_id, int) else None


def set_saved_log_thread_id(
    bot: discord.Bot, guild_id: int, thread_id: int | None
) -> None:
    state = _get_panel_state(bot, guild_id)
    state["thread_id"] = thread_id
    _save_persisted_panel_state(guild_id, state)


def _clear_panel_state(bot: discord.Bot, guild_id: int) -> None:
    panel_state_by_guild = getattr(bot, "panel_state_by_guild", None)
    if panel_state_by_guild is None:
        bot.panel_state_by_guild = {}
    else:
        panel_state_by_guild.pop(guild_id, None)

    _remove_persisted_panel_state(guild_id)


def get_attendance_state(bot: discord.Bot, guild_id: int) -> dict[str, object | None]:
    attendance_state_by_guild = getattr(bot, "attendance_state_by_guild", None)
    if attendance_state_by_guild is None:
        attendance_state_by_guild = {}
        bot.attendance_state_by_guild = attendance_state_by_guild

    state = attendance_state_by_guild.get(guild_id)
    if state is None:
        state = {
            "active": False,
            "channel_id": None,
            "message_id": None,
            "task": None,
            "started_by": None,
            "started_at": None,
            "participants": set(),
        }
        attendance_state_by_guild[guild_id] = state
    return state


def set_attendance_state(
    bot: discord.Bot,
    guild_id: int,
    *,
    channel_id: int,
    message_id: int,
    task: asyncio.Task[None] | None,
    started_by: int | None,
    started_at: object | None,
) -> None:
    state = get_attendance_state(bot, guild_id)
    state["active"] = True
    state["channel_id"] = channel_id
    state["message_id"] = message_id
    state["task"] = task
    state["started_by"] = started_by
    state["started_at"] = started_at
    state["participants"] = set()


def clear_attendance_state(bot: discord.Bot, guild_id: int) -> None:
    state = get_attendance_state(bot, guild_id)
    task = state.get("task")
    current_task = asyncio.current_task()
    if (
        isinstance(task, asyncio.Task)
        and not task.done()
        and task is not current_task
    ):
        task.cancel()

    state["active"] = False
    state["channel_id"] = None
    state["message_id"] = None
    state["task"] = None
    state["started_by"] = None
    state["started_at"] = None
    state["participants"] = set()


async def get_panel_message(
    bot: discord.Bot, guild_id: int
) -> discord.Message | None:
    guild = bot.get_guild(guild_id)
    if guild is None:
        return None

    panel_state = _get_panel_state(bot, guild_id)
    panel_message_id = panel_state.get("message_id")
    panel_channel_id = panel_state.get("channel_id")
    if not isinstance(panel_message_id, int) or not isinstance(panel_channel_id, int):
        return None

    channel = guild.get_channel(panel_channel_id)
    if not isinstance(channel, discord.TextChannel):
        return None

    try:
        return await channel.fetch_message(panel_message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None


def _build_runtime_footer(bot: discord.Bot) -> str:
    runtime_label = getattr(bot, "runtime_label", None)
    if isinstance(runtime_label, str) and runtime_label:
        return runtime_label
    return f"{datetime.now(KST).strftime('%Y-%m-%d %H:%M')} 구동 {VERSION_LABEL}"


def _load_ui_state() -> dict[str, dict[str, int | None]]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not UI_STATE_PATH.exists():
        return {}

    try:
        with UI_STATE_PATH.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except (json.JSONDecodeError, OSError):
        return {}

    if not isinstance(data, dict):
        return {}
    return data


def _write_ui_state(data: dict[str, dict[str, int | None]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with UI_STATE_PATH.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def _load_persisted_panel_state(guild_id: int) -> dict[str, int | None]:
    data = _load_ui_state()
    raw_state = data.get(str(guild_id), {})
    if not isinstance(raw_state, dict):
        return {}
    return {
        "channel_id": _coerce_int(raw_state.get("channel_id")),
        "message_id": _coerce_int(raw_state.get("message_id")),
        "thread_id": _coerce_int(raw_state.get("thread_id")),
    }


def _save_persisted_panel_state(guild_id: int, state: dict[str, int | None]) -> None:
    data = _load_ui_state()
    data[str(guild_id)] = {
        "channel_id": _coerce_int(state.get("channel_id")),
        "message_id": _coerce_int(state.get("message_id")),
        "thread_id": _coerce_int(state.get("thread_id")),
    }
    _write_ui_state(data)


def _remove_persisted_panel_state(guild_id: int) -> None:
    data = _load_ui_state()
    if str(guild_id) not in data:
        return
    data.pop(str(guild_id), None)
    _write_ui_state(data)


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
