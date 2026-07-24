from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import discord

from discord_bot.storage import GuildSettings
from discord_bot.runtime_settings import cached_guild_settings


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
UI_STATE_PATH = DATA_DIR / "ui_state.json"
KST = timezone(timedelta(hours=9))
VERSION_PATH = BOT_DIR / "version.txt"
VERSION_PATTERN = re.compile(r"^\d{1,2}\.\d$")
SUPPORT_DISCORD_ID = 238978205078388747


def build_admin_panel_embed(
    bot: discord.Bot,
    guild: discord.Guild,
    settings: GuildSettings,
) -> discord.Embed:
    embed = discord.Embed(
        title="출석 패널",
        description="출석 시작은 여기서, 설정과 통계는 웹에서 관리합니다.",
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="출석 채널",
        value=_format_channel(guild, settings.admin_channel_id),
        inline=False,
    )
    embed.add_field(
        name="출석 음성채널",
        value=_format_channels(guild, settings.attendance_voice_channel_ids),
        inline=False,
    )
    embed.add_field(
        name="로그채널",
        value=_format_channel(guild, settings.log_channel_id),
        inline=False,
    )
    embed.add_field(
        name="출석 확인 타이머",
        value=_format_timer(settings.timer),
        inline=False,
    )
    embed.add_field(
        name="출석 가능 타이머",
        value=_format_timer(settings.attendance_available_timer),
        inline=False,
    )
    embed.set_footer(text=_build_runtime_footer(bot))
    return embed


def build_attendance_embed(
    guild: discord.Guild,
    voice_channels: list[discord.abc.GuildChannel],
    seconds: int,
) -> discord.Embed:
    minutes = seconds // 60
    embed = discord.Embed(
        title="출석 진행 중",
        description="아래 버튼을 누르거나 `/출석` 명령어로 참여할 수 있습니다.",
        color=discord.Color.green(),
    )
    embed.add_field(
        name="대상 음성채널",
        value="\n".join(channel.mention for channel in voice_channels),
        inline=False,
    )
    embed.add_field(name="타이머", value=_format_timer(seconds), inline=False)
    embed.add_field(
        name="문의",
        value=(
            f"<@{SUPPORT_DISCORD_ID}>\n"
            "출석 중 에러 발생 및 문의사항은 위 유저에게 DM 보내주세요."
        ),
        inline=False,
    )
    if minutes > 0:
        embed.set_footer(text=f"약 {minutes}분 동안 출석이 진행됩니다.")
    else:
        embed.set_footer(text="초 단위 출석이 진행됩니다.")
    return embed


async def rebuild_admin_panel(bot: discord.Bot, guild_id: int) -> discord.Message | None:
    async with _get_panel_lock(bot, guild_id):
        return await _rebuild_admin_panel(bot, guild_id)


async def _rebuild_admin_panel(bot: discord.Bot, guild_id: int) -> discord.Message | None:
    from discord_bot.views.admin_panel import AdminPanelView

    guild = bot.get_guild(guild_id)
    if guild is None:
        return None

    settings = cached_guild_settings(bot, guild_id)
    if settings is None:
        return None
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
    async with _get_panel_lock(bot, guild_id):
        return await _update_admin_panel(bot, guild_id)


async def _update_admin_panel(bot: discord.Bot, guild_id: int) -> discord.Message | None:
    from discord_bot.views.admin_panel import AdminPanelView

    guild = bot.get_guild(guild_id)
    if guild is None:
        return None

    settings = cached_guild_settings(bot, guild_id)
    if settings is None:
        return None
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
        return await _rebuild_admin_panel(bot, guild_id)

    try:
        message = await channel.fetch_message(panel_message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return await _rebuild_admin_panel(bot, guild_id)

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
    if not isinstance(channel, discord.TextChannel):
        return

    panel_state = _get_panel_state(bot, guild.id)
    message_id = panel_state.get("message_id")
    panel_channel_id = panel_state.get("channel_id")
    if not isinstance(message_id, int) or panel_channel_id != channel_id:
        return

    try:
        message = await channel.fetch_message(message_id)
        await message.delete()
    except (discord.Forbidden, discord.NotFound, discord.HTTPException):
        pass
    finally:
        _clear_panel_state(bot, guild.id)


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


async def clear_duplicate_admin_panels(
    channel: discord.TextChannel,
    bot_user_id: int,
    *,
    keep_message_id: int,
    limit: int = 100,
) -> None:
    async for message in channel.history(limit=limit):
        if message.id == keep_message_id or message.author.id != bot_user_id:
            continue
        if not any(embed.title == "출석 패널" for embed in message.embeds):
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


def _format_channels(guild: discord.Guild, channel_ids: tuple[int, ...]) -> str:
    mentions = [
        _format_channel(guild, channel_id)
        for channel_id in channel_ids
        if channel_id is not None
    ]
    return "\n".join(mentions) if mentions else "미설정"


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
        }
        panel_state_by_guild[guild_id] = state
    return state


def _get_panel_lock(bot: discord.Bot, guild_id: int) -> asyncio.Lock:
    locks = getattr(bot, "panel_locks", None)
    if not isinstance(locks, dict):
        locks = {}
        bot.panel_locks = locks
    lock = locks.get(guild_id)
    if not isinstance(lock, asyncio.Lock):
        lock = asyncio.Lock()
        locks[guild_id] = lock
    return lock


def _set_panel_state(
    bot: discord.Bot, guild_id: int, channel_id: int, message_id: int
) -> None:
    state = _get_panel_state(bot, guild_id)
    state["channel_id"] = channel_id
    state["message_id"] = message_id
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
            "expires_at": None,
            "live_session_id": None,
            "voice_channel_id": None,
            "voice_channel_ids": [],
            "attendance_available_timer": None,
            "participants": set(),
            "participant_times": {},
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
    expires_at: object | None = None,
    live_session_id: int | None = None,
    voice_channel_id: int | None = None,
    voice_channel_ids: list[int] | tuple[int, ...] | None = None,
    attendance_available_timer: int | None = None,
) -> None:
    state = get_attendance_state(bot, guild_id)
    state["active"] = True
    state["channel_id"] = channel_id
    state["message_id"] = message_id
    state["task"] = task
    state["started_by"] = started_by
    state["started_at"] = started_at
    state["expires_at"] = expires_at
    state["live_session_id"] = live_session_id
    state["voice_channel_id"] = voice_channel_id
    state["voice_channel_ids"] = list(voice_channel_ids or [])
    state["attendance_available_timer"] = attendance_available_timer
    state["participants"] = set()
    state["participant_times"] = {}


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
    state["expires_at"] = None
    state["live_session_id"] = None
    state["voice_channel_id"] = None
    state["voice_channel_ids"] = []
    state["attendance_available_timer"] = None
    state["participants"] = set()
    state["participant_times"] = {}


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
    return build_runtime_label()


def build_runtime_label() -> str:
    started_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    version = _load_version_label()
    if version is None:
        return f"{started_at} 구동"
    return f"{started_at} 구동 {version}"


def _load_version_label() -> str | None:
    try:
        version = VERSION_PATH.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return version if VERSION_PATTERN.fullmatch(version) else None


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
    }


def _save_persisted_panel_state(guild_id: int, state: dict[str, int | None]) -> None:
    data = _load_ui_state()
    data[str(guild_id)] = {
        "channel_id": _coerce_int(state.get("channel_id")),
        "message_id": _coerce_int(state.get("message_id")),
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
