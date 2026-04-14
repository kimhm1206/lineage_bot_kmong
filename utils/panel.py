from __future__ import annotations

import asyncio

import discord

from db import GuildSettings, get_settings


def build_admin_panel_embed(
    guild: discord.Guild, settings: GuildSettings
) -> discord.Embed:
    embed = discord.Embed(
        title="출석 관리자 패널",
        description="출석 시스템 설정과 버튼을 여기에서 관리합니다.",
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="관리자 채널",
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
    embed.set_footer(text=f"{guild.name} 전용 관리자 UI")
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

    embed = build_admin_panel_embed(guild, settings)
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

    embed = build_admin_panel_embed(guild, settings)
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
        state = {"channel_id": None, "message_id": None}
        panel_state_by_guild[guild_id] = state
    return state


def _set_panel_state(
    bot: discord.Bot, guild_id: int, channel_id: int, message_id: int
) -> None:
    state = _get_panel_state(bot, guild_id)
    state["channel_id"] = channel_id
    state["message_id"] = message_id


def _clear_panel_state(bot: discord.Bot, guild_id: int) -> None:
    panel_state_by_guild = getattr(bot, "panel_state_by_guild", None)
    if panel_state_by_guild is None:
        bot.panel_state_by_guild = {}
        return

    panel_state_by_guild.pop(guild_id, None)


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
