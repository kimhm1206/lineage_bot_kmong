import asyncio
import os
from datetime import datetime, timedelta, timezone

import discord
from dotenv import load_dotenv

from db import get_settings, get_user_attendance_stats, init_db, update_setting
from utils.attendance import (
    AttendanceActionView,
    register_attendance,
    seed_voice_entry_times,
    sync_voice_entry_time,
)
from utils.guild import is_admin_member, is_supported_guild
from utils.panel import clear_old_admin_panel, rebuild_admin_panel, update_admin_panel
from views.admin_panel import AdminPanelView


load_dotenv()

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_BOT_TOKEN이 .env 파일에 설정되어 있지 않습니다.")

intents = discord.Intents.none()
intents.guilds = True
intents.members = True
intents.voice_states = True
intents.messages = True
intents.message_content = True
bot = discord.Bot(intents=intents)
bot.panel_state_by_guild = {}
bot.attendance_state_by_guild = {}
bot.attendance_locks = {}
bot.voice_entry_times_by_guild = {}
bot.commands_synced = False
bot.persistent_views_registered = False
bot.runtime_label = (
    f"{datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M')} "
    "구동 Ver1.3"
)


@bot.event
async def on_ready() -> None:
    if not bot.persistent_views_registered:
        _register_persistent_views()
        bot.persistent_views_registered = True

    if not bot.commands_synced:
        await bot.sync_commands()
        bot.commands_synced = True

    for guild in bot.guilds:
        seed_voice_entry_times(bot, guild)
        await update_admin_panel(bot, guild.id)

    guild_names = ", ".join(guild.name for guild in bot.guilds) or "No Guild"
    print(f"봇 로그인 완료: {bot.user} | 길드: {guild_names}")


def _register_persistent_views() -> None:
    for guild in bot.guilds:
        bot.add_view(AdminPanelView(bot, guild.id))
        bot.add_view(AttendanceActionView(bot, guild.id))


@bot.slash_command(
    name="관리자채널",
    description="출석 채널을 설정합니다.",
)
async def set_admin_channel(
    ctx: discord.ApplicationContext,
    channel: discord.Option(discord.TextChannel, description="출석 채널"),
) -> None:
    guild = ctx.guild
    if guild is None:
        await ctx.respond("서버에서만 사용할 수 있습니다.", ephemeral=True)
        return

    if not is_supported_guild(bot, guild.id):
        await ctx.respond("이 서버에서는 사용할 수 없습니다.", ephemeral=True)
        return

    if not is_admin_member(ctx.author):
        await ctx.respond("권한이 없습니다.", ephemeral=True)
        return

    await ctx.defer(ephemeral=True)

    previous_settings = get_settings(guild.id)
    previous_admin_channel_id = previous_settings.admin_channel_id

    update_setting(guild.id, "admin_channel_id", channel.id)
    await clear_old_admin_panel(bot, guild, previous_admin_channel_id)
    await rebuild_admin_panel(bot, guild.id)

    await ctx.followup.send(
        f"출석 채널이 {channel.mention}으로 설정되었습니다.",
        ephemeral=True,
    )


@bot.slash_command(
    name="출석",
    description="진행 중인 출석에 참여합니다.",
)
async def attend(ctx: discord.ApplicationContext) -> None:
    guild = ctx.guild
    author = ctx.author
    if guild is None or not isinstance(author, discord.Member):
        await ctx.respond("서버에서만 사용할 수 있습니다.", ephemeral=True)
        return

    _, message = await register_attendance(bot, guild.id, author)
    await ctx.respond(message, ephemeral=True)


@bot.slash_command(
    name="일주일",
    description="현재 서버의 최근 7일 유저별 출석 랭킹을 봅니다.",
)
async def weekly_ranking(ctx: discord.ApplicationContext) -> None:
    guild = ctx.guild
    if guild is None:
        await ctx.respond("서버에서만 사용할 수 있습니다.", ephemeral=True)
        return

    if not is_supported_guild(bot, guild.id):
        await ctx.respond("이 서버에서는 사용할 수 없습니다.", ephemeral=True)
        return

    await ctx.defer()

    now = datetime.now(timezone(timedelta(hours=9)))
    start = (now - timedelta(days=7)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    rows = await asyncio.to_thread(
        get_user_attendance_stats,
        guild.id,
        start.strftime("%Y-%m-%d %H:%M:%S"),
        now.strftime("%Y-%m-%d %H:%M:%S"),
        None,
        None,
        50,
    )

    await ctx.followup.send(_build_weekly_ranking_message(guild, rows, start, now))


@bot.event
async def on_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
) -> None:
    guild = member.guild
    sync_voice_entry_time(
        bot,
        guild.id,
        member.id,
        getattr(before.channel, "id", None),
        getattr(after.channel, "id", None),
    )


def _build_weekly_ranking_message(
    guild: discord.Guild,
    rows: list[dict[str, object]],
    start: datetime,
    end: datetime,
) -> str:
    lines = [
        "**일주일 출석 랭킹**",
        "조회 서버: 현재 서버",
        f"서버: {guild.name}",
        (
            f"기간: {start.strftime('%Y-%m-%d %H:%M:%S')} ~ "
            f"{end.strftime('%Y-%m-%d %H:%M:%S')}"
        ),
        f"총 {len(rows)}명",
        "",
        "```text",
        _format_weekly_ranking_table(rows[:20]),
        "```",
    ]
    if len(rows) > 20:
        lines.append("상위 20명까지 표시합니다.")
    return "\n".join(lines)


def _format_weekly_ranking_table(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "조건에 맞는 출석 기록이 없습니다."

    lines = [
        "순위 | 닉네임 | 혈맹 | 출석횟수",
        "--- | --- | --- | ---",
    ]
    for index, row in enumerate(rows, start=1):
        nickname = _clip_text(str(row.get("discord_nickname", "")), 18)
        alliance = _clip_text(str(row.get("alliance_name", "미분류")), 10)
        count = int(row.get("attendance_count", 0))
        lines.append(f"{index} | {nickname} | {alliance} | {count}회")
    return "\n".join(lines)


def _clip_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    if limit <= 1:
        return value[:limit]
    return value[: limit - 1] + "…"


def main() -> None:
    init_db()
    bot.run(TOKEN)


if __name__ == "__main__":
    main()
