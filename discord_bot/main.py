import asyncio
import io
import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone

import discord
from dotenv import load_dotenv

from common import database
from discord_bot.bridge import start_web_bridge
from discord_bot.queue import start_command_queue_worker
from discord_bot.reports import start_report_scheduler
from discord_bot.utils.attendance import (
    AttendanceActionView,
    register_attendance,
    seed_voice_entry_times,
    sync_voice_entry_time,
)
from discord_bot.utils.guild import is_admin_member, is_supported_guild
from discord_bot.utils.panel import VERSION_LABEL, update_admin_panel
from discord_bot.views.admin_panel import AdminPanelView


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
bot.command_queue_task = None
bot.report_scheduler = None
bot.report_scheduler_reload_task = None
bot.web_bridge_task = None
bot.web_bridge_ws = None
bot.attendance_state_publisher = None
bot.runtime_label = (
    f"{datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M')} "
    f"구동 {VERSION_LABEL}"
)

ALLIANCE_LABEL_PATTERN = re.compile(r"\[([^\[\]]+)\]")


@bot.event
async def on_ready() -> None:
    if not bot.persistent_views_registered:
        _register_persistent_views()
        bot.persistent_views_registered = True

    if not bot.commands_synced:
        await bot.sync_commands()
        bot.commands_synced = True

    start_command_queue_worker(bot)
    start_report_scheduler(bot)
    start_web_bridge(bot)

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
        database.get_user_attendance_stats,
        guild.id,
        start.strftime("%Y-%m-%d %H:%M:%S"),
        now.strftime("%Y-%m-%d %H:%M:%S"),
        None,
        None,
        50,
    )

    await ctx.followup.send(_build_weekly_ranking_message(guild, rows, start, now))


@bot.slash_command(
    name="역할매칭",
    description="닉네임 혈맹 표기와 혈맹 역할 매핑이 다른 유저를 조회합니다.",
)
async def role_matching(ctx: discord.ApplicationContext) -> None:
    guild = ctx.guild
    author = ctx.author
    if guild is None or not isinstance(author, discord.Member):
        await ctx.respond("서버에서만 사용할 수 있습니다.", ephemeral=True)
        return

    if not is_supported_guild(bot, guild.id):
        await ctx.respond("이 서버에서는 사용할 수 없습니다.", ephemeral=True)
        return

    if not is_admin_member(author):
        await ctx.respond("권한이 없습니다.", ephemeral=True)
        return

    await ctx.defer(ephemeral=True)

    mappings = await asyncio.to_thread(database.get_guild_alliance_role_mappings, guild.id)
    role_alliance_by_id = {
        int(mapping["role_id"]): str(mapping["alliance_name"])
        for mapping in mappings
    }
    if not role_alliance_by_id:
        await ctx.followup.send("등록된 혈맹 역할 매핑이 없습니다.", ephemeral=True)
        return

    await _ensure_member_cache(guild)
    mismatches = _find_role_nickname_mismatches(guild, role_alliance_by_id)
    message = _build_role_matching_message(guild, mismatches)
    if len(message) <= 1900:
        await ctx.followup.send(message, ephemeral=True)
        return

    attachment = discord.File(
        io.BytesIO(message.encode("utf-8")),
        filename=f"role_matching_{guild.id}.txt",
    )
    await ctx.followup.send(
        (
            f"닉네임 혈맹과 역할 매핑이 다른 유저가 {len(mismatches)}명입니다.\n"
            "목록이 길어서 파일로 첨부합니다."
        ),
        file=attachment,
        ephemeral=True,
    )


@bot.event
async def on_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
) -> None:
    sync_voice_entry_time(
        bot,
        member.guild.id,
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


async def _ensure_member_cache(guild: discord.Guild) -> None:
    try:
        await guild.chunk(cache=True)
    except Exception as exc:
        print(f"멤버 캐시 갱신 실패: guild={guild.id} error={exc}")


def _find_role_nickname_mismatches(
    guild: discord.Guild,
    role_alliance_by_id: dict[int, str],
) -> list[dict[str, str]]:
    mismatches: list[dict[str, str]] = []
    for member in sorted(guild.members, key=lambda item: item.display_name):
        if member.bot:
            continue

        mapped_alliances = _mapped_alliances_for_member(member, role_alliance_by_id)
        if not mapped_alliances:
            continue

        role_alliance = mapped_alliances[0]
        nickname_alliance = _extract_nickname_alliance(member.display_name)
        if _normalize_alliance_label(nickname_alliance) == _normalize_alliance_label(role_alliance):
            continue

        mismatches.append(
            {
                "display_name": member.display_name,
                "discord_id": str(member.id),
                "nickname_alliance": nickname_alliance or "없음",
                "role_alliance": role_alliance,
                "all_role_alliances": ", ".join(dict.fromkeys(mapped_alliances)),
            }
        )
    return mismatches


def _mapped_alliances_for_member(
    member: discord.Member,
    role_alliance_by_id: dict[int, str],
) -> list[str]:
    alliances: list[str] = []
    for role in sorted(
        member.roles,
        key=lambda item: (item.position, item.id),
        reverse=True,
    ):
        if role.is_default():
            continue
        alliance = role_alliance_by_id.get(int(role.id))
        if alliance is not None:
            alliances.append(alliance)
    return alliances


def _extract_nickname_alliance(display_name: str) -> str | None:
    match = ALLIANCE_LABEL_PATTERN.search(display_name)
    if match is None:
        return None
    value = match.group(1).strip()
    return value or None


def _normalize_alliance_label(value: str | None) -> str:
    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKC", value)
    return "".join(normalized.split()).casefold()


def _build_role_matching_message(
    guild: discord.Guild,
    mismatches: list[dict[str, str]],
) -> str:
    lines = [
        "**역할 매칭 점검**",
        f"서버: {guild.name}",
        f"불일치: {len(mismatches)}명",
        "",
    ]
    if not mismatches:
        lines.append("닉네임 혈맹 표기와 역할 매핑이 다른 유저가 없습니다.")
        return "\n".join(lines)

    lines.extend(
        [
            "기준: 혈맹 역할 매핑 중 가장 높은 역할",
            "",
            "```text",
            "닉네임혈맹 -> 역할혈맹 | 유저",
        ]
    )
    for row in mismatches:
        role_alliance = row["role_alliance"]
        if row["all_role_alliances"] != role_alliance:
            role_alliance = f"{role_alliance} ({row['all_role_alliances']})"
        lines.append(
            (
                f"{row['nickname_alliance']} -> {role_alliance} | "
                f"{row['display_name']} ({row['discord_id']})"
            )
        )
    lines.append("```")
    return "\n".join(lines)


def _clip_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    if limit <= 1:
        return value[:limit]
    return value[: limit - 1] + "…"


def main() -> None:
    database.init_schema()
    bot.run(TOKEN)


if __name__ == "__main__":
    main()
