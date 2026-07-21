import asyncio
import io
import os
import re
import time
import unicodedata
from datetime import datetime, timedelta, timezone

import discord
from dotenv import load_dotenv

from common import database
from discord_bot.utils.attendance import (
    AttendanceActionView,
    register_attendance,
    seed_voice_entry_times,
    sync_voice_entry_time,
)
from discord_bot.utils.guild import is_admin_member, is_supported_guild
from discord_bot.utils.panel import (
    build_runtime_label,
    clear_old_admin_panel,
    rebuild_admin_panel,
    update_admin_panel,
)
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
bot.attendance_state_publisher = None
bot.runtime_label = build_runtime_label()

ALLIANCE_LABEL_PATTERN = re.compile(r"\[([^\[\]]+)\]")
ROLE_MATCHING_FETCH_TIMEOUT_SECONDS = 120


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
    description="출석 패널과 출석 메시지를 보낼 채널을 설정합니다.",
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

    previous_settings = await asyncio.to_thread(database.get_settings, guild.id)
    await asyncio.to_thread(
        database.update_setting,
        guild.id,
        "admin_channel_id",
        channel.id,
    )
    await clear_old_admin_panel(bot, guild, previous_settings.admin_channel_id)
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

    await ctx.followup.send(
        "서버 전체 멤버를 조회해서 역할 매칭을 점검합니다. 잠시만 기다려주세요.",
        ephemeral=True,
    )
    try:
        started_at = time.monotonic()
        members = await asyncio.wait_for(
            _fetch_all_guild_members(guild),
            timeout=ROLE_MATCHING_FETCH_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        await ctx.followup.send(
            (
                "전체 멤버 조회가 120초 안에 끝나지 않아 중단했습니다.\n"
                "디스코드 멤버 조회 응답이 지연된 상태라 봇 재시작 후 다시 시도해주세요."
            ),
            ephemeral=True,
        )
        return
    except Exception as exc:
        await ctx.followup.send(
            f"전체 멤버 조회에 실패했습니다. {exc}",
            ephemeral=True,
        )
        return

    elapsed_seconds = time.monotonic() - started_at
    mismatches = _find_role_nickname_mismatches(members, role_alliance_by_id)
    message = _build_role_matching_message(
        guild,
        mismatches,
        checked_count=len(members),
        elapsed_seconds=elapsed_seconds,
    )
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
            f"점검 인원: {len(members)}명 / 소요 시간: {elapsed_seconds:.1f}초\n"
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


async def _fetch_all_guild_members(guild: discord.Guild) -> list[discord.Member]:
    members: list[discord.Member] = []
    async for member in guild.fetch_members(limit=None):
        members.append(member)
    return members


def _find_role_nickname_mismatches(
    members: list[discord.Member],
    role_alliance_by_id: dict[int, str],
) -> list[dict[str, str]]:
    mismatches: list[dict[str, str]] = []
    for member in sorted(members, key=lambda item: item.display_name):
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
    *,
    checked_count: int,
    elapsed_seconds: float,
) -> str:
    lines = [
        "**역할 매칭 점검**",
        f"서버: {guild.name}",
        f"점검 인원: {checked_count}명",
        f"불일치: {len(mismatches)}명",
        f"소요 시간: {elapsed_seconds:.1f}초",
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
