import os

import discord
from dotenv import load_dotenv

from db import get_settings, init_db, update_setting
from utils.attendance import register_attendance
from utils.guild import is_admin_member, is_supported_guild
from utils.panel import clear_old_admin_panel, rebuild_admin_panel


load_dotenv()

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_BOT_TOKEN이 .env 파일에 설정되어 있지 않습니다.")

intents = discord.Intents.none()
intents.guilds = True
intents.members = True
intents.voice_states = True
bot = discord.Bot(intents=intents)
bot.panel_state_by_guild = {}
bot.attendance_state_by_guild = {}
bot.attendance_locks = {}
bot.commands_synced = False


@bot.event
async def on_ready() -> None:
    if not bot.commands_synced:
        await bot.sync_commands()
        bot.commands_synced = True

    for guild in bot.guilds:
        await rebuild_admin_panel(bot, guild.id)

    guild_names = ", ".join(guild.name for guild in bot.guilds) or "No Guild"
    print(f"봇 로그인 완료: {bot.user} | 길드: {guild_names}")


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


def main() -> None:
    init_db()
    bot.run(TOKEN)


if __name__ == "__main__":
    main()
