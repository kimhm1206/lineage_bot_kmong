from __future__ import annotations

import os

import discord


DEVELOPER_DISCORD_ID = int(
    os.getenv("DEVELOPER_DISCORD_ID", "238978205078388747")
)


def is_supported_guild(bot: discord.Bot, guild_id: int) -> bool:
    if bot.get_guild(guild_id) is None:
        return False
    registered = getattr(bot, "registered_guild_ids", None)
    return isinstance(registered, set) and int(guild_id) in registered


def unregistered_guild_message() -> str:
    return (
        "등록되지 않은 서버입니다. "
        f"<@{DEVELOPER_DISCORD_ID}> 개발자에게 문의해주세요."
    )


def is_admin_member(member: discord.Member | None) -> bool:
    if member is None:
        return False
    permissions = member.guild_permissions
    return permissions.administrator or permissions.manage_guild
