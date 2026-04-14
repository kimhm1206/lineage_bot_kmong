from __future__ import annotations

import discord

def is_supported_guild(bot: discord.Bot, guild_id: int) -> bool:
    return bot.get_guild(guild_id) is not None


def is_admin_member(member: discord.Member | None) -> bool:
    if member is None:
        return False
    permissions = member.guild_permissions
    return permissions.administrator or permissions.manage_guild
