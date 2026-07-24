from __future__ import annotations

from typing import Any

import discord

from discord_bot.storage import GuildSettings


def cache_guild_settings(
    bot: discord.Bot,
    settings: GuildSettings,
) -> GuildSettings:
    cache = _settings_cache(bot)
    cache[int(settings.guild_id)] = settings
    return settings


def cached_guild_settings(
    bot: discord.Bot,
    guild_id: int,
) -> GuildSettings | None:
    return _settings_cache(bot).get(int(guild_id))


def remove_guild_settings(bot: discord.Bot, guild_id: int) -> None:
    _settings_cache(bot).pop(int(guild_id), None)


def _settings_cache(bot: discord.Bot) -> dict[int, GuildSettings]:
    cache: Any = getattr(bot, "guild_settings_by_guild", None)
    if not isinstance(cache, dict):
        cache = {}
        bot.guild_settings_by_guild = cache
    return cache
