from __future__ import annotations

import asyncio
import json
import select
from typing import Any

import discord

from discord_bot.reports import reload_report_schedules
from discord_bot.storage import database
from discord_bot.utils.attendance import seed_voice_entry_times
from discord_bot.utils.panel import clear_old_admin_panel, update_admin_panel


CHANNEL_NAME = "lineage_bot_events"
LISTEN_RECONNECT_SECONDS = 5
LISTEN_HEARTBEAT_SECONDS = 30


def start_database_notification_listener(bot: discord.Bot) -> None:
    existing_task = getattr(bot, "database_notification_task", None)
    if isinstance(existing_task, asyncio.Task) and not existing_task.done():
        return
    bot.database_notification_task = asyncio.create_task(_listen(bot))


async def _listen(bot: discord.Bot) -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            with database.connect() as connection:
                connection.set_session(autocommit=True)
                with connection.cursor() as cursor:
                    cursor.execute(f"LISTEN {CHANNEL_NAME}")
                print(f"[bot-events] listening on {CHANNEL_NAME}")

                while not bot.is_closed():
                    ready, _, _ = await asyncio.to_thread(
                        select.select,
                        [connection],
                        [],
                        [],
                        LISTEN_HEARTBEAT_SECONDS,
                    )
                    if not ready:
                        continue
                    connection.poll()
                    while connection.notifies:
                        notification = connection.notifies.pop(0)
                        await _dispatch(bot, notification.payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[bot-events] listener failed: {exc}")
            await asyncio.sleep(LISTEN_RECONNECT_SECONDS)


async def _dispatch(bot: discord.Bot, raw_payload: str) -> None:
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        print("[bot-events] ignored invalid JSON payload")
        return
    if not isinstance(payload, dict):
        return

    event_type = str(payload.get("type") or "")
    guild_id = _optional_int(payload.get("guild_id"))
    data = payload.get("data")
    if not isinstance(data, dict):
        data = {}

    if event_type == "refresh_report_schedules":
        await reload_report_schedules(bot)
        return
    if event_type == "refresh_guild_registry":
        await _refresh_guild_registry(bot)
        return
    if event_type == "refresh_admin_panel" and guild_id is not None:
        await _refresh_admin_panel(bot, guild_id, data)
        return
    print(f"[bot-events] ignored unsupported event: {event_type}")


async def _refresh_guild_registry(bot: discord.Bot) -> None:
    enabled_ids = await asyncio.to_thread(database.enabled_guild_ids)
    previous_ids = set(getattr(bot, "registered_guild_ids", set()))
    bot.registered_guild_ids = enabled_ids
    for guild_id in sorted(enabled_ids - previous_ids):
        guild = bot.get_guild(guild_id)
        if guild is None:
            continue
        seed_voice_entry_times(bot, guild)
        await update_admin_panel(bot, guild_id)
        print(f"[bot-events] enabled guild: {guild.name} ({guild.id})")


async def _refresh_admin_panel(
    bot: discord.Bot,
    guild_id: int,
    data: dict[str, Any],
) -> None:
    if not await asyncio.to_thread(database.is_guild_enabled, guild_id):
        return
    registered_ids = getattr(bot, "registered_guild_ids", None)
    if isinstance(registered_ids, set):
        registered_ids.add(guild_id)

    guild = bot.get_guild(guild_id)
    if guild is None:
        return
    settings = await asyncio.to_thread(database.get_settings, guild_id)
    previous_channel_id = _optional_int(data.get("previous_admin_channel_id"))
    if (
        previous_channel_id is not None
        and previous_channel_id != settings.admin_channel_id
    ):
        await clear_old_admin_panel(bot, guild, previous_channel_id)
    seed_voice_entry_times(bot, guild)
    await update_admin_panel(bot, guild_id)
    print(f"[bot-events] refreshed attendance panel guild_id={guild_id}")


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
