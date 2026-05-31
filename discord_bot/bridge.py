from __future__ import annotations

import asyncio
import json
import os
from typing import Any
from urllib.parse import urlencode

import aiohttp
import discord

from discord_bot.utils.attendance import (
    build_live_attendance_state,
    persist_attendance_snapshot,
    send_attendance_summary,
    send_ranker_attendance_ids,
    start_attendance,
    stop_attendance,
)


DEFAULT_BRIDGE_WS_URL = "ws://127.0.0.1:8000/internal/bot/ws"


def start_web_bridge(bot: discord.Bot) -> None:
    existing_task = getattr(bot, "web_bridge_task", None)
    if isinstance(existing_task, asyncio.Task) and not existing_task.done():
        return

    bot.attendance_state_publisher = lambda guild_id: publish_attendance_state(
        bot,
        guild_id,
    )
    bot.web_bridge_task = asyncio.create_task(_bridge_loop(bot))


async def publish_attendance_state(bot: discord.Bot, guild_id: int) -> None:
    websocket = getattr(bot, "web_bridge_ws", None)
    if websocket is None or websocket.closed:
        return

    try:
        await websocket.send_json(
            {
                "type": "attendance.state",
                "guild_id": str(guild_id),
                "state": build_live_attendance_state(bot, guild_id),
            }
        )
    except Exception:
        return


async def _bridge_loop(bot: discord.Bot) -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    _build_bridge_url(),
                    heartbeat=20,
                    receive_timeout=None,
                ) as websocket:
                    bot.web_bridge_ws = websocket
                    await _send_hello(bot, websocket)
                    await _send_all_attendance_states(bot, websocket)
                    async for message in websocket:
                        if message.type == aiohttp.WSMsgType.TEXT:
                            await _handle_bridge_message(bot, websocket, message.data)
                        elif message.type in {
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        }:
                            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[web-bridge] connection failed: {exc!r}")
        finally:
            if getattr(bot, "web_bridge_ws", None) is not None:
                bot.web_bridge_ws = None

        await asyncio.sleep(_bridge_reconnect_seconds())


async def _send_hello(bot: discord.Bot, websocket: aiohttp.ClientWebSocketResponse) -> None:
    await websocket.send_json(
        {
            "type": "bot.hello",
            "guild_ids": [str(guild.id) for guild in bot.guilds],
        }
    )


async def _send_all_attendance_states(
    bot: discord.Bot,
    websocket: aiohttp.ClientWebSocketResponse,
) -> None:
    for guild in bot.guilds:
        await websocket.send_json(
            {
                "type": "attendance.state",
                "guild_id": str(guild.id),
                "state": build_live_attendance_state(bot, guild.id),
            }
        )


async def _handle_bridge_message(
    bot: discord.Bot,
    websocket: aiohttp.ClientWebSocketResponse,
    raw_message: str,
) -> None:
    try:
        payload = json.loads(raw_message)
    except json.JSONDecodeError:
        return

    if not isinstance(payload, dict):
        return

    message_type = str(payload.get("type") or "")
    request_id = str(payload.get("request_id") or "")
    guild_id = _optional_int(payload.get("guild_id"))

    if message_type == "attendance.subscribe" and guild_id is not None:
        await publish_attendance_state(bot, guild_id)
        return

    if guild_id is None or message_type not in {
        "attendance.start",
        "attendance.stop",
    }:
        return

    if message_type == "attendance.start":
        result = await _handle_start_attendance(bot, guild_id, payload)
    else:
        result = await _handle_stop_attendance(bot, guild_id, payload)

    result.update(
        {
            "type": "attendance.command_result",
            "request_id": request_id,
            "guild_id": str(guild_id),
            "state": build_live_attendance_state(bot, guild_id),
        }
    )
    await websocket.send_json(result)


async def _handle_start_attendance(
    bot: discord.Bot,
    guild_id: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    guild = bot.get_guild(guild_id)
    if guild is None:
        return {"ok": False, "message": "봇이 서버에 접속해 있지 않습니다."}

    member = await _resolve_member(guild, payload.get("requested_by_discord_id"))
    if member is None:
        return {"ok": False, "message": "요청자를 Discord 서버에서 찾을 수 없습니다."}

    ok, message = await start_attendance(bot, guild, member)
    return {"ok": ok, "message": message}


async def _handle_stop_attendance(
    bot: discord.Bot,
    guild_id: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    guild = bot.get_guild(guild_id)
    if guild is None:
        return {"ok": False, "message": "봇이 서버에 접속해 있지 않습니다."}

    member = await _resolve_member(guild, payload.get("requested_by_discord_id"))
    should_save = _payload_bool(payload.get("save_attendance"), default=True)
    result = await stop_attendance(
        bot,
        guild,
        stopped_by=member,
        reason="manual",
    )
    if not result["ok"]:
        return {"ok": False, "message": str(result["message"])}

    snapshot = result["snapshot"]
    stopped_by_mention = member.mention if member is not None else "웹"
    if not should_save:
        await send_attendance_summary(
            bot,
            guild,
            snapshot=snapshot,
            reason="manual",
            stopped_by_mention=stopped_by_mention,
            save_status="기록 저장 X",
        )
        return {
            "ok": True,
            "message": str(result["message"]),
            "attendance_id": None,
            "participant_count": int(result.get("participant_count") or 0),
            "saved": False,
        }

    attendance_id = await persist_attendance_snapshot(bot, guild, snapshot)
    await send_attendance_summary(
        bot,
        guild,
        snapshot=snapshot,
        reason="manual",
        stopped_by_mention=stopped_by_mention,
        save_status=f"DB 저장 완료: #{attendance_id}",
    )
    await send_ranker_attendance_ids(guild, snapshot)
    return {
        "ok": True,
        "message": str(result["message"]),
        "attendance_id": attendance_id,
        "participant_count": int(result.get("participant_count") or 0),
        "saved": True,
    }


async def _resolve_member(
    guild: discord.Guild,
    value: object,
) -> discord.Member | None:
    discord_id = _optional_int(value)
    if discord_id is None:
        return None

    member = guild.get_member(discord_id)
    if member is not None:
        return member

    try:
        return await guild.fetch_member(discord_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None


def _build_bridge_url() -> str:
    bridge_url = os.getenv("BOT_BRIDGE_WS_URL", DEFAULT_BRIDGE_WS_URL)
    token = os.getenv("BOT_BRIDGE_TOKEN") or os.getenv("WEB_SESSION_SECRET")
    if not token:
        token = "lineage-local-web-session"

    separator = "&" if "?" in bridge_url else "?"
    return f"{bridge_url}{separator}{urlencode({'token': token})}"


def _bridge_reconnect_seconds() -> float:
    try:
        return float(os.getenv("BOT_BRIDGE_RECONNECT_SECONDS", "3"))
    except ValueError:
        return 3.0


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _payload_bool(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no", "off", ""}
