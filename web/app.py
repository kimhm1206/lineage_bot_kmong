from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import secrets
import ipaddress
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import parse_qs, urlencode

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from psycopg2.extras import Json

from common import database
from web.session import RememberMeSessionMiddleware


load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DISCORD_API_BASE = "https://discord.com/api/v10"
KST = timezone(timedelta(hours=9))
DISCORD_SETTINGS_CACHE_TTL_SECONDS = int(
    os.getenv("DISCORD_SETTINGS_CACHE_TTL_SECONDS", "90")
)
DISCORD_REDIRECT_URI = os.getenv(
    "DISCORD_REDIRECT_URI",
    "https://test.meetloa.online/auth/discord/callback",
)
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID") or os.getenv(
    "DISCORD_OAUTH_CLIENT_ID", ""
)
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET") or os.getenv(
    "DISCORD_OAUTH_CLIENT_SECRET", ""
)
SESSION_SECRET = os.getenv("WEB_SESSION_SECRET", "lineage-local-web-session")
BOT_BRIDGE_TOKEN = os.getenv("BOT_BRIDGE_TOKEN", SESSION_SECRET)
BOT_BRIDGE_COMMAND_TIMEOUT_SECONDS = float(
    os.getenv("BOT_BRIDGE_COMMAND_TIMEOUT_SECONDS", "8")
)
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
GLOBAL_DEVELOPER_DISCORD_ID = (
    os.getenv("GLOBAL_DEVELOPER_DISCORD_ID")
    or os.getenv("GLOBAL_OWNER_DISCORD_ID")
    or "238978205078388747"
)
DISCORD_ADMINISTRATOR_PERMISSION = 0x8
DISCORD_MANAGE_GUILD_PERMISSION = 0x20
DISCORD_WEB_ADMIN_PERMISSION_MASK = (
    DISCORD_ADMINISTRATOR_PERMISSION | DISCORD_MANAGE_GUILD_PERMISSION
)
LOG_TABS = (
    {"value": "all", "label": "전체"},
    {"value": "attendance", "label": "출석"},
    {"value": "statistics", "label": "통계"},
    {"value": "settings", "label": "설정"},
    {"value": "logs", "label": "로그"},
)
WORK_LOG_TABS = (
    {"value": "all", "label": "전체"},
    {"value": "attendance_add", "label": "출석 추가"},
    {"value": "attendance_delete", "label": "출석 삭제"},
    {"value": "item_create", "label": "아이템 추가"},
    {"value": "item_update", "label": "아이템 수정"},
    {"value": "item_delete", "label": "아이템 삭제"},
    {"value": "loot_create", "label": "드랍 등록"},
    {"value": "loot_update", "label": "드랍 수정"},
    {"value": "loot_delete", "label": "드랍 삭제"},
    {"value": "bid_item", "label": "입찰 아이템"},
    {"value": "bid_status", "label": "입찰 상태"},
)
OTHER_ALLIANCE_VALUE = "__other__"
OTHER_ALLIANCE_LABEL = "-그 외-"
PAGE_REQUIRED_ROLES = {
    "attendance": "user",
    "status": "user",
    "loot": "user",
    "dashboard": "user",
    "my_alliance": "user",
    "settings": "admin",
    "work_logs": "owner",
    "logs": "developer",
    "developer_servers": "developer",
}

_BOT_BRIDGE_WEBSOCKET: WebSocket | None = None
_BOT_BRIDGE_LOCK = asyncio.Lock()
_BOT_BRIDGE_WAITERS: dict[str, asyncio.Future[dict[str, Any]]] = {}
_ATTENDANCE_STATE_CACHE: dict[int, dict[str, Any]] = {}
_ATTENDANCE_BROWSER_CLIENTS: dict[int, set[WebSocket]] = {}
_ATTENDANCE_COMMANDS_IN_FLIGHT: dict[tuple[int, str], str] = {}
REPORT_FREQUENCY_OPTIONS = (
    {"value": "daily", "label": "매일"},
    {"value": "every_3_days", "label": "3일마다"},
    {"value": "weekly", "label": "일주일마다"},
    {"value": "monthly", "label": "매월"},
)
REPORT_PERIOD_OPTIONS = (
    {"value": "today", "label": "오늘"},
    {"value": "yesterday", "label": "어제"},
    {"value": "recent_7_days", "label": "최근 일주일"},
    {"value": "recent_3_days", "label": "최근 3일"},
    {"value": "this_week", "label": "이번 주"},
    {"value": "this_month", "label": "이번 달"},
)
REPORT_DATASET_OPTIONS = (
    {"value": "attendance", "label": "출석"},
)
REPORT_GROUP_OPTIONS = (
    {"value": "alliance", "label": "혈맹별"},
    {"value": "none", "label": "전체"},
)
REPORT_RANK_TARGET_OPTIONS = (
    {"value": "user", "label": "유저"},
    {"value": "alliance", "label": "혈맹"},
)
REPORT_METRIC_OPTIONS = (
    {"value": "attendance_count", "label": "출석 횟수"},
    {"value": "unique_user_count", "label": "참여 인원"},
)
REPORT_OUTPUT_OPTIONS = (
    {"value": "grouped_ranking", "label": "그룹별 랭킹"},
    {"value": "ranking", "label": "랭킹"},
)
REPORT_STATUS_OPTIONS = (
    {"value": "on", "label": "on"},
    {"value": "off", "label": "off"},
)
REPORT_OPTIONS = {
    "frequencies": REPORT_FREQUENCY_OPTIONS,
    "periods": REPORT_PERIOD_OPTIONS,
    "datasets": REPORT_DATASET_OPTIONS,
    "groups": REPORT_GROUP_OPTIONS,
    "rank_targets": REPORT_RANK_TARGET_OPTIONS,
    "metrics": REPORT_METRIC_OPTIONS,
    "outputs": REPORT_OUTPUT_OPTIONS,
    "statuses": REPORT_STATUS_OPTIONS,
}
DEVELOPER_VIEW_MODE_SESSION_KEY = "developer_view_mode"
DEVELOPER_VIEW_MODE_OPTIONS = (
    {"value": "developer", "label": "디벨로퍼"},
    {"value": "owner", "label": "오너"},
    {"value": "bookkeeper", "label": "경리 유저"},
    {"value": "admin", "label": "어드민 유저"},
    {"value": "user", "label": "일반 유저"},
)
DEVELOPER_VIEW_MODE_VALUES = {
    str(option["value"]) for option in DEVELOPER_VIEW_MODE_OPTIONS
}

app = FastAPI(title="Lineage Ops Web")
app.add_middleware(
    RememberMeSessionMiddleware,
    secret_key=SESSION_SECRET,
    same_site="lax",
    https_only=False,
)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")
_DISCORD_BOT_GET_CACHE: dict[str, tuple[float, Any]] = {}
LOCAL_DEVELOPER_USER_ID = os.getenv(
    "LOCAL_DEVELOPER_DISCORD_ID",
    GLOBAL_DEVELOPER_DISCORD_ID,
)


@app.on_event("startup")
def initialize_database_schema() -> None:
    database.init_schema()


def _oauth_ready() -> bool:
    return bool(DISCORD_CLIENT_ID and DISCORD_CLIENT_SECRET)


def _local_developer_auth_enabled() -> bool:
    return database.is_test_mode()


def _is_local_request(request: Request) -> bool:
    host = request.client.host if request.client else ""
    if host in {"localhost", "127.0.0.1", "::1"}:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _local_developer_user() -> dict[str, str]:
    return {
        "id": str(LOCAL_DEVELOPER_USER_ID),
        "username": "local-developer",
        "display_name": "Local Developer",
    }


def _discord_authorize_url(state: str) -> str:
    params = {
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": DISCORD_REDIRECT_URI,
        "response_type": "code",
        "scope": "identify guilds",
        "state": state,
    }
    return f"https://discord.com/oauth2/authorize?{urlencode(params)}"


def _exchange_discord_code(code: str) -> str:
    response = requests.post(
        f"{DISCORD_API_BASE}/oauth2/token",
        data={
            "client_id": DISCORD_CLIENT_ID,
            "client_secret": DISCORD_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": DISCORD_REDIRECT_URI,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    response.raise_for_status()
    token_payload = response.json()
    access_token = token_payload.get("access_token")
    if not access_token:
        raise RuntimeError("Discord access token response is empty.")
    return str(access_token)


def _discord_get(path: str, access_token: str) -> Any:
    response = requests.get(
        f"{DISCORD_API_BASE}{path}",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def _discord_get_user_guilds(access_token: str) -> list[dict[str, Any]]:
    guilds: list[dict[str, Any]] = []
    after: str | None = None
    while True:
        query = {"limit": "200"}
        if after:
            query["after"] = after

        page = _discord_get(f"/users/@me/guilds?{urlencode(query)}", access_token)
        if not isinstance(page, list):
            return guilds

        guilds.extend(
            guild for guild in page if isinstance(guild, dict)
        )
        if len(page) < 200:
            return guilds

        last_guild_id = str((page[-1] or {}).get("id") or "")
        if not last_guild_id or last_guild_id == after:
            return guilds
        after = last_guild_id


def _discord_bot_get(path: str) -> Any:
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN is not configured.")
    response = requests.get(
        f"{DISCORD_API_BASE}{path}",
        headers={"Authorization": f"Bot {DISCORD_BOT_TOKEN}"},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def _discord_bot_get_cached(path: str) -> Any:
    now = monotonic()
    cached = _DISCORD_BOT_GET_CACHE.get(path)
    if cached and cached[0] > now:
        return cached[1]
    value = _discord_bot_get(path)
    _DISCORD_BOT_GET_CACHE[path] = (
        now + DISCORD_SETTINGS_CACHE_TTL_SECONDS,
        value,
    )
    return value


def _normalize_discord_user(user: dict[str, Any]) -> dict[str, str]:
    username = str(user.get("username") or "")
    display_name = str(user.get("global_name") or username or user.get("id"))
    return {
        "id": str(user["id"]),
        "username": username,
        "display_name": display_name,
    }


def _discord_permissions(discord_guild: dict[str, Any] | None) -> int:
    if not discord_guild:
        return 0
    try:
        return int(discord_guild.get("permissions") or 0)
    except (TypeError, ValueError):
        return 0


def _server_role(
    discord_user_id: str,
    discord_guild: dict[str, Any] | None,
) -> str:
    if str(discord_user_id) == GLOBAL_DEVELOPER_DISCORD_ID:
        return "developer"
    if discord_guild and (
        bool(discord_guild.get("owner"))
        or bool(_discord_permissions(discord_guild) & DISCORD_WEB_ADMIN_PERMISSION_MASK)
    ):
        return "admin"
    return "user"


def _role_from_bot_member_permissions(
    guild_id: int,
    discord_user_id: str,
) -> str | None:
    if str(discord_user_id) == GLOBAL_DEVELOPER_DISCORD_ID:
        return "developer"
    if not DISCORD_BOT_TOKEN:
        return None

    try:
        guild = _discord_bot_get_cached(f"/guilds/{guild_id}")
        if str(guild.get("owner_id")) == str(discord_user_id):
            return "admin"

        member = _discord_guild_member(guild_id, discord_user_id)
        roles = _discord_bot_get_cached(f"/guilds/{guild_id}/roles")
    except Exception:
        return None
    if member is None:
        return None

    member_role_ids = {str(role_id) for role_id in member.get("roles", [])}
    member_role_ids.add(str(guild_id))
    permissions = 0
    for role in roles:
        if str(role.get("id")) not in member_role_ids:
            continue
        permissions |= _discord_permissions(role)

    return "admin" if permissions & DISCORD_WEB_ADMIN_PERMISSION_MASK else "user"


def _discord_bot_guild_name(guild_id: int) -> str | None:
    if not DISCORD_BOT_TOKEN:
        return None
    try:
        guild = _discord_bot_get_cached(f"/guilds/{guild_id}")
    except Exception:
        return None
    name = str(guild.get("name") or "").strip()
    return name or None


def _discord_guild_member(
    guild_id: int,
    discord_user_id: str,
) -> dict[str, Any] | None:
    if not DISCORD_BOT_TOKEN:
        return None
    try:
        member = _discord_bot_get_cached(f"/guilds/{guild_id}/members/{discord_user_id}")
    except Exception:
        return None
    return member if isinstance(member, dict) else None


def _guild_member_display_name(
    guild_id: int,
    discord_user_id: str,
    fallback_name: str,
) -> str:
    member = _discord_guild_member(guild_id, discord_user_id)
    if member is None:
        return fallback_name

    nick = str(member.get("nick") or "").strip()
    if nick:
        return nick

    user = member.get("user") or {}
    for key in ("global_name", "username"):
        name = str(user.get(key) or "").strip()
        if name:
            return name
    return fallback_name


def _load_active_alliances() -> list[dict[str, Any]]:
    rows = database.fetchall(
        """
        SELECT alliance_id, alliance_name
        FROM alliances
        WHERE is_active = TRUE
        ORDER BY sort_order ASC NULLS LAST, alliance_name ASC
        """
    )
    return [
        {
            "alliance_id": int(row["alliance_id"]),
            "alliance_name": str(row["alliance_name"]),
        }
        for row in rows
    ]


def _guild_role_mapped_alliance_ids(guild_id: int) -> set[int]:
    return {
        int(mapping["alliance_id"])
        for mapping in database.get_guild_alliance_role_mappings(guild_id)
    }


def _role_scoped_alliance_options(
    guild_id: int,
    options: list[dict[str, Any]],
    *,
    include_other: bool,
) -> list[dict[str, Any]]:
    mapped_ids = _guild_role_mapped_alliance_ids(guild_id)
    if not mapped_ids:
        return options

    scoped_options = [
        option
        for option in options
        if _parse_optional_int(option.get("alliance_id")) in mapped_ids
    ]
    other_ids = [
        int(option["alliance_id"])
        for option in options
        if _parse_optional_int(option.get("alliance_id")) not in mapped_ids
    ]
    if include_other and other_ids:
        scoped_options.append(
            {
                "alliance_id": OTHER_ALLIANCE_VALUE,
                "alliance_name": OTHER_ALLIANCE_LABEL,
                "alliance_ids": other_ids,
                "is_other": True,
            }
        )
    return scoped_options


def _role_scoped_alliance_name_options(guild_id: int) -> list[dict[str, str]]:
    active_options = _load_active_alliances()
    mapped_ids = _guild_role_mapped_alliance_ids(guild_id)
    if not mapped_ids:
        return [
            {
                "value": str(option["alliance_name"]),
                "label": str(option["alliance_name"]),
            }
            for option in active_options
        ]

    options = [
        {
            "value": str(option["alliance_name"]),
            "label": str(option["alliance_name"]),
        }
        for option in active_options
        if int(option["alliance_id"]) in mapped_ids
    ]
    if any(int(option["alliance_id"]) not in mapped_ids for option in active_options):
        options.append({"value": OTHER_ALLIANCE_VALUE, "label": OTHER_ALLIANCE_LABEL})
    return options


def _unmapped_alliance_names(guild_id: int) -> set[str]:
    mapped_ids = _guild_role_mapped_alliance_ids(guild_id)
    if not mapped_ids:
        return set()
    return {
        str(option["alliance_name"])
        for option in _load_active_alliances()
        if int(option["alliance_id"]) not in mapped_ids
    }


def _discord_member_role_ids(guild_id: int, discord_user_id: str) -> set[str]:
    member = _discord_guild_member(guild_id, discord_user_id)
    if member is None:
        return set()
    return {str(role_id) for role_id in member.get("roles", [])}


def _discord_member_role_ids_by_priority(
    guild_id: int,
    discord_user_id: str,
) -> list[str]:
    member_role_ids = _discord_member_role_ids(guild_id, discord_user_id)
    if not member_role_ids:
        return []

    try:
        roles = _discord_bot_get_cached(f"/guilds/{guild_id}/roles")
    except Exception:
        return sorted(member_role_ids)

    role_positions = {
        str(role.get("id")): int(role.get("position") or 0)
        for role in roles
        if str(role.get("id")) in member_role_ids
    }
    return sorted(
        member_role_ids,
        key=lambda role_id: (-role_positions.get(role_id, 0), role_id),
    )


def _unique_alliance_options(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    options_by_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        alliance_id = int(row["alliance_id"])
        options_by_id.setdefault(
            alliance_id,
            {
                "alliance_id": alliance_id,
                "alliance_name": str(row["alliance_name"]),
            },
        )
    return sorted(options_by_id.values(), key=lambda item: item["alliance_name"])


def _member_alliance_options(
    guild_id: int,
    discord_user_id: str,
) -> list[dict[str, Any]]:
    member_role_ids = _discord_member_role_ids_by_priority(guild_id, discord_user_id)
    if not member_role_ids:
        return []

    mappings_by_role_id = {
        str(mapping["role_id"]): mapping
        for mapping in database.get_guild_alliance_role_mappings(guild_id)
    }
    options: list[dict[str, Any]] = []
    seen_alliance_ids: set[int] = set()
    for role_id in member_role_ids:
        mapping = mappings_by_role_id.get(role_id)
        if mapping is None:
            continue

        alliance_id = int(mapping["alliance_id"])
        if alliance_id in seen_alliance_ids:
            continue
        seen_alliance_ids.add(alliance_id)
        options.append(
            {
                "alliance_id": alliance_id,
                "alliance_name": str(mapping["alliance_name"]),
            }
        )
    return options


def _role_display_label(server_role: str, alliance_options: list[dict[str, Any]]) -> str:
    if server_role == "developer":
        return "developer"
    if server_role == "owner":
        return "owner"

    alliance_name = (
        str(alliance_options[0]["alliance_name"]) if alliance_options else ""
    )
    if server_role == "admin":
        return f"{alliance_name} admin" if alliance_name else "admin"
    return alliance_name or "user"


def _developer_view_options(include_developer: bool) -> tuple[dict[str, str], ...]:
    if include_developer:
        return DEVELOPER_VIEW_MODE_OPTIONS
    return tuple(
        option
        for option in DEVELOPER_VIEW_MODE_OPTIONS
        if str(option["value"]) != "developer"
    )


def _developer_view_values(include_developer: bool) -> set[str]:
    return {str(option["value"]) for option in _developer_view_options(include_developer)}


def _developer_view_mode(
    session: dict[str, Any] | None,
    *,
    default: str = "developer",
    include_developer: bool = True,
) -> str:
    options = _developer_view_options(include_developer)
    values = {str(option["value"]) for option in options}
    fallback = (
        default
        if default in values
        else str(options[0]["value"])
        if options
        else "user"
    )
    mode = str((session or {}).get(DEVELOPER_VIEW_MODE_SESSION_KEY) or fallback)
    return mode if mode in values else fallback


def _developer_view_payload(
    mode: str,
    *,
    include_developer: bool = True,
) -> dict[str, Any]:
    options = _developer_view_options(include_developer)
    values = {str(option["value"]) for option in options}
    normalized = (
        mode
        if mode in values
        else str(options[0]["value"])
        if options
        else "user"
    )
    return {
        "active": normalized,
        "options": [
            {
                **option,
                "active": str(option["value"]) == normalized,
            }
            for option in options
        ],
    }


def _apply_developer_view_mode(
    selected_server: dict[str, Any],
    *,
    guild_id: int,
    discord_user_id: str,
    mode: str,
) -> None:
    normalized = mode if mode in DEVELOPER_VIEW_MODE_VALUES else "developer"
    if normalized == "developer":
        return

    role = (
        "owner"
        if normalized == "owner"
        else "admin"
        if normalized in {"admin", "bookkeeper"}
        else "user"
    )
    is_owner_view = normalized == "owner"
    is_bookkeeper_view = normalized == "bookkeeper"
    selected_server["role"] = role
    selected_server["is_owner"] = is_owner_view
    selected_server["is_bookkeeper"] = is_bookkeeper_view
    selected_server["can_manage"] = role == "admin" or is_owner_view
    selected_server["can_bookkeep"] = is_bookkeeper_view or is_owner_view
    selected_server["can_manage_bookkeepers"] = is_owner_view
    selected_server["my_alliance"] = _my_alliance_access(
        guild_id,
        discord_user_id,
        role,
        is_owner_view,
    )
    if is_owner_view:
        selected_server["role_label"] = "owner"
    elif is_bookkeeper_view:
        selected_server["role_label"] = "경리"
    else:
        selected_server["role_label"] = _role_display_label(
            role,
            selected_server["my_alliance"]["options"],
        )


def _my_alliance_access(
    guild_id: int,
    discord_user_id: str,
    server_role: str,
    is_owner: bool = False,
) -> dict[str, Any]:
    if server_role == "developer" or is_owner:
        options = _role_scoped_alliance_options(
            guild_id,
            _load_active_alliances(),
            include_other=True,
        )
    else:
        options = _role_scoped_alliance_options(
            guild_id,
            _member_alliance_options(guild_id, discord_user_id),
            include_other=False,
        )

    return {
        "can_view": bool(options),
        "can_select": len(options) > 1,
        "options": options,
    }


def _is_selected_guild_owner(
    guild_id: int,
    discord_user_id: str,
    selected_server: dict[str, Any],
) -> bool:
    if bool(selected_server.get("discord_owner")):
        return True
    if not DISCORD_BOT_TOKEN:
        return False
    try:
        guild = _discord_bot_get_cached(f"/guilds/{guild_id}")
    except Exception:
        return False
    return str(guild.get("owner_id")) == str(discord_user_id)


def _attendance_guild_ids_for_user(discord_user_id: str) -> set[int]:
    if not str(discord_user_id).isdigit():
        return set()
    rows = database.fetchall(
        """
        SELECT DISTINCT s.guild_id
        FROM attendance_sessions s
        INNER JOIN attendance_entries e ON e.attendance_id = s.attendance_id
        INNER JOIN users u ON u.user_id = e.user_id
        WHERE u.discord_id = %s
        """,
        (int(discord_user_id),),
    )
    return {int(row["guild_id"]) for row in rows}


def _load_accessible_servers(
    discord_user_id: str,
    discord_guilds: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    guild_lookup = {
        str(guild["id"]): guild
        for guild in discord_guilds
        if str(guild.get("id", "")).isdigit()
    }
    guild_ids = [int(guild_id) for guild_id in guild_lookup]
    attendance_guild_ids = _attendance_guild_ids_for_user(discord_user_id)
    candidate_guild_ids = sorted(set(guild_ids) | attendance_guild_ids)
    is_global_developer = str(discord_user_id) == GLOBAL_DEVELOPER_DISCORD_ID
    can_verify_with_bot = bool(DISCORD_BOT_TOKEN)
    if not candidate_guild_ids and not is_global_developer and not can_verify_with_bot:
        return []

    if is_global_developer:
        where_clause = "TRUE"
        params: tuple[Any, ...] = ()
    elif can_verify_with_bot:
        where_clause = "g.is_enabled = TRUE"
        params = ()
    else:
        where_clause = "g.is_enabled = TRUE AND g.guild_id = ANY(%s::bigint[])"
        params = (candidate_guild_ids,)

    rows = database.fetchall(
        f"""
        SELECT
            g.guild_id,
            g.is_enabled,
            gs.admin_channel_id,
            gs.attendance_voice_channel_id,
            gs.log_channel_id,
            COUNT(DISTINCT s.attendance_id) AS session_count,
            COUNT(e.user_id) AS attendance_count,
            MIN(s.started_at) AS first_started_at,
            MAX(s.started_at) AS last_session_started_at,
            MAX(CASE WHEN e.user_id IS NOT NULL THEN s.started_at END)
                AS last_attendance_at
        FROM guilds g
        LEFT JOIN guild_settings gs ON gs.guild_id = g.guild_id
        LEFT JOIN attendance_sessions s ON s.guild_id = g.guild_id
        LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
        WHERE {where_clause}
        GROUP BY
            g.guild_id,
            g.is_enabled,
            gs.admin_channel_id,
            gs.attendance_voice_channel_id,
            gs.log_channel_id
        ORDER BY
            MAX(CASE WHEN e.user_id IS NOT NULL THEN s.started_at END) DESC NULLS LAST,
            MAX(s.started_at) DESC NULLS LAST,
            g.guild_id ASC
        """,
        params,
    )

    servers: list[dict[str, Any]] = []
    for row in rows:
        guild_id = str(row["guild_id"])
        discord_guild = guild_lookup.get(guild_id)
        role_from_bot = _role_from_bot_member_permissions(
            int(guild_id),
            discord_user_id,
        )
        has_attendance_access = int(guild_id) in attendance_guild_ids
        if (
            not is_global_developer
            and discord_guild is None
            and role_from_bot is None
            and not has_attendance_access
        ):
            continue

        role = role_from_bot or _server_role(discord_user_id, discord_guild)
        has_settings = any(
            row.get(column) is not None
            for column in (
                "admin_channel_id",
                "attendance_voice_channel_id",
                "log_channel_id",
            )
        )
        server_name = str((discord_guild or {}).get("name") or "").strip()
        if not server_name:
            server_name = _discord_bot_guild_name(int(guild_id)) or f"Discord 서버 {guild_id}"
        servers.append(
            {
                "guild_id": guild_id,
                "name": server_name,
                "is_enabled": bool(row["is_enabled"]),
                "permissions": _discord_permissions(discord_guild),
                "discord_owner": bool((discord_guild or {}).get("owner")),
                "is_owner": bool((discord_guild or {}).get("owner")),
                "is_bookkeeper": False,
                "role": role,
                "base_role": role,
                "can_manage": role in {"admin", "developer"},
                "can_bookkeep": role == "developer"
                or bool((discord_guild or {}).get("owner")),
                "can_manage_bookkeepers": role == "developer"
                or bool((discord_guild or {}).get("owner")),
                "session_count": int(row["session_count"] or 0),
                "attendance_count": int(row["attendance_count"] or 0),
                "first_started_at": row["first_started_at"] or "",
                "last_started_at": row["last_attendance_at"]
                or row["last_session_started_at"]
                or "",
                "last_session_started_at": row["last_session_started_at"] or "",
                "has_settings": has_settings,
            }
        )
    return servers


def _load_local_developer_servers() -> list[dict[str, Any]]:
    rows = database.fetchall(
        """
        SELECT
            g.guild_id,
            g.is_enabled,
            gs.admin_channel_id,
            gs.attendance_voice_channel_id,
            gs.log_channel_id,
            COUNT(DISTINCT s.attendance_id) AS session_count,
            COUNT(e.user_id) AS attendance_count,
            MIN(s.started_at) AS first_started_at,
            MAX(s.started_at) AS last_session_started_at,
            MAX(CASE WHEN e.user_id IS NOT NULL THEN s.started_at END)
                AS last_attendance_at
        FROM guilds g
        LEFT JOIN guild_settings gs ON gs.guild_id = g.guild_id
        LEFT JOIN attendance_sessions s ON s.guild_id = g.guild_id
        LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
        GROUP BY
            g.guild_id,
            g.is_enabled,
            gs.admin_channel_id,
            gs.attendance_voice_channel_id,
            gs.log_channel_id
        ORDER BY
            MAX(CASE WHEN e.user_id IS NOT NULL THEN s.started_at END) DESC NULLS LAST,
            MAX(s.started_at) DESC NULLS LAST,
            g.guild_id ASC
        """
    )
    return [
        {
            "guild_id": str(row["guild_id"]),
            "name": f"Local 서버 {row['guild_id']}",
            "is_enabled": bool(row["is_enabled"]),
            "permissions": DISCORD_ADMINISTRATOR_PERMISSION,
            "discord_owner": True,
            "is_owner": True,
            "is_bookkeeper": True,
            "role": "developer",
            "base_role": "developer",
            "can_manage": True,
            "can_bookkeep": True,
            "can_manage_bookkeepers": True,
            "session_count": int(row["session_count"] or 0),
            "attendance_count": int(row["attendance_count"] or 0),
            "first_started_at": row["first_started_at"] or "",
            "last_started_at": row["last_attendance_at"]
            or row["last_session_started_at"]
            or "",
            "last_session_started_at": row["last_session_started_at"] or "",
            "has_settings": True,
        }
        for row in rows
    ]


def _local_developer_auth_context(
    guild_id: str | None = None,
    session: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    servers = _load_local_developer_servers()
    if not servers:
        return None

    allowed_servers = {str(server["guild_id"]): server for server in servers}
    selected_guild_id = str(guild_id or servers[0]["guild_id"])
    if selected_guild_id not in allowed_servers:
        selected_guild_id = str(servers[0]["guild_id"])

    selected_server = dict(allowed_servers[selected_guild_id])
    selected_server["role"] = "developer"
    selected_server["base_role"] = "developer"
    selected_server["can_manage"] = True
    selected_server["is_owner"] = True
    selected_server["is_bookkeeper"] = True
    selected_server["can_bookkeep"] = True
    selected_server["can_manage_bookkeepers"] = True
    selected_server["member_display_name"] = "Local Developer"
    selected_server["my_alliance"] = _my_alliance_access(
        int(selected_guild_id),
        str(LOCAL_DEVELOPER_USER_ID),
        "developer",
        True,
    )
    selected_server["role_label"] = "developer"
    view_mode = _developer_view_mode(
        session,
        default="developer",
        include_developer=True,
    )
    _apply_developer_view_mode(
        selected_server,
        guild_id=int(selected_guild_id),
        discord_user_id=str(LOCAL_DEVELOPER_USER_ID),
        mode=view_mode,
    )
    returned_servers = [
        selected_server if str(server["guild_id"]) == selected_guild_id else server
        for server in servers
    ]

    return {
        "user": _local_developer_user(),
        "servers": returned_servers,
        "selected_guild_id": selected_guild_id,
        "selected_server": selected_server,
        "developer_view": _developer_view_payload(
            view_mode,
            include_developer=True,
        ),
        "can_switch_developer_view": True,
    }


def _auth_context_from_session(
    session: dict[str, Any],
    guild_id: str | None = None,
) -> dict[str, Any] | None:
    user = session.get("discord_user")
    servers = session.get("servers") or []
    if not user or not servers:
        return None

    is_global_developer = str(user["id"]) == GLOBAL_DEVELOPER_DISCORD_ID
    if not is_global_developer:
        visibility = database.get_guild_visibility_map(
            [int(server["guild_id"]) for server in servers]
        )
        servers = [
            server
            for server in servers
            if visibility.get(int(server["guild_id"]), False)
        ]
        session["servers"] = servers
        if not servers:
            return None

    allowed_servers = {str(server["guild_id"]): server for server in servers}
    selected_guild_id = str(guild_id or servers[0]["guild_id"])
    if selected_guild_id not in allowed_servers:
        selected_guild_id = str(servers[0]["guild_id"])

    selected_server = dict(allowed_servers[selected_guild_id])
    verified_role = _role_from_bot_member_permissions(
        int(selected_guild_id),
        str(user["id"]),
    )
    if verified_role is None:
        verified_role = _server_role(
            str(user["id"]),
            {
                "owner": selected_server.get("discord_owner"),
                "permissions": selected_server.get("permissions"),
            },
        )
    is_owner = _is_selected_guild_owner(
        int(selected_guild_id),
        str(user["id"]),
        selected_server,
    )
    is_bookkeeper = database.is_guild_bookkeeper(
        int(selected_guild_id),
        int(user["id"]),
    )
    effective_role = verified_role
    if is_bookkeeper and effective_role == "user":
        effective_role = "admin"
    selected_server["base_role"] = verified_role
    selected_server["role"] = effective_role
    selected_server["is_owner"] = is_owner
    selected_server["is_bookkeeper"] = is_bookkeeper
    selected_server["can_manage"] = effective_role in {"admin", "developer"} or is_owner
    selected_server["can_bookkeep"] = (
        effective_role == "developer" or is_owner or is_bookkeeper
    )
    selected_server["can_manage_bookkeepers"] = (
        effective_role == "developer" or is_owner
    )
    selected_server["member_display_name"] = _guild_member_display_name(
        int(selected_guild_id),
        str(user["id"]),
        str(user.get("display_name") or user.get("username") or user["id"]),
    )
    selected_server["my_alliance"] = _my_alliance_access(
        int(selected_guild_id),
        str(user["id"]),
        effective_role,
        is_owner,
    )
    selected_server["role_label"] = _role_display_label(
        effective_role,
        selected_server["my_alliance"]["options"],
    )
    can_switch_view_mode = is_global_developer or is_owner
    include_developer_view = is_global_developer
    view_mode = (
        _developer_view_mode(
            session,
            default="developer" if is_global_developer else "owner",
            include_developer=include_developer_view,
        )
        if can_switch_view_mode
        else "developer"
    )
    if can_switch_view_mode:
        _apply_developer_view_mode(
            selected_server,
            guild_id=int(selected_guild_id),
            discord_user_id=str(user["id"]),
            mode=view_mode,
        )
    returned_servers = [
        selected_server if str(server["guild_id"]) == selected_guild_id else server
        for server in servers
    ]

    return {
        "user": user,
        "servers": returned_servers,
        "selected_guild_id": selected_guild_id,
        "selected_server": selected_server,
        "developer_view": _developer_view_payload(
            view_mode,
            include_developer=include_developer_view,
        ),
        "can_switch_developer_view": can_switch_view_mode,
    }


def _auth_context(
    request: Request,
    guild_id: str | None = None,
) -> dict[str, Any] | None:
    if _local_developer_auth_enabled() and _is_local_request(request):
        return _local_developer_auth_context(guild_id, request.session)
    return _auth_context_from_session(request.session, guild_id)


def _wants_json(request: Request) -> bool:
    return (
        request.headers.get("x-requested-with") == "fetch"
        or "application/json" in request.headers.get("accept", "")
    )


async def _urlencoded_form_data(request: Request) -> dict[str, str]:
    body = (await request.body()).decode("utf-8")
    parsed = parse_qs(body, keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


def _can_manage_selected_server(auth: dict[str, Any]) -> bool:
    return bool(auth["selected_server"].get("can_manage"))


def _can_bookkeep_selected_server(auth: dict[str, Any]) -> bool:
    return bool(auth["selected_server"].get("can_bookkeep"))


def _can_owner_manage_selected_server(auth: dict[str, Any]) -> bool:
    selected_server = auth["selected_server"]
    return (
        str(selected_server.get("role")) == "developer"
        or bool(selected_server.get("is_owner"))
    )


def _can_manage_bookkeepers(auth: dict[str, Any]) -> bool:
    return bool(auth["selected_server"].get("can_manage_bookkeepers"))


def _is_developer_auth(auth: dict[str, Any]) -> bool:
    return str(auth["selected_server"].get("role")) == "developer"


def _settings_forbidden_redirect(selected_guild_id: int) -> RedirectResponse:
    return RedirectResponse(
        f"/attendance?guild_id={selected_guild_id}",
        status_code=303,
    )


def _safe_redirect_path(value: str | None) -> str | None:
    if not value or not value.startswith("/") or value.startswith("//"):
        return None
    return value


def _request_path_with_query(request: Request) -> str:
    path = request.url.path
    return f"{path}?{request.url.query}" if request.url.query else path


def _auth_redirect(request: Request) -> RedirectResponse:
    next_url = _request_path_with_query(request)
    return RedirectResponse(
        f"/login?{urlencode({'next': next_url})}",
        status_code=303,
    )


def _render(
    request: Request,
    template_name: str,
    context: dict[str, Any] | None = None,
    status_code: int = 200,
) -> HTMLResponse:
    context = dict(context or {})
    active_page = str(context.get("active_page") or "")
    context.setdefault(
        "page_required_role",
        PAGE_REQUIRED_ROLES.get(active_page, "user"),
    )
    return templates.TemplateResponse(
        request,
        template_name,
        context,
        status_code=status_code,
    )


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return None


def _dashboard_period_dates(
    period: str | None,
) -> tuple[str | None, str | None]:
    today = datetime.now(KST).date()
    if period == "7d":
        return (today - timedelta(days=6)).isoformat(), today.isoformat()
    if period == "30d":
        return (today - timedelta(days=29)).isoformat(), today.isoformat()
    if period == "month":
        return today.replace(day=1).isoformat(), today.isoformat()
    if period == "all":
        return None, None
    return None, None


def _date_bounds(
    start_date: str | None,
    end_date: str | None,
) -> tuple[str | None, str | None, str, str]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if start and end and start > end:
        start, end = end, start
    start_value = start.strftime("%Y-%m-%d") if start else ""
    end_value = end.strftime("%Y-%m-%d") if end else ""
    start_at = f"{start_value} 00:00:00" if start_value else None
    end_at = f"{end_value} 23:59:59" if end_value else None
    return start_at, end_at, start_value, end_value


def _dashboard_url(
    guild_id: int,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    search: str | None = None,
    alliance: str | None = None,
    limit: int | None = None,
) -> str:
    params: dict[str, Any] = {"guild_id": guild_id}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if search:
        params["search"] = search
    if alliance:
        params["alliance"] = alliance
    if limit:
        params["limit"] = limit
    return f"/dashboard?{urlencode(params)}"


def _dashboard_csv_url(
    guild_id: int,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    search: str | None = None,
    alliance: str | None = None,
) -> str:
    params: dict[str, Any] = {"guild_id": guild_id}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if search:
        params["search"] = search
    if alliance:
        params["alliance"] = alliance
    return f"/dashboard/export.csv?{urlencode(params)}"


def _status_url(guild_id: int, page: int) -> str:
    return f"/status?{urlencode({'guild_id': guild_id, 'page': page})}"


def _pagination_items(
    guild_id: int,
    current_page: int,
    total_pages: int,
) -> list[dict[str, Any]]:
    if total_pages <= 1:
        return []

    pages = {total_pages, current_page}
    pages.update(range(1, min(5, total_pages) + 1))
    for page in range(current_page - 2, current_page + 3):
        if 1 <= page <= total_pages:
            pages.add(page)

    items: list[dict[str, Any]] = []
    previous_page = 0
    for page in sorted(pages):
        if previous_page and page - previous_page > 1:
            items.append({"type": "ellipsis", "label": "..."})
        items.append(
            {
                "type": "page",
                "label": str(page),
                "page": page,
                "href": _status_url(guild_id, page),
                "active": page == current_page,
            }
        )
        previous_page = page
    return items


def _quick_dashboard_filters(
    guild_id: int,
    *,
    start_date: str,
    end_date: str,
    search: str,
    alliance: str,
    limit: int,
) -> list[dict[str, Any]]:
    items = []
    for label, period in (
        ("최근 7일", "7d"),
        ("최근 30일", "30d"),
        ("이번 달", "month"),
        ("전체", "all"),
    ):
        quick_start, quick_end = _dashboard_period_dates(period)
        items.append(
            {
                "label": label,
                "href": _dashboard_url(
                    guild_id,
                    start_date=quick_start,
                    end_date=quick_end,
                    search=search,
                    alliance=alliance,
                    limit=limit,
                ),
                "active": (quick_start or "") == start_date
                and (quick_end or "") == end_date,
            }
        )
    return items


def _overview_from_attendance_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    session_keys = {row["started_at"] for row in rows if row.get("started_at")}
    attendance_rows = [row for row in rows if row.get("discord_id") is not None]
    unique_users = {row["discord_id"] for row in attendance_rows}
    session_count = len(session_keys)
    total_count = len(attendance_rows)
    return {
        "session_count": session_count,
        "total_attendance_count": total_count,
        "unique_user_count": len(unique_users),
        "average_attendance_count": round(total_count / session_count)
        if session_count
        else 0,
    }


def _daily_stats_from_attendance_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, set[Any]]] = {}
    for row in rows:
        started_at = str(row.get("started_at") or "")
        if not started_at:
            continue
        day = started_at[:10]
        bucket = grouped.setdefault(
            day,
            {"sessions": set(), "attendance": set(), "users": set()},
        )
        bucket["sessions"].add(started_at)
        if row.get("discord_id") is not None:
            bucket["attendance"].add((started_at, row["discord_id"]))
            bucket["users"].add(row["discord_id"])
    return [
        {
            "attendance_date": day,
            "session_count": len(values["sessions"]),
            "attendance_count": len(values["attendance"]),
            "unique_user_count": len(values["users"]),
        }
        for day, values in sorted(grouped.items(), reverse=True)
    ]


def _alliance_stats_from_attendance_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, set[Any]]] = {}
    for row in rows:
        if row.get("discord_id") is None:
            continue
        alliance_name = str(row.get("alliance_name") or "미분류")
        bucket = grouped.setdefault(
            alliance_name,
            {"sessions": set(), "attendance": set(), "users": set()},
        )
        started_at = str(row.get("started_at") or "")
        bucket["sessions"].add(started_at)
        bucket["attendance"].add((started_at, row["discord_id"]))
        bucket["users"].add(row["discord_id"])
    return sorted(
        [
            {
                "alliance_name": alliance_name,
                "session_count": len(values["sessions"]),
                "attendance_count": len(values["attendance"]),
                "unique_user_count": len(values["users"]),
            }
            for alliance_name, values in grouped.items()
        ],
        key=lambda row: (-row["attendance_count"], row["alliance_name"]),
    )


def _user_stats_from_attendance_rows(
    rows: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    grouped: dict[int, dict[str, Any]] = {}
    for row in rows:
        discord_id = _parse_optional_int(row.get("discord_id"))
        if discord_id is None:
            continue
        bucket = grouped.setdefault(
            discord_id,
            {
                "discord_id": discord_id,
                "discord_nickname": str(row.get("discord_nickname") or ""),
                "alliance_name": str(row.get("alliance_name") or "미분류"),
                "attendance_keys": set(),
            },
        )
        started_at = str(row.get("started_at") or "")
        if started_at:
            bucket["attendance_keys"].add(started_at)
    ranked = sorted(
        grouped.values(),
        key=lambda row: (
            -len(row["attendance_keys"]),
            str(row["discord_nickname"]),
        ),
    )[:limit]
    return [
        {
            "user_id": index,
            "discord_id": int(row["discord_id"]),
            "discord_nickname": str(row["discord_nickname"]),
            "alliance_name": str(row["alliance_name"]),
            "attendance_count": len(row["attendance_keys"]),
        }
        for index, row in enumerate(ranked, start=1)
    ]


def _percent(value: int | float | Decimal, total: int | float | Decimal) -> float:
    total_decimal = Decimal(str(total or 0))
    if total_decimal <= 0:
        return 0.0
    value_decimal = Decimal(str(value or 0))
    return float((value_decimal / total_decimal) * Decimal("100"))


def _percent_text(value: int | float | Decimal, total: int | float | Decimal) -> str:
    return f"{_percent(value, total):.1f}%"


def _attendance_where(
    guild_id: int,
    start_at: str | None,
    end_at: str | None,
) -> tuple[str, list[Any]]:
    clauses = ["s.guild_id = %s"]
    params: list[Any] = [guild_id]
    if start_at:
        clauses.append("s.started_at >= %s")
        params.append(start_at)
    if end_at:
        clauses.append("s.started_at <= %s")
        params.append(end_at)
    return " AND ".join(clauses), params


def _count_period_sessions(
    guild_id: int,
    start_at: str | None,
    end_at: str | None,
) -> int:
    where_sql, params = _attendance_where(guild_id, start_at, end_at)
    row = database.fetchone(
        f"""
        SELECT COUNT(*) AS session_count
        FROM attendance_sessions s
        WHERE {where_sql}
        """,
        tuple(params),
    )
    return int(row["session_count"] or 0) if row else 0


def _alliance_attendance_member_rows(
    guild_id: int,
    start_at: str | None,
    end_at: str | None,
) -> list[dict[str, Any]]:
    where_sql, params = _attendance_where(guild_id, start_at, end_at)
    rows = database.fetchall(
        f"""
        SELECT
            s.attendance_id,
            s.started_at,
            u.user_id,
            u.discord_id,
            u.discord_nickname,
            u.alliance_id,
            COALESCE(a.alliance_name, '미분류') AS alliance_name
        FROM attendance_sessions s
        INNER JOIN attendance_entries e ON e.attendance_id = s.attendance_id
        INNER JOIN users u ON u.user_id = e.user_id
        LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
        WHERE {where_sql}
        """,
        tuple(params),
    )
    return [
        {
            "attendance_id": int(row["attendance_id"]),
            "started_at": str(row["started_at"] or ""),
            "user_id": int(row["user_id"]),
            "discord_id": int(row["discord_id"]),
            "discord_nickname": str(row["discord_nickname"]),
            "alliance_id": _parse_optional_int(row["alliance_id"]),
            "alliance_name": str(row["alliance_name"] or "미분류"),
        }
        for row in rows
    ]


def _filter_alliance_rows(
    rows: list[dict[str, Any]],
    alliance_ids: int | set[int] | list[int] | tuple[int, ...],
) -> list[dict[str, Any]]:
    allowed_ids = (
        {int(alliance_ids)}
        if isinstance(alliance_ids, int)
        else {int(alliance_id) for alliance_id in alliance_ids}
    )
    return [row for row in rows if row.get("alliance_id") in allowed_ids]


def _selected_alliance_ids(selected_alliance: dict[str, Any]) -> set[int]:
    if selected_alliance.get("is_other"):
        return {
            int(alliance_id)
            for alliance_id in selected_alliance.get("alliance_ids", [])
        }
    alliance_id = _parse_optional_int(selected_alliance.get("alliance_id"))
    return {alliance_id} if alliance_id is not None else set()


def _alliance_overview(
    guild_id: int,
    alliance_ids: int | set[int] | list[int] | tuple[int, ...],
    start_at: str | None,
    end_at: str | None,
) -> dict[str, Any]:
    total_sessions = _count_period_sessions(guild_id, start_at, end_at)
    rows = _filter_alliance_rows(
        _alliance_attendance_member_rows(guild_id, start_at, end_at),
        alliance_ids,
    )
    alliance_sessions = len({row["attendance_id"] for row in rows})
    attendance_count = len(rows)
    unique_users = len({row["user_id"] for row in rows})
    average_count = round(attendance_count / alliance_sessions, 1) if alliance_sessions else 0
    return {
        "total_sessions": total_sessions,
        "alliance_session_count": alliance_sessions,
        "attendance_count": attendance_count,
        "unique_user_count": unique_users,
        "average_count": average_count,
        "session_rate": _percent_text(alliance_sessions, total_sessions),
    }


def _alliance_user_rankings(
    guild_id: int,
    alliance_ids: int | set[int] | list[int] | tuple[int, ...],
    start_at: str | None,
    end_at: str | None,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    total_sessions = _count_period_sessions(guild_id, start_at, end_at)
    rows = _filter_alliance_rows(
        _alliance_attendance_member_rows(guild_id, start_at, end_at),
        alliance_ids,
    )
    grouped: dict[int, dict[str, Any]] = {}
    for row in rows:
        bucket = grouped.setdefault(
            int(row["user_id"]),
            {
                "user_id": int(row["user_id"]),
                "discord_nickname": str(row["discord_nickname"]),
                "attendance_ids": set(),
                "last_attended_at": "",
            },
        )
        bucket["attendance_ids"].add(int(row["attendance_id"]))
        started_at = str(row["started_at"] or "")
        if started_at > str(bucket["last_attended_at"] or ""):
            bucket["last_attended_at"] = started_at

    ranked_rows = sorted(
        grouped.values(),
        key=lambda row: (-len(row["attendance_ids"]), row["discord_nickname"]),
    )[: int(limit)]
    return [
        {
            "rank": index,
            "user_id": int(row["user_id"]),
            "discord_nickname": str(row["discord_nickname"]),
            "attendance_count": len(row["attendance_ids"]),
            "participation_rate": _percent(
                len(row["attendance_ids"]),
                total_sessions,
            ),
            "participation_rate_text": _percent_text(
                len(row["attendance_ids"]),
                total_sessions,
            ),
            "last_attended_at": row["last_attended_at"] or "",
        }
        for index, row in enumerate(ranked_rows, start=1)
    ]


def _alliance_hour_stats(
    guild_id: int,
    alliance_ids: int | set[int] | list[int] | tuple[int, ...],
    start_at: str | None,
    end_at: str | None,
) -> list[dict[str, Any]]:
    rows = _filter_alliance_rows(
        _alliance_attendance_member_rows(guild_id, start_at, end_at),
        alliance_ids,
    )
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        started_at = str(row["started_at"] or "")
        hour_label = started_at[11:13] if len(started_at) >= 13 else ""
        if not hour_label.isdigit():
            continue
        bucket = grouped.setdefault(
            hour_label,
            {"attendance_count": 0, "sessions": set(), "users": set()},
        )
        bucket["attendance_count"] += 1
        bucket["sessions"].add(int(row["attendance_id"]))
        bucket["users"].add(int(row["user_id"]))
    ranked_rows = sorted(
        grouped.items(),
        key=lambda item: (-int(item[1]["attendance_count"]), item[0]),
    )[:24]
    return [
        {
            "hour": hour_label,
            "label": f"{int(hour_label):02d}:00",
            "attendance_count": int(values["attendance_count"]),
            "session_count": len(values["sessions"]),
            "unique_user_count": len(values["users"]),
            "average_count": (
                round(int(values["attendance_count"]) / len(values["sessions"]), 1)
                if values["sessions"]
                else 0
            ),
        }
        for hour_label, values in ranked_rows
    ]


def _alliance_daily_rows(
    guild_id: int,
    alliance_ids: int | set[int] | list[int] | tuple[int, ...],
    start_at: str | None,
    end_at: str | None,
) -> list[dict[str, Any]]:
    rows = _filter_alliance_rows(
        _alliance_attendance_member_rows(guild_id, start_at, end_at),
        alliance_ids,
    )
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        started_at = str(row["started_at"] or "")
        attendance_date = started_at[:10]
        if not attendance_date:
            continue
        bucket = grouped.setdefault(
            attendance_date,
            {"attendance_count": 0, "sessions": set(), "users": set()},
        )
        bucket["attendance_count"] += 1
        bucket["sessions"].add(int(row["attendance_id"]))
        bucket["users"].add(int(row["user_id"]))
    ranked_rows = sorted(grouped.items(), key=lambda item: item[0], reverse=True)[:90]
    return [
        {
            "attendance_date": attendance_date,
            "attendance_count": int(values["attendance_count"]),
            "session_count": len(values["sessions"]),
            "unique_user_count": len(values["users"]),
        }
        for attendance_date, values in ranked_rows
    ]


def _alliance_weekday_stats(daily_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    weekday_labels = ("월", "화", "수", "목", "금", "토", "일")
    grouped = {
        index: {"attendance_count": 0, "session_count": 0, "unique_user_total": 0}
        for index in range(7)
    }
    for row in daily_rows:
        try:
            day = datetime.strptime(str(row["attendance_date"]), "%Y-%m-%d").date()
        except ValueError:
            continue
        bucket = grouped[day.weekday()]
        bucket["attendance_count"] += int(row["attendance_count"])
        bucket["session_count"] += int(row["session_count"])
        bucket["unique_user_total"] += int(row["unique_user_count"])
    return [
        {
            "label": weekday_labels[index],
            "attendance_count": values["attendance_count"],
            "session_count": values["session_count"],
            "average_count": (
                round(values["attendance_count"] / values["session_count"], 1)
                if values["session_count"]
                else 0
            ),
        }
        for index, values in sorted(
            grouped.items(),
            key=lambda item: (-item[1]["attendance_count"], item[0]),
        )
        if values["attendance_count"] > 0
    ]


def _current_week_bounds() -> tuple[str, str]:
    today = datetime.now(KST).date()
    start = today - timedelta(days=today.weekday())
    return f"{start.isoformat()} 00:00:00", f"{today.isoformat()} 23:59:59"


def _current_month_bounds() -> tuple[str, str]:
    today = datetime.now(KST).date()
    start = today.replace(day=1)
    return f"{start.isoformat()} 00:00:00", f"{today.isoformat()} 23:59:59"


def _my_alliance_url(
    guild_id: int,
    *,
    alliance_id: int | str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    params: dict[str, Any] = {"guild_id": guild_id}
    if alliance_id:
        params["alliance_id"] = alliance_id
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    return f"/my-alliance?{urlencode(params)}"


def _settings_to_dict(settings: Any) -> dict[str, int | None]:
    return {
        "guild_id": settings.guild_id,
        "admin_channel_id": settings.admin_channel_id,
        "attendance_voice_channel_id": settings.attendance_voice_channel_id,
        "log_channel_id": settings.log_channel_id,
        "timer": settings.timer,
        "attendance_available_timer": settings.attendance_available_timer,
    }


def _normalize_discord_channel(channel: dict[str, Any]) -> dict[str, Any]:
    channel_id = str(channel["id"])
    name = str(channel.get("name") or channel_id)
    parent_id = channel.get("parent_id")
    return {
        "id": channel_id,
        "name": name,
        "label": name,
        "type": int(channel.get("type") or 0),
        "parent_id": str(parent_id) if parent_id else "",
        "position": int(channel.get("position") or 0),
    }


def _load_guild_channels(guild_id: int) -> dict[str, Any]:
    channels = _discord_bot_get_cached(f"/guilds/{guild_id}/channels")
    normalized = [_normalize_discord_channel(channel) for channel in channels]
    normalized.sort(key=lambda channel: (channel["position"], channel["name"].lower()))
    return {
        "text": [
            channel
            for channel in normalized
            if channel["type"] in {0, 5}
        ],
        "voice": [
            channel
            for channel in normalized
            if channel["type"] == 2
        ],
    }


def _normalize_discord_role(role: dict[str, Any]) -> dict[str, Any]:
    role_id = str(role["id"])
    name = str(role.get("name") or role_id)
    return {
        "id": role_id,
        "name": name,
        "label": name,
        "position": int(role.get("position") or 0),
        "managed": bool(role.get("managed")),
    }


def _load_guild_roles(guild_id: int) -> list[dict[str, Any]]:
    roles = _discord_bot_get_cached(f"/guilds/{guild_id}/roles")
    normalized = [
        _normalize_discord_role(role)
        for role in roles
        if str(role.get("id")) != str(guild_id)
    ]
    normalized.sort(key=lambda role: (-role["position"], role["name"].lower()))
    return normalized


def _channel_ids(channels: list[dict[str, Any]]) -> set[int]:
    return {int(channel["id"]) for channel in channels}


def _role_ids(roles: list[dict[str, Any]]) -> set[int]:
    return {int(role["id"]) for role in roles}


def _find_role_name(roles: list[dict[str, Any]], role_id: int) -> str | None:
    for role in roles:
        if int(role["id"]) == role_id:
            return str(role["label"])
    return None


def _parse_optional_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    return int(value)


def _validate_channel_value(
    field_label: str,
    raw_value: str | None,
    allowed_ids: set[int],
    errors: list[str],
) -> int | None:
    try:
        channel_id = _parse_optional_int(raw_value)
    except ValueError:
        errors.append(f"{field_label} 값이 올바르지 않습니다.")
        return None
    if channel_id is not None and channel_id not in allowed_ids:
        errors.append(f"{field_label}은 이 서버의 선택 가능한 채널이어야 합니다.")
        return None
    return channel_id


def _validate_timer_value(
    field_label: str,
    raw_value: str | None,
    errors: list[str],
) -> int | None:
    try:
        timer_value = _parse_optional_int(raw_value)
    except ValueError:
        errors.append(f"{field_label}은 숫자로 입력해야 합니다.")
        return None
    if timer_value is not None and not 1 <= timer_value <= 86400:
        errors.append(f"{field_label}은 1초부터 86400초 사이로 입력해야 합니다.")
        return None
    return timer_value


def _settings_form_from_values(values: dict[str, Any]) -> dict[str, Any]:
    return {
        "admin_channel_id": values.get("admin_channel_id"),
        "attendance_voice_channel_id": values.get("attendance_voice_channel_id"),
        "log_channel_id": values.get("log_channel_id"),
        "timer": values.get("timer"),
        "attendance_available_timer": values.get("attendance_available_timer"),
    }


def _alliance_role_form_from_values(values: dict[str, Any]) -> dict[str, str]:
    return {
        "alliance_name": str(values.get("alliance_name") or ""),
        "role_id": str(values.get("role_id") or ""),
    }


def _settings_discord_options(
    guild_id: int,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], str, str]:
    channel_error = ""
    role_error = ""
    channels = {"text": [], "voice": []}
    roles: list[dict[str, Any]] = []
    try:
        channels = _load_guild_channels(guild_id)
    except Exception as exc:
        channel_error = f"Discord 채널 목록을 불러오지 못했습니다. {exc}"
    try:
        roles = _load_guild_roles(guild_id)
    except Exception as exc:
        role_error = f"Discord 역할 목록을 불러오지 못했습니다. {exc}"
    return channels, roles, channel_error, role_error


def _settings_template_context(
    auth: dict[str, Any],
    guild_id: int,
    *,
    guild_settings: Any,
    form: dict[str, Any],
    channels: dict[str, list[dict[str, Any]]],
    roles: list[dict[str, Any]],
    channel_error: str,
    role_error: str,
    saved: str | None,
    errors: list[str],
    report_form: dict[str, str],
    alliance_role_form: dict[str, str] | None = None,
    item_price_form: dict[str, str] | None = None,
    settings_active_tab: str | None = None,
) -> dict[str, Any]:
    return {
        "auth": auth,
        "settings": _settings_to_dict(guild_settings),
        "form": form,
        "channels": channels,
        "roles": roles,
        "channel_error": channel_error,
        "role_error": role_error,
        "saved": saved,
        "errors": errors,
        "report_options": REPORT_OPTIONS,
        "report_form": report_form,
        "report_settings": _load_report_settings(guild_id),
        "alliance_role_form": alliance_role_form or {"alliance_name": "", "role_id": ""},
        "alliance_role_mappings": database.get_guild_alliance_role_mappings(guild_id),
        "bookkeepers": (
            database.get_guild_bookkeepers(guild_id)
            if auth["selected_server"].get("can_manage_bookkeepers")
            else []
        ),
        "bookkeeper_candidates": (
            database.get_guild_bookkeeper_candidates(guild_id)
            if auth["selected_server"].get("can_manage_bookkeepers")
            else []
        ),
        "settings_active_tab": settings_active_tab or _settings_active_tab(saved),
        "active_page": "settings",
    }


def _settings_active_tab(saved: str | None) -> str:
    if saved in {"report", "report_status", "report_deleted", "report_error"}:
        return "reports"
    if saved in {"alliance_role", "alliance_role_deleted", "alliance_role_error"}:
        return "alliance"
    if saved in {"bookkeeper", "bookkeeper_deleted", "bookkeeper_error"}:
        return "bookkeepers"
    if saved == "1":
        return "channels"
    return "alliance"


def _default_report_form() -> dict[str, str]:
    return {
        "report_name": "",
        "frequency": "",
        "run_time": "",
        "period_type": "",
        "dataset": "",
        "group_by": "",
        "rank_target": "",
        "metric": "",
        "limit": "",
        "output": "",
        "title": "",
        "group_header": "",
        "row_template": "",
        "empty_text": "",
        "channel_id": "",
        "status": "on",
    }


def _default_item_price_form() -> dict[str, str]:
    return {
        "item_name": "",
        "default_price": "",
    }


def _item_price_form_from_values(values: dict[str, Any]) -> dict[str, str]:
    return {
        "item_name": str(values.get("item_name") or ""),
        "default_price": str(values.get("default_price") or ""),
    }


def _option_label(options: tuple[dict[str, str], ...], value: str) -> str:
    return next((option["label"] for option in options if option["value"] == value), value)


def _report_sentence(report: dict[str, Any]) -> str:
    return (
        f"{report['frequency_label']} {report['run_time_label']} · "
        f"{report['period_label']} · {report['group_label']} "
        f"{report['metric_label']} TOP {report['limit']} · "
        f"#{report['channel_name']}"
    )


def _format_run_time(value: str | None) -> str:
    if not value:
        return "00시"
    hour, _, minute = value.partition(":")
    if minute == "00" or not minute:
        return f"{int(hour):02d}시"
    return f"{int(hour):02d}시 {int(minute):02d}분"


def _report_json(value: Any, fallback: dict[str, Any]) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            return parsed
    return dict(fallback)


def _coerce_report_limit(raw_value: str | None, errors: list[str]) -> int:
    try:
        value = int(raw_value or 10)
    except ValueError:
        errors.append("표시 개수는 숫자로 입력해주세요.")
        return 10
    if value < 1 or value > 30:
        errors.append("표시 개수는 1부터 30 사이로 입력해주세요.")
        return max(1, min(value, 30))
    return value


def _clean_template_value(
    raw_value: str | None,
    fallback: str,
    *,
    max_length: int = 120,
) -> str:
    value = (raw_value or "").strip()
    if not value:
        return fallback
    return value[:max_length]


def _report_configs_from_form(
    form_data: dict[str, Any],
    errors: list[str],
    *,
    require_channel: bool,
    channels: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, str], dict[str, Any], dict[str, Any], dict[str, Any], int | None, str | None]:
    defaults = _default_report_form()
    report_name = _clean_template_value(
        form_data.get("report_name"),
        defaults["report_name"],
        max_length=80,
    )
    frequency = _validate_report_option(
        "발송 주기",
        form_data.get("frequency"),
        REPORT_FREQUENCY_OPTIONS,
        errors,
    )
    run_time = _validate_run_time(form_data.get("run_time"), errors)
    period_type = _validate_report_option(
        "조회 기간",
        form_data.get("period_type"),
        REPORT_PERIOD_OPTIONS,
        errors,
    )
    dataset = _validate_report_option(
        "데이터",
        form_data.get("dataset"),
        REPORT_DATASET_OPTIONS,
        errors,
    )
    group_by = _validate_report_option(
        "그룹",
        form_data.get("group_by"),
        REPORT_GROUP_OPTIONS,
        errors,
    )
    rank_target = _validate_report_option(
        "랭킹 대상",
        form_data.get("rank_target"),
        REPORT_RANK_TARGET_OPTIONS,
        errors,
    )
    metric = _validate_report_option(
        "집계값",
        form_data.get("metric"),
        REPORT_METRIC_OPTIONS,
        errors,
    )
    output = _validate_report_option(
        "출력 방식",
        form_data.get("output"),
        REPORT_OUTPUT_OPTIONS,
        errors,
    )
    if rank_target == "alliance":
        group_by = "none"
        output = "ranking"
    limit = _coerce_report_limit(form_data.get("limit"), errors)
    title = _clean_template_value(
        form_data.get("title"),
        defaults["title"],
        max_length=100,
    )
    group_header = _clean_template_value(
        form_data.get("group_header"),
        defaults["group_header"],
        max_length=80,
    )
    row_template = _clean_template_value(
        form_data.get("row_template"),
        defaults["row_template"],
        max_length=100,
    )
    empty_text = _clean_template_value(
        form_data.get("empty_text"),
        defaults["empty_text"],
        max_length=80,
    )

    channel_id: int | None = None
    channel_name: str | None = None
    if require_channel:
        channel_rows = channels or []
        channel_id = _validate_channel_value(
            "알람 채널",
            form_data.get("channel_id"),
            _channel_ids(channel_rows),
            errors,
        )
        channel_name = (
            _find_channel_name(channel_rows, channel_id)
            if channel_id is not None
            else None
        )
        if channel_id is None or channel_name is None:
            errors.append("알람을 받을 Discord 텍스트 채널을 선택해주세요.")
    elif form_data.get("channel_id"):
        try:
            channel_id = int(form_data.get("channel_id") or "")
        except ValueError:
            channel_id = None

    schedule_json = {
        "type": frequency,
        "time": run_time,
        "timezone": "Asia/Seoul",
    }
    query_json = {
        "dataset": dataset,
        "period": period_type,
        "group_by": group_by,
        "rank_target": rank_target,
        "metric": metric,
        "limit": limit,
    }
    render_json = {
        "output": output,
        "title": title,
        "group_header": group_header,
        "row": row_template,
        "empty": empty_text,
    }
    report_form = {
        "report_name": report_name,
        "frequency": frequency,
        "run_time": run_time,
        "period_type": period_type,
        "dataset": dataset,
        "group_by": group_by,
        "rank_target": rank_target,
        "metric": metric,
        "limit": str(limit),
        "output": output,
        "title": title,
        "group_header": group_header,
        "row_template": row_template,
        "empty_text": empty_text,
        "channel_id": str(channel_id or form_data.get("channel_id") or ""),
        "status": "on",
    }
    return report_form, schedule_json, query_json, render_json, channel_id, channel_name


def _report_period_bounds(period_type: str, now: datetime | None = None) -> tuple[datetime, datetime]:
    current = (now or datetime.now(KST)).astimezone(KST)
    today = current.date()
    if period_type == "yesterday":
        target = today - timedelta(days=1)
        return _report_day_start(target), _report_day_end(target)
    if period_type == "recent_3_days":
        return _report_day_start(today - timedelta(days=2)), _report_day_end(today)
    if period_type == "recent_7_days":
        return _report_day_start(today - timedelta(days=6)), _report_day_end(today)
    if period_type == "this_week":
        return _report_day_start(today - timedelta(days=today.weekday())), _report_day_end(today)
    if period_type == "this_month":
        return _report_day_start(today.replace(day=1)), _report_day_end(today)
    return _report_day_start(today), _report_day_end(today)


def _report_day_start(value: Any) -> datetime:
    return datetime.combine(value, datetime.min.time(), tzinfo=KST)


def _report_day_end(value: Any) -> datetime:
    return datetime.combine(value, datetime.max.time().replace(microsecond=0), tzinfo=KST)


def _report_format_datetime(value: datetime) -> str:
    return value.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")


def _render_report_preview(
    guild_id: int,
    schedule_json: dict[str, Any],
    query_json: dict[str, Any],
    render_json: dict[str, Any],
    *,
    guild_name: str | None = None,
) -> str:
    start_at, end_at = _report_period_bounds(str(query_json.get("period") or "today"))
    warning = ""
    try:
        rows = database.get_report_attendance_ranking(
            guild_id,
            _report_format_datetime(start_at),
            _report_format_datetime(end_at),
            group_by=str(query_json.get("group_by") or "alliance"),
            rank_target=str(query_json.get("rank_target") or "user"),
            metric=str(query_json.get("metric") or "attendance_count"),
            limit=int(query_json.get("limit") or 10),
        )
    except Exception as exc:
        rows = []
        warning = f"\n\n미리보기 데이터 조회 실패: {exc}"
        print(f"[report-preview] failed guild_id={guild_id}: {exc}")
    preview = _format_report_message(
        rows,
        schedule_json,
        query_json,
        render_json,
        guild_name=guild_name,
        start_at=start_at,
        end_at=end_at,
    )
    return f"{preview}{warning}"


def _format_report_message(
    rows: list[dict[str, Any]],
    schedule_json: dict[str, Any],
    query_json: dict[str, Any],
    render_json: dict[str, Any],
    *,
    guild_name: str | None,
    start_at: datetime,
    end_at: datetime,
) -> str:
    title = str(render_json.get("title") or "통계 알림")
    group_template = str(render_json.get("group_header") or "{group_name}")
    row_template = str(render_json.get("row") or "{rank}. {label} - {value}회")
    empty_text = str(render_json.get("empty") or "출석 기록 없음")
    output = str(render_json.get("output") or "grouped_ranking")
    schedule_text = (
        f"{_option_label(REPORT_FREQUENCY_OPTIONS, str(schedule_json.get('type') or 'daily'))} "
        f"{_format_run_time(str(schedule_json.get('time') or '00:00'))}"
    )
    lines = [
        f"**{title}**",
        f"서버: {guild_name or '선택 서버'}",
        f"기간: {_report_format_datetime(start_at)} ~ {_report_format_datetime(end_at)}",
        f"예약: {schedule_text}",
        "",
    ]
    if not rows:
        lines.append(empty_text)
        return "\n".join(lines)

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("group_name") or "전체"), []).append(row)

    if output == "ranking" or str(query_json.get("group_by")) == "none":
        lines.append("```")
        lines.extend(_format_report_rows(rows, row_template))
        lines.append("```")
        return "\n".join(lines)

    for group_name, group_rows in grouped.items():
        lines.append(_safe_report_template(group_template, group_name=group_name))
        lines.append("```")
        lines.extend(_format_report_rows(group_rows, row_template))
        lines.append("```")
        lines.append("")
    return "\n".join(lines).strip()


def _format_report_rows(rows: list[dict[str, Any]], row_template: str) -> list[str]:
    lines = []
    for index, row in enumerate(rows, start=1):
        lines.append(
            _safe_report_template(
                row_template,
                rank=int(row.get("rank") or index),
                label=str(row.get("label") or "-"),
                value=int(row.get("value") or 0),
                group_name=str(row.get("group_name") or "전체"),
            )
        )
    return lines


def _safe_report_template(template: str, **values: Any) -> str:
    try:
        return template.format(**values)
    except (KeyError, IndexError, ValueError):
        return str(template)


def _decimal_from_form(
    field_label: str,
    raw_value: str | None,
    errors: list[str],
    *,
    default: Decimal = Decimal("0"),
    minimum: Decimal = Decimal("0"),
) -> Decimal:
    value = (raw_value or "").replace(",", "").strip()
    if not value:
        return default
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        errors.append(f"{field_label}은 숫자로 입력해주세요.")
        return default
    if parsed < minimum:
        errors.append(f"{field_label}은 {minimum} 이상이어야 합니다.")
        return default
    return parsed


def _decimal_input(value: Any) -> str:
    decimal_value = Decimal(str(value or "0"))
    return format(decimal_value.normalize(), "f") if decimal_value else "0"


def _rounded_integer(value: Any) -> Decimal:
    return Decimal(str(value or "0")).quantize(Decimal("1"), rounding=ROUND_FLOOR)


def _rounded_divide(value: Any, divisor: int | Decimal) -> Decimal:
    divisor_decimal = Decimal(str(divisor or 0))
    if divisor_decimal <= 0:
        return Decimal("0")
    return _rounded_integer(_rounded_integer(value) / divisor_decimal)


def _rounded_allocation(value: Any, count: int) -> list[Decimal]:
    total = _rounded_integer(value)
    if count <= 0:
        return []
    base = _rounded_divide(total, count)
    return [base for _ in range(count)]


def _rounded_allocation_text(amounts: list[Decimal]) -> str:
    if not amounts:
        return "0"
    minimum = min(amounts)
    maximum = max(amounts)
    if minimum == maximum:
        return _money_text(minimum)
    return f"{_money_text(minimum)}~{_money_text(maximum)}"


def _rounded_allocation_cash_text(amounts: list[Decimal], adena_rate: Any) -> str:
    if not amounts:
        return "0원"
    cash_amounts = [_cash_from_adena(amount, adena_rate) for amount in amounts]
    minimum = min(cash_amounts)
    maximum = max(cash_amounts)
    if minimum == maximum:
        return _cash_text(minimum)
    return f"{_cash_text(minimum)}~{_cash_text(maximum)}"


def _cash_from_adena(adena_amount: Any, adena_rate: Any) -> Decimal:
    amount = _rounded_integer(adena_amount)
    rate = _rounded_integer(adena_rate)
    if amount <= 0 or rate <= 0:
        return Decimal("0")
    return _rounded_integer(amount / Decimal("10000") * rate)


def _money_text(value: Any, places: int = 0) -> str:
    decimal_value = Decimal(str(value or "0"))
    quantizer = Decimal("1") if places == 0 else Decimal(f"0.{'0' * (places - 1)}1")
    rounded = decimal_value.quantize(quantizer, rounding=ROUND_FLOOR)
    if rounded == rounded.to_integral():
        return f"{int(rounded):,}"
    return f"{rounded:,.{places}f}".rstrip("0").rstrip(".")


def _cash_text(value: Any) -> str:
    return f"{_money_text(value)}원"


def _cash_price_to_game_money(cash_price: Any, adena_rate: Any) -> Decimal:
    cash = Decimal(str(cash_price or "0"))
    rate = Decimal(str(adena_rate or "0"))
    if rate <= 0:
        return Decimal("0")
    return cash / rate * Decimal("10000")


def _item_cash_price(item_id: int, items: list[dict[str, Any]]) -> Decimal | None:
    for item in items:
        if int(item["item_id"]) == int(item_id):
            return Decimal(str(item.get("default_price") or "0"))
    return None


def _loot_prices_from_form(
    form_data: dict[str, Any],
    item_prices: list[dict[str, Any]],
    item_id: int | None,
    errors: list[str],
) -> tuple[Decimal, Decimal, Decimal]:
    adena_rate = _decimal_from_form(
        "아데나 시세",
        form_data.get("adena_rate"),
        errors,
    )
    cash_price = _decimal_from_form(
        "원화 시세",
        form_data.get("cash_price_krw"),
        errors,
    )
    if cash_price <= 0 and item_id is not None:
        cash_price = _item_cash_price(item_id, item_prices) or Decimal("0")

    if cash_price > 0 and adena_rate > 0:
        return cash_price, _cash_price_to_game_money(cash_price, adena_rate), adena_rate
    if cash_price > 0 and adena_rate <= 0:
        errors.append("원화 시세를 입력한 경우 아데나 시세도 입력해주세요.")

    sale_price = _decimal_from_form(
        "분배 아데나",
        form_data.get("sale_price"),
        errors,
    )
    return cash_price, sale_price, adena_rate


def _fee_rate_from_form(
    form_data: dict[str, Any],
    errors: list[str],
) -> Decimal:
    fee_percent = _decimal_from_form(
        "수수료",
        form_data.get("fee_percent"),
        errors,
        default=Decimal("10"),
    )
    return fee_percent / Decimal("100")


def _decorate_item_prices(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    decorated = []
    for item in items:
        row = dict(item)
        row["default_price_input"] = _decimal_input(item.get("default_price"))
        row["default_price_text"] = _money_text(item.get("default_price"))
        decorated.append(row)
    return decorated


def _decorate_loot_events(
    events: list[dict[str, Any]],
    viewer_discord_id: int | None = None,
    guild_id: int | None = None,
    viewer_current_alliance_ids: set[int] | None = None,
) -> list[dict[str, Any]]:
    decorated_events = []
    member_group_cache: dict[int, dict[int, dict[str, Any]]] = {}
    fee_rule_cache: dict[int, list[dict[str, Any]]] = {}

    def member_groups_for(alliance_id: int | None) -> dict[int, dict[str, Any]]:
        if guild_id is None or alliance_id is None:
            return {}
        if alliance_id not in member_group_cache:
            member_group_cache[alliance_id] = database.get_member_payout_groups(
                guild_id,
                alliance_id,
            )
        return member_group_cache[alliance_id]

    def fee_rules_for(alliance_id: int | None) -> list[dict[str, Any]]:
        if guild_id is None or alliance_id is None:
            return []
        if alliance_id not in fee_rule_cache:
            fee_rule_cache[alliance_id] = database.get_alliance_payout_fee_rules(
                guild_id,
                alliance_id,
            )
        return fee_rule_cache[alliance_id]

    for event in events:
        row = dict(event)
        participant_count = int(event.get("total_participant_count") or 0)
        total_sale = _rounded_integer(event.get("total_sale_amount"))
        fee_rate = Decimal(str(event.get("fee_rate") or "0"))
        fee_amount = _rounded_integer(total_sale * fee_rate)
        total_net = total_sale - fee_amount
        per_member = _rounded_divide(total_net, participant_count)
        adena_rate = _rounded_integer(event.get("adena_rate"))
        total_sale_cash = _cash_from_adena(total_sale, adena_rate)
        fee_amount_cash = _cash_from_adena(fee_amount, adena_rate)
        total_net_cash = _cash_from_adena(total_net, adena_rate)
        per_member_cash = _cash_from_adena(per_member, adena_rate)

        row["cash_price_input"] = _decimal_input(event.get("cash_price_krw"))
        row["cash_price_text"] = _money_text(event.get("cash_price_krw"), places=0)
        row["sale_price_input"] = _decimal_input(event.get("sale_price"))
        row["sale_price_text"] = _money_text(total_sale)
        row["adena_rate_input"] = _decimal_input(event.get("adena_rate"))
        row["adena_rate_text"] = _money_text(adena_rate)
        row["fee_percent_input"] = _decimal_input(fee_rate * Decimal("100"))
        row["per_member_amount_display"] = per_member
        row["per_member_text"] = _money_text(per_member)
        row["per_member_cash_amount_display"] = per_member_cash
        row["per_member_cash_text"] = _cash_text(per_member_cash)
        row["total_sale_amount_display"] = total_sale
        row["total_net_amount_display"] = total_net
        row["fee_amount_display"] = fee_amount
        row["total_sale_cash_amount_display"] = total_sale_cash
        row["total_net_cash_amount_display"] = total_net_cash
        row["fee_amount_cash_amount_display"] = fee_amount_cash
        row["total_sale_text"] = _money_text(total_sale)
        row["total_net_text"] = _money_text(total_net)
        row["fee_amount_text"] = _money_text(fee_amount)
        row["total_sale_cash_text"] = _cash_text(total_sale_cash)
        row["total_net_cash_text"] = _cash_text(total_net_cash)
        row["fee_amount_cash_text"] = _cash_text(fee_amount_cash)
        row["fee_rate_percent_text"] = _money_text(
            Decimal(str(event.get("fee_rate") or "0")) * Decimal("100"),
        )
        event_datetime = _loot_event_datetime(row)
        row["distribution_card_time_label"] = (
            event_datetime.strftime("%m/%d %H시")
            if event_datetime is not None
            else str(event.get("event_time_label") or event.get("event_date") or "")
        )
        row["converted_text"] = _money_text(
            Decimal(str(event.get("total_net_amount") or "0"))
            * Decimal(str(event.get("adena_rate") or "0")),
            places=2,
        )
        payouts = []
        released_payout_alliance_ids: set[int] = set()
        viewer_participated = False
        viewer_release_visible = False
        viewer_alliance_ids: set[int] = set()
        viewer_alliance_names: set[str] = set()
        viewer_user_id = 0
        viewer_user_ids_by_alliance: dict[int, int] = {}
        members_by_alliance: dict[int, list[dict[str, Any]]] = {}
        for alliance in event.get("alliances", []):
            alliance_id = _parse_optional_int(alliance.get("alliance_id"))
            alliance_name = str(alliance.get("alliance_name") or "미분류")
            members = []
            for member in alliance.get("members", []):
                member_row = dict(member)
                if (
                    viewer_discord_id is not None
                    and int(member_row.get("discord_id") or 0) == viewer_discord_id
                ):
                    viewer_participated = True
                    viewer_user_id = int(member_row.get("user_id") or 0)
                    if alliance_id is not None:
                        viewer_alliance_ids.add(alliance_id)
                        viewer_user_ids_by_alliance[alliance_id] = int(
                            member_row.get("user_id") or 0
                        )
                    viewer_alliance_names.add(alliance_name)
                    member_row["is_viewer"] = True
                else:
                    member_row["is_viewer"] = False
                members.append(member_row)
            alliance["members"] = members
            alliance["alliance_id"] = alliance_id
            if alliance_id is not None:
                members_by_alliance[alliance_id] = members

        viewer_event_amount = Decimal("0")
        viewer_paid_amount = Decimal("0")
        viewer_unpaid_amount = Decimal("0")
        viewer_internal_fee_amount = Decimal("0")
        viewer_alliance_fee_amount = Decimal("0")
        viewer_internal_fee_share_amount = Decimal("0")
        viewer_alliance_amount = Decimal("0")
        viewer_gross_per_member_amount = Decimal("0")
        viewer_per_member_amount = Decimal("0")
        viewer_payout_alliance_ids = (
            set(viewer_current_alliance_ids or set())
            if viewer_current_alliance_ids
            else set(viewer_alliance_ids)
        )
        for payout in event.get("alliance_payouts", []):
            payout_row = dict(payout)
            payout_alliance_id = _parse_optional_int(payout.get("alliance_id"))
            payout_count = int(payout.get("participant_count") or 0)
            payout_net = _rounded_integer(payout.get("net_amount"))
            payout_per_member = _rounded_divide(payout_net, payout_count)
            payout_net_cash = _cash_from_adena(payout_net, adena_rate)
            payout_per_member_cash = _cash_from_adena(payout_per_member, adena_rate)
            payout_row["net_amount_display"] = payout_net
            payout_row["per_member_amount_display"] = payout_per_member
            payout_row["net_amount_cash_display"] = payout_net_cash
            payout_row["per_member_amount_cash_display"] = payout_per_member_cash
            payout_row["net_amount_text"] = _money_text(payout_net)
            payout_row["per_member_text"] = _money_text(payout_per_member)
            payout_row["net_amount_cash_text"] = _cash_text(payout_net_cash)
            payout_row["per_member_cash_text"] = _cash_text(payout_per_member_cash)
            payout_row["status_label"] = (
                "분배완료" if payout.get("payout_status") == "paid" else "미완료"
            )
            payout_row["next_status"] = (
                "unpaid" if payout.get("payout_status") == "paid" else "paid"
            )
            payout_row["next_status_label"] = (
                "미완료로 변경" if payout.get("payout_status") == "paid" else "완료 처리"
            )
            if payout.get("payout_status") == "paid" and payout_alliance_id is not None:
                released_payout_alliance_ids.add(payout_alliance_id)
            if (
                payout_alliance_id is not None
                and payout_alliance_id in viewer_payout_alliance_ids
                and payout.get("payout_status") == "paid"
            ):
                viewer_release_visible = True
                member_record = member_groups_for(payout_alliance_id).get(
                    int(event.get("distribution_id") or 0)
                )
                selected_rules = (
                    member_record.get("fee_lines", [])
                    if member_record is not None
                    else fee_rules_for(payout_alliance_id)
                )
                fee_lines_raw = _member_payout_fee_lines_from_rules(payout_net, selected_rules)
                internal_fee_amount = sum(
                    (Decimal(str(line["fee_amount"])) for line in fee_lines_raw),
                    Decimal("0"),
                )
                distributable_amount = payout_net - internal_fee_amount
                alliance_members = members_by_alliance.get(payout_alliance_id, [])
                alliance_member_count = len(alliance_members) or payout_count
                gross_share = _rounded_divide(total_sale, participant_count)
                alliance_fee_share = _rounded_integer(gross_share * fee_rate)
                internal_fee_share = _rounded_divide(internal_fee_amount, alliance_member_count)
                viewer_share = _rounded_divide(distributable_amount, alliance_member_count)
                viewer_payout_user_id = viewer_user_ids_by_alliance.get(
                    payout_alliance_id,
                    viewer_user_id,
                )
                paid_statuses = (
                    member_record.get("statuses", {})
                    if member_record is not None
                    else {}
                )
                is_viewer_paid = bool(paid_statuses.get(viewer_payout_user_id, False))

                viewer_alliance_amount += payout_net
                viewer_internal_fee_amount += internal_fee_amount
                viewer_alliance_fee_amount += alliance_fee_share
                viewer_internal_fee_share_amount += internal_fee_share
                viewer_gross_per_member_amount = gross_share
                viewer_event_amount += viewer_share
                viewer_per_member_amount = viewer_share
                if is_viewer_paid:
                    viewer_paid_amount += viewer_share
                else:
                    viewer_unpaid_amount += viewer_share
            payouts.append(payout_row)
        row["alliance_payouts"] = payouts
        row["has_released_payout"] = bool(released_payout_alliance_ids)
        row["all_payouts_released"] = bool(payouts) and all(
            payout.get("payout_status") == "paid" for payout in payouts
        )
        row["viewer_participated"] = viewer_participated
        row["viewer_release_visible"] = viewer_release_visible
        row["viewer_participation_label"] = "참여" if viewer_participated else "미참여"
        row["viewer_alliance_names"] = sorted(viewer_alliance_names)
        row["viewer_alliance_amount"] = viewer_alliance_amount
        row["viewer_alliance_amount_text"] = _money_text(viewer_alliance_amount)
        row["viewer_alliance_cash_amount"] = _cash_from_adena(viewer_alliance_amount, adena_rate)
        row["viewer_alliance_cash_text"] = _cash_text(row["viewer_alliance_cash_amount"])
        row["viewer_internal_fee_amount"] = viewer_internal_fee_amount
        row["viewer_internal_fee_amount_text"] = _money_text(viewer_internal_fee_amount)
        row["viewer_internal_fee_cash_amount"] = _cash_from_adena(
            viewer_internal_fee_amount,
            adena_rate,
        )
        row["viewer_internal_fee_cash_text"] = _cash_text(
            row["viewer_internal_fee_cash_amount"]
        )
        row["viewer_alliance_fee_amount"] = viewer_alliance_fee_amount
        row["viewer_alliance_fee_amount_text"] = _money_text(viewer_alliance_fee_amount)
        row["viewer_alliance_fee_cash_amount"] = _cash_from_adena(
            viewer_alliance_fee_amount,
            adena_rate,
        )
        row["viewer_alliance_fee_cash_text"] = _cash_text(row["viewer_alliance_fee_cash_amount"])
        row["viewer_internal_fee_share_amount"] = viewer_internal_fee_share_amount
        row["viewer_internal_fee_share_amount_text"] = _money_text(
            viewer_internal_fee_share_amount
        )
        row["viewer_internal_fee_share_cash_amount"] = _cash_from_adena(
            viewer_internal_fee_share_amount,
            adena_rate,
        )
        row["viewer_internal_fee_share_cash_text"] = _cash_text(
            row["viewer_internal_fee_share_cash_amount"]
        )
        row["viewer_gross_per_member_amount"] = viewer_gross_per_member_amount
        row["viewer_gross_per_member_text"] = _money_text(viewer_gross_per_member_amount)
        row["viewer_gross_per_member_cash_amount"] = _cash_from_adena(
            viewer_gross_per_member_amount,
            adena_rate,
        )
        row["viewer_gross_per_member_cash_text"] = _cash_text(
            row["viewer_gross_per_member_cash_amount"]
        )
        row["viewer_per_member_amount"] = viewer_per_member_amount
        row["viewer_per_member_text"] = _money_text(viewer_per_member_amount)
        row["viewer_per_member_cash_amount"] = _cash_from_adena(
            viewer_per_member_amount,
            adena_rate,
        )
        row["viewer_per_member_cash_text"] = _cash_text(row["viewer_per_member_cash_amount"])
        row["viewer_event_amount"] = viewer_event_amount
        row["viewer_event_amount_text"] = _money_text(viewer_event_amount)
        row["viewer_event_cash_amount"] = _cash_from_adena(viewer_event_amount, adena_rate)
        row["viewer_event_cash_text"] = _cash_text(row["viewer_event_cash_amount"])
        row["viewer_paid_amount"] = viewer_paid_amount
        row["viewer_paid_amount_text"] = _money_text(viewer_paid_amount)
        row["viewer_paid_cash_amount"] = _cash_from_adena(viewer_paid_amount, adena_rate)
        row["viewer_paid_cash_text"] = _cash_text(row["viewer_paid_cash_amount"])
        row["viewer_unpaid_amount"] = viewer_unpaid_amount
        row["viewer_unpaid_amount_text"] = _money_text(viewer_unpaid_amount)
        row["viewer_unpaid_cash_amount"] = _cash_from_adena(viewer_unpaid_amount, adena_rate)
        row["viewer_unpaid_cash_text"] = _cash_text(row["viewer_unpaid_cash_amount"])
        if not viewer_participated:
            row["viewer_payout_label"] = "미참여"
            row["viewer_payout_card_label"] = "미참여"
            row["viewer_payout_meta"] = "참여하지 않은 드랍"
            row["viewer_payout_class"] = "is-empty"
        elif viewer_unpaid_amount > 0:
            row["viewer_payout_label"] = "미수령"
            row["viewer_payout_card_label"] = "미수령"
            row["viewer_payout_meta"] = f"받은 {_money_text(viewer_paid_amount)} · 미수령 {_money_text(viewer_unpaid_amount)}"
            row["viewer_payout_class"] = "is-unpaid"
        else:
            row["viewer_payout_label"] = "수령완료"
            row["viewer_payout_card_label"] = "수령"
            row["viewer_payout_meta"] = f"받은 {_money_text(viewer_paid_amount)}"
            row["viewer_payout_class"] = "is-paid"
        payout_total = len(payouts)
        unpaid_count = sum(
            1
            for payout in payouts
            if payout.get("payout_status") != "paid"
        )
        paid_count = payout_total - unpaid_count
        if payout_total == 0:
            row["payout_summary_label"] = "분배 없음"
            row["payout_summary_meta"] = "혈맹 0개"
            row["payout_summary_class"] = "is-empty"
        elif unpaid_count == 0:
            row["payout_summary_label"] = "분배완료"
            row["payout_summary_meta"] = f"{paid_count}/{payout_total} 혈맹"
            row["payout_summary_class"] = "is-paid"
        else:
            row["payout_summary_label"] = "미완료"
            row["payout_summary_meta"] = f"{unpaid_count}개 미완료"
            row["payout_summary_class"] = "is-unpaid"
        decorated_events.append(row)
    return decorated_events


def _loot_event_datetime(event: dict[str, Any]) -> datetime | None:
    value = str(event.get("attendance_started_at") or "")
    try:
        return datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _loot_period_bounds(period: str | None) -> tuple[str, datetime | None, str]:
    normalized = period if period in {"all", "30d", "7d"} else "all"
    now = datetime.now(KST).replace(tzinfo=None)
    if normalized == "all":
        return normalized, None, "전체 기간"
    if normalized == "30d":
        return normalized, now - timedelta(days=30), "최근 한달"
    return "7d", now - timedelta(days=7), "최근 일주일"


def _filter_loot_events_by_period(
    events: list[dict[str, Any]],
    start_at: datetime | None,
) -> list[dict[str, Any]]:
    if start_at is None:
        return events
    return [
        event
        for event in events
        if (_loot_event_datetime(event) or datetime.min) >= start_at
    ]


def _filter_loot_events_by_viewer(
    events: list[dict[str, Any]],
    mine_only: bool,
) -> list[dict[str, Any]]:
    released_events = [
        event
        for event in events
        if event.get("viewer_release_visible")
        or (
            not event.get("viewer_participated")
            and event.get("all_payouts_released")
        )
    ]
    if not mine_only:
        return released_events
    return [event for event in released_events if event.get("viewer_release_visible")]


def _normalize_loot_status_filter(status: str | None) -> str:
    normalized = str(status or "").lower()
    if normalized in {"all", "전체"}:
        return "all"
    if normalized in {"paid", "received", "done", "수령"}:
        return "paid"
    if normalized in {"unpaid", "pending", "미수령"}:
        return "unpaid"
    return "unpaid"


def _filter_loot_events_by_status(
    events: list[dict[str, Any]],
    status: str,
) -> list[dict[str, Any]]:
    if status == "paid":
        return [
            event
            for event in events
            if event.get("viewer_participated")
            and Decimal(str(event.get("viewer_unpaid_amount") or "0")) <= 0
        ]
    if status == "unpaid":
        return [
            event
            for event in events
            if event.get("viewer_participated")
            and Decimal(str(event.get("viewer_unpaid_amount") or "0")) > 0
        ]
    return events


def _loot_distribution_summary(events: list[dict[str, Any]]) -> dict[str, str]:
    total = sum(
        (Decimal(str(event.get("viewer_event_amount") or "0")) for event in events),
        Decimal("0"),
    )
    paid = sum(
        (Decimal(str(event.get("viewer_paid_amount") or "0")) for event in events),
        Decimal("0"),
    )
    unpaid = sum(
        (Decimal(str(event.get("viewer_unpaid_amount") or "0")) for event in events),
        Decimal("0"),
    )
    total_cash = sum(
        (Decimal(str(event.get("viewer_event_cash_amount") or "0")) for event in events),
        Decimal("0"),
    )
    paid_cash = sum(
        (Decimal(str(event.get("viewer_paid_cash_amount") or "0")) for event in events),
        Decimal("0"),
    )
    unpaid_cash = sum(
        (Decimal(str(event.get("viewer_unpaid_cash_amount") or "0")) for event in events),
        Decimal("0"),
    )
    participated_count = sum(1 for event in events if event.get("viewer_participated"))
    paid_count = sum(
        1
        for event in events
        if event.get("viewer_participated")
        and Decimal(str(event.get("viewer_paid_amount") or "0")) > 0
    )
    unpaid_count = sum(
        1
        for event in events
        if event.get("viewer_participated")
        and Decimal(str(event.get("viewer_unpaid_amount") or "0")) > 0
    )
    return {
        "total_amount_text": _money_text(total),
        "total_cash_text": _cash_text(total_cash),
        "paid_amount_text": _money_text(paid),
        "paid_cash_text": _cash_text(paid_cash),
        "unpaid_amount_text": _money_text(unpaid),
        "unpaid_cash_text": _cash_text(unpaid_cash),
        "participated_count": str(participated_count),
        "paid_count": str(paid_count),
        "unpaid_count": str(unpaid_count),
        "event_count": str(len(events)),
    }


def _loot_alliance_selection(
    auth: dict[str, Any],
    alliance_id: int | str | None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    access = auth["selected_server"].get("my_alliance") or {}
    options = access.get("options") or []
    if not options:
        return None, []
    allowed_by_value = {str(option["alliance_id"]): option for option in options}
    selected_value = (
        str(alliance_id)
        if alliance_id is not None and str(alliance_id) in allowed_by_value
        else str(options[0]["alliance_id"])
    )
    return allowed_by_value[selected_value], options


def _decorate_alliance_fee_rules(
    rules: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    decorated = []
    for rule in rules:
        fee_rate = Decimal(str(rule.get("fee_rate") or "0"))
        row = dict(rule)
        row["fee_percent_input"] = _decimal_input(fee_rate * Decimal("100"))
        row["fee_percent_text"] = _money_text(fee_rate * Decimal("100"), places=2)
        decorated.append(row)
    return decorated


def _member_payout_fee_lines_from_rules(
    total_amount: Decimal,
    rules: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "rule_name": str(rule.get("rule_name") or ""),
            "fee_rate": Decimal(str(rule.get("fee_rate") or "0")),
            "fee_percent_text": _money_text(
                Decimal(str(rule.get("fee_rate") or "0")) * Decimal("100"),
                places=2,
            ),
            "fee_amount": _rounded_integer(total_amount * Decimal(str(rule.get("fee_rate") or "0"))),
        }
        for rule in rules
    ]


def _decorate_member_fee_lines(
    fee_lines: list[dict[str, Any]],
    adena_rate: Decimal,
) -> list[dict[str, Any]]:
    decorated = []
    for line in fee_lines:
        fee_amount = _rounded_integer(line.get("fee_amount"))
        row = dict(line)
        row["fee_percent_text"] = _money_text(
            Decimal(str(line.get("fee_rate") or "0")) * Decimal("100"),
            places=2,
        )
        row["fee_amount_text"] = _money_text(fee_amount)
        row["fee_amount_cash_text"] = _cash_text(_cash_from_adena(fee_amount, adena_rate))
        decorated.append(row)
    return decorated


def _my_alliance_payout_context(
    guild_id: int,
    selected_alliance: dict[str, Any] | None,
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    if selected_alliance is None:
        return {
            "selected_alliance": None,
            "fee_rules": [],
            "events": [],
            "recipients": [],
            "summary": {
                "total_text": "0",
                "total_cash_text": "0원",
                "fee_text": "0",
                "fee_cash_text": "0원",
                "unsettled_text": "0",
                "unsettled_cash_text": "0원",
                "settled_count": 0,
                "unsettled_count": 0,
            },
        }

    selected_alliance_ids = sorted(_selected_alliance_ids(selected_alliance))
    is_other_selection = bool(selected_alliance.get("is_other"))
    fee_rule_cache: dict[int, list[dict[str, Any]]] = {}
    member_group_cache: dict[int, dict[int, dict[str, Any]]] = {}

    def fee_rules_for(alliance_id: int) -> list[dict[str, Any]]:
        if alliance_id not in fee_rule_cache:
            fee_rule_cache[alliance_id] = _decorate_alliance_fee_rules(
                database.get_alliance_payout_fee_rules(guild_id, alliance_id)
            )
        return fee_rule_cache[alliance_id]

    def member_groups_for(alliance_id: int) -> dict[int, dict[str, Any]]:
        if alliance_id not in member_group_cache:
            member_group_cache[alliance_id] = database.get_member_payout_groups(
                guild_id,
                alliance_id,
            )
        return member_group_cache[alliance_id]

    fee_rules = (
        []
        if is_other_selection or not selected_alliance_ids
        else fee_rules_for(selected_alliance_ids[0])
    )
    rows = []
    recipient_summaries: dict[int, dict[str, Any]] = {}
    total_amount_sum = Decimal("0")
    total_cash_sum = Decimal("0")
    fee_amount_sum = Decimal("0")
    fee_cash_sum = Decimal("0")
    unsettled_amount_sum = Decimal("0")
    unsettled_cash_sum = Decimal("0")
    settled_count = 0
    unsettled_count = 0

    for event in events:
        for alliance_id in selected_alliance_ids:
            payout = next(
                (
                    item
                    for item in event.get("alliance_payouts", [])
                    if _parse_optional_int(item.get("alliance_id")) == alliance_id
                ),
                None,
            )
            if (
                not payout
                or not event.get("distribution_id")
                or payout.get("payout_status") != "paid"
            ):
                continue

            member_record = member_groups_for(alliance_id).get(
                int(event["distribution_id"])
            )
            adena_rate = Decimal(str(event.get("adena_rate") or "0"))
            participants = next(
                (
                    alliance.get("members", [])
                    for alliance in event.get("alliances", [])
                    if _parse_optional_int(alliance.get("alliance_id")) == alliance_id
                ),
                [],
            )
            participant_count = len(participants) or int(
                payout.get("participant_count") or 0
            )
            total_amount = _rounded_integer(payout.get("net_amount"))
            selected_rules = (
                member_record.get("fee_lines", [])
                if member_record is not None
                else fee_rules_for(alliance_id)
            )
            paid_statuses = (
                member_record.get("statuses", {})
                if member_record is not None
                else {}
            )
            fee_lines_raw = _member_payout_fee_lines_from_rules(
                total_amount,
                selected_rules,
            )
            fee_amount = sum(
                (Decimal(str(line["fee_amount"])) for line in fee_lines_raw),
                Decimal("0"),
            )
            distributable_amount = total_amount - fee_amount
            member_amounts = _rounded_allocation(distributable_amount, participant_count)
            fallback_member_amount = (
                member_amounts[0]
                if member_amounts
                else _rounded_divide(distributable_amount, participant_count)
            )
            fee_lines = _decorate_member_fee_lines(fee_lines_raw, adena_rate)
            recipients = []
            paid_recipient_count = 0
            unpaid_amount = Decimal("0")
            for index, member in enumerate(participants):
                member_user_id = int(member.get("user_id") or 0)
                member_amount = (
                    member_amounts[index]
                    if index < len(member_amounts)
                    else fallback_member_amount
                )
                is_paid = bool(paid_statuses.get(member_user_id, False))
                if is_paid:
                    paid_recipient_count += 1
                payout_status = "paid" if is_paid else "unpaid"
                recipients.append(
                    {
                        "alliance_id": alliance_id,
                        "user_id": member_user_id,
                        "display_name": str(member.get("discord_nickname") or ""),
                        "distribution_id": int(event["distribution_id"]),
                        "loot_event_id": event["loot_event_id"],
                        "item_name": event["item_name"],
                        "attendance_started_at": event["attendance_started_at"]
                        or event["event_date"],
                        "payout_amount": member_amount,
                        "payout_amount_text": _money_text(member_amount),
                        "payout_amount_cash_text": _cash_text(
                            _cash_from_adena(member_amount, adena_rate)
                        ),
                        "payout_status": payout_status,
                        "status_label": "지급 완료" if is_paid else "미완료",
                        "status_class": "is-paid" if is_paid else "is-unpaid",
                        "next_status": "unpaid" if is_paid else "paid",
                        "next_status_label": "미완료로 변경"
                        if is_paid
                        else "완료 처리",
                    }
                )
                summary = recipient_summaries.setdefault(
                    member_user_id,
                    {
                        "user_id": member_user_id,
                        "alliance_ids": set(),
                        "display_name": str(member.get("discord_nickname") or ""),
                        "total_amount": Decimal("0"),
                        "paid_amount": Decimal("0"),
                        "unpaid_amount": Decimal("0"),
                        "total_count": 0,
                        "paid_count": 0,
                        "unpaid_count": 0,
                        "unpaid_distribution_ids": [],
                        "items": [],
                    },
                )
                summary["alliance_ids"].add(alliance_id)
                summary["total_amount"] += member_amount
                summary["total_count"] += 1
                summary["items"].append(
                    {
                        "distribution_id": int(event["distribution_id"]),
                        "loot_event_id": event["loot_event_id"],
                        "alliance_id": alliance_id,
                        "item_name": event["item_name"],
                        "attendance_started_at": event["attendance_started_at"]
                        or event["event_date"],
                        "amount_text": _money_text(member_amount),
                        "status": payout_status,
                        "status_label": "지급 완료" if is_paid else "미완료",
                        "status_class": "is-paid" if is_paid else "is-unpaid",
                        "next_status": "unpaid" if is_paid else "paid",
                        "next_status_label": "미완료로 변경"
                        if is_paid
                        else "완료 처리",
                    }
                )
                if is_paid:
                    summary["paid_amount"] += member_amount
                    summary["paid_count"] += 1
                else:
                    summary["unpaid_amount"] += member_amount
                    summary["unpaid_count"] += 1
                    summary["unpaid_distribution_ids"].append(
                        int(event["distribution_id"])
                    )
                    unpaid_amount += member_amount

            recipient_total_count = len(recipients)
            status = (
                "paid"
                if recipient_total_count > 0
                and paid_recipient_count == recipient_total_count
                else "unpaid"
            )
            if status == "paid":
                settled_count += 1
            else:
                unsettled_count += 1
                unsettled_amount_sum += unpaid_amount
                unsettled_cash_sum += _cash_from_adena(unpaid_amount, adena_rate)

            total_amount_sum += total_amount
            total_cash_sum += _cash_from_adena(total_amount, adena_rate)
            fee_amount_sum += fee_amount
            fee_cash_sum += _cash_from_adena(fee_amount, adena_rate)
            rows.append(
                {
                    "alliance_id": alliance_id,
                    "distribution_id": int(event["distribution_id"]),
                    "loot_event_id": event["loot_event_id"],
                    "item_name": event["item_name"],
                    "attendance_started_at": event["attendance_started_at"]
                    or event["event_date"],
                    "participant_count": participant_count,
                    "total_amount_text": _money_text(total_amount),
                    "total_amount_cash_text": _cash_text(
                        _cash_from_adena(total_amount, adena_rate)
                    ),
                    "fee_amount_text": _money_text(fee_amount),
                    "fee_amount_cash_text": _cash_text(
                        _cash_from_adena(fee_amount, adena_rate)
                    ),
                    "distributable_amount_text": _money_text(distributable_amount),
                    "distributable_amount_cash_text": _cash_text(
                        _cash_from_adena(distributable_amount, adena_rate)
                    ),
                    "per_member_text": _rounded_allocation_text(member_amounts),
                    "per_member_cash_text": _rounded_allocation_cash_text(
                        member_amounts,
                        adena_rate,
                    ),
                    "status": status,
                    "status_label": "정산완료" if status == "paid" else "미정산",
                    "status_class": "is-paid" if status == "paid" else "is-unpaid",
                    "fee_lines": fee_lines,
                    "recipients": recipients,
                }
            )

    recipient_rows = []
    for summary in recipient_summaries.values():
        unpaid_amount = Decimal(str(summary["unpaid_amount"]))
        paid_amount = Decimal(str(summary["paid_amount"]))
        total_amount = Decimal(str(summary["total_amount"]))
        recipient_rows.append(
            {
                **summary,
                "total_amount_text": _money_text(total_amount),
                "paid_amount_text": _money_text(paid_amount),
                "unpaid_amount_text": _money_text(unpaid_amount),
                "status": "unpaid" if unpaid_amount > 0 else "paid",
                "status_label": "미완료 있음" if unpaid_amount > 0 else "전체 완료",
                "status_class": "is-unpaid" if unpaid_amount > 0 else "is-paid",
                "unpaid_distribution_ids_text": ",".join(
                    str(distribution_id)
                    for distribution_id in summary["unpaid_distribution_ids"]
                ),
            }
        )
    recipient_rows.sort(
        key=lambda item: (
            -Decimal(str(item["unpaid_amount"] or "0")),
            -int(item["unpaid_count"] or 0),
            str(item["display_name"]),
        )
    )

    return {
        "selected_alliance": selected_alliance,
        "fee_rules": fee_rules,
        "events": rows,
        "recipients": recipient_rows,
        "summary": {
            "total_text": _money_text(total_amount_sum),
            "total_cash_text": _cash_text(total_cash_sum),
            "fee_text": _money_text(fee_amount_sum),
            "fee_cash_text": _cash_text(fee_cash_sum),
            "unsettled_text": _money_text(unsettled_amount_sum),
            "unsettled_cash_text": _cash_text(unsettled_cash_sum),
            "settled_count": settled_count,
            "unsettled_count": unsettled_count,
        },
    }


def _mapped_loot_alliances(guild_id: int) -> list[dict[str, Any]]:
    seen: set[int] = set()
    alliances = []
    for mapping in database.get_guild_alliance_role_mappings(guild_id):
        alliance_id = _parse_optional_int(mapping.get("alliance_id"))
        if alliance_id is None or alliance_id in seen:
            continue
        seen.add(alliance_id)
        alliances.append(
            {
                "alliance_id": alliance_id,
                "alliance_name": str(mapping.get("alliance_name") or ""),
            }
        )
    alliances.sort(key=lambda item: str(item["alliance_name"]))
    return alliances


def _alliance_payout_group_context(
    guild_id: int,
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    mapped_alliances = _mapped_loot_alliances(guild_id)
    alliance_rows = {
        int(alliance["alliance_id"]): {
            "alliance_id": int(alliance["alliance_id"]),
            "alliance_name": str(alliance["alliance_name"]),
            "total_amount": Decimal("0"),
            "unpaid_amount": Decimal("0"),
            "total_count": 0,
            "paid_count": 0,
            "unpaid_count": 0,
            "unpaid_distribution_ids": [],
            "items": [],
        }
        for alliance in mapped_alliances
    }

    for event in events:
        adena_rate = Decimal(str(event.get("adena_rate") or "0"))
        for payout in event.get("alliance_payouts", []):
            alliance_id = _parse_optional_int(payout.get("alliance_id"))
            if alliance_id is None or alliance_id not in alliance_rows:
                continue
            amount = _rounded_integer(payout.get("net_amount"))
            is_paid = payout.get("payout_status") == "paid"
            distribution_id = int(event.get("distribution_id") or 0)
            row = alliance_rows[alliance_id]
            row["total_amount"] += amount
            row["total_count"] += 1
            if is_paid:
                row["paid_count"] += 1
            else:
                row["unpaid_amount"] += amount
                row["unpaid_count"] += 1
                if distribution_id > 0:
                    row["unpaid_distribution_ids"].append(distribution_id)
            row["items"].append(
                {
                    "alliance_id": alliance_id,
                    "distribution_id": distribution_id,
                    "loot_event_id": int(event.get("loot_event_id") or 0),
                    "attendance_id": int(event.get("attendance_id") or 0),
                    "item_name": str(event.get("item_name") or ""),
                    "attendance_started_at": event.get("attendance_started_at")
                    or event.get("event_date")
                    or "",
                    "participant_count": int(payout.get("participant_count") or 0),
                    "amount": amount,
                    "amount_text": _money_text(amount),
                    "amount_cash_text": _cash_text(_cash_from_adena(amount, adena_rate)),
                    "per_member_text": payout.get("per_member_text") or "0",
                    "status": "paid" if is_paid else "unpaid",
                    "status_label": "분배완료" if is_paid else "미완료",
                    "status_class": "is-paid" if is_paid else "is-unpaid",
                    "next_status": "unpaid" if is_paid else "paid",
                    "next_status_label": "미완료로 변경" if is_paid else "완료 처리",
                }
            )

    rows = []
    for row in alliance_rows.values():
        total_amount = Decimal(str(row["total_amount"]))
        unpaid_amount = Decimal(str(row["unpaid_amount"]))
        rows.append(
            {
                **row,
                "total_amount_text": _money_text(total_amount),
                "unpaid_amount_text": _money_text(unpaid_amount),
                "status": "unpaid" if unpaid_amount > 0 else "paid",
                "status_label": "미완료 있음" if unpaid_amount > 0 else "전체 완료",
                "status_class": "is-unpaid" if unpaid_amount > 0 else "is-paid",
                "unpaid_distribution_ids_text": ",".join(
                    str(distribution_id)
                    for distribution_id in row["unpaid_distribution_ids"]
                ),
            }
        )
    rows.sort(
        key=lambda item: (
            -Decimal(str(item["unpaid_amount"] or "0")),
            -int(item["unpaid_count"] or 0),
            str(item["alliance_name"]),
        )
    )
    return {
        "alliances": rows,
        "mapped_alliances": mapped_alliances,
    }


def _loot_url(
    guild_id: int,
    *,
    period: str | None = None,
    mine: bool | None = None,
    status: str | None = None,
    alliance_id: int | str | None = None,
    tab: str = "distribution",
) -> str:
    params: dict[str, Any] = {"guild_id": guild_id}
    if period:
        params["period"] = period
    if mine is True:
        params["mine"] = "1"
    elif mine is False:
        params["mine"] = "0"
    if status:
        params["status"] = status
    if alliance_id:
        params["alliance_id"] = alliance_id
    return f"/loot?{urlencode(params)}#{tab}"


def _loot_period_filters(
    guild_id: int,
    active_period: str,
    mine_only: bool = False,
    status: str = "",
) -> list[dict[str, str | bool]]:
    return [
        {
            "label": "전체",
            "href": _loot_url(guild_id, period="all", mine=mine_only, status=status),
            "active": active_period == "all",
        },
        {
            "label": "최근 한달",
            "href": _loot_url(guild_id, period="30d", mine=mine_only, status=status),
            "active": active_period == "30d",
        },
        {
            "label": "최근 일주일",
            "href": _loot_url(guild_id, period="7d", mine=mine_only, status=status),
            "active": active_period == "7d",
        },
    ]


def _loot_participation_filters(
    guild_id: int,
    active_period: str,
    mine_only: bool,
    status: str = "",
) -> list[dict[str, str | bool]]:
    return [
        {
            "label": "내가 참여한 기록만",
            "href": _loot_url(guild_id, period=active_period, mine=True, status=status),
            "active": mine_only,
        },
        {
            "label": "전체 기록",
            "href": _loot_url(guild_id, period=active_period, mine=False, status=status),
            "active": not mine_only,
        },
    ]


def _loot_status_filters(
    guild_id: int,
    active_period: str,
    mine_only: bool,
    status: str,
) -> list[dict[str, str | bool]]:
    return [
        {
            "label": "전체",
            "href": _loot_url(guild_id, period=active_period, mine=mine_only, status="all"),
            "active": status == "all",
        },
        {
            "label": "수령",
            "href": _loot_url(guild_id, period=active_period, mine=mine_only, status="paid"),
            "active": status == "paid",
        },
        {
            "label": "미수령",
            "href": _loot_url(
                guild_id,
                period=active_period,
                mine=mine_only,
                status="unpaid",
            ),
            "active": status == "unpaid",
        },
    ]


def _loot_payout_payload(guild_id: int, distribution_id: int) -> dict[str, Any]:
    for event in _decorate_loot_events(database.get_loot_drop_events(guild_id, limit=100)):
        if int(event.get("distribution_id") or 0) != distribution_id:
            continue
        return {
            "summary": {
                "label": event["payout_summary_label"],
                "meta": event["payout_summary_meta"],
                "class_name": event["payout_summary_class"],
            },
            "payouts": [
                {
                    "alliance_id": str(payout["alliance_id"]),
                    "payout_status": payout["payout_status"],
                    "status_label": payout["status_label"],
                    "next_status": payout["next_status"],
                    "next_status_label": payout["next_status_label"],
                }
                for payout in event.get("alliance_payouts", [])
            ],
        }
    raise ValueError("Distribution was not found.")


def _loot_redirect(
    guild_id: int,
    *,
    saved: str | None = None,
    alliance_id: str | None = None,
    tab: str = "my-alliance-payouts",
) -> RedirectResponse:
    params: dict[str, Any] = {"guild_id": guild_id}
    if saved:
        params["saved"] = saved
    if alliance_id:
        params["alliance_id"] = alliance_id
    return RedirectResponse(
        f"/loot?{urlencode(params)}#{tab}",
        status_code=303,
    )


def _allowed_loot_alliance_id(
    auth: dict[str, Any],
    alliance_id: int | None,
) -> int | None:
    access = auth["selected_server"].get("my_alliance") or {}
    options = access.get("options") or []
    if alliance_id is not None:
        allowed_ids: set[int] = set()
        for option in options:
            option_id = (
                int(option["alliance_id"])
                if str(option.get("alliance_id") or "").isdigit()
                else None
            )
            if option_id is not None:
                allowed_ids.add(option_id)
            if option.get("is_other"):
                allowed_ids.update(
                    int(other_id)
                    for other_id in option.get("alliance_ids", [])
                    if str(other_id).isdigit()
                )
        if int(alliance_id) not in allowed_ids:
            return None
        return int(alliance_id)
    selected_alliance, _options = _loot_alliance_selection(auth, alliance_id)
    if selected_alliance is None:
        return None
    if selected_alliance.get("is_other"):
        return None
    return int(selected_alliance["alliance_id"])


def _validate_run_time(raw_value: str | None, errors: list[str]) -> str:
    value = raw_value or "00:00"
    parts = value.split(":")
    if len(parts) != 2:
        errors.append("발송 시간은 HH:MM 형식이어야 합니다.")
        return "00:00"
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        errors.append("발송 시간은 숫자로 입력해야 합니다.")
        return "00:00"
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        errors.append("발송 시간은 00:00부터 23:59 사이여야 합니다.")
        return "00:00"
    return f"{hour:02d}:{minute:02d}"


def _load_report_settings(guild_id: int) -> list[dict[str, Any]]:
    rows = database.fetchall(
        """
        SELECT
            r.report_setting_id,
            r.guild_id,
            r.created_by_discord_id,
            r.updated_by_discord_id,
            r.report_name,
            r.frequency,
            r.period_type,
            r.subject_type,
            r.result_type,
            r.run_time,
            r.channel_id,
            r.channel_name,
            r.schedule_json,
            r.query_json,
            r.render_json,
            r.status,
            r.last_sent_at,
            r.next_run_at,
            r.updated_at,
            COALESCE(created_user.discord_nickname, r.created_by_discord_id::text)
                AS created_by_name,
            COALESCE(updated_user.discord_nickname, r.updated_by_discord_id::text)
                AS updated_by_name
        FROM scheduled_report_settings r
        LEFT JOIN users created_user
            ON created_user.discord_id = r.created_by_discord_id
        LEFT JOIN users updated_user
            ON updated_user.discord_id = r.updated_by_discord_id
        WHERE r.guild_id = %s
          AND r.status <> 'delete'
        ORDER BY
            CASE r.status WHEN 'on' THEN 0 ELSE 1 END,
            r.updated_at DESC,
            r.report_setting_id DESC
        """,
        (guild_id,),
    )
    reports: list[dict[str, Any]] = []
    for row in rows:
        schedule_json = _report_json(
            row.get("schedule_json"),
            {"type": row["frequency"], "time": row["run_time"], "timezone": "Asia/Seoul"},
        )
        query_json = _report_json(
            row.get("query_json"),
            {
                "dataset": "attendance",
                "period": row["period_type"],
                "group_by": "alliance" if row["subject_type"] == "alliance" else "none",
                "rank_target": row["subject_type"],
                "metric": "attendance_count",
                "limit": 10,
            },
        )
        render_json = _report_json(
            row.get("render_json"),
            {
                "output": "ranking" if row["result_type"] == "ranking" else "grouped_ranking",
                "title": row["report_name"] or "통계 알림",
                "group_header": "{group_name}",
                "row": "{rank}. {label} - {value}회",
                "empty": "출석 기록 없음",
            },
        )
        frequency = str(schedule_json.get("type") or row["frequency"] or "daily")
        run_time = str(schedule_json.get("time") or row["run_time"] or "00:00")
        period_type = str(query_json.get("period") or row["period_type"] or "today")
        group_by = str(query_json.get("group_by") or "alliance")
        rank_target = str(query_json.get("rank_target") or row["subject_type"] or "user")
        metric = str(query_json.get("metric") or "attendance_count")
        output = str(render_json.get("output") or "grouped_ranking")
        limit = int(query_json.get("limit") or 10)
        report = {
            "report_setting_id": int(row["report_setting_id"]),
            "report_name": str(row["report_name"] or "통계 알림"),
            "frequency": frequency,
            "period_type": period_type,
            "dataset": str(query_json.get("dataset") or "attendance"),
            "group_by": group_by,
            "rank_target": rank_target,
            "metric": metric,
            "output": output,
            "limit": limit,
            "run_time": run_time,
            "run_time_label": _format_run_time(run_time),
            "channel_id": row["channel_id"],
            "channel_name": str(row["channel_name"]),
            "title": str(render_json.get("title") or row["report_name"] or "통계 알림"),
            "status": str(row["status"]),
            "created_by_discord_id": row["created_by_discord_id"],
            "updated_by_discord_id": row["updated_by_discord_id"],
            "created_by_name": row["created_by_name"] or "",
            "updated_by_name": row["updated_by_name"] or "",
            "last_sent_at": row["last_sent_at"] or "",
            "next_run_at": row["next_run_at"] or "",
            "updated_at": row["updated_at"],
            "frequency_label": _option_label(
                REPORT_FREQUENCY_OPTIONS,
                frequency,
            ),
            "period_label": _option_label(
                REPORT_PERIOD_OPTIONS,
                period_type,
            ),
            "group_label": _option_label(
                REPORT_GROUP_OPTIONS,
                group_by,
            ),
            "rank_target_label": _option_label(
                REPORT_RANK_TARGET_OPTIONS,
                rank_target,
            ),
            "metric_label": _option_label(
                REPORT_METRIC_OPTIONS,
                metric,
            ),
            "output_label": _option_label(
                REPORT_OUTPUT_OPTIONS,
                output,
            ),
        }
        report["sentence"] = _report_sentence(report)
        reports.append(report)
    return reports


def _validate_report_option(
    field_label: str,
    raw_value: str | None,
    options: tuple[dict[str, str], ...],
    errors: list[str],
) -> str:
    allowed_values = {option["value"] for option in options}
    if raw_value in allowed_values:
        return str(raw_value)
    errors.append(f"{field_label} 값을 선택해주세요.")
    return options[0]["value"]


def _find_channel_name(channels: list[dict[str, Any]], channel_id: int) -> str | None:
    for channel in channels:
        if int(channel["id"]) == channel_id:
            return str(channel["label"])
    return None


def _latest_attendance_sessions(guild_id: int, limit: int = 20) -> list[dict[str, Any]]:
    rows = database.fetchall(
        """
        SELECT
            s.attendance_id,
            s.started_at,
            s.ended_at,
            s.started_by_discord_id,
            COALESCE(
                MAX(starter.discord_nickname),
                s.started_by_discord_id::text
            ) AS started_by_name,
            COUNT(e.user_id) AS participant_count
        FROM attendance_sessions s
        LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
        LEFT JOIN users starter ON starter.discord_id = s.started_by_discord_id
        WHERE s.guild_id = %s
        GROUP BY
            s.attendance_id,
            s.started_at,
            s.ended_at,
            s.started_by_discord_id
        ORDER BY s.started_at DESC
        LIMIT %s
        """,
        (guild_id, limit),
    )
    return [
        {
            "attendance_id": int(row["attendance_id"]),
            "started_at": str(row["started_at"]),
            "ended_at": str(row["ended_at"]),
            "started_by_discord_id": row["started_by_discord_id"],
            "started_by_name": row["started_by_name"] or "",
            "participant_count": int(row["participant_count"] or 0),
        }
        for row in rows
    ]


def _loot_attendance_options(guild_id: int) -> list[dict[str, Any]]:
    sessions = database.get_attendance_status_sessions(guild_id, 50, 0)
    for session in sessions:
        alliance_summary = ", ".join(
            f"{alliance['alliance_name']} {alliance['count']}명"
            for alliance in session.get("alliances", [])
        )
        session["label"] = (
            f"#{session['attendance_id']} · {session['started_at']} · "
            f"총 {session['participant_count']}명"
        )
        session["alliance_summary"] = alliance_summary or "출석 없음"
    return sessions


def _default_loot_form(guild_id: int) -> dict[str, str]:
    latest_adena_rate = database.get_latest_adena_rate(guild_id)
    return {
        "attendance_id": "",
        "item_id": "",
        "cash_price_krw": "",
        "sale_price": "",
        "adena_rate": _decimal_input(latest_adena_rate),
        "fee_percent": "10",
        "memo": "",
        "excluded_alliance_ids": "",
    }


def _loot_form_from_values(values: dict[str, Any], guild_id: int) -> dict[str, str]:
    form = _default_loot_form(guild_id)
    for key in form:
        if key in values:
            form[key] = str(values.get(key) or "")
    return form


def _parse_loot_excluded_alliance_ids(value: str | None) -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()
    for item in str(value or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            alliance_id = int(item)
        except ValueError:
            continue
        if alliance_id <= 0 or alliance_id in seen:
            continue
        ids.append(alliance_id)
        seen.add(alliance_id)
    return ids


def _loot_template_context(
    auth: dict[str, Any],
    guild_id: int,
    *,
    saved: str | None,
    errors: list[str],
    loot_form: dict[str, str] | None = None,
    item_price_form: dict[str, str] | None = None,
    period: str | None = None,
    alliance_id: int | str | None = None,
    mine: str | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    active_period, period_start, period_label = _loot_period_bounds(period)
    active_status = _normalize_loot_status_filter(status)
    mine_only = (
        True
        if mine is None
        else str(mine or "").lower() in {"1", "true", "yes", "on", "mine"}
    )
    try:
        viewer_discord_id = int(auth["user"]["id"])
    except (KeyError, TypeError, ValueError):
        viewer_discord_id = None
    viewer_current_alliance_ids = (
        {
            int(option["alliance_id"])
            for option in _member_alliance_options(guild_id, str(viewer_discord_id))
            if str(option.get("alliance_id") or "").isdigit()
        }
        if viewer_discord_id is not None
        else set()
    )
    all_events = _decorate_loot_events(
        database.get_loot_drop_events(guild_id, limit=5000),
        viewer_discord_id,
        guild_id,
        viewer_current_alliance_ids,
    )
    loot_events = _filter_loot_events_by_period(all_events, period_start)
    distribution_events = _filter_loot_events_by_status(
        _filter_loot_events_by_viewer(loot_events, mine_only),
        active_status,
    )
    selected_alliance, alliance_options = _loot_alliance_selection(auth, alliance_id)
    return {
        "auth": auth,
        "saved": saved,
        "errors": errors,
        "loot_form": loot_form or _default_loot_form(guild_id),
        "item_price_form": item_price_form or _default_item_price_form(),
        "attendance_options": _loot_attendance_options(guild_id),
        "item_prices": _decorate_item_prices(database.get_item_price_settings(guild_id)),
        "loot_events": loot_events,
        "distribution_events": distribution_events,
        "loot_summary": _loot_distribution_summary(distribution_events),
        "bid_dashboard": database.get_bid_item_dashboard(guild_id),
        "alliance_payout_groups": _alliance_payout_group_context(
            guild_id,
            loot_events,
        ),
        "my_alliance_payouts": _my_alliance_payout_context(
            guild_id,
            selected_alliance,
            loot_events,
        ),
        "loot_alliance_options": alliance_options,
        "loot_period": {
            "active": active_period,
            "label": period_label,
            "mine_only": mine_only,
            "status": active_status,
            "filters": _loot_period_filters(
                guild_id,
                active_period,
                mine_only,
                active_status,
            ),
            "participation_filters": _loot_participation_filters(
                guild_id,
                active_period,
                mine_only,
                active_status,
            ),
            "status_filters": _loot_status_filters(
                guild_id,
                active_period,
                mine_only,
                active_status,
            ),
        },
        "active_page": "loot",
    }


def _command_category(command_type: str) -> str:
    normalized = command_type.lower()
    if "attendance" in normalized:
        return "attendance"
    if "stat" in normalized or "report" in normalized:
        return "statistics"
    if "setting" in normalized or "config" in normalized or "panel" in normalized:
        return "settings"
    return "logs"


def _command_category_label(category: str) -> str:
    return next((tab["label"] for tab in LOG_TABS if tab["value"] == category), "로그")


def _work_log_action_label(action_type: str) -> str:
    return next(
        (tab["label"] for tab in WORK_LOG_TABS if tab["value"] == action_type),
        action_type,
    )


def _work_log_actor_name(auth: dict[str, Any]) -> str:
    return str(
        auth["selected_server"].get("member_display_name")
        or auth["user"].get("display_name")
        or auth["user"].get("username")
        or auth["user"].get("id")
        or ""
    )


def _record_work_log(
    auth: dict[str, Any],
    guild_id: int,
    *,
    action_type: str,
    target_type: str,
    target_id: int | None,
    summary: str,
    details: dict[str, Any] | None = None,
) -> None:
    try:
        database.add_work_log(
            guild_id,
            actor_discord_id=int(auth["user"]["id"]),
            actor_display_name=_work_log_actor_name(auth),
            actor_role=str(auth["selected_server"].get("role") or "user"),
            action_type=action_type,
            target_type=target_type,
            target_id=target_id,
            summary=summary,
            details=details,
        )
    except Exception:
        pass


def _work_log_user_label(user_id: int) -> str:
    row = database.fetchone(
        """
        SELECT
            u.discord_nickname,
            COALESCE(a.alliance_name, '미분류') AS alliance_name
        FROM users u
        LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
        WHERE u.user_id = %s
        """,
        (user_id,),
    )
    if row is None:
        return f"User {user_id}"
    return f"{row['discord_nickname']} ({row['alliance_name']})"


def _work_log_loot_label(guild_id: int, loot_event_id: int) -> str:
    row = database.fetchone(
        """
        SELECT
            le.loot_event_id,
            le.attendance_id,
            le.event_time_label,
            COALESCE(li.item_name_snapshot, le.title, '드랍') AS item_name
        FROM loot_events le
        LEFT JOIN loot_event_items li ON li.loot_event_id = le.loot_event_id
        WHERE le.guild_id = %s
          AND le.loot_event_id = %s
        ORDER BY li.loot_item_id ASC
        LIMIT 1
        """,
        (guild_id, loot_event_id),
    )
    if row is None:
        return f"드랍 #{loot_event_id}"
    time_label = str(row["event_time_label"] or "").strip()
    time_part = f" · {time_label}" if time_label else ""
    return f"{row['item_name']} · 출석 #{row['attendance_id']}{time_part}"


def _work_log_item_label(item_id: int) -> str:
    row = database.fetchone(
        """
        SELECT item_name, default_price
        FROM items
        WHERE item_id = %s
        """,
        (item_id,),
    )
    if row is None:
        return f"아이템 #{item_id}"
    price = _decimal_input(row["default_price"])
    price_part = f" · {price}원" if price else ""
    return f"{row['item_name']}{price_part}"


def _latest_command_queue(guild_id: int, limit: int = 10) -> list[dict[str, Any]]:
    rows = database.fetchall(
        """
        SELECT
            command_id,
            command_type,
            status,
            requested_by_discord_id,
            created_at,
            processed_at,
            error_message
        FROM bot_command_queue
        WHERE guild_id = %s
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (guild_id, limit),
    )
    commands: list[dict[str, Any]] = []
    for row in rows:
        category = _command_category(str(row["command_type"]))
        commands.append(
            {
                "command_id": int(row["command_id"]),
                "command_type": str(row["command_type"]),
                "category": category,
                "category_label": _command_category_label(category),
                "status": str(row["status"]),
                "requested_by_discord_id": row["requested_by_discord_id"],
                "created_at": row["created_at"],
                "processed_at": row["processed_at"],
                "error_message": row["error_message"] or "",
            }
        )
    return commands


def _pending_attendance_command(guild_id: int) -> dict[str, Any] | None:
    for (
        pending_guild_id,
        command_type,
    ), request_id in _ATTENDANCE_COMMANDS_IN_FLIGHT.items():
        if pending_guild_id != guild_id:
            continue
        label = "출석 시작" if command_type == "attendance.start" else "출석 종료"
        return {
            "command_id": request_id,
            "command_type": command_type,
            "status": "processing",
            "label": label,
            "created_at": "",
        }
    return None


def _empty_live_attendance_state() -> dict[str, Any]:
    return {
        "active": False,
        "participant_count": 0,
        "server_now": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "session": None,
        "participants": [],
    }


def _live_attendance_state(guild_id: int) -> dict[str, Any]:
    state = _ATTENDANCE_STATE_CACHE.get(guild_id)
    if not state:
        return _empty_live_attendance_state()

    refreshed_state = dict(state)
    refreshed_state["server_now"] = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    return refreshed_state


async def _broadcast_live_attendance_state(
    guild_id: int,
    state: dict[str, Any],
) -> None:
    clients = set(_ATTENDANCE_BROWSER_CLIENTS.get(guild_id) or set())
    stale_clients: list[WebSocket] = []
    for client in clients:
        try:
            await client.send_json(state)
        except Exception:
            stale_clients.append(client)

    if stale_clients:
        connected_clients = _ATTENDANCE_BROWSER_CLIENTS.get(guild_id)
        if connected_clients is not None:
            for client in stale_clients:
                connected_clients.discard(client)


async def _send_bot_bridge_message(payload: dict[str, Any]) -> bool:
    websocket = _BOT_BRIDGE_WEBSOCKET
    if websocket is None:
        return False

    try:
        await websocket.send_json(payload)
        return True
    except Exception:
        return False


async def _send_attendance_bot_command(
    guild_id: int,
    command_type: str,
    requested_by_discord_id: int,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if _BOT_BRIDGE_WEBSOCKET is None:
        return {"ok": False, "message": "봇 브리지가 연결되어 있지 않습니다."}

    pending_key = (guild_id, command_type)
    if _pending_attendance_command(guild_id) is not None:
        return {"ok": False, "busy": True, "message": "이미 처리 중인 요청이 있습니다."}

    request_id = secrets.token_urlsafe(12)
    future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
    _BOT_BRIDGE_WAITERS[request_id] = future
    _ATTENDANCE_COMMANDS_IN_FLIGHT[pending_key] = request_id
    message = {
        "type": command_type,
        "request_id": request_id,
        "guild_id": str(guild_id),
        "requested_by_discord_id": str(requested_by_discord_id),
        **(payload or {}),
    }

    try:
        if not await _send_bot_bridge_message(message):
            return {"ok": False, "message": "봇 브리지로 명령을 보낼 수 없습니다."}
        return await asyncio.wait_for(
            future,
            timeout=BOT_BRIDGE_COMMAND_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        return {"ok": False, "message": "봇 응답 시간이 초과되었습니다."}
    finally:
        _BOT_BRIDGE_WAITERS.pop(request_id, None)
        if _ATTENDANCE_COMMANDS_IN_FLIGHT.get(pending_key) == request_id:
            _ATTENDANCE_COMMANDS_IN_FLIGHT.pop(pending_key, None)


def _enqueue_bot_command(
    guild_id: int,
    command_type: str,
    requested_by_discord_id: int,
    payload: dict[str, Any] | None = None,
) -> None:
    with database.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bot_command_queue (
                    guild_id,
                    command_type,
                    payload_json,
                    requested_by_discord_id
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    guild_id,
                    command_type,
                    Json({"source": "web", **(payload or {})}),
                    requested_by_discord_id,
                ),
            )
        connection.commit()


def _enqueue_attendance_command(
    guild_id: int,
    command_type: str,
    requested_by_discord_id: int,
    payload: dict[str, Any] | None = None,
) -> None:
    _enqueue_bot_command(guild_id, command_type, requested_by_discord_id, payload)


def _enqueue_report_scheduler_refresh(
    guild_id: int,
    requested_by_discord_id: int,
) -> None:
    _enqueue_bot_command(
        guild_id,
        "refresh_report_schedules",
        requested_by_discord_id,
    )


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    if _auth_context(request):
        return RedirectResponse("/attendance", status_code=303)
    return RedirectResponse("/login", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login(
    request: Request,
    next_url: str | None = Query(None, alias="next"),
):
    redirect_to = _safe_redirect_path(next_url)
    if _auth_context(request):
        return RedirectResponse(redirect_to or "/attendance", status_code=303)
    if _local_developer_auth_enabled():
        return _render(
            request,
            "login_failed.html",
            {"reason": "로컬 개발 모드는 이 PC에서만 접근할 수 있습니다."},
            status_code=403,
        )
    return _render(
        request,
        "login.html",
        {
            "config_ready": _oauth_ready(),
            "redirect_uri": DISCORD_REDIRECT_URI,
            "next_url": redirect_to or "",
        },
    )


@app.get("/auth/discord/login")
def discord_login(
    request: Request,
    remember_me: str | None = None,
    next_url: str | None = Query(None, alias="next"),
):
    if _local_developer_auth_enabled():
        if _is_local_request(request):
            return RedirectResponse(
                _safe_redirect_path(next_url) or "/attendance",
                status_code=303,
            )
        return _render(
            request,
            "login_failed.html",
            {"reason": "로컬 개발 모드는 Discord 로그인을 사용하지 않습니다."},
            status_code=403,
        )
    if not _oauth_ready():
        return _render(
            request,
            "login.html",
            {
                "config_ready": False,
                "redirect_uri": DISCORD_REDIRECT_URI,
                "next_url": _safe_redirect_path(next_url) or "",
                "error_message": "Discord OAuth 환경변수가 아직 설정되지 않았습니다.",
            },
            status_code=500,
        )
    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = state
    request.session["remember_me"] = remember_me == "1"
    redirect_to = _safe_redirect_path(next_url)
    if redirect_to:
        request.session["login_next"] = redirect_to
    return RedirectResponse(_discord_authorize_url(state), status_code=303)


@app.get("/auth/discord/callback", response_class=HTMLResponse)
def discord_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
):
    expected_state = request.session.get("oauth_state")
    request.session.pop("oauth_state", None)

    if error:
        request.session.clear()
        return _render(
            request,
            "login_failed.html",
            {"reason": f"Discord 인증이 취소되었습니다. ({error})"},
            status_code=401,
        )
    if not code or not state or state != expected_state:
        request.session.clear()
        return _render(
            request,
            "login_failed.html",
            {"reason": "Discord 로그인 상태값이 올바르지 않습니다."},
            status_code=401,
        )

    try:
        access_token = _exchange_discord_code(code)
        discord_user = _discord_get("/users/@me", access_token)
        discord_guilds = _discord_get_user_guilds(access_token)
        servers = _load_accessible_servers(str(discord_user["id"]), discord_guilds)
    except Exception:
        request.session.clear()
        return _render(
            request,
            "login_failed.html",
            {"reason": "Discord 정보 확인 또는 DB 조회 중 오류가 발생했습니다."},
            status_code=500,
        )

    if not servers:
        request.session.clear()
        return _render(
            request,
            "login_failed.html",
            {"reason": "현재 계정이 가입한 서버 중 관리 대상 서버가 없습니다."},
            status_code=403,
        )

    request.session["discord_user"] = _normalize_discord_user(discord_user)
    request.session["servers"] = servers
    redirect_to = _safe_redirect_path(request.session.pop("login_next", None))
    return RedirectResponse(redirect_to or "/attendance", status_code=303)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


@app.post("/developer/view-mode")
async def set_developer_view_mode(request: Request):
    form_data = await _urlencoded_form_data(request)
    next_url = _safe_redirect_path(form_data.get("next_url")) or "/attendance"
    is_local_developer = _local_developer_auth_enabled() and _is_local_request(request)
    session_user = request.session.get("discord_user") or {}
    is_global_developer = str(session_user.get("id") or "") == GLOBAL_DEVELOPER_DISCORD_ID
    is_owner = False
    if session_user and request.session.get("servers"):
        servers = request.session.get("servers") or []
        allowed_servers = {str(server["guild_id"]): server for server in servers}
        query = next_url.split("?", 1)[1].split("#", 1)[0] if "?" in next_url else ""
        requested_guild_id = str((parse_qs(query).get("guild_id") or [""])[-1])
        selected_guild_id = (
            requested_guild_id
            if requested_guild_id in allowed_servers
            else str(servers[0]["guild_id"])
            if servers
            else ""
        )
        if selected_guild_id:
            is_owner = _is_selected_guild_owner(
                int(selected_guild_id),
                str(session_user["id"]),
                dict(allowed_servers[selected_guild_id]),
            )
    if not (is_local_developer or is_global_developer or is_owner):
        return RedirectResponse(next_url, status_code=303)

    mode = str(form_data.get("mode") or "developer")
    allowed_values = _developer_view_values(is_local_developer or is_global_developer)
    request.session[DEVELOPER_VIEW_MODE_SESSION_KEY] = (
        mode
        if mode in allowed_values
        else "developer"
        if is_local_developer or is_global_developer
        else "owner"
    )
    return RedirectResponse(next_url, status_code=303)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    guild_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    search: str | None = None,
    alliance: str | None = None,
    limit: int = 25,
    period: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if period:
        start_date, end_date = _dashboard_period_dates(period)
    start_at, end_at, start_value, end_value = _date_bounds(start_date, end_date)
    search_value = (search or "").strip()
    alliance_value = (alliance or "").strip()
    limit_value = limit if limit in {10, 25, 50, 100, 200} else 25
    alliance_options = _role_scoped_alliance_name_options(selected_guild_id)
    other_alliance_names = _unmapped_alliance_names(selected_guild_id)
    is_other_alliance_filter = (
        alliance_value == OTHER_ALLIANCE_VALUE and bool(other_alliance_names)
    )
    query_alliance_value = None if is_other_alliance_filter else alliance_value or None

    filtered_rows = database.get_attendance_export_rows(
        selected_guild_id,
        start_at,
        end_at,
        search_value or None,
        query_alliance_value,
    )
    if is_other_alliance_filter:
        filtered_rows = [
            row
            for row in filtered_rows
            if str(row.get("alliance_name") or "미분류") in other_alliance_names
        ]
    should_compute_filtered_totals = bool(search_value or alliance_value)
    if should_compute_filtered_totals:
        overview = _overview_from_attendance_rows(filtered_rows)
        daily_stats = _daily_stats_from_attendance_rows(filtered_rows)[:60]
        alliance_stats = _alliance_stats_from_attendance_rows(filtered_rows)
    else:
        overview = database.get_attendance_overview(selected_guild_id, start_at, end_at)
        daily_stats = database.get_daily_attendance_stats(
            selected_guild_id,
            start_at,
            end_at,
        )[:60]
        alliance_stats = database.get_alliance_attendance_stats(
            selected_guild_id,
            start_at,
            end_at,
            query_alliance_value,
        )
    if should_compute_filtered_totals:
        top_users = _user_stats_from_attendance_rows(filtered_rows, limit_value)
    else:
        top_users = database.get_user_attendance_stats(
            selected_guild_id,
            start_at,
            end_at,
            search_value or None,
            query_alliance_value,
            limit_value,
        )
    recent_sessions = database.get_attendance_status_sessions(selected_guild_id, 8, 0)

    return _render(
        request,
        "dashboard.html",
        {
            "auth": auth,
            "overview": overview,
            "daily_stats": daily_stats,
            "top_users": top_users,
            "alliance_stats": alliance_stats,
            "recent_sessions": recent_sessions,
            "alliance_options": alliance_options,
            "filters": {
                "start_date": start_value,
                "end_date": end_value,
                "search": search_value,
                "alliance": alliance_value,
                "export_href": _dashboard_csv_url(
                    selected_guild_id,
                    start_date=start_value,
                    end_date=end_value,
                    search=search_value,
                    alliance=alliance_value,
                ),
                "limit": limit_value,
                "quick": _quick_dashboard_filters(
                    selected_guild_id,
                    start_date=start_value,
                    end_date=end_value,
                    search=search_value,
                    alliance=alliance_value,
                    limit=limit_value,
                ),
                "reset_href": _dashboard_url(selected_guild_id),
                "active_count": sum(
                    1
                    for value in (
                        start_value,
                        end_value,
                        search_value,
                        alliance_value,
                    )
                    if value
                ),
            },
            "active_page": "dashboard",
        },
    )


@app.get("/dashboard/export.csv")
def dashboard_export_csv(
    request: Request,
    guild_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    search: str | None = None,
    alliance: str | None = None,
    period: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if period:
        start_date, end_date = _dashboard_period_dates(period)
    start_at, end_at, start_value, end_value = _date_bounds(start_date, end_date)
    search_value = (search or "").strip()
    alliance_value = (alliance or "").strip()
    other_alliance_names = _unmapped_alliance_names(selected_guild_id)
    is_other_alliance_filter = (
        alliance_value == OTHER_ALLIANCE_VALUE and bool(other_alliance_names)
    )
    query_alliance_value = None if is_other_alliance_filter else alliance_value or None
    rows = database.get_attendance_export_rows(
        selected_guild_id,
        start_at,
        end_at,
        search_value or None,
        query_alliance_value,
    )
    if is_other_alliance_filter:
        rows = [
            row
            for row in rows
            if str(row.get("alliance_name") or "미분류") in other_alliance_names
        ]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["출석시간", "혈맹", "닉네임", "Discord ID"])
    for row in rows:
        writer.writerow(
            [
                row["started_at"],
                row["alliance_name"],
                row["discord_nickname"] or "",
                row["discord_id"] or "",
            ]
        )

    filename_parts = ["attendance"]
    if start_value or end_value:
        filename_parts.append(start_value or "start")
        filename_parts.append(end_value or "end")
    filename = "-".join(filename_parts) + ".csv"
    return Response(
        "\ufeff" + output.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/my-alliance", response_class=HTMLResponse)
def my_alliance(
    request: Request,
    guild_id: str | None = None,
    alliance_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    access = auth["selected_server"].get("my_alliance") or {}
    alliance_options = access.get("options") or []
    if not alliance_options:
        return RedirectResponse(
            f"/attendance?guild_id={selected_guild_id}",
            status_code=303,
        )

    allowed_by_value = {
        str(option["alliance_id"]): option for option in alliance_options
    }
    selected_alliance_value = (
        str(alliance_id)
        if alliance_id is not None and str(alliance_id) in allowed_by_value
        else str(alliance_options[0]["alliance_id"])
    )
    selected_alliance = allowed_by_value[selected_alliance_value]
    selected_alliance_ids = _selected_alliance_ids(selected_alliance)
    start_at, end_at, start_value, end_value = _date_bounds(start_date, end_date)

    overview = _alliance_overview(
        selected_guild_id,
        selected_alliance_ids,
        start_at,
        end_at,
    )
    user_rankings = _alliance_user_rankings(
        selected_guild_id,
        selected_alliance_ids,
        start_at,
        end_at,
        limit=200,
    )
    hour_stats = _alliance_hour_stats(
        selected_guild_id,
        selected_alliance_ids,
        start_at,
        end_at,
    )
    daily_rows = _alliance_daily_rows(
        selected_guild_id,
        selected_alliance_ids,
        start_at,
        end_at,
    )
    week_start_at, week_end_at = _current_week_bounds()
    month_start_at, month_end_at = _current_month_bounds()
    weekly_rankings = _alliance_user_rankings(
        selected_guild_id,
        selected_alliance_ids,
        week_start_at,
        week_end_at,
        limit=10,
    )
    monthly_rankings = _alliance_user_rankings(
        selected_guild_id,
        selected_alliance_ids,
        month_start_at,
        month_end_at,
        limit=10,
    )

    today = datetime.now(KST).date()
    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    filters = {
        "start_date": start_value,
        "end_date": end_value,
        "alliance_id": selected_alliance_value,
        "quick": [
            {
                "label": "전체",
                "href": _my_alliance_url(
                    selected_guild_id,
                    alliance_id=selected_alliance_value,
                ),
                "active": not start_value and not end_value,
            },
            {
                "label": "이번 주",
                "href": _my_alliance_url(
                    selected_guild_id,
                    alliance_id=selected_alliance_value,
                    start_date=week_start.isoformat(),
                    end_date=today.isoformat(),
                ),
                "active": start_value == week_start.isoformat()
                and end_value == today.isoformat(),
            },
            {
                "label": "이번 달",
                "href": _my_alliance_url(
                    selected_guild_id,
                    alliance_id=selected_alliance_value,
                    start_date=month_start.isoformat(),
                    end_date=today.isoformat(),
                ),
                "active": start_value == month_start.isoformat()
                and end_value == today.isoformat(),
            },
        ],
    }

    return _render(
        request,
        "my_alliance.html",
        {
            "auth": auth,
            "selected_alliance": selected_alliance,
            "alliance_options": alliance_options,
            "can_select_alliance": bool(access.get("can_select")),
            "overview": overview,
            "user_rankings": user_rankings,
            "hour_stats": hour_stats,
            "weekday_stats": _alliance_weekday_stats(daily_rows),
            "daily_rows": daily_rows[:30],
            "weekly_rankings": weekly_rankings,
            "monthly_rankings": monthly_rankings,
            "filters": filters,
            "active_page": "my_alliance",
        },
    )


@app.get("/attendance", response_class=HTMLResponse)
def attendance(
    request: Request,
    guild_id: str | None = None,
    queued: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    return _render(
        request,
        "attendance.html",
        {
            "auth": auth,
            "live_state": _live_attendance_state(selected_guild_id),
            "sessions": _latest_attendance_sessions(selected_guild_id),
            "attendance_command_pending": _pending_attendance_command(
                selected_guild_id
            ),
            "queued": queued,
            "active_page": "attendance",
        },
    )


@app.get("/status", response_class=HTMLResponse)
def attendance_status(
    request: Request,
    guild_id: str | None = None,
    page: int = 1,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    page_size = 10
    total_count = database.count_attendance_status_sessions(selected_guild_id)
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    current_page = min(max(1, page), total_pages)
    offset = (current_page - 1) * page_size
    sessions = database.get_attendance_status_sessions(
        selected_guild_id,
        page_size,
        offset,
    )
    can_manage_status = _can_manage_selected_server(auth)
    edit_candidates = (
        {
            int(session["attendance_id"]): database.get_attendance_edit_candidates(
                selected_guild_id,
                int(session["attendance_id"]),
            )
            for session in sessions
        }
        if can_manage_status
        else {}
    )
    return _render(
        request,
        "status.html",
        {
            "auth": auth,
            "sessions": sessions,
            "can_manage_status": can_manage_status,
            "edit_candidates": edit_candidates,
            "pagination": {
                "current_page": current_page,
                "total_pages": total_pages,
                "total_count": total_count,
                "page_size": page_size,
                "first_href": _status_url(selected_guild_id, 1),
                "prev_href": _status_url(selected_guild_id, max(1, current_page - 1)),
                "next_href": _status_url(
                    selected_guild_id,
                    min(total_pages, current_page + 1),
                ),
                "last_href": _status_url(selected_guild_id, total_pages),
                "has_previous": current_page > 1,
                "has_next": current_page < total_pages,
                "items": _pagination_items(
                    selected_guild_id,
                    current_page,
                    total_pages,
                ),
            },
            "active_page": "status",
        },
    )


@app.post("/status/add-entry")
async def add_attendance_status_entry(
    request: Request,
    guild_id: str | None = None,
    page: int = 1,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return RedirectResponse(
            f"/status?guild_id={selected_guild_id}&page={max(1, page)}",
            status_code=303,
        )

    form_data = await _urlencoded_form_data(request)
    attendance_id = 0
    try:
        attendance_id = int(form_data.get("attendance_id") or "")
        user_id = int(form_data.get("user_id") or "")
        if attendance_id <= 0 or user_id <= 0:
            raise ValueError
        database.add_attendance_entry(selected_guild_id, attendance_id, user_id)
        user_label = _work_log_user_label(user_id)
        _record_work_log(
            auth,
            selected_guild_id,
            action_type="attendance_add",
            target_type="attendance",
            target_id=attendance_id,
            summary=f"출석 #{attendance_id}에 {user_label} 추가",
            details={"attendance_id": attendance_id, "user_id": user_id},
        )
    except Exception:
        pass
    return RedirectResponse(
        f"/status?guild_id={selected_guild_id}&page={max(1, page)}#attendance-{attendance_id}",
        status_code=303,
    )


@app.post("/status/delete-entry")
async def delete_attendance_status_entry(
    request: Request,
    guild_id: str | None = None,
    page: int = 1,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return RedirectResponse(
            f"/status?guild_id={selected_guild_id}&page={max(1, page)}",
            status_code=303,
        )

    form_data = await _urlencoded_form_data(request)
    attendance_id = 0
    try:
        attendance_id = int(form_data.get("attendance_id") or "")
        user_id = int(form_data.get("user_id") or "")
        if attendance_id <= 0 or user_id <= 0:
            raise ValueError
        user_label = _work_log_user_label(user_id)
        database.delete_attendance_entry(selected_guild_id, attendance_id, user_id)
        _record_work_log(
            auth,
            selected_guild_id,
            action_type="attendance_delete",
            target_type="attendance",
            target_id=attendance_id,
            summary=f"출석 #{attendance_id}에서 {user_label} 삭제",
            details={"attendance_id": attendance_id, "user_id": user_id},
        )
    except Exception:
        pass
    return RedirectResponse(
        f"/status?guild_id={selected_guild_id}&page={max(1, page)}#attendance-{attendance_id}",
        status_code=303,
    )


@app.get("/loot", response_class=HTMLResponse)
def loot_drops(
    request: Request,
    guild_id: str | None = None,
    saved: str | None = None,
    period: str | None = None,
    alliance_id: str | None = None,
    mine: str | None = None,
    status: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    return _render(
        request,
        "loot.html",
        _loot_template_context(
            auth,
            selected_guild_id,
            saved=saved,
            errors=[],
            period=period,
            alliance_id=alliance_id,
            mine=mine,
            status=status,
        ),
    )


@app.post("/loot", response_class=HTMLResponse)
async def create_loot_drop(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_bookkeep_selected_server(auth):
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=forbidden",
            status_code=303,
        )

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    errors: list[str] = []
    try:
        attendance_id = int(form_data.get("attendance_id") or "")
    except ValueError:
        attendance_id = 0
    if attendance_id <= 0:
        errors.append("드랍 기준 출석 회차를 선택해주세요.")

    try:
        item_id = _parse_optional_int(form_data.get("item_id"))
    except ValueError:
        item_id = None
        errors.append("아이템 선택값이 올바르지 않습니다.")
    if item_id is None:
        errors.append("아이템 설정에서 등록된 아이템을 선택해주세요.")

    item_prices = database.get_item_price_settings(selected_guild_id)
    cash_price_krw, sale_price, adena_rate = _loot_prices_from_form(
        form_data,
        item_prices,
        item_id,
        errors,
    )
    fee_rate = _fee_rate_from_form(form_data, errors)
    excluded_alliance_ids = _parse_loot_excluded_alliance_ids(
        form_data.get("excluded_alliance_ids"),
    )
    form_data["cash_price_krw"] = _decimal_input(cash_price_krw)
    form_data["sale_price"] = _decimal_input(sale_price)
    form_data["fee_percent"] = _decimal_input(fee_rate * Decimal("100"))
    form_data["excluded_alliance_ids"] = ",".join(
        str(alliance_id) for alliance_id in excluded_alliance_ids
    )

    if errors:
        return _render(
            request,
            "loot.html",
            _loot_template_context(
                auth,
                selected_guild_id,
                saved="",
                errors=errors,
                loot_form=_loot_form_from_values(form_data, selected_guild_id),
            ),
            status_code=400,
        )

    try:
        loot_event_id = database.create_loot_drop(
            selected_guild_id,
            attendance_id=attendance_id,
            item_id=item_id,
            item_name="",
            cash_price_krw=cash_price_krw,
            sale_price=sale_price,
            adena_rate=adena_rate,
            fee_rate=fee_rate,
            memo=str(form_data.get("memo") or ""),
            created_by_discord_id=int(auth["user"]["id"]),
            excluded_alliance_ids=excluded_alliance_ids,
        )
        _record_work_log(
            auth,
            selected_guild_id,
            action_type="loot_create",
            target_type="loot",
            target_id=loot_event_id,
            summary=f"드랍 등록: {_work_log_loot_label(selected_guild_id, loot_event_id)}",
            details={
                "loot_event_id": loot_event_id,
                "attendance_id": attendance_id,
                "item_id": item_id,
                "cash_price_krw": _decimal_input(cash_price_krw),
                "sale_price": _decimal_input(sale_price),
                "adena_rate": _decimal_input(adena_rate),
                "fee_rate": _decimal_input(fee_rate),
                "excluded_alliance_ids": excluded_alliance_ids,
            },
        )
    except ValueError as exc:
        return _render(
            request,
            "loot.html",
            _loot_template_context(
                auth,
                selected_guild_id,
                saved="",
                errors=[str(exc)],
                loot_form=_loot_form_from_values(form_data, selected_guild_id),
            ),
            status_code=400,
        )

    return RedirectResponse(
        f"/loot?guild_id={selected_guild_id}&saved=created",
        status_code=303,
    )


@app.post("/loot/bids/status")
async def update_loot_bid_item_status(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    wants_json = request.headers.get("x-requested-with") == "fetch"
    if not _can_bookkeep_selected_server(auth):
        if wants_json:
            return JSONResponse(
                {"ok": False, "message": "경리 이상 권한이 필요합니다."},
                status_code=403,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=forbidden#item-bids",
            status_code=303,
        )

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        bid_item_id = int(form_data.get("bid_item_id") or "")
        alliance_id = int(form_data.get("alliance_id") or "")
        is_completed = str(form_data.get("is_completed") or "").lower() in {
            "1",
            "true",
            "yes",
            "on",
            "completed",
        }
        if bid_item_id <= 0:
            raise ValueError
        if alliance_id <= 0:
            raise ValueError
        result = database.set_bid_item_alliance_status(
            selected_guild_id,
            bid_item_id,
            alliance_id=alliance_id,
            is_completed=is_completed,
            updated_by_discord_id=int(auth["user"]["id"]),
        )
        _record_work_log(
            auth,
            selected_guild_id,
            action_type="bid_status",
            target_type="bid_item",
            target_id=bid_item_id,
            summary=(
                f"입찰 상태: {result['item_name']} / "
                f"{result['alliance_name']} -> {result['status_label']}"
            ),
            details=result,
        )
        if wants_json:
            return JSONResponse({"ok": True, "result": result})
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=bid_status#item-bids",
            status_code=303,
        )
    except Exception as exc:
        if wants_json:
            return JSONResponse(
                {"ok": False, "message": str(exc) or "입찰 상태 변경에 실패했습니다."},
                status_code=400,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=bid_error#item-bids",
            status_code=303,
        )


@app.post("/loot/bids/items")
async def save_loot_bid_item(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    wants_json = request.headers.get("x-requested-with") == "fetch"
    if not _can_bookkeep_selected_server(auth):
        if wants_json:
            return JSONResponse(
                {"ok": False, "message": "경리 이상 권한이 필요합니다."},
                status_code=403,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=forbidden#item-bids",
            status_code=303,
        )

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        bid_item_id_raw = str(form_data.get("bid_item_id") or "").strip()
        bid_item_id = int(bid_item_id_raw) if bid_item_id_raw else None
        item_name = str(form_data.get("item_name") or "").strip()
        is_free = str(form_data.get("is_free") or "").lower() in {
            "1",
            "true",
            "yes",
            "on",
            "free",
        }
        item = database.upsert_bid_item(
            selected_guild_id,
            bid_item_id=bid_item_id,
            item_name=item_name,
            is_free=is_free,
        )
        _record_work_log(
            auth,
            selected_guild_id,
            action_type="bid_item",
            target_type="bid_item",
            target_id=int(item["bid_item_id"]),
            summary=(
                f"입찰 아이템 {'수정' if bid_item_id else '추가'}: "
                f"{item['item_name']} · {'무료나눔' if item['is_free'] else '유료'}"
            ),
            details=item,
        )
        if wants_json:
            return JSONResponse({"ok": True, "result": item})
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=bid_item#item-bids",
            status_code=303,
        )
    except Exception as exc:
        if wants_json:
            return JSONResponse(
                {"ok": False, "message": str(exc) or "입찰 아이템 저장에 실패했습니다."},
                status_code=400,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=bid_item_error#item-bids",
            status_code=303,
        )


@app.post("/loot/bids/items/delete")
async def delete_loot_bid_item(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_bookkeep_selected_server(auth):
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=forbidden#item-bids",
            status_code=303,
        )

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        bid_item_id = int(form_data.get("bid_item_id") or "")
        if bid_item_id <= 0:
            raise ValueError
        item = database.deactivate_bid_item(selected_guild_id, bid_item_id)
        _record_work_log(
            auth,
            selected_guild_id,
            action_type="bid_item_delete",
            target_type="bid_item",
            target_id=int(item["bid_item_id"]),
            summary=f"입찰 아이템 삭제: {item['item_name']}",
            details=item,
        )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=bid_item#item-bids",
            status_code=303,
        )
    except Exception:
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=bid_item_error#item-bids",
            status_code=303,
        )


@app.post("/loot/update")
async def update_loot_drop(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_owner_manage_selected_server(auth):
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=forbidden",
            status_code=303,
        )

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    errors: list[str] = []
    try:
        loot_event_id = int(form_data.get("loot_event_id") or "")
    except ValueError:
        loot_event_id = 0
    if loot_event_id <= 0:
        errors.append("드랍 기록 ID가 올바르지 않습니다.")
    cash_price_krw, sale_price, adena_rate = _loot_prices_from_form(
        form_data,
        database.get_item_price_settings(selected_guild_id),
        None,
        errors,
    )
    fee_rate = _fee_rate_from_form(form_data, errors)
    if errors:
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=error#alliance-payouts",
            status_code=303,
        )

    try:
        database.update_loot_drop(
            selected_guild_id,
            loot_event_id,
            cash_price_krw=cash_price_krw,
            sale_price=sale_price,
            adena_rate=adena_rate,
            fee_rate=fee_rate,
            memo=str(form_data.get("memo") or ""),
        )
        _record_work_log(
            auth,
            selected_guild_id,
            action_type="loot_update",
            target_type="loot",
            target_id=loot_event_id,
            summary=f"드랍 수정: {_work_log_loot_label(selected_guild_id, loot_event_id)}",
            details={
                "loot_event_id": loot_event_id,
                "cash_price_krw": _decimal_input(cash_price_krw),
                "sale_price": _decimal_input(sale_price),
                "adena_rate": _decimal_input(adena_rate),
                "fee_rate": _decimal_input(fee_rate),
            },
        )
    except ValueError:
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=error#alliance-payouts",
            status_code=303,
        )
    return RedirectResponse(
        f"/loot?guild_id={selected_guild_id}&saved=updated#alliance-payouts",
        status_code=303,
    )


@app.post("/loot/payout-status")
async def update_loot_payout_status(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "로그인이 필요합니다."},
                status_code=401,
            )
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "관리자 권한이 필요합니다."},
                status_code=403,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=forbidden#alliance-payouts",
            status_code=303,
        )

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        distribution_id = int(form_data.get("distribution_id") or "")
        alliance_id = int(form_data.get("alliance_id") or "")
    except ValueError:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "분배 상태 값을 확인하지 못했습니다."},
                status_code=400,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=error#alliance-payouts",
            status_code=303,
        )
    payout_status = str(form_data.get("payout_status") or "")
    try:
        database.update_distribution_alliance_payout_status(
            selected_guild_id,
            distribution_id,
            alliance_id,
            payout_status,
        )
        payload = _loot_payout_payload(selected_guild_id, distribution_id)
    except ValueError:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "분배 상태를 변경하지 못했습니다."},
                status_code=400,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=error#alliance-payouts",
            status_code=303,
        )
    if _wants_json(request):
        return JSONResponse({"ok": True, **payload})
    return RedirectResponse(
        f"/loot?guild_id={selected_guild_id}&saved=payout#alliance-payouts",
        status_code=303,
    )


@app.post("/loot/payout-status-all")
async def update_all_loot_payout_status(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        if _wants_json(request):
            return JSONResponse({"ok": False, "message": "로그인이 필요합니다."}, status_code=401)
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        if _wants_json(request):
            return JSONResponse({"ok": False, "message": "관리자 권한이 필요합니다."}, status_code=403)
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=forbidden#alliance-payouts",
            status_code=303,
        )

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        distribution_id = int(form_data.get("distribution_id") or "")
    except ValueError:
        distribution_id = 0
    payout_status = str(form_data.get("payout_status") or "paid")
    try:
        database.update_all_distribution_alliance_payout_status(
            selected_guild_id,
            distribution_id,
            payout_status,
        )
        payload = _loot_payout_payload(selected_guild_id, distribution_id)
    except ValueError:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "전체 분배 상태를 변경하지 못했습니다."},
                status_code=400,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=error#alliance-payouts",
            status_code=303,
        )
    if _wants_json(request):
        return JSONResponse({"ok": True, **payload})
    return RedirectResponse(
        f"/loot?guild_id={selected_guild_id}&saved=payout#alliance-payouts",
        status_code=303,
    )


@app.post("/loot/payouts/alliance-settle-all")
async def settle_alliance_payout_group(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        if _wants_json(request):
            return JSONResponse({"ok": False, "message": "로그인이 필요합니다."}, status_code=401)
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        if _wants_json(request):
            return JSONResponse({"ok": False, "message": "관리자 권한이 필요합니다."}, status_code=403)
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=forbidden#alliance-payouts",
            status_code=303,
        )

    form_data = await _urlencoded_form_data(request)
    try:
        alliance_id = int(form_data.get("alliance_id") or "")
    except ValueError:
        alliance_id = 0
    mapped_alliance_ids = {
        int(alliance["alliance_id"])
        for alliance in _mapped_loot_alliances(selected_guild_id)
    }
    distribution_ids = []
    for raw_id in str(form_data.get("distribution_ids") or "").split(","):
        try:
            distribution_id = int(raw_id.strip())
        except ValueError:
            continue
        if distribution_id > 0:
            distribution_ids.append(distribution_id)
    if alliance_id <= 0 or alliance_id not in mapped_alliance_ids or not distribution_ids:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "완료 처리할 혈맹 분배 항목이 없습니다."},
                status_code=400,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=error#alliance-payouts",
            status_code=303,
        )

    updated_ids = []
    try:
        for distribution_id in sorted(set(distribution_ids)):
            database.update_distribution_alliance_payout_status(
                selected_guild_id,
                distribution_id,
                alliance_id,
                "paid",
            )
            updated_ids.append(distribution_id)
    except ValueError:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "혈맹별 분배를 완료 처리하지 못했습니다."},
                status_code=400,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=error#alliance-payouts",
            status_code=303,
        )

    if _wants_json(request):
        return JSONResponse(
            {
                "ok": True,
                "alliance_id": alliance_id,
                "distribution_ids": updated_ids,
                "payout_status": "paid",
            }
        )
    return RedirectResponse(
        f"/loot?guild_id={selected_guild_id}&saved=payout#alliance-payouts",
        status_code=303,
    )


@app.post("/loot/alliance-fee-rules")
async def create_alliance_fee_rule(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)
    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return _loot_redirect(selected_guild_id, saved="forbidden")

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        requested_alliance_id = int(form_data.get("alliance_id") or "")
    except ValueError:
        requested_alliance_id = None
    alliance_id = _allowed_loot_alliance_id(auth, requested_alliance_id)
    if alliance_id is None:
        return _loot_redirect(selected_guild_id, saved="forbidden")

    errors: list[str] = []
    fee_rate = _fee_rate_from_form(form_data, errors)
    if errors:
        return _loot_redirect(selected_guild_id, saved="error", alliance_id=alliance_id)
    try:
        database.create_alliance_payout_fee_rule(
            selected_guild_id,
            alliance_id,
            rule_name=str(form_data.get("rule_name") or ""),
            fee_rate=fee_rate,
            created_by_discord_id=int(auth["user"]["id"]),
        )
    except ValueError:
        return _loot_redirect(selected_guild_id, saved="error", alliance_id=alliance_id)
    return _loot_redirect(selected_guild_id, saved="fee_rule", alliance_id=alliance_id)


@app.post("/loot/alliance-fee-rules/delete")
async def delete_alliance_fee_rule(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)
    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return _loot_redirect(selected_guild_id, saved="forbidden")

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        requested_alliance_id = int(form_data.get("alliance_id") or "")
        rule_id = int(form_data.get("rule_id") or "")
    except ValueError:
        requested_alliance_id = None
        rule_id = 0
    alliance_id = _allowed_loot_alliance_id(auth, requested_alliance_id)
    if alliance_id is None:
        return _loot_redirect(selected_guild_id, saved="forbidden")
    try:
        database.deactivate_alliance_payout_fee_rule(
            selected_guild_id,
            alliance_id,
            rule_id,
        )
    except ValueError:
        return _loot_redirect(selected_guild_id, saved="error", alliance_id=alliance_id)
    return _loot_redirect(selected_guild_id, saved="fee_rule", alliance_id=alliance_id)


@app.post("/loot/member-payouts/settle")
async def settle_member_payout(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        if _wants_json(request):
            return JSONResponse({"ok": False, "message": "로그인이 필요합니다."}, status_code=401)
        return _auth_redirect(request)
    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        if _wants_json(request):
            return JSONResponse({"ok": False, "message": "관리자 권한이 필요합니다."}, status_code=403)
        return _loot_redirect(selected_guild_id, saved="forbidden")

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        requested_alliance_id = int(form_data.get("alliance_id") or "")
        distribution_id = int(form_data.get("distribution_id") or "")
    except ValueError:
        requested_alliance_id = None
        distribution_id = 0
    alliance_id = _allowed_loot_alliance_id(auth, requested_alliance_id)
    if alliance_id is None:
        if _wants_json(request):
            return JSONResponse({"ok": False, "message": "혈맹 권한이 없습니다."}, status_code=403)
        return _loot_redirect(selected_guild_id, saved="forbidden")
    try:
        database.settle_member_payout(
            selected_guild_id,
            distribution_id,
            alliance_id,
            updated_by_discord_id=int(auth["user"]["id"]),
        )
    except ValueError:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "혈맹원 분배를 완료 처리하지 못했습니다."},
                status_code=400,
            )
        return _loot_redirect(selected_guild_id, saved="error", alliance_id=alliance_id)
    if _wants_json(request):
        return JSONResponse({"ok": True})
    return _loot_redirect(selected_guild_id, saved="member_payout", alliance_id=alliance_id)


@app.post("/loot/member-payouts/recipient-status")
async def update_member_payout_recipient_status(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        if _wants_json(request):
            return JSONResponse({"ok": False, "message": "로그인이 필요합니다."}, status_code=401)
        return _auth_redirect(request)
    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        if _wants_json(request):
            return JSONResponse({"ok": False, "message": "관리자 권한이 필요합니다."}, status_code=403)
        return _loot_redirect(selected_guild_id, saved="forbidden")

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        requested_alliance_id = int(form_data.get("alliance_id") or "")
        distribution_id = int(form_data.get("distribution_id") or "")
        user_id = int(form_data.get("user_id") or "")
    except ValueError:
        requested_alliance_id = None
        distribution_id = 0
        user_id = 0
    alliance_id = _allowed_loot_alliance_id(auth, requested_alliance_id)
    if alliance_id is None:
        if _wants_json(request):
            return JSONResponse({"ok": False, "message": "혈맹 권한이 없습니다."}, status_code=403)
        return _loot_redirect(selected_guild_id, saved="forbidden")
    payout_status = str(form_data.get("payout_status") or "")
    try:
        database.update_member_payout_recipient_status(
            selected_guild_id,
            distribution_id,
            alliance_id,
            user_id,
            payout_status,
            updated_by_discord_id=int(auth["user"]["id"]),
        )
    except ValueError:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "혈맹원 분배 상태를 변경하지 못했습니다."},
                status_code=400,
            )
        return _loot_redirect(selected_guild_id, saved="error", alliance_id=alliance_id)
    if _wants_json(request):
        return JSONResponse({"ok": True, "payout_status": payout_status})
    return _loot_redirect(selected_guild_id, saved="member_payout", alliance_id=alliance_id)


@app.post("/loot/member-payouts/recipient-settle-all")
async def settle_member_payout_recipient_all(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        if _wants_json(request):
            return JSONResponse({"ok": False, "message": "로그인이 필요합니다."}, status_code=401)
        return _auth_redirect(request)
    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        if _wants_json(request):
            return JSONResponse({"ok": False, "message": "관리자 권한이 필요합니다."}, status_code=403)
        return _loot_redirect(selected_guild_id, saved="forbidden")

    form_data = await _urlencoded_form_data(request)
    try:
        requested_alliance_id = int(form_data.get("alliance_id") or "")
        user_id = int(form_data.get("user_id") or "")
    except ValueError:
        requested_alliance_id = None
        user_id = 0
    alliance_id = _allowed_loot_alliance_id(auth, requested_alliance_id)
    if alliance_id is None:
        if _wants_json(request):
            return JSONResponse({"ok": False, "message": "혈맹 권한이 없습니다."}, status_code=403)
        return _loot_redirect(selected_guild_id, saved="forbidden")

    distribution_ids = []
    for raw_id in str(form_data.get("distribution_ids") or "").split(","):
        try:
            distribution_id = int(raw_id.strip())
        except ValueError:
            continue
        if distribution_id > 0:
            distribution_ids.append(distribution_id)
    if user_id <= 0 or not distribution_ids:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "완료 처리할 미분배 항목이 없습니다."},
                status_code=400,
            )
        return _loot_redirect(selected_guild_id, saved="error", alliance_id=alliance_id)

    updated_ids = []
    skipped_ids = []
    for distribution_id in sorted(set(distribution_ids)):
        try:
            database.update_member_payout_recipient_status(
                selected_guild_id,
                distribution_id,
                alliance_id,
                user_id,
                "paid",
                updated_by_discord_id=int(auth["user"]["id"]),
            )
            updated_ids.append(distribution_id)
        except ValueError:
            skipped_ids.append(distribution_id)

    if not updated_ids:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "유저별 분배를 완료 처리하지 못했습니다."},
                status_code=400,
            )
        return _loot_redirect(selected_guild_id, saved="error", alliance_id=alliance_id)

    if _wants_json(request):
        return JSONResponse(
            {
                "ok": True,
                "user_id": user_id,
                "distribution_ids": updated_ids,
                "skipped_distribution_ids": skipped_ids,
                "payout_status": "paid",
            }
        )
    return _loot_redirect(selected_guild_id, saved="member_payout", alliance_id=alliance_id)


@app.post("/loot/member-payouts/settle-all")
async def settle_all_member_payouts(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)
    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return _loot_redirect(selected_guild_id, saved="forbidden")

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        requested_alliance_id = int(form_data.get("alliance_id") or "")
    except ValueError:
        requested_alliance_id = None
    alliance_id = _allowed_loot_alliance_id(auth, requested_alliance_id)
    if alliance_id is None:
        return _loot_redirect(selected_guild_id, saved="forbidden")
    try:
        database.settle_all_member_payouts(
            selected_guild_id,
            alliance_id,
            updated_by_discord_id=int(auth["user"]["id"]),
        )
    except ValueError:
        return _loot_redirect(selected_guild_id, saved="error", alliance_id=alliance_id)
    return _loot_redirect(selected_guild_id, saved="member_payout", alliance_id=alliance_id)


@app.post("/loot/delete")
async def delete_loot_drop(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "로그인이 필요합니다."},
                status_code=401,
            )
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_owner_manage_selected_server(auth):
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "서버 오너 권한이 필요합니다."},
                status_code=403,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=forbidden#alliance-payouts",
            status_code=303,
        )

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        loot_event_id = int(form_data.get("loot_event_id") or "")
        if loot_event_id <= 0:
            raise ValueError
        loot_label = _work_log_loot_label(selected_guild_id, loot_event_id)
        database.delete_loot_drop(selected_guild_id, loot_event_id)
        _record_work_log(
            auth,
            selected_guild_id,
            action_type="loot_delete",
            target_type="loot",
            target_id=loot_event_id,
            summary=f"드랍 삭제: {loot_label}",
            details={"loot_event_id": loot_event_id},
        )
    except ValueError:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "드랍 기록을 삭제하지 못했습니다."},
                status_code=400,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=error#alliance-payouts",
            status_code=303,
        )

    if _wants_json(request):
        return JSONResponse({"ok": True, "loot_event_id": str(loot_event_id)})
    return RedirectResponse(
        f"/loot?guild_id={selected_guild_id}&saved=deleted#alliance-payouts",
        status_code=303,
    )


@app.get("/settings", response_class=HTMLResponse)
def settings(
    request: Request,
    guild_id: str | None = None,
    saved: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return _settings_forbidden_redirect(selected_guild_id)

    guild_settings = database.get_settings(selected_guild_id)
    channels, roles, channel_error, role_error = _settings_discord_options(selected_guild_id)

    return _render(
        request,
        "settings.html",
        _settings_template_context(
            auth,
            selected_guild_id,
            guild_settings=guild_settings,
            form=_settings_to_dict(guild_settings),
            channels=channels,
            roles=roles,
            channel_error=channel_error,
            role_error=role_error,
            saved=saved,
            errors=[],
            report_form=_default_report_form(),
        ),
    )


@app.post("/settings", response_class=HTMLResponse)
async def update_settings(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return _settings_forbidden_redirect(selected_guild_id)

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }

    channels, roles, channel_error, role_error = _settings_discord_options(selected_guild_id)

    errors: list[str] = []
    text_channel_ids = _channel_ids(channels["text"])
    voice_channel_ids = _channel_ids(channels["voice"])
    settings_values = {
        "admin_channel_id": _validate_channel_value(
            "출석 패널 채널",
            form_data.get("admin_channel_id"),
            text_channel_ids,
            errors,
        ),
        "attendance_voice_channel_id": _validate_channel_value(
            "출석 음성채널",
            form_data.get("attendance_voice_channel_id"),
            voice_channel_ids,
            errors,
        ),
        "log_channel_id": _validate_channel_value(
            "로그 채널",
            form_data.get("log_channel_id"),
            text_channel_ids,
            errors,
        ),
        "timer": _validate_timer_value(
            "출석 진행 시간",
            form_data.get("timer"),
            errors,
        ),
        "attendance_available_timer": _validate_timer_value(
            "출석 가능 대기 시간",
            form_data.get("attendance_available_timer"),
            errors,
        ),
    }
    if channel_error:
        errors.append("채널 목록 확인이 필요합니다. Discord 봇 토큰과 서버 권한을 확인해주세요.")

    previous_settings = database.get_settings(selected_guild_id)
    if errors:
        return _render(
            request,
            "settings.html",
            _settings_template_context(
                auth,
                selected_guild_id,
                guild_settings=previous_settings,
                form=_settings_form_from_values(form_data),
                channels=channels,
                roles=roles,
                channel_error=channel_error,
                role_error=role_error,
                saved="",
                errors=errors,
                report_form=_default_report_form(),
                settings_active_tab="channels",
            ),
            status_code=400,
        )

    for column, value in settings_values.items():
        database.update_setting(selected_guild_id, column, value)

    _enqueue_bot_command(
        selected_guild_id,
        "refresh_admin_panel",
        int(auth["user"]["id"]),
        {
            "previous_admin_channel_id": previous_settings.admin_channel_id,
            "updated_columns": list(settings_values),
        },
    )
    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved=1",
        status_code=303,
    )


@app.post("/settings/alliance-roles", response_class=HTMLResponse)
async def upsert_alliance_role_mapping(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return _settings_forbidden_redirect(selected_guild_id)

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    channels, roles, channel_error, role_error = _settings_discord_options(selected_guild_id)
    errors: list[str] = []
    alliance_name = str(form_data.get("alliance_name") or "").strip()
    if not alliance_name:
        errors.append("혈맹 이름을 입력해주세요.")
    try:
        role_id = _parse_optional_int(form_data.get("role_id"))
    except ValueError:
        role_id = None
        errors.append("Discord 역할을 선택해주세요.")
    allowed_role_ids = _role_ids(roles)
    if role_id is None:
        errors.append("Discord 역할을 선택해주세요.")
    elif role_id not in allowed_role_ids:
        errors.append("선택한 Discord 역할을 서버에서 확인할 수 없습니다.")
    role_name = _find_role_name(roles, role_id) if role_id is not None else None
    if role_error:
        errors.append("Discord 역할 목록 확인이 필요합니다. 봇 토큰과 서버 권한을 확인해주세요.")

    if errors:
        guild_settings = database.get_settings(selected_guild_id)
        return _render(
            request,
            "settings.html",
            _settings_template_context(
                auth,
                selected_guild_id,
                guild_settings=guild_settings,
                form=_settings_to_dict(guild_settings),
                channels=channels,
                roles=roles,
                channel_error=channel_error,
                role_error=role_error,
                saved="",
                errors=errors,
                report_form=_default_report_form(),
                alliance_role_form=_alliance_role_form_from_values(form_data),
                settings_active_tab="alliance",
            ),
            status_code=400,
        )

    database.upsert_guild_alliance_role_mapping(
        selected_guild_id,
        int(role_id),
        str(role_name or role_id),
        alliance_name,
    )
    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved=alliance_role",
        status_code=303,
    )


@app.post("/settings/alliance-roles/delete")
async def delete_alliance_role_mapping(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return _settings_forbidden_redirect(selected_guild_id)

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        mapping_id = int(form_data.get("mapping_id") or "")
    except ValueError:
        return RedirectResponse(
            f"/settings?guild_id={selected_guild_id}&saved=alliance_role_error",
            status_code=303,
        )

    database.delete_guild_alliance_role_mapping(selected_guild_id, mapping_id)
    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved=alliance_role_deleted",
        status_code=303,
    )


@app.post("/settings/bookkeepers", response_class=HTMLResponse)
async def add_bookkeeper(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_bookkeepers(auth):
        return _settings_forbidden_redirect(selected_guild_id)

    form_data = await _urlencoded_form_data(request)
    try:
        user_id = int(form_data.get("user_id") or "")
        if user_id <= 0:
            raise ValueError
        database.add_guild_bookkeeper(
            selected_guild_id,
            user_id,
            added_by_discord_id=int(auth["user"]["id"]),
        )
    except ValueError:
        return RedirectResponse(
            f"/settings?guild_id={selected_guild_id}&saved=bookkeeper_error#bookkeepers",
            status_code=303,
        )

    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved=bookkeeper#bookkeepers",
        status_code=303,
    )


@app.post("/settings/bookkeepers/delete")
async def delete_bookkeeper(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_bookkeepers(auth):
        return _settings_forbidden_redirect(selected_guild_id)

    form_data = await _urlencoded_form_data(request)
    try:
        user_id = int(form_data.get("user_id") or "")
        if user_id <= 0:
            raise ValueError
    except ValueError:
        return RedirectResponse(
            f"/settings?guild_id={selected_guild_id}&saved=bookkeeper_error#bookkeepers",
            status_code=303,
        )

    database.delete_guild_bookkeeper(selected_guild_id, user_id)
    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved=bookkeeper_deleted#bookkeepers",
        status_code=303,
    )


@app.post("/settings/items", response_class=HTMLResponse)
async def create_item_price(
    request: Request,
    guild_id: str | None = None,
    return_to: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_bookkeep_selected_server(auth):
        return _settings_forbidden_redirect(selected_guild_id)

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    errors: list[str] = []
    item_name = str(form_data.get("item_name") or "").strip()
    if not item_name:
        errors.append("아이템 이름을 입력해주세요.")
    default_price = _decimal_from_form(
        "원화 시세",
        form_data.get("default_price"),
        errors,
    )
    if errors:
        if return_to == "loot":
            return _render(
                request,
                "loot.html",
                _loot_template_context(
                    auth,
                    selected_guild_id,
                    saved="",
                    errors=errors,
                    item_price_form=_item_price_form_from_values(form_data),
                ),
                status_code=400,
            )
        guild_settings = database.get_settings(selected_guild_id)
        channels, roles, channel_error, role_error = _settings_discord_options(
            selected_guild_id,
        )
        return _render(
            request,
            "settings.html",
            _settings_template_context(
                auth,
                selected_guild_id,
                guild_settings=guild_settings,
                form=_settings_to_dict(guild_settings),
                channels=channels,
                roles=roles,
                channel_error=channel_error,
                role_error=role_error,
                saved="",
                errors=errors,
                report_form=_default_report_form(),
                item_price_form=_item_price_form_from_values(form_data),
                settings_active_tab="items",
            ),
            status_code=400,
        )

    item_id = database.upsert_item_price(
        selected_guild_id,
        item_name=item_name,
        default_price=default_price,
        category="",
        memo="",
        is_bid_item=True,
    )
    _record_work_log(
        auth,
        selected_guild_id,
        action_type="item_create",
        target_type="item",
        target_id=item_id,
        summary=f"아이템 추가: {item_name} · {_decimal_input(default_price)}원",
        details={
            "item_id": item_id,
            "item_name": item_name,
            "default_price": _decimal_input(default_price),
        },
    )
    return _item_price_redirect(selected_guild_id, "item_price", return_to)


@app.post("/settings/items/update")
async def update_item_price(
    request: Request,
    guild_id: str | None = None,
    return_to: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_bookkeep_selected_server(auth):
        return _settings_forbidden_redirect(selected_guild_id)

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    errors: list[str] = []
    try:
        item_id = int(form_data.get("item_id") or "")
    except ValueError:
        item_id = 0
        errors.append("아이템 ID가 올바르지 않습니다.")
    item_name = str(form_data.get("item_name") or "").strip()
    if not item_name:
        errors.append("아이템 이름을 입력해주세요.")
    default_price = _decimal_from_form(
        "원화 시세",
        form_data.get("default_price"),
        errors,
    )
    if errors:
        return _item_price_redirect(selected_guild_id, "item_price_error", return_to)

    try:
        database.update_item_price(
            selected_guild_id,
            item_id,
            item_name=item_name,
            default_price=default_price,
            category="",
            memo="",
            is_bid_item=True,
        )
        _record_work_log(
            auth,
            selected_guild_id,
            action_type="item_update",
            target_type="item",
            target_id=item_id,
            summary=f"아이템 수정: {item_name} · {_decimal_input(default_price)}원",
            details={
                "item_id": item_id,
                "item_name": item_name,
                "default_price": _decimal_input(default_price),
            },
        )
    except ValueError:
        return _item_price_redirect(selected_guild_id, "item_price_error", return_to)
    return _item_price_redirect(selected_guild_id, "item_price", return_to)


@app.post("/settings/items/delete")
async def delete_item_price(
    request: Request,
    guild_id: str | None = None,
    return_to: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_bookkeep_selected_server(auth):
        return _settings_forbidden_redirect(selected_guild_id)

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        item_id = int(form_data.get("item_id") or "")
    except ValueError:
        item_id = 0
    if item_id <= 0:
        return _item_price_redirect(selected_guild_id, "item_price_error", return_to)
    item_label = _work_log_item_label(item_id)
    database.deactivate_item_price(selected_guild_id, item_id)
    _record_work_log(
        auth,
        selected_guild_id,
        action_type="item_delete",
        target_type="item",
        target_id=item_id,
        summary=f"아이템 삭제: {item_label}",
        details={"item_id": item_id},
    )
    return _item_price_redirect(selected_guild_id, "item_price_deleted", return_to)


def _item_price_redirect(
    selected_guild_id: int,
    saved: str,
    return_to: str | None,
) -> RedirectResponse:
    if return_to == "loot":
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved={saved}#item-settings",
            status_code=303,
        )
    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved={saved}",
        status_code=303,
    )


@app.post("/settings/reports", response_class=HTMLResponse)
async def create_report_setting(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return _settings_forbidden_redirect(selected_guild_id)

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    errors: list[str] = []
    channels, roles, channel_error, role_error = _settings_discord_options(selected_guild_id)
    if channel_error:
        errors.append("알람을 받을 채널 목록을 확인할 수 없습니다.")

    (
        report_form,
        schedule_json,
        query_json,
        render_json,
        channel_id,
        channel_name,
    ) = _report_configs_from_form(
        form_data,
        errors,
        require_channel=True,
        channels=channels["text"],
    )
    guild_settings = database.get_settings(selected_guild_id)
    if errors:
        return _render(
            request,
            "settings.html",
            _settings_template_context(
                auth,
                selected_guild_id,
                guild_settings=guild_settings,
                form=_settings_to_dict(guild_settings),
                channels=channels,
                roles=roles,
                channel_error=channel_error,
                role_error=role_error,
                saved="",
                errors=errors,
                report_form=report_form,
                settings_active_tab="reports",
            ),
            status_code=400,
        )

    with database.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO scheduled_report_settings (
                    guild_id,
                    created_by_discord_id,
                    updated_by_discord_id,
                    report_name,
                    frequency,
                    run_time,
                    period_type,
                    subject_type,
                    result_type,
                    channel_id,
                    channel_name,
                    schedule_json,
                    query_json,
                    render_json,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    selected_guild_id,
                    int(auth["user"]["id"]),
                    int(auth["user"]["id"]),
                    report_form["report_name"],
                    schedule_json["type"],
                    schedule_json["time"],
                    query_json["period"],
                    query_json["rank_target"],
                    render_json["output"],
                    channel_id,
                    channel_name,
                    Json(schedule_json),
                    Json(query_json),
                    Json(render_json),
                    "on",
                ),
            )
        connection.commit()

    _enqueue_report_scheduler_refresh(selected_guild_id, int(auth["user"]["id"]))
    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved=report",
        status_code=303,
    )


@app.post("/settings/reports/preview")
async def preview_report_setting(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return JSONResponse({"ok": False, "errors": ["로그인이 필요합니다."]}, status_code=401)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return JSONResponse({"ok": False, "errors": ["권한이 없습니다."]}, status_code=403)

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    errors: list[str] = []
    _, schedule_json, query_json, render_json, _, _ = _report_configs_from_form(
        form_data,
        errors,
        require_channel=False,
    )
    if errors:
        return JSONResponse({"ok": False, "errors": errors}, status_code=400)

    guild_name = str(auth.get("selected_server", {}).get("name") or "")
    preview = _render_report_preview(
        selected_guild_id,
        schedule_json,
        query_json,
        render_json,
        guild_name=guild_name,
    )
    return JSONResponse({"ok": True, "preview": preview})


@app.post("/settings/reports/status")
async def update_report_status(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return _settings_forbidden_redirect(selected_guild_id)

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    errors: list[str] = []
    status = _validate_report_option(
        "상태",
        form_data.get("status"),
        REPORT_STATUS_OPTIONS,
        errors,
    )
    try:
        report_setting_id = int(form_data.get("report_setting_id") or "")
    except ValueError:
        report_setting_id = 0
        errors.append("알람 설정 ID가 올바르지 않습니다.")

    if errors:
        return RedirectResponse(
            f"/settings?guild_id={selected_guild_id}&saved=report_error",
            status_code=303,
        )

    with database.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE scheduled_report_settings
                SET
                    status = %s,
                    next_run_at = CASE WHEN %s = 'on' THEN NULL ELSE next_run_at END,
                    updated_by_discord_id = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE report_setting_id = %s
                  AND guild_id = %s
                """,
                (
                    status,
                    status,
                    int(auth["user"]["id"]),
                    report_setting_id,
                    selected_guild_id,
                ),
            )
        connection.commit()

    _enqueue_report_scheduler_refresh(selected_guild_id, int(auth["user"]["id"]))
    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved=report_status",
        status_code=303,
    )


@app.post("/settings/reports/delete")
async def delete_report_setting(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return _settings_forbidden_redirect(selected_guild_id)

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    try:
        report_setting_id = int(form_data.get("report_setting_id") or "")
    except ValueError:
        report_setting_id = 0

    if report_setting_id <= 0:
        return RedirectResponse(
            f"/settings?guild_id={selected_guild_id}&saved=report_error",
            status_code=303,
        )

    with database.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE scheduled_report_settings
                SET
                    status = 'delete',
                    next_run_at = NULL,
                    updated_by_discord_id = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE report_setting_id = %s
                  AND guild_id = %s
                """,
                (
                    int(auth["user"]["id"]),
                    report_setting_id,
                    selected_guild_id,
                ),
            )
        connection.commit()

    _enqueue_report_scheduler_refresh(selected_guild_id, int(auth["user"]["id"]))
    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved=report_deleted",
        status_code=303,
    )


@app.get("/logs", response_class=HTMLResponse)
def logs(
    request: Request,
    guild_id: str | None = None,
    category: str = "all",
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _is_developer_auth(auth):
        return RedirectResponse(
            f"/attendance?guild_id={selected_guild_id}",
            status_code=303,
        )

    selected_category = (
        category
        if category in {str(tab["value"]) for tab in LOG_TABS}
        else "all"
    )
    commands = _latest_command_queue(selected_guild_id, limit=80)
    if selected_category != "all":
        commands = [
            command
            for command in commands
            if command["category"] == selected_category
        ]

    return _render(
        request,
        "logs.html",
        {
            "auth": auth,
            "commands": commands,
            "log_tabs": LOG_TABS,
            "selected_category": selected_category,
            "active_page": "logs",
        },
    )


@app.get("/work-logs", response_class=HTMLResponse)
def work_logs(
    request: Request,
    guild_id: str | None = None,
    action: str = "all",
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_owner_manage_selected_server(auth):
        return RedirectResponse(
            f"/attendance?guild_id={selected_guild_id}",
            status_code=303,
        )

    allowed_actions = {str(tab["value"]) for tab in WORK_LOG_TABS}
    selected_action = action if action in allowed_actions else "all"
    logs = database.get_work_logs(
        selected_guild_id,
        action_type=None if selected_action == "all" else selected_action,
        limit=160,
    )
    for row in logs:
        row["action_label"] = _work_log_action_label(str(row["action_type"]))

    return _render(
        request,
        "work_logs.html",
        {
            "auth": auth,
            "work_logs": logs,
            "work_log_tabs": WORK_LOG_TABS,
            "selected_action": selected_action,
            "active_page": "work_logs",
        },
    )


@app.get("/developer/servers", response_class=HTMLResponse)
def developer_servers(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _is_developer_auth(auth):
        return RedirectResponse(
            f"/attendance?guild_id={selected_guild_id}",
            status_code=303,
        )

    rows = []
    for row in database.get_developer_guild_rows():
        server_guild_id = int(row["guild_id"])
        rows.append(
            {
                "guild_id": str(server_guild_id),
                "name": _discord_bot_guild_name(server_guild_id)
                or f"Discord 서버 {server_guild_id}",
                "is_enabled": bool(row["is_enabled"]),
                "session_count": int(row["session_count"] or 0),
                "attendance_count": int(row["attendance_count"] or 0),
                "last_started_at": row["last_attendance_at"]
                or row["last_session_started_at"]
                or "",
                "has_settings": any(
                    row.get(column) is not None
                    for column in (
                        "admin_channel_id",
                        "attendance_voice_channel_id",
                        "log_channel_id",
                    )
                ),
            }
        )

    return _render(
        request,
        "developer_servers.html",
        {
            "auth": auth,
            "servers": rows,
            "active_page": "developer_servers",
        },
    )


@app.post("/developer/servers/status")
async def update_developer_server_status(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _is_developer_auth(auth):
        return RedirectResponse(
            f"/attendance?guild_id={selected_guild_id}",
            status_code=303,
        )

    form_data = await _urlencoded_form_data(request)
    try:
        target_guild_id = int(form_data.get("target_guild_id") or "")
        status = str(form_data.get("status") or "")
        if target_guild_id <= 0 or status not in {"enabled", "disabled"}:
            raise ValueError
        database.set_guild_enabled(target_guild_id, status == "enabled")
    except Exception:
        pass
    return RedirectResponse(
        f"/developer/servers?guild_id={selected_guild_id}",
        status_code=303,
    )


@app.post("/attendance/start")
async def start_attendance(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return RedirectResponse(
            f"/attendance?guild_id={selected_guild_id}&queued=forbidden",
            status_code=303,
        )

    user_id = int(auth["user"]["id"])
    if _pending_attendance_command(selected_guild_id) is not None:
        return RedirectResponse(
            f"/attendance?guild_id={selected_guild_id}&queued=busy",
            status_code=303,
        )

    result = await _send_attendance_bot_command(
        selected_guild_id,
        "attendance.start",
        user_id,
    )
    if not result.get("ok"):
        status = "busy" if result.get("busy") else "bot_offline"
        return RedirectResponse(
            f"/attendance?guild_id={selected_guild_id}&queued={status}",
            status_code=303,
        )
    return RedirectResponse(
        f"/attendance?guild_id={selected_guild_id}&queued=start",
        status_code=303,
    )


@app.post("/attendance/stop")
async def stop_attendance(
    request: Request,
    guild_id: str | None = None,
    save: str | None = "1",
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
        return RedirectResponse(
            f"/attendance?guild_id={selected_guild_id}&queued=forbidden",
            status_code=303,
        )

    user_id = int(auth["user"]["id"])
    if _pending_attendance_command(selected_guild_id) is not None:
        return RedirectResponse(
            f"/attendance?guild_id={selected_guild_id}&queued=busy",
            status_code=303,
        )

    should_save = str(save or "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    result = await _send_attendance_bot_command(
        selected_guild_id,
        "attendance.stop",
        user_id,
        {"save_attendance": should_save},
    )
    if not result.get("ok"):
        status = "busy" if result.get("busy") else "bot_offline"
        return RedirectResponse(
            f"/attendance?guild_id={selected_guild_id}&queued={status}",
            status_code=303,
        )
    queued_status = "stop_saved" if should_save else "stop_skipped"
    return RedirectResponse(
        f"/attendance?guild_id={selected_guild_id}&queued={queued_status}",
        status_code=303,
    )


@app.websocket("/internal/bot/ws")
async def bot_bridge_websocket(websocket: WebSocket) -> None:
    token = websocket.query_params.get("token") or websocket.headers.get(
        "x-internal-token"
    )
    if not BOT_BRIDGE_TOKEN or not secrets.compare_digest(token or "", BOT_BRIDGE_TOKEN):
        await websocket.close(code=1008)
        return

    await websocket.accept()
    global _BOT_BRIDGE_WEBSOCKET
    async with _BOT_BRIDGE_LOCK:
        previous_websocket = _BOT_BRIDGE_WEBSOCKET
        _BOT_BRIDGE_WEBSOCKET = websocket
        if previous_websocket is not None and previous_websocket is not websocket:
            try:
                await previous_websocket.close(code=1012)
            except Exception:
                pass

    try:
        while True:
            message = await websocket.receive_json()
            await _handle_bot_bridge_message(message)
    except WebSocketDisconnect:
        return
    finally:
        async with _BOT_BRIDGE_LOCK:
            if _BOT_BRIDGE_WEBSOCKET is websocket:
                _BOT_BRIDGE_WEBSOCKET = None


async def _handle_bot_bridge_message(message: dict[str, Any]) -> None:
    if not isinstance(message, dict):
        return

    message_type = str(message.get("type") or "")
    guild_id = _parse_optional_int(message.get("guild_id"))
    if message_type == "attendance.state" and guild_id is not None:
        state = message.get("state")
        if isinstance(state, dict):
            _ATTENDANCE_STATE_CACHE[guild_id] = state
            await _broadcast_live_attendance_state(guild_id, state)
        return

    if message_type == "attendance.command_result":
        request_id = str(message.get("request_id") or "")
        if guild_id is not None:
            state = message.get("state")
            if isinstance(state, dict):
                _ATTENDANCE_STATE_CACHE[guild_id] = state
                await _broadcast_live_attendance_state(guild_id, state)

        waiter = _BOT_BRIDGE_WAITERS.get(request_id)
        if waiter is not None and not waiter.done():
            waiter.set_result(message)
        return


@app.websocket("/ws/attendance/{guild_id}")
async def attendance_websocket(websocket: WebSocket, guild_id: str) -> None:
    auth = _auth_context_from_session(websocket.session, guild_id)
    if not auth or auth["selected_guild_id"] != str(guild_id):
        await websocket.close(code=1008)
        return

    await websocket.accept()
    selected_guild_id = int(auth["selected_guild_id"])
    clients = _ATTENDANCE_BROWSER_CLIENTS.setdefault(selected_guild_id, set())
    clients.add(websocket)
    await websocket.send_json(_live_attendance_state(selected_guild_id))
    await _send_bot_bridge_message(
        {
            "type": "attendance.subscribe",
            "guild_id": str(selected_guild_id),
            "request_id": secrets.token_urlsafe(8),
        }
    )

    try:
        while True:
            message = await websocket.receive_json()
            await _handle_attendance_browser_message(
                websocket,
                auth,
                selected_guild_id,
                message,
            )
    except WebSocketDisconnect:
        return
    finally:
        clients.discard(websocket)


async def _handle_attendance_browser_message(
    websocket: WebSocket,
    auth: dict[str, Any],
    guild_id: int,
    message: dict[str, Any],
) -> None:
    if not isinstance(message, dict):
        return

    message_type = str(message.get("type") or "")
    if message_type not in {"attendance.start", "attendance.stop"}:
        return

    if not _can_manage_selected_server(auth):
        await websocket.send_json(
            {
                "type": "attendance.command_result",
                "ok": False,
                "message": "이 서버의 관리자 권한이 있는 계정만 출석을 제어할 수 있습니다.",
                "state": _live_attendance_state(guild_id),
            }
        )
        return

    user_id = int(auth["user"]["id"])
    command_payload: dict[str, Any] = {}
    if message_type == "attendance.stop":
        command_payload["save_attendance"] = bool(
            message.get("save_attendance", True)
        )

    result = await _send_attendance_bot_command(
        guild_id,
        message_type,
        user_id,
        command_payload,
    )
    await websocket.send_json(
        {
            "type": "attendance.command_result",
            **result,
            "state": result.get("state") or _live_attendance_state(guild_id),
        }
    )
