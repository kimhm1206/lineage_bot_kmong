from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
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


@app.on_event("startup")
def initialize_database_schema() -> None:
    database.init_schema()


def _oauth_ready() -> bool:
    return bool(DISCORD_CLIENT_ID and DISCORD_CLIENT_SECRET)


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

    alliance_name = (
        str(alliance_options[0]["alliance_name"]) if alliance_options else ""
    )
    if server_role == "admin":
        return f"{alliance_name} admin" if alliance_name else "admin"
    return alliance_name or "user"


def _my_alliance_access(
    guild_id: int,
    discord_user_id: str,
    server_role: str,
) -> dict[str, Any]:
    if server_role == "developer":
        options = _load_active_alliances()
    else:
        options = _member_alliance_options(guild_id, discord_user_id)

    return {
        "can_view": bool(options),
        "can_select": len(options) > 1,
        "options": options,
    }


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

    where_clause = (
        "TRUE"
        if is_global_developer or can_verify_with_bot
        else "g.guild_id = ANY(%s::bigint[])"
    )
    params: tuple[Any, ...] = (
        () if is_global_developer or can_verify_with_bot else (candidate_guild_ids,)
    )

    rows = database.fetchall(
        f"""
        SELECT
            g.guild_id,
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
                "permissions": _discord_permissions(discord_guild),
                "discord_owner": bool((discord_guild or {}).get("owner")),
                "role": role,
                "can_manage": role in {"admin", "developer"},
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


def _auth_context_from_session(
    session: dict[str, Any],
    guild_id: str | None = None,
) -> dict[str, Any] | None:
    user = session.get("discord_user")
    servers = session.get("servers") or []
    if not user or not servers:
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
    selected_server["role"] = verified_role
    selected_server["can_manage"] = verified_role in {"admin", "developer"}
    selected_server["member_display_name"] = _guild_member_display_name(
        int(selected_guild_id),
        str(user["id"]),
        str(user.get("display_name") or user.get("username") or user["id"]),
    )
    selected_server["my_alliance"] = _my_alliance_access(
        int(selected_guild_id),
        str(user["id"]),
        verified_role,
    )
    selected_server["role_label"] = _role_display_label(
        verified_role,
        selected_server["my_alliance"]["options"],
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
    }


def _auth_context(
    request: Request,
    guild_id: str | None = None,
) -> dict[str, Any] | None:
    return _auth_context_from_session(request.session, guild_id)


def _wants_json(request: Request) -> bool:
    return (
        request.headers.get("x-requested-with") == "fetch"
        or "application/json" in request.headers.get("accept", "")
    )


def _can_manage_selected_server(auth: dict[str, Any]) -> bool:
    return bool(auth["selected_server"].get("can_manage"))


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
    return templates.TemplateResponse(
        request,
        template_name,
        context or {},
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
    alliance_id: int,
) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("alliance_id") == alliance_id]


def _alliance_overview(
    guild_id: int,
    alliance_id: int,
    start_at: str | None,
    end_at: str | None,
) -> dict[str, Any]:
    total_sessions = _count_period_sessions(guild_id, start_at, end_at)
    rows = _filter_alliance_rows(
        _alliance_attendance_member_rows(guild_id, start_at, end_at),
        alliance_id,
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
    alliance_id: int,
    start_at: str | None,
    end_at: str | None,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    total_sessions = _count_period_sessions(guild_id, start_at, end_at)
    rows = _filter_alliance_rows(
        _alliance_attendance_member_rows(guild_id, start_at, end_at),
        alliance_id,
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
    alliance_id: int,
    start_at: str | None,
    end_at: str | None,
) -> list[dict[str, Any]]:
    rows = _filter_alliance_rows(
        _alliance_attendance_member_rows(guild_id, start_at, end_at),
        alliance_id,
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
    alliance_id: int,
    start_at: str | None,
    end_at: str | None,
) -> list[dict[str, Any]]:
    rows = _filter_alliance_rows(
        _alliance_attendance_member_rows(guild_id, start_at, end_at),
        alliance_id,
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
    alliance_id: int | None = None,
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
        "item_price_form": item_price_form or _default_item_price_form(),
        "item_prices": _decorate_item_prices(database.get_item_price_settings(guild_id)),
        "settings_active_tab": settings_active_tab or _settings_active_tab(saved),
        "active_page": "settings",
    }


def _settings_active_tab(saved: str | None) -> str:
    if saved in {"report", "report_status", "report_deleted", "report_error"}:
        return "reports"
    if saved in {"alliance_role", "alliance_role_deleted", "alliance_role_error"}:
        return "alliance"
    if saved in {"item_price", "item_price_deleted", "item_price_error"}:
        return "items"
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


def _money_text(value: Any, places: int = 2) -> str:
    decimal_value = Decimal(str(value or "0"))
    quantizer = Decimal("1") if places == 0 else Decimal(f"0.{'0' * (places - 1)}1")
    rounded = decimal_value.quantize(quantizer)
    if rounded == rounded.to_integral():
        return f"{int(rounded):,}"
    return f"{rounded:,.{places}f}".rstrip("0").rstrip(".")


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
        "머니 시세",
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
        errors.append("원화 시세를 입력한 경우 머니 시세도 입력해주세요.")

    sale_price = _decimal_from_form(
        "게임머니 판매금",
        form_data.get("sale_price"),
        errors,
    )
    return cash_price, sale_price, adena_rate


def _decorate_item_prices(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    decorated = []
    for item in items:
        row = dict(item)
        row["default_price_input"] = _decimal_input(item.get("default_price"))
        row["default_price_text"] = _money_text(item.get("default_price"))
        decorated.append(row)
    return decorated


def _decorate_loot_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    decorated_events = []
    for event in events:
        row = dict(event)
        row["cash_price_input"] = _decimal_input(event.get("cash_price_krw"))
        row["cash_price_text"] = _money_text(event.get("cash_price_krw"), places=0)
        row["sale_price_input"] = _decimal_input(event.get("sale_price"))
        row["sale_price_text"] = _money_text(event.get("sale_price"))
        row["adena_rate_input"] = _decimal_input(event.get("adena_rate"))
        row["adena_rate_text"] = _money_text(event.get("adena_rate"), places=6)
        row["per_member_text"] = _money_text(event.get("per_member_amount"))
        row["total_sale_text"] = _money_text(event.get("total_sale_amount"))
        row["total_net_text"] = _money_text(event.get("total_net_amount"))
        row["fee_amount_text"] = _money_text(event.get("fee_amount"))
        row["fee_rate_percent_text"] = _money_text(
            Decimal(str(event.get("fee_rate") or "0")) * Decimal("100"),
        )
        row["converted_text"] = _money_text(
            Decimal(str(event.get("total_net_amount") or "0"))
            * Decimal(str(event.get("adena_rate") or "0")),
            places=2,
        )
        payouts = []
        for payout in event.get("alliance_payouts", []):
            payout_row = dict(payout)
            payout_row["net_amount_text"] = _money_text(payout.get("net_amount"))
            payout_row["per_member_text"] = _money_text(payout.get("per_member_amount"))
            payout_row["status_label"] = (
                "분배완료" if payout.get("payout_status") == "paid" else "미완료"
            )
            payout_row["next_status"] = (
                "unpaid" if payout.get("payout_status") == "paid" else "paid"
            )
            payout_row["next_status_label"] = (
                "미완료로 변경" if payout.get("payout_status") == "paid" else "완료 처리"
            )
            payouts.append(payout_row)
        row["alliance_payouts"] = payouts
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
        buyer_options = []
        for alliance in event.get("alliances", []):
            alliance_name = str(alliance.get("alliance_name") or "미분류")
            for member in alliance.get("members", []):
                member_name = str(member).strip()
                if member_name:
                    buyer_options.append(
                        {
                            "name": member_name,
                            "alliance": alliance_name,
                        }
                    )
        row["buyer_options"] = buyer_options
        decorated_events.append(row)
    return decorated_events


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
        "buyer_name": "",
        "memo": "",
    }


def _loot_form_from_values(values: dict[str, Any], guild_id: int) -> dict[str, str]:
    form = _default_loot_form(guild_id)
    for key in form:
        if key in values:
            form[key] = str(values.get(key) or "")
    return form


def _loot_template_context(
    auth: dict[str, Any],
    guild_id: int,
    *,
    saved: str | None,
    errors: list[str],
    loot_form: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "auth": auth,
        "saved": saved,
        "errors": errors,
        "loot_form": loot_form or _default_loot_form(guild_id),
        "attendance_options": _loot_attendance_options(guild_id),
        "item_prices": _decorate_item_prices(database.get_item_price_settings(guild_id)),
        "loot_events": _decorate_loot_events(database.get_loot_drop_events(guild_id)),
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

    filtered_rows = database.get_attendance_export_rows(
        selected_guild_id,
        start_at,
        end_at,
        search_value or None,
        alliance_value or None,
    )
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
            alliance_value or None,
        )
    top_users = database.get_user_attendance_stats(
        selected_guild_id,
        start_at,
        end_at,
        search_value or None,
        alliance_value or None,
        limit_value,
    )
    recent_sessions = database.get_attendance_status_sessions(selected_guild_id, 8, 0)
    alliance_options = database.get_alliance_names()

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
    rows = database.get_attendance_export_rows(
        selected_guild_id,
        start_at,
        end_at,
        search_value or None,
        alliance_value or None,
    )

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
    alliance_id: int | None = None,
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

    allowed_by_id = {int(option["alliance_id"]): option for option in alliance_options}
    selected_alliance_id = (
        int(alliance_id)
        if alliance_id is not None and int(alliance_id) in allowed_by_id
        else int(alliance_options[0]["alliance_id"])
    )
    selected_alliance = allowed_by_id[selected_alliance_id]
    start_at, end_at, start_value, end_value = _date_bounds(start_date, end_date)

    overview = _alliance_overview(
        selected_guild_id,
        selected_alliance_id,
        start_at,
        end_at,
    )
    user_rankings = _alliance_user_rankings(
        selected_guild_id,
        selected_alliance_id,
        start_at,
        end_at,
        limit=200,
    )
    hour_stats = _alliance_hour_stats(
        selected_guild_id,
        selected_alliance_id,
        start_at,
        end_at,
    )
    daily_rows = _alliance_daily_rows(
        selected_guild_id,
        selected_alliance_id,
        start_at,
        end_at,
    )
    week_start_at, week_end_at = _current_week_bounds()
    month_start_at, month_end_at = _current_month_bounds()
    weekly_rankings = _alliance_user_rankings(
        selected_guild_id,
        selected_alliance_id,
        week_start_at,
        week_end_at,
        limit=10,
    )
    monthly_rankings = _alliance_user_rankings(
        selected_guild_id,
        selected_alliance_id,
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
        "alliance_id": selected_alliance_id,
        "quick": [
            {
                "label": "전체",
                "href": _my_alliance_url(
                    selected_guild_id,
                    alliance_id=selected_alliance_id,
                ),
                "active": not start_value and not end_value,
            },
            {
                "label": "이번 주",
                "href": _my_alliance_url(
                    selected_guild_id,
                    alliance_id=selected_alliance_id,
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
                    alliance_id=selected_alliance_id,
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
    return _render(
        request,
        "status.html",
        {
            "auth": auth,
            "sessions": sessions,
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


@app.get("/loot", response_class=HTMLResponse)
def loot_drops(
    request: Request,
    guild_id: str | None = None,
    saved: str | None = None,
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
    if not _can_manage_selected_server(auth):
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
        errors.append("설정에서 등록된 아이템을 선택해주세요.")

    item_prices = database.get_item_price_settings(selected_guild_id)
    cash_price_krw, sale_price, adena_rate = _loot_prices_from_form(
        form_data,
        item_prices,
        item_id,
        errors,
    )
    form_data["cash_price_krw"] = _decimal_input(cash_price_krw)
    form_data["sale_price"] = _decimal_input(sale_price)

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
        database.create_loot_drop(
            selected_guild_id,
            attendance_id=attendance_id,
            item_id=item_id,
            item_name="",
            cash_price_krw=cash_price_krw,
            sale_price=sale_price,
            adena_rate=adena_rate,
            buyer_name=str(form_data.get("buyer_name") or ""),
            memo=str(form_data.get("memo") or ""),
            created_by_discord_id=int(auth["user"]["id"]),
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


@app.post("/loot/update")
async def update_loot_drop(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    if not _can_manage_selected_server(auth):
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
    if errors:
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=error#payouts",
            status_code=303,
        )

    try:
        database.update_loot_drop(
            selected_guild_id,
            loot_event_id,
            cash_price_krw=cash_price_krw,
            sale_price=sale_price,
            adena_rate=adena_rate,
            buyer_name=str(form_data.get("buyer_name") or ""),
            memo=str(form_data.get("memo") or ""),
        )
    except ValueError:
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=error#payouts",
            status_code=303,
        )
    return RedirectResponse(
        f"/loot?guild_id={selected_guild_id}&saved=updated#payouts",
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
            f"/loot?guild_id={selected_guild_id}&saved=forbidden#payouts",
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
            f"/loot?guild_id={selected_guild_id}&saved=error#payouts",
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
            f"/loot?guild_id={selected_guild_id}&saved=error#payouts",
            status_code=303,
        )
    if _wants_json(request):
        return JSONResponse({"ok": True, **payload})
    return RedirectResponse(
        f"/loot?guild_id={selected_guild_id}&saved=payout#payouts",
        status_code=303,
    )


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
    if not _can_manage_selected_server(auth):
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "관리자 권한이 필요합니다."},
                status_code=403,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=forbidden#payouts",
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
        database.delete_loot_drop(selected_guild_id, loot_event_id)
    except ValueError:
        if _wants_json(request):
            return JSONResponse(
                {"ok": False, "message": "드랍 기록을 삭제하지 못했습니다."},
                status_code=400,
            )
        return RedirectResponse(
            f"/loot?guild_id={selected_guild_id}&saved=error#payouts",
            status_code=303,
        )

    if _wants_json(request):
        return JSONResponse({"ok": True, "loot_event_id": str(loot_event_id)})
    return RedirectResponse(
        f"/loot?guild_id={selected_guild_id}&saved=deleted#payouts",
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


@app.post("/settings/items", response_class=HTMLResponse)
async def create_item_price(
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
    item_name = str(form_data.get("item_name") or "").strip()
    if not item_name:
        errors.append("아이템 이름을 입력해주세요.")
    default_price = _decimal_from_form(
        "원화 시세",
        form_data.get("default_price"),
        errors,
    )
    if errors:
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

    database.upsert_item_price(
        selected_guild_id,
        item_name=item_name,
        default_price=default_price,
        category="",
        memo="",
        is_bid_item=True,
    )
    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved=item_price",
        status_code=303,
    )


@app.post("/settings/items/update")
async def update_item_price(
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
        return RedirectResponse(
            f"/settings?guild_id={selected_guild_id}&saved=item_price_error",
            status_code=303,
        )

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
    except ValueError:
        return RedirectResponse(
            f"/settings?guild_id={selected_guild_id}&saved=item_price_error",
            status_code=303,
        )
    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved=item_price",
        status_code=303,
    )


@app.post("/settings/items/delete")
async def delete_item_price(
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
        item_id = int(form_data.get("item_id") or "")
    except ValueError:
        item_id = 0
    if item_id <= 0:
        return RedirectResponse(
            f"/settings?guild_id={selected_guild_id}&saved=item_price_error",
            status_code=303,
        )
    database.deactivate_item_price(selected_guild_id, item_id)
    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved=item_price_deleted",
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
