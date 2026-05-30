from __future__ import annotations

import asyncio
import os
import secrets
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from psycopg2.extras import Json

from common import database
from web.session import RememberMeSessionMiddleware


load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DISCORD_API_BASE = "https://discord.com/api/v10"
DISCORD_REDIRECT_URI = os.getenv(
    "DISCORD_REDIRECT_URI",
    "http://localhost:8000/auth/discord/callback",
)
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID") or os.getenv(
    "DISCORD_OAUTH_CLIENT_ID", ""
)
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET") or os.getenv(
    "DISCORD_OAUTH_CLIENT_SECRET", ""
)
SESSION_SECRET = os.getenv("WEB_SESSION_SECRET", "lineage-local-web-session")
GLOBAL_OWNER_DISCORD_ID = os.getenv("GLOBAL_OWNER_DISCORD_ID") or "238978205078388747"
DISCORD_ADMINISTRATOR_PERMISSION = 0x8
LOG_TABS = (
    {"value": "all", "label": "전체"},
    {"value": "attendance", "label": "출석"},
    {"value": "statistics", "label": "통계"},
    {"value": "settings", "label": "설정"},
    {"value": "logs", "label": "로그"},
)

app = FastAPI(title="Lineage Ops Web")
app.add_middleware(
    RememberMeSessionMiddleware,
    secret_key=SESSION_SECRET,
    same_site="lax",
    https_only=False,
)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")


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
    if str(discord_user_id) == GLOBAL_OWNER_DISCORD_ID:
        return "owner"
    if discord_guild and (
        bool(discord_guild.get("owner"))
        or bool(_discord_permissions(discord_guild) & DISCORD_ADMINISTRATOR_PERMISSION)
    ):
        return "admin"
    return "user"


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
    is_global_owner = str(discord_user_id) == GLOBAL_OWNER_DISCORD_ID
    if not guild_ids and not is_global_owner:
        return []

    where_clause = "TRUE" if is_global_owner else "g.guild_id = ANY(%s::bigint[])"
    params: tuple[Any, ...] = () if is_global_owner else (guild_ids,)

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
        role = _server_role(discord_user_id, discord_guild)
        has_settings = any(
            row.get(column) is not None
            for column in (
                "admin_channel_id",
                "attendance_voice_channel_id",
                "log_channel_id",
            )
        )
        servers.append(
            {
                "guild_id": guild_id,
                "name": str(
                    (discord_guild or {}).get("name") or f"Discord 서버 {guild_id}"
                ),
                "role": role,
                "can_manage": role in {"admin", "owner"},
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

    return {
        "user": user,
        "servers": servers,
        "selected_guild_id": selected_guild_id,
        "selected_server": allowed_servers[selected_guild_id],
    }


def _auth_context(
    request: Request,
    guild_id: str | None = None,
) -> dict[str, Any] | None:
    return _auth_context_from_session(request.session, guild_id)


def _can_manage_selected_server(auth: dict[str, Any]) -> bool:
    return bool(auth["selected_server"].get("can_manage"))


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


def _command_category(command_type: str) -> str:
    normalized = command_type.lower()
    if "attendance" in normalized:
        return "attendance"
    if "stat" in normalized or "report" in normalized:
        return "statistics"
    if "setting" in normalized or "config" in normalized:
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


def _live_attendance_state(guild_id: int) -> dict[str, Any]:
    state = database.get_live_attendance_state(guild_id)
    participants = state.get("participants") or []
    session = state.get("session") or {}
    return {
        "active": bool(state.get("active")),
        "participant_count": len(participants),
        "session": {
            "live_session_id": session.get("live_session_id"),
            "started_at": session.get("started_at") or "",
            "expires_at": session.get("expires_at") or "",
            "started_by_discord_id": session.get("started_by_discord_id"),
            "discord_channel_id": session.get("discord_channel_id"),
            "discord_message_id": session.get("discord_message_id"),
            "status": session.get("status") or "idle",
        }
        if session
        else None,
        "participants": participants,
    }


def _enqueue_attendance_command(
    guild_id: int,
    command_type: str,
    requested_by_discord_id: int,
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
                    Json({"source": "web"}),
                    requested_by_discord_id,
                ),
            )
        connection.commit()


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
        discord_guilds = _discord_get("/users/@me/guilds", access_token)
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
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect(request)

    selected_guild_id = int(auth["selected_guild_id"])
    overview = database.get_attendance_overview(selected_guild_id)
    daily_stats = database.get_daily_attendance_stats(selected_guild_id)[:14]
    top_users = database.get_user_attendance_stats(selected_guild_id, limit=10)
    alliance_stats = database.get_alliance_attendance_stats(selected_guild_id)

    return _render(
        request,
        "dashboard.html",
        {
            "auth": auth,
            "overview": overview,
            "daily_stats": daily_stats,
            "top_users": top_users,
            "alliance_stats": alliance_stats,
            "active_page": "dashboard",
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
            "queued": queued,
            "active_page": "attendance",
        },
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
def start_attendance(
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
    _enqueue_attendance_command(selected_guild_id, "start_attendance", user_id)
    return RedirectResponse(
        f"/attendance?guild_id={selected_guild_id}&queued=start",
        status_code=303,
    )


@app.post("/attendance/stop")
def stop_attendance(
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
    _enqueue_attendance_command(selected_guild_id, "stop_attendance", user_id)
    return RedirectResponse(
        f"/attendance?guild_id={selected_guild_id}&queued=stop",
        status_code=303,
    )


@app.websocket("/ws/attendance/{guild_id}")
async def attendance_websocket(websocket: WebSocket, guild_id: str) -> None:
    auth = _auth_context_from_session(websocket.session, guild_id)
    if not auth or auth["selected_guild_id"] != str(guild_id):
        await websocket.close(code=1008)
        return

    await websocket.accept()
    selected_guild_id = int(auth["selected_guild_id"])
    try:
        while True:
            await websocket.send_json(_live_attendance_state(selected_guild_id))
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        return
