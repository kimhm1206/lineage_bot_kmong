from __future__ import annotations

import os
import secrets
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from psycopg2.extras import Json
from starlette.middleware.sessions import SessionMiddleware

import db


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

app = FastAPI(title="Lineage Ops Web")
app.add_middleware(
    SessionMiddleware,
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


def _load_accessible_servers(discord_guilds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    guild_lookup = {
        str(guild["id"]): guild
        for guild in discord_guilds
        if str(guild.get("id", "")).isdigit()
    }
    guild_ids = [int(guild_id) for guild_id in guild_lookup]
    if not guild_ids:
        return []

    rows = db._fetchall(
        """
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
        WHERE g.guild_id = ANY(%s::bigint[])
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
        (guild_ids,),
    )

    servers: list[dict[str, Any]] = []
    for row in rows:
        guild_id = str(row["guild_id"])
        discord_guild = guild_lookup.get(guild_id, {})
        session_count = int(row["session_count"] or 0)
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
                "name": str(discord_guild.get("name") or f"Discord 서버 {guild_id}"),
                "session_count": session_count,
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


def _auth_context(
    request: Request,
    guild_id: str | None = None,
) -> dict[str, Any] | None:
    user = request.session.get("discord_user")
    servers = request.session.get("servers") or []
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


def _auth_redirect() -> RedirectResponse:
    return RedirectResponse("/login", status_code=303)


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
    rows = db._fetchall(
        """
        SELECT
            s.attendance_id,
            s.started_at,
            s.ended_at,
            s.started_by_discord_id,
            COUNT(e.user_id) AS participant_count
        FROM attendance_sessions s
        LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
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
            "participant_count": int(row["participant_count"] or 0),
        }
        for row in rows
    ]


def _latest_command_queue(guild_id: int, limit: int = 10) -> list[dict[str, Any]]:
    rows = db._fetchall(
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
    return [
        {
            "command_id": int(row["command_id"]),
            "command_type": str(row["command_type"]),
            "status": str(row["status"]),
            "requested_by_discord_id": row["requested_by_discord_id"],
            "created_at": row["created_at"],
            "processed_at": row["processed_at"],
            "error_message": row["error_message"] or "",
        }
        for row in rows
    ]


def _enqueue_attendance_command(
    guild_id: int,
    command_type: str,
    requested_by_discord_id: int,
) -> None:
    with db._connect() as connection:
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
        return RedirectResponse("/dashboard", status_code=303)
    return RedirectResponse("/login", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login(request: Request):
    if _auth_context(request):
        return RedirectResponse("/dashboard", status_code=303)
    return _render(
        request,
        "login.html",
        {
            "config_ready": _oauth_ready(),
            "redirect_uri": DISCORD_REDIRECT_URI,
        },
    )


@app.get("/auth/discord/login")
def discord_login(request: Request):
    if not _oauth_ready():
        return _render(
            request,
            "login.html",
            {
                "config_ready": False,
                "redirect_uri": DISCORD_REDIRECT_URI,
                "error_message": "Discord OAuth 환경변수가 아직 설정되지 않았습니다.",
            },
            status_code=500,
        )
    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = state
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
        servers = _load_accessible_servers(discord_guilds)
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
    return RedirectResponse("/dashboard", status_code=303)


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
        return _auth_redirect()

    selected_guild_id = int(auth["selected_guild_id"])
    overview = db.get_attendance_overview(selected_guild_id)
    daily_stats = db.get_daily_attendance_stats(selected_guild_id)[:14]
    top_users = db.get_user_attendance_stats(selected_guild_id, limit=10)
    alliance_stats = db.get_alliance_attendance_stats(selected_guild_id)

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
        return _auth_redirect()

    selected_guild_id = int(auth["selected_guild_id"])
    return _render(
        request,
        "attendance.html",
        {
            "auth": auth,
            "sessions": _latest_attendance_sessions(selected_guild_id),
            "commands": _latest_command_queue(selected_guild_id),
            "queued": queued,
            "active_page": "attendance",
        },
    )


@app.post("/attendance/start")
def start_attendance(
    request: Request,
    guild_id: str | None = None,
):
    auth = _auth_context(request, guild_id)
    if not auth:
        return _auth_redirect()

    selected_guild_id = int(auth["selected_guild_id"])
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
        return _auth_redirect()

    selected_guild_id = int(auth["selected_guild_id"])
    user_id = int(auth["user"]["id"])
    _enqueue_attendance_command(selected_guild_id, "stop_attendance", user_id)
    return RedirectResponse(
        f"/attendance?guild_id={selected_guild_id}&queued=stop",
        status_code=303,
    )
