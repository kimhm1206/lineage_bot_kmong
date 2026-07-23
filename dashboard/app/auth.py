from __future__ import annotations

import ipaddress
import secrets
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from dashboard.app.config import BASE_DIR, get_settings
from dashboard.app.database import SessionLocal


router = APIRouter(tags=["auth"])
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))
PUBLIC_PREFIXES = (
    "/static",
    "/login",
    "/auth/discord",
    "/health",
    "/docs",
    "/redoc",
    "/openapi.json",
)
ROLE_PRIORITY = {
    "user": 0,
    "clan_accountant": 1,
    "clan_manager": 2,
    "alliance_manager": 3,
    "owner": 4,
    "developer": 5,
}
SCOPE_ROLES = {1: "alliance_manager", 2: "clan_manager", 3: "clan_accountant"}
GLOBAL_DEVELOPER_DISCORD_ID = 238978205078388747


def _safe_next(value: str | None) -> str:
    path = str(value or "").strip()
    if not path.startswith("/") or path.startswith("//"):
        return "/"
    return path


def _is_loopback(request: Request) -> bool:
    host = request.client.host if request.client else ""
    if host in {"localhost", "127.0.0.1", "::1"}:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _wants_json(request: Request) -> bool:
    return request.url.path.startswith("/api/") or "application/json" in request.headers.get(
        "accept", ""
    )


async def _enabled_guild_access(
    discord_user_id: int,
    oauth_guild_ids: set[int],
) -> tuple[list[dict[str, Any]], dict[int, str], dict[int, tuple[int, ...]]]:
    is_developer = discord_user_id == GLOBAL_DEVELOPER_DISCORD_ID
    async with SessionLocal() as session:
        guild_rows = (
            await session.execute(
                text("""
                    SELECT guild_id, guild_name, owner_discord_id
                    FROM guilds
                    WHERE is_enabled IS TRUE
                    ORDER BY COALESCE(guild_name, guild_id::TEXT)
                """)
            )
        ).mappings().all()
        visible = [
            dict(row)
            for row in guild_rows
            if is_developer or int(row["guild_id"]) in oauth_guild_ids
        ]
        if not visible:
            return [], {}, {}
        guild_ids = [int(row["guild_id"]) for row in visible]
        assignments = (
            await session.execute(
                text("""
                    SELECT guild_id, scope_code
                    FROM guild_user_assignments
                    WHERE discord_user_id = :discord_user_id
                      AND guild_id = ANY(:guild_ids)
                """),
                {"discord_user_id": discord_user_id, "guild_ids": guild_ids},
            )
        ).mappings().all()

    roles = {guild_id: "user" for guild_id in guild_ids}
    scopes: dict[int, set[int]] = {guild_id: set() for guild_id in guild_ids}
    for row in visible:
        guild_id = int(row["guild_id"])
        if is_developer:
            roles[guild_id] = "developer"
            scopes[guild_id] = {1, 2, 3}
        elif row["owner_discord_id"] is not None and int(row["owner_discord_id"]) == discord_user_id:
            roles[guild_id] = "owner"
            scopes[guild_id] = {1, 2, 3}
    for assignment in assignments:
        guild_id = int(assignment["guild_id"])
        scope_code = int(assignment["scope_code"])
        scopes[guild_id].add(scope_code)
        role = SCOPE_ROLES.get(scope_code, "user")
        if ROLE_PRIORITY[role] > ROLE_PRIORITY[roles[guild_id]]:
            roles[guild_id] = role
    return visible, roles, {
        guild_id: tuple(sorted(scope_codes))
        for guild_id, scope_codes in scopes.items()
    }


class AuthContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        if request.url.path.startswith(PUBLIC_PREFIXES):
            return await call_next(request)

        settings = get_settings()
        local_bypass = (
            settings.auth_local_bypass
            and settings.environment.strip().lower() in {"local", "development", "test"}
            and _is_loopback(request)
        )
        session_user = request.session.get("discord_user") or {}
        if local_bypass:
            discord_user_id = GLOBAL_DEVELOPER_DISCORD_ID
            session_user = {
                "id": str(discord_user_id),
                "username": "local-developer",
                "display_name": "Local Developer",
                "avatar_url": "",
            }
            oauth_guild_ids: set[int] = set()
        else:
            try:
                discord_user_id = int(session_user.get("id") or 0)
            except (TypeError, ValueError):
                discord_user_id = 0
            oauth_guild_ids = {
                int(value)
                for value in request.session.get("discord_guild_ids", [])
                if str(value).isdigit()
            }
            if discord_user_id <= 0:
                if _wants_json(request):
                    return JSONResponse(
                        {"ok": False, "message": "로그인이 필요합니다."},
                        status_code=401,
                    )
                next_url = urlencode({"next": _safe_next(str(request.url.path))})
                return RedirectResponse(f"/login?{next_url}", status_code=303)

        guilds, roles, scopes = await _enabled_guild_access(discord_user_id, oauth_guild_ids)
        if not guilds:
            if local_bypass:
                request.state.discord_user = session_user
                request.state.discord_user_id = discord_user_id
                request.state.access_role = "developer"
                request.state.access_scopes = (1, 2, 3)
                request.state.allowed_guild_ids = ()
                request.state.selected_guild_id = None
                return await call_next(request)
            request.session.clear()
            return templates.TemplateResponse(
                request,
                "pages/auth/failed.html",
                {"reason": "접근 가능한 운영 서버가 없습니다."},
                status_code=403,
            )

        allowed_ids = tuple(int(row["guild_id"]) for row in guilds)
        raw_requested = request.query_params.get("guild_id")
        try:
            requested_guild_id = int(raw_requested or 0)
        except ValueError:
            requested_guild_id = 0
        session_guild_id = int(request.session.get("selected_guild_id") or 0)
        selected_guild_id = (
            requested_guild_id
            if requested_guild_id in allowed_ids
            else session_guild_id
            if session_guild_id in allowed_ids
            else allowed_ids[0]
        )
        if not local_bypass and request.session.get("selected_guild_id") != selected_guild_id:
            request.session["selected_guild_id"] = selected_guild_id

        request.state.discord_user = session_user
        request.state.discord_user_id = discord_user_id
        request.state.allowed_guild_ids = allowed_ids
        request.state.selected_guild_id = selected_guild_id
        request.state.server_roles = roles
        request.state.access_role = roles.get(selected_guild_id, "user")
        request.state.access_scopes = scopes.get(selected_guild_id, ())
        return await call_next(request)


def _authorize_url(state: str) -> str:
    settings = get_settings()
    return "https://discord.com/oauth2/authorize?" + urlencode(
        {
            "client_id": settings.discord_client_id,
            "redirect_uri": settings.discord_redirect_uri,
            "response_type": "code",
            "scope": "identify guilds",
            "state": state,
        }
    )


async def _oauth_get(client: httpx.AsyncClient, path: str, token: str) -> Any:
    response = await client.get(
        path,
        headers={"Authorization": f"Bearer {token}"},
    )
    response.raise_for_status()
    return response.json()


async def _oauth_guilds(client: httpx.AsyncClient, token: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    after = ""
    while True:
        params = {"limit": 200}
        if after:
            params["after"] = after
        response = await client.get(
            "/users/@me/guilds",
            params=params,
            headers={"Authorization": f"Bearer {token}"},
        )
        response.raise_for_status()
        page = response.json()
        if not isinstance(page, list):
            break
        rows.extend(row for row in page if isinstance(row, dict))
        if len(page) < 200:
            break
        after = str(page[-1].get("id") or "")
        if not after:
            break
    return rows


@router.get("/login", response_class=HTMLResponse)
async def login(request: Request, next: str | None = None):
    settings = get_settings()
    if request.session.get("discord_user"):
        return RedirectResponse(_safe_next(next), status_code=303)
    return templates.TemplateResponse(
        request,
        "pages/auth/login.html",
        {
            "app_name": settings.app_name,
            "oauth_ready": bool(settings.discord_client_id and settings.discord_client_secret),
            "next_url": _safe_next(next),
        },
    )


@router.get("/auth/discord/login")
async def discord_login(
    request: Request,
    remember_me: str | None = None,
    next: str | None = None,
):
    settings = get_settings()
    if not settings.discord_client_id or not settings.discord_client_secret:
        return templates.TemplateResponse(
            request,
            "pages/auth/failed.html",
            {"reason": "Discord OAuth 환경변수가 설정되지 않았습니다."},
            status_code=500,
        )
    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = state
    request.session["remember_me"] = remember_me == "1"
    request.session["login_next"] = _safe_next(next)
    return RedirectResponse(_authorize_url(state), status_code=303)


@router.get("/auth/discord/callback", response_class=HTMLResponse)
async def discord_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
):
    settings = get_settings()
    expected_state = request.session.pop("oauth_state", None)
    if error or not code or not state or state != expected_state:
        request.session.clear()
        return templates.TemplateResponse(
            request,
            "pages/auth/failed.html",
            {"reason": "Discord 인증이 취소되었거나 로그인 상태값이 올바르지 않습니다."},
            status_code=401,
        )
    try:
        async with httpx.AsyncClient(
            base_url=settings.discord_api_base.rstrip("/"),
            timeout=httpx.Timeout(15.0),
        ) as client:
            token_response = await client.post(
                "/oauth2/token",
                data={
                    "client_id": settings.discord_client_id,
                    "client_secret": settings.discord_client_secret,
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": settings.discord_redirect_uri,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            token_response.raise_for_status()
            access_token = str(token_response.json().get("access_token") or "")
            if not access_token:
                raise RuntimeError("Discord access token is empty")
            user = await _oauth_get(client, "/users/@me", access_token)
            oauth_guilds = await _oauth_guilds(client, access_token)
        discord_user_id = int(user["id"])
        guild_ids = {int(row["id"]) for row in oauth_guilds if str(row.get("id", "")).isdigit()}
        visible_guilds, _, _ = await _enabled_guild_access(discord_user_id, guild_ids)
        if not visible_guilds:
            raise PermissionError("가입한 서버 중 접근 가능한 운영 서버가 없습니다.")
    except PermissionError as exc:
        request.session.clear()
        return templates.TemplateResponse(
            request,
            "pages/auth/failed.html",
            {"reason": str(exc)},
            status_code=403,
        )
    except Exception:
        request.session.clear()
        return templates.TemplateResponse(
            request,
            "pages/auth/failed.html",
            {"reason": "Discord 정보 확인 중 오류가 발생했습니다."},
            status_code=500,
        )

    avatar_hash = str(user.get("avatar") or "")
    avatar_url = (
        f"https://cdn.discordapp.com/avatars/{discord_user_id}/{avatar_hash}.png?size=128"
        if avatar_hash
        else ""
    )
    request.session["discord_user"] = {
        "id": str(discord_user_id),
        "username": str(user.get("username") or discord_user_id),
        "display_name": str(user.get("global_name") or user.get("username") or discord_user_id),
        "avatar_url": avatar_url,
    }
    request.session["discord_guild_ids"] = sorted(guild_ids)
    request.session["selected_guild_id"] = int(visible_guilds[0]["guild_id"])
    return RedirectResponse(_safe_next(request.session.pop("login_next", "/")), status_code=303)


@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)
