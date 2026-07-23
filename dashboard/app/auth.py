from __future__ import annotations

import ipaddress
import secrets
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from dashboard.app.config import BASE_DIR, get_settings
from dashboard.app.database import SessionLocal
from dashboard.app.services.audit_service import AuditActor, bind_actor, reset_actor


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
DEVELOPER_VIEW_MODE_SESSION_KEY = "developer_view_mode"
DEVELOPER_VIEW_ALLIANCE_SESSION_KEY = "developer_view_alliance_id"
OAUTH_REDIRECT_URI_SESSION_KEY = "oauth_redirect_uri"
DEVELOPER_VIEW_MODES = {
    "developer": {
        "label": "디벨로퍼",
        "role": "developer",
        "scopes": (1, 2, 3),
        "requires_alliance": False,
    },
    "owner": {
        "label": "오너",
        "role": "owner",
        "scopes": (1, 2, 3),
        "requires_alliance": False,
    },
    "alliance_manager": {
        "label": "연합관리자",
        "role": "alliance_manager",
        "scopes": (1,),
        "requires_alliance": True,
    },
    "clan_manager": {
        "label": "혈맹관리자",
        "role": "clan_manager",
        "scopes": (2,),
        "requires_alliance": True,
    },
    "clan_accountant": {
        "label": "혈맹경리",
        "role": "clan_accountant",
        "scopes": (3,),
        "requires_alliance": True,
    },
    "user": {
        "label": "유저",
        "role": "user",
        "scopes": (),
        "requires_alliance": True,
    },
}


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


def _is_loopback_host(value: str) -> bool:
    host = value.strip().lower()
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _oauth_redirect_uri(request: Request) -> str:
    settings = get_settings()
    request_host = str(request.url.hostname or "")
    if _is_loopback_host(request_host):
        return settings.discord_redirect_uri_local
    return settings.discord_redirect_uri


def _canonical_oauth_login_url(
    request: Request,
    redirect_uri: str,
) -> str | None:
    target = urlsplit(redirect_uri)
    if not target.scheme or not target.netloc:
        return None
    current_origin = (
        request.url.scheme.lower(),
        request.url.netloc.lower(),
    )
    target_origin = (target.scheme.lower(), target.netloc.lower())
    if current_origin == target_origin:
        return None
    return urlunsplit(
        (
            target.scheme,
            target.netloc,
            request.url.path,
            request.url.query,
            "",
        )
    )


def _wants_json(request: Request) -> bool:
    return request.url.path.startswith("/api/") or "application/json" in request.headers.get(
        "accept", ""
    )


def _developer_view_mode(session: dict[str, Any]) -> str:
    mode = str(session.get(DEVELOPER_VIEW_MODE_SESSION_KEY) or "developer")
    return mode if mode in DEVELOPER_VIEW_MODES else "developer"


def _developer_view_alliance_id(session: dict[str, Any]) -> int | None:
    try:
        alliance_id = int(session.get(DEVELOPER_VIEW_ALLIANCE_SESSION_KEY) or 0)
    except (TypeError, ValueError):
        return None
    return alliance_id if alliance_id > 0 else None


def _require_global_developer(request: Request) -> None:
    if not bool(getattr(request.state, "is_global_developer", False)):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="디벨로퍼 계정에서만 사용할 수 있습니다.",
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
    async def _call_with_actor(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
        session_user: dict[str, object],
    ) -> Response:
        actor_token = bind_actor(
            AuditActor(
                discord_id=int(request.state.discord_user_id or 0),
                display_name=str(
                    session_user.get("display_name")
                    or session_user.get("username")
                    or request.state.discord_user_id
                ),
                access_role=str(request.state.access_role or "user"),
            )
        )
        try:
            return await call_next(request)
        finally:
            reset_actor(actor_token)

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

        is_global_developer = discord_user_id == GLOBAL_DEVELOPER_DISCORD_ID
        guilds, roles, scopes = await _enabled_guild_access(discord_user_id, oauth_guild_ids)
        if not guilds:
            if local_bypass:
                request.state.discord_user = session_user
                request.state.discord_user_id = discord_user_id
                request.state.is_global_developer = True
                request.state.developer_view_mode = "developer"
                request.state.developer_view_alliance_id = None
                request.state.access_role = "developer"
                request.state.access_scopes = (1, 2, 3)
                request.state.allowed_guild_ids = ()
                request.state.selected_guild_id = None
                return await self._call_with_actor(
                    request,
                    call_next,
                    session_user,
                )
            request.session.clear()
            return templates.TemplateResponse(
                request,
                "pages/auth/failed.html",
                {"reason": "접근 가능한 운영 서버가 없습니다."},
                status_code=403,
            )

        allowed_ids = tuple(int(row["guild_id"]) for row in guilds)
        session_guild_id = int(request.session.get("selected_guild_id") or 0)
        selected_guild_id = (
            session_guild_id
            if session_guild_id in allowed_ids
            else allowed_ids[0]
        )
        if not local_bypass and request.session.get("selected_guild_id") != selected_guild_id:
            request.session["selected_guild_id"] = selected_guild_id

        request.state.discord_user = session_user
        request.state.discord_user_id = discord_user_id
        request.state.is_global_developer = is_global_developer
        request.state.allowed_guild_ids = allowed_ids
        request.state.selected_guild_id = selected_guild_id
        request.state.server_roles = roles
        effective_role = roles.get(selected_guild_id, "user")
        effective_scopes = scopes.get(selected_guild_id, ())
        view_mode = "developer" if is_global_developer else ""
        view_alliance_id = None
        if is_global_developer:
            view_mode = _developer_view_mode(request.session)
            view_config = DEVELOPER_VIEW_MODES[view_mode]
            effective_role = str(view_config["role"])
            effective_scopes = tuple(view_config["scopes"])
            if bool(view_config["requires_alliance"]):
                view_alliance_id = _developer_view_alliance_id(request.session)
        request.state.developer_view_mode = view_mode
        request.state.developer_view_alliance_id = view_alliance_id
        request.state.access_role = effective_role
        request.state.access_scopes = effective_scopes
        return await self._call_with_actor(request, call_next, session_user)


def _authorize_url(state: str, redirect_uri: str) -> str:
    settings = get_settings()
    return "https://discord.com/oauth2/authorize?" + urlencode(
        {
            "client_id": settings.discord_client_id,
            "redirect_uri": redirect_uri,
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
    redirect_uri = _oauth_redirect_uri(request)
    canonical_login_url = _canonical_oauth_login_url(request, redirect_uri)
    if canonical_login_url is not None:
        return RedirectResponse(canonical_login_url, status_code=303)
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
    request.session[OAUTH_REDIRECT_URI_SESSION_KEY] = redirect_uri
    return RedirectResponse(
        _authorize_url(state, redirect_uri),
        status_code=303,
    )


@router.get("/auth/discord/callback", response_class=HTMLResponse)
async def discord_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
):
    settings = get_settings()
    expected_state = request.session.pop("oauth_state", None)
    session_redirect_uri = str(
        request.session.pop(OAUTH_REDIRECT_URI_SESSION_KEY, "") or ""
    )
    allowed_redirect_uris = {
        settings.discord_redirect_uri,
        settings.discord_redirect_uri_local,
    }
    redirect_uri = (
        session_redirect_uri
        if session_redirect_uri in allowed_redirect_uris
        else _oauth_redirect_uri(request)
    )
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
                    "redirect_uri": redirect_uri,
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


@router.get("/auth/developer-view/options")
async def developer_view_options(request: Request):
    _require_global_developer(request)
    guild_id = getattr(request.state, "selected_guild_id", None)
    alliances: list[dict[str, Any]] = []
    if guild_id is not None:
        async with SessionLocal() as session:
            rows = (
                await session.execute(
                    text("""
                        SELECT DISTINCT a.alliance_id,
                                        COALESCE(a.display_name, a.alliance_name) AS alliance_name,
                                        COALESCE(a.sort_order, 2147483647) AS resolved_sort_order
                        FROM alliances a
                        JOIN guild_alliance_role_mappings m
                          ON m.alliance_id = a.alliance_id
                         AND m.guild_id = :guild_id
                        WHERE a.is_active IS TRUE
                        ORDER BY resolved_sort_order, alliance_name
                    """),
                    {"guild_id": int(guild_id)},
                )
            ).mappings().all()
            alliances = [
                {
                    "alliance_id": int(row["alliance_id"]),
                    "alliance_name": str(row["alliance_name"]),
                }
                for row in rows
            ]
    return JSONResponse(
        {
            "ok": True,
            "active_mode": str(getattr(request.state, "developer_view_mode", "developer")),
            "active_alliance_id": getattr(
                request.state,
                "developer_view_alliance_id",
                None,
            ),
            "modes": [
                {
                    "value": value,
                    "label": str(config["label"]),
                    "requires_alliance": bool(config["requires_alliance"]),
                }
                for value, config in DEVELOPER_VIEW_MODES.items()
            ],
            "alliances": alliances,
        }
    )


@router.post("/auth/developer-view")
async def set_developer_view(request: Request):
    _require_global_developer(request)
    try:
        payload = await request.json()
    except ValueError:
        payload = {}
    mode = str(payload.get("mode") or "").strip()
    config = DEVELOPER_VIEW_MODES.get(mode)
    if config is None:
        return JSONResponse(
            {"ok": False, "message": "선택한 권한을 확인해 주세요."},
            status_code=422,
        )

    alliance_id: int | None = None
    if bool(config["requires_alliance"]):
        try:
            alliance_id = int(payload.get("alliance_id") or 0)
        except (TypeError, ValueError):
            alliance_id = 0
        guild_id = getattr(request.state, "selected_guild_id", None)
        if guild_id is None or alliance_id <= 0:
            return JSONResponse(
                {"ok": False, "message": "확인할 혈맹을 선택해 주세요."},
                status_code=422,
            )
        async with SessionLocal() as session:
            mapped_alliance_id = await session.scalar(
                text("""
                    SELECT m.alliance_id
                    FROM guild_alliance_role_mappings m
                    JOIN alliances a ON a.alliance_id = m.alliance_id
                    WHERE m.guild_id = :guild_id
                      AND m.alliance_id = :alliance_id
                      AND a.is_active IS TRUE
                    LIMIT 1
                """),
                {
                    "guild_id": int(guild_id),
                    "alliance_id": alliance_id,
                },
            )
        if mapped_alliance_id is None:
            return JSONResponse(
                {"ok": False, "message": "현재 서버에 역할 매핑된 혈맹만 선택할 수 있습니다."},
                status_code=422,
            )

    request.session[DEVELOPER_VIEW_MODE_SESSION_KEY] = mode
    if alliance_id is None:
        request.session.pop(DEVELOPER_VIEW_ALLIANCE_SESSION_KEY, None)
    else:
        request.session[DEVELOPER_VIEW_ALLIANCE_SESSION_KEY] = alliance_id
    return JSONResponse(
        {
            "ok": True,
            "message": f"{config['label']} 시점으로 전환했습니다.",
        }
    )
