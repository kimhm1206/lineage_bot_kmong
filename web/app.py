from __future__ import annotations

import asyncio
import os
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode

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
KST = timezone(timedelta(hours=9))
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
REPORT_FREQUENCY_OPTIONS = (
    {"value": "daily", "label": "매일"},
    {"value": "every_3_days", "label": "3일마다"},
    {"value": "weekly", "label": "일주일마다"},
)
REPORT_PERIOD_OPTIONS = (
    {"value": "recent_7_days", "label": "최근 일주일"},
    {"value": "yesterday", "label": "전날"},
    {"value": "recent_3_days", "label": "최근 3일"},
)
REPORT_SUBJECT_OPTIONS = (
    {"value": "user", "label": "유저별"},
    {"value": "alliance", "label": "혈맹별"},
)
REPORT_RESULT_OPTIONS = (
    {"value": "ranking", "label": "순위"},
    {"value": "all", "label": "전체"},
)
REPORT_STATUS_OPTIONS = (
    {"value": "on", "label": "on"},
    {"value": "off", "label": "off"},
    {"value": "delete", "label": "delete"},
)
REPORT_OPTIONS = {
    "frequencies": REPORT_FREQUENCY_OPTIONS,
    "periods": REPORT_PERIOD_OPTIONS,
    "subjects": REPORT_SUBJECT_OPTIONS,
    "results": REPORT_RESULT_OPTIONS,
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
        guild = _discord_bot_get(f"/guilds/{guild_id}")
        if str(guild.get("owner_id")) == str(discord_user_id):
            return "admin"

        member = _discord_bot_get(f"/guilds/{guild_id}/members/{discord_user_id}")
        roles = _discord_bot_get(f"/guilds/{guild_id}/roles")
    except Exception:
        return None

    member_role_ids = {str(role_id) for role_id in member.get("roles", [])}
    member_role_ids.add(str(guild_id))
    permissions = 0
    for role in roles:
        if str(role.get("id")) not in member_role_ids:
            continue
        permissions |= _discord_permissions(role)

    return "admin" if permissions & DISCORD_WEB_ADMIN_PERMISSION_MASK else "user"


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
    is_global_developer = str(discord_user_id) == GLOBAL_DEVELOPER_DISCORD_ID
    if not guild_ids and not is_global_developer:
        return []

    where_clause = "TRUE" if is_global_developer else "g.guild_id = ANY(%s::bigint[])"
    params: tuple[Any, ...] = () if is_global_developer else (guild_ids,)

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
    channels = _discord_bot_get(f"/guilds/{guild_id}/channels")
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


def _channel_ids(channels: list[dict[str, Any]]) -> set[int]:
    return {int(channel["id"]) for channel in channels}


def _parse_optional_int(value: str | None) -> int | None:
    if value is None or value.strip() == "":
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


def _default_report_form() -> dict[str, str]:
    return {
        "frequency": "daily",
        "run_time": "00:00",
        "period_type": "recent_7_days",
        "subject_type": "user",
        "result_type": "all",
        "channel_id": "",
        "status": "on",
    }


def _option_label(options: tuple[dict[str, str], ...], value: str) -> str:
    return next((option["label"] for option in options if option["value"] == value), value)


def _report_sentence(report: dict[str, Any]) -> str:
    return (
        f"{report['frequency_label']} {report['run_time_label']}에 "
        f"{report['period_label']}의 "
        f"{report['subject_label']} {report['result_label']} 통계를 "
        f"#{report['channel_name']}로 받습니다."
    )


def _format_run_time(value: str | None) -> str:
    if not value:
        return "00시"
    hour, _, minute = value.partition(":")
    if minute == "00" or not minute:
        return f"{int(hour):02d}시"
    return f"{int(hour):02d}시 {int(minute):02d}분"


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
        ORDER BY
            CASE r.status WHEN 'on' THEN 0 WHEN 'off' THEN 1 ELSE 2 END,
            r.updated_at DESC,
            r.report_setting_id DESC
        """,
        (guild_id,),
    )
    reports: list[dict[str, Any]] = []
    for row in rows:
        report = {
            "report_setting_id": int(row["report_setting_id"]),
            "frequency": str(row["frequency"]),
            "period_type": str(row["period_type"]),
            "subject_type": str(row["subject_type"]),
            "result_type": str(row["result_type"]),
            "run_time": str(row["run_time"] or "00:00"),
            "run_time_label": _format_run_time(str(row["run_time"] or "00:00")),
            "channel_id": row["channel_id"],
            "channel_name": str(row["channel_name"]),
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
                str(row["frequency"]),
            ),
            "period_label": _option_label(
                REPORT_PERIOD_OPTIONS,
                str(row["period_type"]),
            ),
            "subject_label": _option_label(
                REPORT_SUBJECT_OPTIONS,
                str(row["subject_type"]),
            ),
            "result_label": _option_label(
                REPORT_RESULT_OPTIONS,
                str(row["result_type"]),
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
) -> None:
    _enqueue_bot_command(guild_id, command_type, requested_by_discord_id)


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
    attendance_rows = filtered_rows[:200]
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
            "attendance_rows": attendance_rows,
            "alliance_options": alliance_options,
            "filters": {
                "start_date": start_value,
                "end_date": end_value,
                "search": search_value,
                "alliance": alliance_value,
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
    guild_settings = database.get_settings(selected_guild_id)
    channel_error = ""
    channels = {"text": [], "voice": []}
    try:
        channels = _load_guild_channels(selected_guild_id)
    except Exception as exc:
        channel_error = f"Discord 채널 목록을 불러오지 못했습니다. {exc}"

    return _render(
        request,
        "settings.html",
        {
            "auth": auth,
            "settings": _settings_to_dict(guild_settings),
            "form": _settings_to_dict(guild_settings),
            "channels": channels,
            "channel_error": channel_error,
            "saved": saved,
            "errors": [],
            "report_options": REPORT_OPTIONS,
            "report_form": _default_report_form(),
            "report_settings": _load_report_settings(selected_guild_id),
            "active_page": "settings",
        },
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
        return RedirectResponse(
            f"/settings?guild_id={selected_guild_id}&saved=forbidden",
            status_code=303,
        )

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }

    channel_error = ""
    channels = {"text": [], "voice": []}
    try:
        channels = _load_guild_channels(selected_guild_id)
    except Exception as exc:
        channel_error = f"Discord 채널 목록을 불러오지 못했습니다. {exc}"

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
            {
                "auth": auth,
                "settings": _settings_to_dict(previous_settings),
                "form": _settings_form_from_values(form_data),
                "channels": channels,
                "channel_error": channel_error,
                "saved": "",
                "errors": errors,
                "report_options": REPORT_OPTIONS,
                "report_form": _default_report_form(),
                "report_settings": _load_report_settings(selected_guild_id),
                "active_page": "settings",
            },
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
        return RedirectResponse(
            f"/settings?guild_id={selected_guild_id}&saved=forbidden",
            status_code=303,
        )

    body = (await request.body()).decode("utf-8")
    form_data = {
        key: values[-1] if values else ""
        for key, values in parse_qs(body, keep_blank_values=True).items()
    }
    errors: list[str] = []
    channel_error = ""
    channels = {"text": [], "voice": []}
    try:
        channels = _load_guild_channels(selected_guild_id)
    except Exception as exc:
        channel_error = f"Discord 채널 목록을 불러오지 못했습니다. {exc}"
        errors.append("알람을 받을 채널 목록을 확인할 수 없습니다.")

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
    subject_type = _validate_report_option(
        "통계 대상",
        form_data.get("subject_type"),
        REPORT_SUBJECT_OPTIONS,
        errors,
    )
    result_type = _validate_report_option(
        "결과 형태",
        form_data.get("result_type"),
        REPORT_RESULT_OPTIONS,
        errors,
    )
    status = _validate_report_option(
        "상태",
        form_data.get("status") or "on",
        REPORT_STATUS_OPTIONS,
        errors,
    )
    channel_id = _validate_channel_value(
        "알람 채널",
        form_data.get("channel_id"),
        _channel_ids(channels["text"]),
        errors,
    )
    channel_name = (
        _find_channel_name(channels["text"], channel_id)
        if channel_id is not None
        else None
    )
    if channel_id is None or channel_name is None:
        errors.append("알람을 받을 Discord 텍스트 채널을 선택해주세요.")

    report_form = {
        "frequency": frequency,
        "run_time": run_time,
        "period_type": period_type,
        "subject_type": subject_type,
        "result_type": result_type,
        "channel_id": str(channel_id or ""),
        "status": status,
    }
    guild_settings = database.get_settings(selected_guild_id)
    if errors:
        return _render(
            request,
            "settings.html",
            {
                "auth": auth,
                "settings": _settings_to_dict(guild_settings),
                "form": _settings_to_dict(guild_settings),
                "channels": channels,
                "channel_error": channel_error,
                "saved": "",
                "errors": errors,
                "report_options": REPORT_OPTIONS,
                "report_form": report_form,
                "report_settings": _load_report_settings(selected_guild_id),
                "active_page": "settings",
            },
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
                    frequency,
                    run_time,
                    period_type,
                    subject_type,
                    result_type,
                    channel_id,
                    channel_name,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    selected_guild_id,
                    int(auth["user"]["id"]),
                    int(auth["user"]["id"]),
                    frequency,
                    run_time,
                    period_type,
                    subject_type,
                    result_type,
                    channel_id,
                    channel_name,
                    status,
                ),
            )
        connection.commit()

    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved=report",
        status_code=303,
    )


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
        return RedirectResponse(
            f"/settings?guild_id={selected_guild_id}&saved=forbidden",
            status_code=303,
        )

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
                    updated_by_discord_id = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE report_setting_id = %s
                  AND guild_id = %s
                """,
                (
                    status,
                    int(auth["user"]["id"]),
                    report_setting_id,
                    selected_guild_id,
                ),
            )
        connection.commit()

    return RedirectResponse(
        f"/settings?guild_id={selected_guild_id}&saved=report_status",
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
