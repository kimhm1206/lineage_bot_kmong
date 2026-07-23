from __future__ import annotations

from fastapi import Request

from dashboard.app.config import get_settings
from dashboard.app.security import (
    can_manage_alliance_operations,
    can_manage_clan_treasury,
    current_access_role,
)
from dashboard.app.ui.navigation import get_navigation


def build_template_context(
    request: Request,
    *,
    active_nav: str,
    page_title: str,
    page_description: str,
    page_kicker: str = "Dashboard V2",
    page_badge: str = "DESIGN BASE",
) -> dict[str, object]:
    settings = get_settings()
    access_role = current_access_role(request)
    navigation = get_navigation(
        active_nav,
        access_role=access_role,
        can_manage_alliance=can_manage_alliance_operations(request),
        can_manage_clan=can_manage_clan_treasury(request),
    )
    active_nav_group = next(
        (str(group["id"]) for group in navigation if group["is_active"]),
        str(navigation[0]["id"]) if navigation else "",
    )
    return {
        "request": request,
        "app_name": settings.app_name,
        "environment": settings.environment,
        "navigation": navigation,
        "active_nav_group": active_nav_group,
        "current_access_role": access_role,
        "current_access_role_label": {
            "developer": "Developer",
            "owner": "Owner",
            "alliance_manager": "Alliance manager",
            "clan_manager": "Clan manager",
            "clan_accountant": "Clan accountant",
        }.get(access_role, "User"),
        "current_discord_user": getattr(request.state, "discord_user", None),
        "active_nav": active_nav,
        "page_title": page_title,
        "page_description": page_description,
        "page_kicker": page_kicker,
        "page_badge": page_badge,
    }
