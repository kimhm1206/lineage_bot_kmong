from __future__ import annotations

from fastapi import Request

from dashboard.app.config import get_settings
from dashboard.app.security import current_access_role, is_developer
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
    navigation = get_navigation(active_nav, developer_access=is_developer(request))
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
        "active_nav": active_nav,
        "page_title": page_title,
        "page_description": page_description,
        "page_kicker": page_kicker,
        "page_badge": page_badge,
    }
