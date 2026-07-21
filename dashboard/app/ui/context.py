from __future__ import annotations

from fastapi import Request

from dashboard.app.config import get_settings
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
    return {
        "request": request,
        "app_name": settings.app_name,
        "environment": settings.environment,
        "navigation": get_navigation(active_nav),
        "active_nav": active_nav,
        "page_title": page_title,
        "page_description": page_description,
        "page_kicker": page_kicker,
        "page_badge": page_badge,
    }
