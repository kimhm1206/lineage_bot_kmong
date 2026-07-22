from __future__ import annotations

from fastapi import HTTPException, Request, status

from dashboard.app.config import get_settings


DEVELOPER_ENVIRONMENTS = {"local", "development", "test"}


def current_access_role(request: Request) -> str:
    """Resolve the web role without trusting client-provided headers or cookies."""
    role = str(getattr(request.state, "access_role", "") or "").strip().lower()
    if role:
        return role
    if get_settings().environment.strip().lower() in DEVELOPER_ENVIRONMENTS:
        return "developer"
    return "user"


def is_developer(request: Request) -> bool:
    return current_access_role(request) == "developer"


def can_manage_alliance_treasury(request: Request) -> bool:
    return current_access_role(request) in {"developer", "owner", "alliance_manager"}


def can_manage_clan_treasury(request: Request) -> bool:
    return current_access_role(request) in {"developer", "owner", "clan_manager", "clan_accountant"}


async def require_developer(request: Request) -> None:
    if not is_developer(request):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="개발자 전용 기능입니다.")
