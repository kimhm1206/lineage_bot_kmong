from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from dashboard.app.routes.reports import _require_report_access
from dashboard.app.routes.settings import _require_alliance_configuration
from dashboard.app.security import (
    can_manage_alliance_operations,
    can_manage_notifications,
)
from dashboard.app.ui.navigation import get_navigation


def _request(*, role: str = "user", scopes: tuple[int, ...] = ()) -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(
            access_role=role,
            access_scopes=scopes,
        )
    )


def _visible_navigation_ids(**kwargs: object) -> set[str]:
    return {
        str(item["id"])
        for group in get_navigation("home.personal", **kwargs)
        for item in group["nav_items"]
    }


def test_alliance_management_requires_alliance_manager_or_higher() -> None:
    assert can_manage_alliance_operations(_request(scopes=(1,)))
    assert not can_manage_alliance_operations(_request(scopes=(2,)))
    assert not can_manage_alliance_operations(_request(scopes=(3,)))
    assert can_manage_alliance_operations(_request(role="owner"))
    assert can_manage_alliance_operations(_request(role="developer"))


def test_notification_management_allows_alliance_and_clan_managers() -> None:
    assert can_manage_notifications(_request(scopes=(1,)))
    assert can_manage_notifications(_request(scopes=(2,)))
    assert not can_manage_notifications(_request(scopes=(3,)))
    assert not can_manage_notifications(_request())
    assert can_manage_notifications(_request(role="owner"))
    assert can_manage_notifications(_request(role="developer"))


def test_management_navigation_uses_the_same_permission_boundaries() -> None:
    alliance_manager_ids = _visible_navigation_ids(
        access_role="alliance_manager",
        can_manage_alliance=True,
        can_manage_clan=False,
        can_configure_clan=False,
        can_manage_notifications=True,
    )
    clan_manager_ids = _visible_navigation_ids(
        access_role="clan_manager",
        can_manage_alliance=False,
        can_manage_clan=True,
        can_configure_clan=True,
        can_manage_notifications=True,
    )
    clan_accountant_ids = _visible_navigation_ids(
        access_role="clan_accountant",
        can_manage_alliance=False,
        can_manage_clan=True,
        can_configure_clan=False,
        can_manage_notifications=False,
    )

    assert "operations.alliances" in alliance_manager_ids
    assert "operations.notifications" in alliance_manager_ids
    assert "operations.alliances" not in clan_manager_ids
    assert "operations.notifications" in clan_manager_ids
    assert "operations.notifications" not in clan_accountant_ids


def test_route_guards_enforce_management_permissions() -> None:
    with pytest.raises(HTTPException) as alliance_error:
        _require_alliance_configuration(_request(scopes=(2,)), None)
    assert alliance_error.value.status_code == 403
    _require_alliance_configuration(_request(scopes=(1,)), None)

    with pytest.raises(HTTPException) as notification_error:
        _require_report_access(_request(scopes=(3,)))
    assert notification_error.value.status_code == 403
    _require_report_access(_request(scopes=(1,)))
    _require_report_access(_request(scopes=(2,)))
