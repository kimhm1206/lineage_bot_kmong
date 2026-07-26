from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from dashboard.app.auth import DEVELOPER_VIEW_MODES, ROLE_PRIORITY, SCOPE_ROLES
from dashboard.app.routes.reports import _require_report_access
from dashboard.app.routes.settings import (
    _require_alliance_configuration,
    _require_owner_configuration,
)
from dashboard.app.security import (
    can_manage_alliance_managers,
    can_manage_alliance_operations,
    can_manage_clan_configuration,
    can_manage_clan_treasury,
    can_manage_notifications,
    can_manage_operational_assignments,
    can_select_alliances,
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


def test_alliance_accountant_scope_and_role_priority_are_registered() -> None:
    assert SCOPE_ROLES[4] == "alliance_accountant"
    assert ROLE_PRIORITY["clan_manager"] < ROLE_PRIORITY["alliance_accountant"]
    assert ROLE_PRIORITY["alliance_accountant"] < ROLE_PRIORITY["alliance_manager"]
    assert DEVELOPER_VIEW_MODES["alliance_accountant"]["scopes"] == (4,)
    assert 4 in DEVELOPER_VIEW_MODES["owner"]["scopes"]


def test_alliance_management_requires_alliance_manager_or_higher() -> None:
    assert can_manage_alliance_operations(_request(scopes=(1,)))
    assert can_manage_alliance_operations(_request(scopes=(4,)))
    assert not can_manage_alliance_operations(_request(scopes=(2,)))
    assert not can_manage_alliance_operations(_request(scopes=(3,)))
    assert can_manage_alliance_operations(_request(role="owner"))
    assert can_manage_alliance_operations(_request(role="developer"))


def test_notification_management_allows_alliance_and_clan_managers() -> None:
    assert can_manage_notifications(_request(scopes=(1,)))
    assert can_manage_notifications(_request(scopes=(4,)))
    assert can_manage_notifications(_request(scopes=(2,)))
    assert not can_manage_notifications(_request(scopes=(3,)))
    assert not can_manage_notifications(_request())
    assert can_manage_notifications(_request(role="owner"))
    assert can_manage_notifications(_request(role="developer"))


@pytest.mark.asyncio
async def test_alliance_manager_is_owner_equivalent_except_manager_grants() -> None:
    manager = _request(role="alliance_manager", scopes=(1,))
    accountant = _request(role="alliance_accountant", scopes=(4,))

    assert await can_select_alliances(manager, None, 1)
    assert can_manage_clan_treasury(manager)
    assert can_manage_clan_configuration(manager)
    assert can_manage_operational_assignments(manager)
    assert not can_manage_alliance_managers(manager)

    assert can_manage_alliance_operations(accountant)
    assert not can_manage_clan_treasury(accountant)
    assert not can_manage_clan_configuration(accountant)
    assert not can_manage_operational_assignments(accountant)

    assert can_manage_alliance_managers(_request(role="owner"))
    assert can_manage_alliance_managers(_request(role="developer"))


def test_management_navigation_uses_the_same_permission_boundaries() -> None:
    alliance_manager_ids = _visible_navigation_ids(
        access_role="alliance_manager",
        can_manage_alliance=True,
        can_manage_clan=True,
        can_configure_clan=True,
        can_manage_notifications=True,
    )
    alliance_accountant_ids = _visible_navigation_ids(
        access_role="alliance_accountant",
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
    assert "operations.delegation" in alliance_manager_ids
    assert "operations.alliances" in alliance_accountant_ids
    assert "operations.notifications" in alliance_accountant_ids
    assert "operations.delegation" not in alliance_accountant_ids
    assert "operations.alliances" not in clan_manager_ids
    assert "operations.notifications" in clan_manager_ids
    assert "operations.notifications" not in clan_accountant_ids


def test_route_guards_enforce_management_permissions() -> None:
    with pytest.raises(HTTPException) as alliance_error:
        _require_alliance_configuration(_request(scopes=(2,)), None)
    assert alliance_error.value.status_code == 403
    _require_alliance_configuration(_request(scopes=(1,)), None)
    _require_alliance_configuration(_request(scopes=(4,)), None)

    with pytest.raises(HTTPException) as notification_error:
        _require_report_access(_request(scopes=(3,)))
    assert notification_error.value.status_code == 403
    _require_report_access(_request(scopes=(1,)))
    _require_report_access(_request(scopes=(4,)))
    _require_report_access(_request(scopes=(2,)))


@pytest.mark.asyncio
async def test_alliance_manager_cannot_grant_or_remove_alliance_managers() -> None:
    with pytest.raises(HTTPException) as manager_error:
        await _require_owner_configuration(
            _request(role="alliance_manager", scopes=(1,)),
            None,
            1,
        )
    assert manager_error.value.status_code == 403

    await _require_owner_configuration(_request(role="owner"), None, 1)
    await _require_owner_configuration(_request(role="developer"), None, 1)
