from __future__ import annotations

from dashboard.app.ui.help_guides import GUIDES, get_help_guide_groups
from dashboard.app.ui.navigation import NAV_GROUPS, get_navigation


def test_every_navigation_item_has_a_help_guide() -> None:
    navigation_ids = {
        item.id
        for group in NAV_GROUPS
        for item in group.items
    }

    assert set(GUIDES) == navigation_ids


def test_help_guides_follow_visible_navigation() -> None:
    navigation = get_navigation(
        "home.personal",
        access_role="user",
        can_manage_alliance=False,
        can_manage_clan=False,
        can_configure_clan=False,
        can_manage_notifications=False,
    )
    visible_ids = {
        str(item["id"])
        for group in navigation
        for item in group["nav_items"]
    }
    help_ids = {
        str(guide["id"])
        for group in get_help_guide_groups(navigation)
        for guide in group["guides"]
    }

    assert help_ids == visible_ids
    assert "developer.system" not in help_ids


def test_help_guides_expose_minimum_access_labels() -> None:
    navigation = get_navigation(
        "home.personal",
        access_role="developer",
        can_manage_alliance=True,
        can_manage_clan=True,
        can_configure_clan=True,
        can_manage_notifications=True,
    )
    guides = {
        str(guide["id"]): guide
        for group in get_help_guide_groups(navigation)
        for guide in group["guides"]
    }

    assert guides["home.personal"]["access_label"] == "유저"
    assert guides["operations.delegation"]["access_label"] == "오너"
    assert guides["developer.system"]["access_label"] == "디벨로퍼"
    assert "access_suffix" not in guides["developer.system"]
