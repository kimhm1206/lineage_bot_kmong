from dashboard.app.security import (
    CLAN_ACCESS_DETAIL,
    CLAN_ACCESS_MANAGE,
    CLAN_ACCESS_OWN,
    clan_user_access_mode,
    clan_visibility_allows,
)


def test_clan_visibility_policy_matrix() -> None:
    assert clan_visibility_allows(1, can_manage=True, is_member=False)
    assert not clan_visibility_allows(1, can_manage=False, is_member=True)
    assert clan_visibility_allows(2, can_manage=False, is_member=True)
    assert not clan_visibility_allows(2, can_manage=False, is_member=False)
    assert not clan_visibility_allows(3, can_manage=False, is_member=False)


def test_clan_user_access_modes() -> None:
    assert clan_user_access_mode(1, can_manage=False) == CLAN_ACCESS_OWN
    assert clan_user_access_mode(2, can_manage=False) == CLAN_ACCESS_DETAIL
    assert clan_user_access_mode(3, can_manage=False) == CLAN_ACCESS_OWN
    assert clan_user_access_mode(3, can_manage=True) == CLAN_ACCESS_MANAGE
