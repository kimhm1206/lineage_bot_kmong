import pytest

from dashboard.app.services.settlement_math import (
    member_share_after_fees,
    proportional_fee,
)


def test_member_share_matches_per_rule_flooring() -> None:
    assert proportional_fee(1_000, 100_000) == 100
    assert member_share_after_fees(1_000, [100_000, 50_000], 2) == 425


def test_member_share_floors_each_fee_before_dividing() -> None:
    assert member_share_after_fees(101, [500_000, 500_000], 2) == 0


def test_member_share_requires_participants() -> None:
    with pytest.raises(ValueError, match="member_count"):
        member_share_after_fees(1_000, [], 0)
