from __future__ import annotations

from collections.abc import Iterable


PPM_BASE = 1_000_000


def proportional_fee(amount: int, rate_ppm: int) -> int:
    return int(amount) * int(rate_ppm) // PPM_BASE


def member_share_after_fees(
    amount: int,
    fee_rates_ppm: Iterable[int],
    member_count: int,
) -> int:
    if member_count <= 0:
        raise ValueError("member_count must be positive")
    total_fee = sum(proportional_fee(amount, rate) for rate in fee_rates_ppm)
    return max(int(amount) - total_fee, 0) // int(member_count)
