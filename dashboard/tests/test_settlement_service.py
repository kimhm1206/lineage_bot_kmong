import asyncio

import pytest

from dashboard.app.services import settlement_service


class _MappingsResult:
    def __init__(self, *, one=None, rows=None, scalar_rows=None):
        self._one = one
        self._rows = rows or []
        self._scalar_rows = scalar_rows or []

    def mappings(self):
        return self

    def one_or_none(self):
        return self._one

    def one(self):
        return self._one

    def all(self):
        return self._rows

    def scalars(self):
        return self._scalar_rows


class _PayoutSession:
    def __init__(self):
        self.inserted = []

    async def execute(self, statement, params):
        sql = str(statement)
        if "SELECT d.guild_id, d.gross_adena" in sql:
            return _MappingsResult(
                one={
                    "guild_id": 100,
                    "gross_adena": 1_000,
                }
            )
        if "SELECT p.alliance_id, COUNT(*)::BIGINT AS member_count" in sql:
            assert "buyer_alliance_id" not in sql
            assert params == {"drop_id": 9}
            return _MappingsResult(
                rows=[
                    {"alliance_id": 7, "member_count": 2},
                    {"alliance_id": 8, "member_count": 3},
                ]
            )
        if "INSERT INTO settlement_payout_objects" in sql:
            self.inserted.append(dict(params))
            return _MappingsResult()
        raise AssertionError(f"Unexpected SQL: {sql}")


class _ClanChildSession:
    def __init__(self):
        self.inserted = []

    async def execute(self, statement, params):
        sql = str(statement)
        if "SELECT po.drop_id, po.recipient_alliance_id" in sql:
            return _MappingsResult(
                one={
                    "drop_id": 9,
                    "recipient_alliance_id": 7,
                    "amount_adena": 1_000,
                    "guild_id": 100,
                }
            )
        if "SELECT user_id" in sql and "settlement_drop_participants" in sql:
            assert "buyer_user_id" not in sql
            return _MappingsResult(scalar_rows=[101, 102])
        if "INSERT INTO settlement_payout_objects" in sql:
            self.inserted.append(dict(params))
            return _MappingsResult()
        raise AssertionError(f"Unexpected SQL: {sql}")

    async def scalar(self, statement, params):
        assert "SELECT COUNT(*) FROM settlement_payout_objects" in str(statement)
        return 0


class _DistributionStateSession:
    def __init__(self, rows):
        self.rows = rows

    async def execute(self, statement, params):
        sql = str(statement)
        assert "FOR UPDATE OF po" in sql
        assert params == {"drop_id": 9, "guild_id": 100}
        return _MappingsResult(rows=self.rows)


class _SalePriceSession:
    def __init__(self):
        self.calls = []

    async def execute(self, statement, params):
        sql = str(statement)
        self.calls.append((sql, dict(params)))
        if "SELECT s.status_code, i.item_id, i.default_price" in sql:
            return _MappingsResult(
                one={"status_code": 0, "item_id": 77, "default_price": 100_000}
            )
        return _MappingsResult()

    async def scalar(self, statement, params):
        assert "guild_alliance_role_mappings" in str(statement)
        assert params == {"guild_id": 100, "alliance_id": 7}
        return 1


def test_initial_sale_payouts_are_not_distribution_activity() -> None:
    session = _DistributionStateSession(
        [
            {"status_code": 0, "parent_payout_object_id": None},
            {"status_code": 0, "parent_payout_object_id": None},
        ]
    )

    assert not asyncio.run(
        settlement_service._distribution_started(
            session,
            drop_id=9,
            guild_id=100,
        )
    )


def test_processed_payout_blocks_sale_changes() -> None:
    session = _DistributionStateSession(
        [{"status_code": 1, "parent_payout_object_id": None}]
    )

    assert asyncio.run(
        settlement_service._distribution_started(
            session,
            drop_id=9,
            guild_id=100,
        )
    )


def test_generated_member_payout_blocks_sale_changes() -> None:
    session = _DistributionStateSession(
        [{"status_code": 0, "parent_payout_object_id": 77}]
    )

    assert asyncio.run(
        settlement_service._distribution_started(
            session,
            drop_id=9,
            guild_id=100,
        )
    )


def test_buyer_alliance_is_included_in_alliance_payouts(monkeypatch) -> None:
    async def no_fee_rules(*_args, **_kwargs):
        return []

    monkeypatch.setattr(settlement_service, "_latest_fee_rules", no_fee_rules)
    session = _PayoutSession()

    asyncio.run(settlement_service._build_alliance_payouts(session, drop_id=9))

    assert session.inserted == [
        {"drop_id": 9, "alliance_id": 7, "amount": 400},
        {"drop_id": 9, "alliance_id": 8, "amount": 600},
    ]


def test_buyer_is_included_when_listed_as_attendance_participant(monkeypatch) -> None:
    async def no_fee_rules(*_args, **_kwargs):
        return []

    monkeypatch.setattr(settlement_service, "_latest_fee_rules", no_fee_rules)
    session = _ClanChildSession()

    asyncio.run(
        settlement_service._build_clan_children(
            session,
            parent_payout_object_id=77,
        )
    )

    assert [row["user_id"] for row in session.inserted] == [101, 102]
    assert [row["amount"] for row in session.inserted] == [500, 500]


def test_sale_price_updates_drop_and_latest_item_price(monkeypatch) -> None:
    async def distribution_not_started(*_args, **_kwargs):
        return False

    async def no_op(*_args, **_kwargs):
        return None

    monkeypatch.setattr(settlement_service, "_distribution_started", distribution_not_started)
    monkeypatch.setattr(settlement_service, "_build_alliance_payouts", no_op)
    monkeypatch.setattr(settlement_service, "_audit", no_op)
    session = _SalePriceSession()

    result = asyncio.run(
        settlement_service.complete_sale(
            session,
            drop_id=9,
            guild_id=100,
            buyer_alliance_id=7,
            buyer_user_id=None,
            cash_price_krw=150_000,
            adena_market_rate=1_500,
        )
    )

    item_update = next(
        params for sql, params in session.calls if "UPDATE items" in sql
    )
    drop_update = next(
        params for sql, params in session.calls if "UPDATE settlement_drops" in sql
    )
    assert item_update == {"cash_price": 150_000, "item_id": 77, "guild_id": 100}
    assert drop_update["cash_price"] == 150_000
    assert drop_update["gross_adena"] == 1_000_000
    assert result.affected_ids == (9,)


def test_sale_rejects_gross_adena_overflow_before_writes() -> None:
    class Session:
        async def execute(self, statement, params):
            raise AssertionError(f"Unexpected database access: {statement}")

        async def scalar(self, statement, params):
            raise AssertionError(f"Unexpected database access: {statement}")

    with pytest.raises(
        settlement_service.SettlementError,
        match="저장 가능한 범위를 초과",
    ):
        asyncio.run(
            settlement_service.complete_sale(
                Session(),
                drop_id=9,
                guild_id=100,
                buyer_alliance_id=7,
                buyer_user_id=None,
                cash_price_krw=settlement_service.BIGINT_MAX,
                adena_market_rate=1,
            )
        )


def test_alliance_rounding_remainder_is_credited_to_alliance_treasury(monkeypatch) -> None:
    class Session:
        async def scalar(self, statement, params):
            assert "d.gross_adena" in str(statement)
            assert params == {"guild_id": 100, "drop_id": 9}
            return 3

    calls = []

    async def source_type_id(*_args, **_kwargs):
        return 10

    async def credit(*_args, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(settlement_service, "_treasury_source_type_id", source_type_id)
    monkeypatch.setattr(settlement_service, "_treasury_credit", credit)

    asyncio.run(
        settlement_service._credit_alliance_rounding_remainder(
            Session(), guild_id=100, drop_id=9
        )
    )

    assert calls == [{
        "guild_id": 100,
        "alliance_id": None,
        "scope_code": 1,
        "source_type_id": 10,
        "source_id": 9,
        "amount": 3,
        "category_name": "분배 후 나머지",
        "memo": "분배 후 나머지 Drop#9",
    }]


def test_clan_rounding_remainder_is_credited_to_clan_treasury(monkeypatch) -> None:
    class Session:
        async def execute(self, statement, params):
            assert "parent.amount_adena" in str(statement)
            assert params == {"parent_id": 77}
            return _MappingsResult(one={
                "guild_id": 100,
                "drop_id": 9,
                "recipient_alliance_id": 7,
                "remainder": 2,
            })

    calls = []

    async def source_type_id(*_args, **_kwargs):
        return 10

    async def credit(*_args, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(settlement_service, "_treasury_source_type_id", source_type_id)
    monkeypatch.setattr(settlement_service, "_treasury_credit", credit)

    asyncio.run(
        settlement_service._credit_clan_rounding_remainder(
            Session(), parent_payout_object_id=77
        )
    )

    assert calls == [{
        "guild_id": 100,
        "alliance_id": 7,
        "scope_code": 2,
        "source_type_id": 10,
        "source_id": 77,
        "amount": 2,
        "category_name": "분배 후 나머지",
        "memo": "분배 후 나머지 Drop#9",
    }]


def test_processed_payout_cannot_be_reverted_to_pending() -> None:
    class Session:
        async def execute(self, statement, params):
            assert "FOR UPDATE OF po" in str(statement)
            assert params == {"payout_id": 77}
            return _MappingsResult(one={"object_code": 1, "status_code": 1})

    try:
        asyncio.run(
            settlement_service.set_payout_status(
                Session(), payout_object_id=77, status_code=0
            )
        )
    except settlement_service.SettlementError as exc:
        assert str(exc) == "각혈 분배 이후 완료·귀속 정산은 취소할 수 없습니다."
    else:
        raise AssertionError("완료된 정산의 취소를 차단해야 합니다.")
