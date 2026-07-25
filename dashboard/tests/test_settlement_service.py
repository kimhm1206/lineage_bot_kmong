import asyncio

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
