import asyncio

from dashboard.app.services import settlement_service


class _MappingsResult:
    def __init__(self, *, one=None, rows=None):
        self._one = one
        self._rows = rows or []

    def mappings(self):
        return self

    def one_or_none(self):
        return self._one

    def all(self):
        return self._rows


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
