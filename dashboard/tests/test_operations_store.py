import asyncio

from dashboard.app.services.operations_store import (
    personal_distribution_page,
    restrict_clan_settlement_entities,
)


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class _PersonalEstimateSession:
    def __init__(self):
        self.estimate_sql = ""

    async def execute(self, statement, params):
        sql = str(statement)
        if "SELECT DISTINCT u.user_id" in sql:
            assert params == {"guild_id": 100}
            return _Rows([
                {
                    "user_id": 20,
                    "user_name": "테스트",
                    "alliance_id": 7,
                    "alliance_name": "혈맹A",
                }
            ])
        if "WITH history AS" in sql:
            return _Rows([])
        if "parent.amount_adena AS alliance_amount_adena" in sql:
            self.estimate_sql = sql
            assert params == {"guild_id": 100, "user_id": 20}
            return _Rows([
                {
                    "payout_object_id": 91,
                    "alliance_amount_adena": 1_000,
                    "alliance_id": 7,
                    "attendance_id": 55,
                    "item_name": "예상 아이템",
                    "occurred_at": 1_700_000_000,
                    "member_count": 2,
                    "fee_rates_ppm": [100_000, 50_000],
                    "occurred_at_label": "2026-08-12 10:00",
                }
            ])
        raise AssertionError(f"Unexpected SQL: {sql}")


def test_own_access_keeps_only_signed_in_member() -> None:
    page_data = {
        "entities": [
            {
                "entity_type": "fee",
                "target_id": 1,
                "pending_amount": 500,
            },
            {
                "entity_type": "member",
                "target_id": 10,
                "pending_amount": 1_200,
            },
            {
                "entity_type": "member",
                "target_id": 20,
                "pending_amount": 3_400,
            },
        ],
        "summary_cards": [],
    }

    result = restrict_clan_settlement_entities(
        page_data,
        access_mode="own",
        user_id=20,
    )

    assert [row["target_id"] for row in result["entities"]] == [20]
    assert result["summary_cards"][0]["value"] == "3,400"


def test_detail_access_preserves_page_data() -> None:
    page_data = {"entities": [{"target_id": 10}], "summary_cards": []}
    assert (
        restrict_clan_settlement_entities(
            page_data,
            access_mode="detail",
            user_id=10,
        )
        is page_data
    )


def test_personal_estimate_uses_only_pending_sold_parent_without_children() -> None:
    session = _PersonalEstimateSession()

    result = asyncio.run(
        personal_distribution_page(
            session,
            guild_id=100,
            user_id=20,
            period_days=30,
        )
    )

    assert result["estimated_total_label"] == "425"
    assert result["estimated_count"] == 1
    assert result["estimates"][0]["estimated_amount"] == 425
    assert "sale.status_code = 1" in session.estimate_sql
    assert "parent.status_code = 0" in session.estimate_sql
    assert "participant.user_id = :user_id" in session.estimate_sql
    assert "NOT EXISTS" in session.estimate_sql
    assert "period_days" not in session.estimate_sql
