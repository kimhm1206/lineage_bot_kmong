from dashboard.app.services.operations_store import (
    restrict_clan_settlement_entities,
)


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
