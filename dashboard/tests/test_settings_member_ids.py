from dashboard.app.identifiers import json_safe_snowflakes
from dashboard.app.routes.settings import _member_rows


SNOWFLAKE = 1513092714030436391


def test_discord_snowflakes_are_serialized_as_strings() -> None:
    rows, names = _member_rows(
        [
            {
                "nick": "혈맹 경리",
                "roles": [],
                "user": {
                    "id": str(SNOWFLAKE),
                    "username": "accountant",
                    "bot": False,
                },
            }
        ]
    )

    assert rows[0]["discord_id"] == str(SNOWFLAKE)
    assert names[SNOWFLAKE] == "혈맹 경리"


def test_nested_frontend_snowflakes_are_strings() -> None:
    payload = json_safe_snowflakes(
        {
            "guild_id": SNOWFLAKE,
            "members": [
                {
                    "user_id": 42,
                    "discord_id": SNOWFLAKE,
                    "admin_channel_id": SNOWFLAKE,
                    "alliance_id": 7,
                }
            ],
            "role_id": SNOWFLAKE,
            "owner_id": None,
        }
    )

    assert payload == {
        "guild_id": str(SNOWFLAKE),
        "members": [
            {
                "user_id": 42,
                "discord_id": str(SNOWFLAKE),
                "admin_channel_id": str(SNOWFLAKE),
                "alliance_id": 7,
            }
        ],
        "role_id": str(SNOWFLAKE),
        "owner_id": None,
    }
