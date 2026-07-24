from dashboard.app.routes.settings import _member_rows


def test_discord_snowflakes_are_serialized_as_strings() -> None:
    snowflake = 1513092714030436391
    rows, names = _member_rows(
        [
            {
                "nick": "혈맹 경리",
                "roles": [],
                "user": {
                    "id": str(snowflake),
                    "username": "accountant",
                    "bot": False,
                },
            }
        ]
    )

    assert rows[0]["discord_id"] == str(snowflake)
    assert names[snowflake] == "혈맹 경리"
