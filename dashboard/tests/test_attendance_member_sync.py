from __future__ import annotations

from typing import Any

import pytest

from dashboard.app.services.workspace_store import sync_discord_members


class _Mappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class _Result:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _Mappings:
        return _Mappings(self._rows)


class _Session:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def execute(self, _statement: object, params: dict[str, Any]) -> _Result:
        self.calls.append(params)
        if len(self.calls) == 1:
            return _Result([{"role_id": 50, "alliance_id": 7}])
        return _Result([])


@pytest.mark.asyncio
async def test_sync_discord_members_includes_new_and_unmapped_humans() -> None:
    session = _Session()

    discord_ids = await sync_discord_members(
        session,  # type: ignore[arg-type]
        guild_id=99,
        members=[
            {
                "nick": "혈맹원",
                "roles": ["50"],
                "user": {"id": "101", "username": "member", "bot": False},
            },
            {
                "roles": [],
                "user": {
                    "id": "102",
                    "global_name": "신규 유저",
                    "username": "new-user",
                    "bot": False,
                },
            },
            {
                "roles": [],
                "user": {"id": "103", "username": "helper", "bot": True},
            },
        ],
    )

    assert discord_ids == [101, 102]
    assert session.calls[1] == {
        "alliance_id": 7,
        "discord_id": 101,
        "discord_nickname": "혈맹원",
    }
    assert session.calls[2] == {
        "alliance_id": None,
        "discord_id": 102,
        "discord_nickname": "신규 유저",
    }
