from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from dashboard.app.services import bot_events


class FakeSession:
    def __init__(self) -> None:
        self.executed = []
        self.committed = False
        self.rolled_back = False

    async def execute(self, statement, params):
        self.executed.append((statement, params))

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True


class FakeAckListener:
    def __init__(self, acknowledgement=None) -> None:
        self.acknowledgement = acknowledgement
        self.discarded = []

    def create_waiter(self, event_id):
        future = asyncio.get_running_loop().create_future()
        if self.acknowledgement is not None:
            future.set_result(
                {
                    "event_id": event_id,
                    **self.acknowledgement,
                }
            )
        return future

    def discard_waiter(self, event_id):
        self.discarded.append(event_id)


@pytest.mark.asyncio
async def test_publish_bot_event_waits_for_successful_bot_ack(monkeypatch) -> None:
    session = FakeSession()
    listener = FakeAckListener({"ok": True, "message": "applied"})
    monkeypatch.setattr(bot_events, "bot_event_ack_listener", listener)

    result = await bot_events.publish_bot_event(
        session,
        "refresh_admin_panel",
        guild_id=123,
    )

    assert session.committed is True
    assert result.published is True
    assert result.acknowledged is True
    assert result.applied is True
    assert result.message == "applied"
    payload = session.executed[0][1]["payload"]
    assert '"event_id":' in payload
    assert '"guild_id":123' in payload


@pytest.mark.asyncio
async def test_publish_bot_event_reports_ack_timeout(monkeypatch) -> None:
    session = FakeSession()
    listener = FakeAckListener()
    monkeypatch.setattr(bot_events, "bot_event_ack_listener", listener)
    monkeypatch.setattr(
        bot_events,
        "get_settings",
        lambda: SimpleNamespace(bot_event_ack_timeout_seconds=0.01),
    )

    result = await bot_events.publish_bot_event(
        session,
        "refresh_report_schedules",
        guild_id=123,
    )

    assert session.committed is True
    assert result.published is True
    assert result.acknowledged is False
    assert result.applied is False
    assert listener.discarded
