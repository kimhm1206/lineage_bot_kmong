from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.config import get_settings
from dashboard.app.services.bot_event_acks import bot_event_ack_listener


logger = logging.getLogger(__name__)
CHANNEL_NAME = "lineage_bot_events"


@dataclass(frozen=True, slots=True)
class BotEventResult:
    published: bool
    acknowledged: bool
    applied: bool
    message: str


async def publish_bot_event(
    session: AsyncSession,
    event_type: str,
    *,
    guild_id: int | None = None,
    data: dict[str, Any] | None = None,
) -> BotEventResult:
    event_id = uuid4().hex
    payload = {
        "event_id": event_id,
        "type": str(event_type),
        "guild_id": int(guild_id) if guild_id is not None else None,
        "data": dict(data or {}),
    }
    waiter = bot_event_ack_listener.create_waiter(event_id)
    try:
        await session.execute(
            text("SELECT pg_notify(:channel_name, :payload)"),
            {
                "channel_name": CHANNEL_NAME,
                "payload": json.dumps(
                    payload,
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
            },
        )
        await session.commit()
    except SQLAlchemyError as exc:
        await session.rollback()
        bot_event_ack_listener.discard_waiter(event_id)
        logger.exception("Failed to publish bot event: %s", event_type)
        return BotEventResult(
            published=False,
            acknowledged=False,
            applied=False,
            message="PostgreSQL 봇 이벤트 발행에 실패했습니다.",
        )

    try:
        acknowledgement = await asyncio.wait_for(
            waiter,
            timeout=get_settings().bot_event_ack_timeout_seconds,
        )
    except TimeoutError:
        bot_event_ack_listener.discard_waiter(event_id)
        return BotEventResult(
            published=True,
            acknowledged=False,
            applied=False,
            message="봇 응답 시간이 초과되었습니다. 봇 실행 상태와 DB 연결을 확인해 주세요.",
        )

    applied = bool(acknowledgement.get("ok"))
    return BotEventResult(
        published=True,
        acknowledged=True,
        applied=applied,
        message=str(
            acknowledgement.get("message")
            or ("봇 반영 완료" if applied else "봇 반영 실패")
        ),
    )
