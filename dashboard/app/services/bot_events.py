from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession


logger = logging.getLogger(__name__)
CHANNEL_NAME = "lineage_bot_events"


async def publish_bot_event(
    session: AsyncSession,
    event_type: str,
    *,
    guild_id: int | None = None,
    data: dict[str, Any] | None = None,
) -> bool:
    payload = {
        "type": str(event_type),
        "guild_id": int(guild_id) if guild_id is not None else None,
        "data": dict(data or {}),
    }
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
        return True
    except SQLAlchemyError:
        await session.rollback()
        logger.exception("Failed to publish bot event: %s", event_type)
        return False
