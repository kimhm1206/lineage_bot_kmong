from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.database import get_session
from dashboard.app.services.bot_event_acks import bot_event_ack_listener


router = APIRouter(tags=["health"])


@router.get("/health")
async def health(session: AsyncSession = Depends(get_session)) -> dict[str, str | bool]:
    await session.execute(text("SELECT 1"))
    return {
        "status": "ok",
        "database": "ok",
        "bot_ack_listener": bot_event_ack_listener.connected,
    }
