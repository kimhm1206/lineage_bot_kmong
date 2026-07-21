from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.app.config import BASE_DIR
from dashboard.app.database import get_session


router = APIRouter()
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))


@router.get("/")
async def index(request: Request, session: AsyncSession = Depends(get_session)):
    summary = await _load_summary(session)
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "summary": summary,
        },
    )


async def _load_summary(session: AsyncSession) -> dict[str, int]:
    result = await session.execute(
        text(
            """
            SELECT 'guilds' AS key, COUNT(*)::bigint AS value FROM guilds
            UNION ALL
            SELECT 'users', COUNT(*)::bigint FROM users
            UNION ALL
            SELECT 'attendance_sessions', COUNT(*)::bigint FROM attendance_sessions
            UNION ALL
            SELECT 'settlement_drops', COUNT(*)::bigint FROM settlement_drops
            UNION ALL
            SELECT 'payout_pending', COUNT(*)::bigint
            FROM settlement_payout_objects
            WHERE status_code = 0
            UNION ALL
            SELECT 'treasury_accounts', COUNT(*)::bigint FROM treasury_accounts
            """
        )
    )
    return {str(row.key): int(row.value) for row in result}
