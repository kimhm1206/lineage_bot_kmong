from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from dashboard.app.config import BASE_DIR, get_settings
from dashboard.app.database import (
    SessionLocal,
    apply_local_schema_cleanup,
    close_database,
    ensure_settings_schema,
    ping_database,
)
from dashboard.app.routes import developer, health, home, operations, settings as settings_routes, workspaces
from dashboard.app.services import settlement_service
from dashboard.app.services.discord_api import discord_api
from dashboard.app.services.settlement_service import SettlementError


@asynccontextmanager
async def lifespan(app: FastAPI):
    await ping_database()
    await ensure_settings_schema()
    schema_changed = await apply_local_schema_cleanup()
    if schema_changed:
        async with SessionLocal() as session:
            await settlement_service.normalize_all_open_settlements(session)
            await session.commit()
    try:
        yield
    finally:
        await discord_api.close()
        await close_database()


settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    lifespan=lifespan,
)


@app.exception_handler(SettlementError)
async def settlement_error_handler(_: Request, exc: SettlementError) -> JSONResponse:
    return JSONResponse({"ok": False, "message": str(exc)}, status_code=422)

app.mount(
    "/static",
    StaticFiles(directory=str(BASE_DIR / "app" / "static")),
    name="static",
)

app.include_router(health.router)
app.include_router(home.router)
app.include_router(settings_routes.router)
app.include_router(developer.router)
app.include_router(operations.router)
app.include_router(workspaces.router)
