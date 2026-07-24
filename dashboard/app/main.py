from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from dashboard.app.config import BASE_DIR, get_settings
from dashboard.app.auth import AuthContextMiddleware, router as auth_router
from dashboard.app.database import (
    close_database,
    ping_database,
)
from dashboard.app.routes import (
    developer,
    health,
    home,
    operations,
    reports,
    settings as settings_routes,
    workspaces,
)
from dashboard.app.session import RememberMeSessionMiddleware
from dashboard.app.services.discord_api import discord_api
from dashboard.app.services.bot_event_acks import bot_event_ack_listener
from dashboard.app.services.settlement_service import SettlementError


@asynccontextmanager
async def lifespan(app: FastAPI):
    await ping_database()
    await bot_event_ack_listener.start()
    try:
        yield
    finally:
        await bot_event_ack_listener.stop()
        await discord_api.close()
        await close_database()


settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    lifespan=lifespan,
)
app.add_middleware(AuthContextMiddleware)
app.add_middleware(
    RememberMeSessionMiddleware,
    secret_key=settings.session_secret,
    same_site="lax",
    https_only=settings.session_https_only,
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
app.include_router(auth_router)
app.include_router(home.router)
app.include_router(settings_routes.router)
app.include_router(developer.router)
app.include_router(operations.router)
app.include_router(reports.router)
app.include_router(workspaces.router)
