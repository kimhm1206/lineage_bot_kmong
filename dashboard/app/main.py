from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from dashboard.app.config import BASE_DIR, get_settings
from dashboard.app.database import apply_local_schema_cleanup, close_database, ensure_settings_schema, ping_database
from dashboard.app.routes import developer, health, home, settings as settings_routes
from dashboard.app.services.discord_api import discord_api


@asynccontextmanager
async def lifespan(app: FastAPI):
    await ping_database()
    await ensure_settings_schema()
    await apply_local_schema_cleanup()
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

app.mount(
    "/static",
    StaticFiles(directory=str(BASE_DIR / "app" / "static")),
    name="static",
)

app.include_router(health.router)
app.include_router(home.router)
app.include_router(settings_routes.router)
app.include_router(developer.router)
