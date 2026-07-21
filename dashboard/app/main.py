from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from dashboard.app.config import BASE_DIR, get_settings
from dashboard.app.database import close_database, ping_database
from dashboard.app.routes import health, home


@asynccontextmanager
async def lifespan(app: FastAPI):
    await ping_database()
    yield
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

