from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    app_name: str = "Lineage Dashboard V2"
    environment: str = "local"
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:testest@127.0.0.1:5432/testdb",
    )
    db_echo: bool = False
    discord_bot_token: str = ""
    discord_api_base: str = "https://discord.com/api/v10"
    discord_cache_ttl_seconds: int = 300

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
