from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from dashboard.app.config import get_settings


settings = get_settings()

engine: AsyncEngine = create_async_engine(
    settings.database_url,
    echo=settings.db_echo,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = async_sessionmaker(
    engine,
    expire_on_commit=False,
    autoflush=False,
)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session


async def ping_database() -> None:
    async with engine.begin() as connection:
        await connection.execute(text("SELECT 1"))


async def ensure_settings_schema() -> None:
    statements = (
        """
        CREATE TABLE IF NOT EXISTS guild_user_assignments (
            assignment_id BIGSERIAL PRIMARY KEY,
            guild_id BIGINT NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
            discord_user_id BIGINT NOT NULL,
            scope_code SMALLINT NOT NULL CHECK (scope_code IN (1, 2, 3)),
            alliance_id BIGINT REFERENCES alliances(alliance_id) ON DELETE CASCADE,
            assigned_by_discord_user_id BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT chk_guild_user_assignment_scope
                CHECK (
                    (scope_code = 1 AND alliance_id IS NULL)
                    OR (scope_code IN (2, 3) AND alliance_id IS NOT NULL)
                )
        )
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_guild_alliance_manager_user
        ON guild_user_assignments (guild_id, discord_user_id, scope_code)
        WHERE scope_code = 1
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_guild_clan_assignment_user
        ON guild_user_assignments (guild_id, alliance_id, discord_user_id, scope_code)
        WHERE scope_code IN (2, 3)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_guild_user_assignments_lookup
        ON guild_user_assignments (guild_id, scope_code, alliance_id)
        """,
        """
        CREATE TABLE IF NOT EXISTS alliance_access_policies (
            guild_id BIGINT NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
            alliance_id BIGINT NOT NULL REFERENCES alliances(alliance_id) ON DELETE CASCADE,
            distribution_visibility_code SMALLINT NOT NULL DEFAULT 2
                CHECK (distribution_visibility_code IN (1, 2, 3)),
            treasury_visibility_code SMALLINT NOT NULL DEFAULT 3
                CHECK (treasury_visibility_code IN (1, 2, 3)),
            user_access_code SMALLINT NOT NULL DEFAULT 2
                CHECK (user_access_code IN (1, 2, 3)),
            updated_by_discord_user_id BIGINT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (guild_id, alliance_id)
        )
        """,
    )
    async with engine.begin() as connection:
        for statement in statements:
            await connection.execute(text(statement))


async def close_database() -> None:
    await engine.dispose()
