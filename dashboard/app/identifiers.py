from __future__ import annotations

from collections.abc import Mapping
from typing import Any


_SNOWFLAKE_KEYS = {
    "discord_id",
    "discord_user_id",
    "guild_id",
    "owner_id",
    "owner_discord_id",
    "role_id",
    "channel_id",
}
_SNOWFLAKE_SUFFIXES = (
    "_channel_id",
    "_discord_id",
    "_guild_id",
    "_role_id",
)


def is_snowflake_key(key: object) -> bool:
    normalized = str(key)
    return normalized in _SNOWFLAKE_KEYS or normalized.endswith(_SNOWFLAKE_SUFFIXES)


def snowflake_text(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def json_safe_snowflakes(value: Any) -> Any:
    """Return frontend JSON data with every named Discord snowflake as text."""
    if isinstance(value, Mapping):
        return {
            key: (
                snowflake_text(item)
                if is_snowflake_key(key)
                else json_safe_snowflakes(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [json_safe_snowflakes(item) for item in value]
    return value
