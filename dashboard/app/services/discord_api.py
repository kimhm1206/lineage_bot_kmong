from __future__ import annotations

import asyncio
from dataclasses import dataclass
from time import monotonic
from typing import Any

import httpx

from dashboard.app.config import get_settings


class DiscordApiError(RuntimeError):
    pass


@dataclass
class _CacheEntry:
    expires_at: float
    value: Any


class DiscordRestClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._token = settings.discord_bot_token.strip()
        self._base_url = settings.discord_api_base.rstrip("/")
        self._ttl = max(settings.discord_cache_ttl_seconds, 0)
        self._cache: dict[str, _CacheEntry] = {}
        self._lock = asyncio.Lock()
        self._client: httpx.AsyncClient | None = None

    @property
    def configured(self) -> bool:
        return bool(self._token)

    def clear_cache(self, prefix: str = "") -> None:
        if not prefix:
            self._cache.clear()
            return
        for key in tuple(self._cache):
            if key.startswith(prefix):
                self._cache.pop(key, None)

    def clear_members_cache(self, guild_id: int) -> None:
        """Clear both the merged member list and its paginated REST responses."""
        self.clear_cache(f"members:{guild_id}")
        self.clear_cache(f"members-page:{guild_id}:")

    def _http_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                headers={"Authorization": f"Bot {self._token}"},
                timeout=httpx.Timeout(15.0),
            )
        return self._client

    async def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
        self._client = None

    async def _get(self, path: str, *, params: dict[str, Any] | None = None, cache_key: str | None = None) -> Any:
        if not self._token:
            raise DiscordApiError("루트 .env에 DISCORD_BOT_TOKEN을 설정해 주세요.")

        key = cache_key or path
        cached = self._cache.get(key)
        if cached and cached.expires_at > monotonic():
            return cached.value

        async with self._lock:
            cached = self._cache.get(key)
            if cached and cached.expires_at > monotonic():
                return cached.value

            try:
                response = await self._http_client().get(path, params=params)
            except httpx.HTTPError as exc:
                raise DiscordApiError("Discord API에 연결하지 못했습니다.") from exc

            if response.status_code == 401:
                raise DiscordApiError("Discord 봇 토큰이 유효하지 않습니다.")
            if response.status_code == 403:
                raise DiscordApiError("Discord 정보를 조회할 권한이 없습니다.")
            if response.status_code == 429:
                raise DiscordApiError("Discord 요청이 많습니다. 잠시 후 다시 시도해 주세요.")
            if response.is_error:
                raise DiscordApiError(f"Discord API 조회 실패 ({response.status_code})")

            value = response.json()
            if self._ttl:
                self._cache[key] = _CacheEntry(monotonic() + self._ttl, value)
            return value

    async def guild(self, guild_id: int) -> dict[str, Any]:
        return await self._get(f"/guilds/{guild_id}", cache_key=f"guild:{guild_id}")

    async def channels(self, guild_id: int) -> list[dict[str, Any]]:
        data = await self._get(f"/guilds/{guild_id}/channels", cache_key=f"channels:{guild_id}")
        return sorted(data, key=lambda item: (item.get("position", 0), item.get("name", "").casefold()))

    async def roles(self, guild_id: int) -> list[dict[str, Any]]:
        data = await self._get(f"/guilds/{guild_id}/roles", cache_key=f"roles:{guild_id}")
        return sorted(
            (item for item in data if item.get("name") != "@everyone"),
            key=lambda item: (-item.get("position", 0), item.get("name", "").casefold()),
        )

    async def members(self, guild_id: int) -> list[dict[str, Any]]:
        cache_key = f"members:{guild_id}"
        cached = self._cache.get(cache_key)
        if cached and cached.expires_at > monotonic():
            return cached.value

        members: list[dict[str, Any]] = []
        after = "0"
        while True:
            page = await self._get(
                f"/guilds/{guild_id}/members",
                params={"limit": 1000, "after": after},
                cache_key=f"members-page:{guild_id}:{after}",
            )
            members.extend(page)
            if len(page) < 1000:
                break
            after = page[-1]["user"]["id"]

        members.sort(key=self.member_display_name)
        if self._ttl:
            self._cache[cache_key] = _CacheEntry(monotonic() + self._ttl, members)
        return members

    async def send_message(self, channel_id: int, content: str) -> dict[str, Any]:
        if not self._token:
            raise DiscordApiError("루트 .env에 DISCORD_BOT_TOKEN을 설정해 주세요.")
        try:
            response = await self._http_client().post(
                f"/channels/{channel_id}/messages",
                json={"content": content},
            )
        except httpx.HTTPError as exc:
            raise DiscordApiError("Discord 메시지를 전송하지 못했습니다.") from exc
        if response.status_code == 401:
            raise DiscordApiError("Discord 봇 토큰이 유효하지 않습니다.")
        if response.status_code == 403:
            raise DiscordApiError("선택한 채널에 메시지를 보낼 권한이 없습니다.")
        if response.status_code == 429:
            raise DiscordApiError("Discord 요청이 많습니다. 잠시 후 다시 시도해 주세요.")
        if response.is_error:
            raise DiscordApiError(f"Discord 메시지 전송 실패 ({response.status_code})")
        return response.json()

    @staticmethod
    def member_display_name(member: dict[str, Any]) -> str:
        user = member.get("user", {})
        return (
            member.get("nick")
            or user.get("global_name")
            or user.get("username")
            or user.get("id", "알 수 없음")
        ).casefold()


discord_api = DiscordRestClient()
