from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import asyncpg

from dashboard.app.config import get_settings


logger = logging.getLogger(__name__)
ACK_CHANNEL_NAME = "lineage_bot_event_acks"


class BotEventAckListener:
    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._connection: asyncpg.Connection | None = None
        self._ready = asyncio.Event()
        self._stopping = asyncio.Event()
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="bot-event-ack-listener")
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=5)
        except TimeoutError:
            logger.warning("Bot event ACK listener did not become ready within 5 seconds")

    async def stop(self) -> None:
        self._stopping.set()
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._ready.clear()
        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()

    def create_waiter(self, event_id: str) -> asyncio.Future[dict[str, Any]]:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[event_id] = future
        return future

    def discard_waiter(self, event_id: str) -> None:
        self._pending.pop(event_id, None)

    @property
    def connected(self) -> bool:
        return self._ready.is_set()

    async def _run(self) -> None:
        settings = get_settings()
        dsn = settings.database_url.replace(
            "postgresql+asyncpg://",
            "postgresql://",
            1,
        )
        while not self._stopping.is_set():
            connection: asyncpg.Connection | None = None
            try:
                connection = await asyncpg.connect(dsn)
                self._connection = connection
                await connection.add_listener(ACK_CHANNEL_NAME, self._on_notification)
                self._ready.set()
                while not self._stopping.is_set() and not connection.is_closed():
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Bot event ACK listener failed")
            finally:
                self._ready.clear()
                self._connection = None
                if connection is not None and not connection.is_closed():
                    try:
                        await connection.remove_listener(
                            ACK_CHANNEL_NAME,
                            self._on_notification,
                        )
                    except Exception:
                        pass
                    await connection.close()
            if not self._stopping.is_set():
                await asyncio.sleep(2)

    def _on_notification(
        self,
        _: asyncpg.Connection,
        __: int,
        ___: str,
        raw_payload: str,
    ) -> None:
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return
        event_id = str(payload.get("event_id") or "")
        if not event_id:
            return
        future = self._pending.pop(event_id, None)
        if future is not None and not future.done():
            future.set_result(payload)


bot_event_ack_listener = BotEventAckListener()
