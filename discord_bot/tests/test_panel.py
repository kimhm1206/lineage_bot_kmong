from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from discord_bot.utils import panel


class _Bot:
    def __init__(self) -> None:
        self.panel_locks: dict[int, asyncio.Lock] = {}


class _Message:
    def __init__(
        self,
        message_id: int,
        author_id: int,
        embed_title: str,
    ) -> None:
        self.id = message_id
        self.author = type("Author", (), {"id": author_id})()
        self.embeds = [type("Embed", (), {"title": embed_title})()]
        self.deleted = False

    async def delete(self) -> None:
        self.deleted = True


class _Channel:
    def __init__(self, messages: list[_Message]) -> None:
        self.messages = messages

    async def history(self, *, limit: int):
        for message in self.messages[:limit]:
            yield message


class PanelUpdateTests(unittest.IsolatedAsyncioTestCase):
    async def test_same_guild_panel_updates_are_serialized(self) -> None:
        bot = _Bot()
        active = 0
        max_active = 0

        async def fake_update(_bot: _Bot, _guild_id: int) -> None:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0)
            active -= 1

        with patch.object(panel, "_update_admin_panel", new=fake_update):
            await asyncio.gather(
                panel.update_admin_panel(bot, 1),
                panel.update_admin_panel(bot, 1),
            )

        self.assertEqual(max_active, 1)
        self.assertEqual(len(bot.panel_locks), 1)

    async def test_startup_cleanup_deletes_only_duplicate_admin_panels(self) -> None:
        current = _Message(10, 7, "출석 패널")
        duplicate = _Message(11, 7, "출석 패널")
        attendance = _Message(12, 7, "출석 진행 중")
        another_author = _Message(13, 8, "출석 패널")
        channel = _Channel([current, duplicate, attendance, another_author])

        await panel.clear_duplicate_admin_panels(
            channel,
            7,
            keep_message_id=current.id,
        )

        self.assertFalse(current.deleted)
        self.assertTrue(duplicate.deleted)
        self.assertFalse(attendance.deleted)
        self.assertFalse(another_author.deleted)


if __name__ == "__main__":
    unittest.main()
