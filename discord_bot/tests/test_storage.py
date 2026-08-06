from __future__ import annotations

import unittest
from contextlib import contextmanager
from unittest.mock import patch

from discord_bot.storage import BotDatabase


class FakeCursor:
    def __init__(self) -> None:
        self.execute_calls = []
        self.bulk_calls = []
        self._one_rows = [
            {"allowed": 1},
            {"attendance_id": 77},
        ]
        self._many_rows = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=()):
        self.execute_calls.append((sql, params))

    def fetchone(self):
        return self._one_rows.pop(0)

    def fetchall(self):
        rows = self._many_rows
        self._many_rows = []
        return rows


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class AttendanceStorageTests(unittest.TestCase):
    def test_attendance_management_accepts_registered_operator(self):
        database = BotDatabase.__new__(BotDatabase)

        with patch.object(
            database,
            "_fetchone",
            return_value={"allowed": 1},
        ) as fetchone:
            allowed = database.can_manage_attendance(123, 456)

        self.assertTrue(allowed)
        sql, params = fetchone.call_args.args
        self.assertIn("assignment.scope_code IN (1, 2, 3, 4, 5)", sql)
        self.assertEqual(params, (123, 456, 456))

    def test_attendance_management_rejects_unassigned_user(self):
        database = BotDatabase.__new__(BotDatabase)

        with patch.object(database, "_fetchone", return_value=None):
            allowed = database.can_manage_attendance(123, 456)

        self.assertFalse(allowed)

    def test_attendance_participants_are_saved_with_two_bulk_statements(self):
        connection = FakeConnection()
        database = BotDatabase.__new__(BotDatabase)

        @contextmanager
        def connect():
            yield connection

        database.connect = connect

        def fake_execute_values(cursor, sql, argslist, **_):
            rows = list(argslist)
            cursor.bulk_calls.append((sql, rows))
            if "INSERT INTO users" in sql:
                cursor._many_rows = [
                    {
                        "user_id": index + 1,
                        "discord_id": int(values[1]),
                    }
                    for index, values in enumerate(rows)
                ]

        participants = [
            {
                "alliance_id": 1,
                "discord_id": 1000 + index,
                "discord_nickname": f"user-{index}",
            }
            for index in range(100)
        ]

        with patch("discord_bot.storage.execute_values", fake_execute_values):
            attendance_id = database.save_attendance_session(
                guild_id=123,
                started_at="2026-07-24 12:00:00",
                started_by_discord_id=999,
                participants=participants,
            )

        self.assertEqual(attendance_id, 77)
        self.assertTrue(connection.committed)
        self.assertEqual(len(connection.cursor_instance.execute_calls), 2)
        self.assertEqual(len(connection.cursor_instance.bulk_calls), 2)
        self.assertEqual(len(connection.cursor_instance.bulk_calls[0][1]), 100)
        self.assertEqual(len(connection.cursor_instance.bulk_calls[1][1]), 100)


if __name__ == "__main__":
    unittest.main()
