from __future__ import annotations

import json
import re
import sqlite3
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


JSON_COLUMN_NAMES = {
    "attendance_voice_channel_ids",
    "details_json",
    "excluded_alliance_ids",
    "payload_json",
    "query_json",
    "render_json",
    "result_json",
    "rule_json",
    "schedule_json",
}


class Json:
    """Small psycopg2.extras.Json-compatible value wrapper for SQLite."""

    def __init__(self, adapted: Any):
        self.adapted = adapted


def _adapt_json(value: Json) -> str:
    return json.dumps(value.adapted, ensure_ascii=False, separators=(",", ":"))


sqlite3.register_adapter(Decimal, str)
sqlite3.register_adapter(datetime, lambda value: value.isoformat(sep=" "))
sqlite3.register_adapter(date, lambda value: value.isoformat())
sqlite3.register_adapter(Json, _adapt_json)


def _row_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> sqlite3.Row:
    # sqlite3.Row gives the same mapping/index access shape used by RealDictCursor.
    return sqlite3.Row(cursor, row)


def _decode_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    decoded = dict(row)
    for column_name in JSON_COLUMN_NAMES.intersection(decoded):
        value = decoded[column_name]
        if not isinstance(value, str) or not value:
            continue
        try:
            decoded[column_name] = json.loads(value)
        except json.JSONDecodeError:
            pass
    return decoded


_PLACEHOLDER = re.compile(r"%s")
_PARAM_MARKER = re.compile(r"__SQLITE_PARAM_(\d+)__")
_NOT_ANY = re.compile(
    r"NOT\s*\(\s*([A-Za-z_][A-Za-z0-9_.]*)\s*=\s*"
    r"ANY\(\s*(__SQLITE_PARAM_\d+__)(?:::[A-Za-z0-9_\[\]]+)?\s*\)\s*\)",
    flags=re.IGNORECASE,
)
_ANY = re.compile(
    r"([A-Za-z_][A-Za-z0-9_.]*)\s*=\s*"
    r"ANY\(\s*(__SQLITE_PARAM_\d+__)(?:::[A-Za-z0-9_\[\]]+)?\s*\)",
    flags=re.IGNORECASE,
)


def _translate_sql(sql: str, params: tuple[Any, ...]) -> tuple[str, tuple[Any, ...]]:
    marker_index = 0

    def mark_parameter(_: re.Match[str]) -> str:
        nonlocal marker_index
        marker = f"__SQLITE_PARAM_{marker_index}__"
        marker_index += 1
        return marker

    translated = _PLACEHOLDER.sub(mark_parameter, sql)
    if marker_index != len(params):
        raise ValueError(
            f"SQL parameter mismatch: query expects {marker_index}, received {len(params)}"
        )

    translated = _NOT_ANY.sub(r"\1 NOT IN (\2)", translated)
    translated = _ANY.sub(r"\1 IN (\2)", translated)
    translated = re.sub(r"\bILIKE\b", "LIKE", translated, flags=re.IGNORECASE)
    translated = re.sub(
        r"('(?:''|[^'])*')::(?:text|bigint|integer|numeric|boolean)",
        r"\1",
        translated,
        flags=re.IGNORECASE,
    )
    translated = re.sub(
        r"([A-Za-z_][A-Za-z0-9_.]*)::text\b",
        r"CAST(\1 AS TEXT)",
        translated,
        flags=re.IGNORECASE,
    )
    translated = re.sub(
        r"(__SQLITE_PARAM_\d+__)::[A-Za-z0-9_\[\]]+",
        r"\1",
        translated,
        flags=re.IGNORECASE,
    )
    translated = re.sub(
        r"\s+FOR\s+UPDATE(?:\s+SKIP\s+LOCKED)?",
        "",
        translated,
        flags=re.IGNORECASE,
    )

    output_params: list[Any] = []

    def bind_parameter(match: re.Match[str]) -> str:
        value = params[int(match.group(1))]
        if isinstance(value, (list, tuple, set)):
            values = list(value)
            if not values:
                return "NULL"
            output_params.extend(_normalize_param(item) for item in values)
            return ", ".join("?" for _ in values)
        output_params.append(_normalize_param(value))
        return "?"

    translated = _PARAM_MARKER.sub(bind_parameter, translated)
    return translated, tuple(output_params)


def _normalize_param(value: Any) -> Any:
    if isinstance(value, Json):
        return _adapt_json(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return value


class SQLiteCursor:
    def __init__(self, cursor: sqlite3.Cursor):
        self._cursor = cursor

    def __enter__(self) -> SQLiteCursor:
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def description(self) -> Any:
        return self._cursor.description

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> SQLiteCursor:
        translated_sql, translated_params = _translate_sql(sql, tuple(params))
        self._cursor.execute(translated_sql, translated_params)
        return self

    def executemany(
        self,
        sql: str,
        params: list[tuple[Any, ...]] | tuple[tuple[Any, ...], ...],
    ) -> SQLiteCursor:
        if not params:
            return self
        translated_rows: list[tuple[Any, ...]] = []
        translated_sql = ""
        for row in params:
            row_sql, row_params = _translate_sql(sql, tuple(row))
            if translated_sql and translated_sql != row_sql:
                raise ValueError("SQLite executemany requires stable parameter shapes")
            translated_sql = row_sql
            translated_rows.append(row_params)
        self._cursor.executemany(translated_sql, translated_rows)
        return self

    def fetchone(self) -> dict[str, Any] | None:
        return _decode_row(self._cursor.fetchone())

    def fetchall(self) -> list[dict[str, Any]]:
        return [row for raw in self._cursor.fetchall() if (row := _decode_row(raw))]

    def close(self) -> None:
        self._cursor.close()


class SQLiteConnection:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._connection = sqlite3.connect(path, timeout=30.0)
        self._connection.row_factory = _row_factory
        self._connection.execute("PRAGMA busy_timeout=30000")
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.create_function(
            "LEFT",
            2,
            lambda value, length: str(value or "")[: int(length)],
        )

    def __enter__(self) -> SQLiteConnection:
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()

    def cursor(self) -> SQLiteCursor:
        return SQLiteCursor(self._connection.cursor())

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> SQLiteCursor:
        cursor = self.cursor()
        return cursor.execute(sql, params)

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


def connect_sqlite(path: Path) -> SQLiteConnection:
    return SQLiteConnection(path)
