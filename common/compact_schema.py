from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROLE_CODES = {
    "user": 0,
    "admin": 1,
    "bookkeeper": 2,
    "owner": 3,
    "developer": 4,
    "viewer": 5,
}

ENTITY_TYPES = (
    (1, "attendance"),
    (2, "item"),
    (3, "loot"),
    (4, "bid_item"),
    (5, "payout"),
    (6, "treasury"),
)

ACTION_TYPES = (
    (1, "attendance_add", 1),
    (2, "attendance_delete", 1),
    (3, "item_create", 2),
    (4, "item_update", 2),
    (5, "item_delete", 2),
    (6, "loot_create", 3),
    (7, "loot_update", 3),
    (8, "loot_delete", 3),
    (9, "bid_item", 4),
    (10, "bid_item_delete", 4),
    (11, "bid_status", 4),
    (12, "payout_status", 5),
    (13, "treasury_deposit", 6),
    (14, "treasury_withdrawal", 6),
    (15, "treasury_reversal", 6),
)


COMPACT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE IF NOT EXISTS audit_entity_types (
    entity_type_id INTEGER PRIMARY KEY,
    entity_code TEXT NOT NULL UNIQUE
) STRICT;

CREATE TABLE IF NOT EXISTS audit_action_types (
    action_type_id INTEGER PRIMARY KEY,
    action_code TEXT NOT NULL UNIQUE,
    entity_type_id INTEGER NOT NULL REFERENCES audit_entity_types(entity_type_id)
) STRICT;

CREATE TABLE IF NOT EXISTS audit_actors (
    actor_id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id) ON DELETE SET NULL,
    discord_id INTEGER NOT NULL UNIQUE,
    fallback_name TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS audit_events (
    audit_event_id INTEGER PRIMARY KEY,
    guild_id INTEGER NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
    actor_id INTEGER REFERENCES audit_actors(actor_id) ON DELETE SET NULL,
    actor_role INTEGER NOT NULL DEFAULT 0,
    action_type_id INTEGER NOT NULL REFERENCES audit_action_types(action_type_id),
    target_id INTEGER,
    occurred_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

-- 삭제된 대상도 로그에서 ID가 남아야 하므로 문맥 ID에는 의도적으로 FK를 걸지 않는다.
CREATE TABLE IF NOT EXISTS audit_event_contexts (
    audit_event_id INTEGER PRIMARY KEY REFERENCES audit_events(audit_event_id) ON DELETE CASCADE,
    attendance_id INTEGER,
    user_id INTEGER,
    loot_event_id INTEGER,
    item_id INTEGER,
    bid_item_id INTEGER,
    alliance_id INTEGER,
    result_id INTEGER,
    state_code INTEGER,
    amount_value INTEGER
) STRICT;

CREATE INDEX IF NOT EXISTS idx_audit_events_guild_time
    ON audit_events(guild_id, occurred_at DESC, audit_event_id DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_guild_action_time
    ON audit_events(guild_id, action_type_id, occurred_at DESC, audit_event_id DESC);
CREATE INDEX IF NOT EXISTS idx_audit_context_attendance
    ON audit_event_contexts(attendance_id) WHERE attendance_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_audit_context_loot
    ON audit_event_contexts(loot_event_id) WHERE loot_event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_audit_context_user
    ON audit_event_contexts(user_id) WHERE user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS catalog_item_versions (
    item_version_id INTEGER PRIMARY KEY,
    item_id INTEGER NOT NULL REFERENCES items(item_id) ON DELETE RESTRICT,
    item_name TEXT NOT NULL,
    valid_from INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE INDEX IF NOT EXISTS idx_item_versions_item_time
    ON catalog_item_versions(item_id, valid_from DESC, item_version_id DESC);

CREATE TABLE IF NOT EXISTS settlement_drops (
    drop_id INTEGER PRIMARY KEY,
    guild_id INTEGER NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
    attendance_id INTEGER NOT NULL REFERENCES attendance_sessions(attendance_id) ON DELETE RESTRICT,
    item_version_id INTEGER NOT NULL REFERENCES catalog_item_versions(item_version_id) ON DELETE RESTRICT,
    cash_price_krw INTEGER NOT NULL CHECK(cash_price_krw >= 0),
    adena_market_rate INTEGER NOT NULL CHECK(adena_market_rate > 0),
    gross_adena INTEGER NOT NULL CHECK(gross_adena >= 0),
    occurred_at INTEGER NOT NULL,
    created_by_user_id INTEGER REFERENCES users(user_id) ON DELETE SET NULL
) STRICT;

CREATE TABLE IF NOT EXISTS settlement_drop_participants (
    drop_id INTEGER NOT NULL REFERENCES settlement_drops(drop_id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(user_id) ON DELETE RESTRICT,
    alliance_id INTEGER REFERENCES alliances(alliance_id) ON DELETE RESTRICT,
    PRIMARY KEY(drop_id, user_id)
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS settlement_drop_excluded_alliances (
    drop_id INTEGER NOT NULL REFERENCES settlement_drops(drop_id) ON DELETE CASCADE,
    alliance_id INTEGER NOT NULL REFERENCES alliances(alliance_id) ON DELETE RESTRICT,
    PRIMARY KEY(drop_id, alliance_id)
) WITHOUT ROWID, STRICT;

CREATE INDEX IF NOT EXISTS idx_settlement_drops_guild_time
    ON settlement_drops(guild_id, occurred_at DESC, drop_id DESC);
CREATE INDEX IF NOT EXISTS idx_drop_participants_user
    ON settlement_drop_participants(user_id, drop_id);
CREATE INDEX IF NOT EXISTS idx_drop_participants_alliance
    ON settlement_drop_participants(alliance_id, drop_id)
    WHERE alliance_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS settlement_fee_rules (
    fee_rule_id INTEGER PRIMARY KEY,
    guild_id INTEGER NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
    alliance_id INTEGER REFERENCES alliances(alliance_id) ON DELETE CASCADE,
    scope_code INTEGER NOT NULL CHECK(scope_code IN (1, 2)),
    is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0, 1))
) STRICT;

CREATE TABLE IF NOT EXISTS settlement_fee_rule_versions (
    fee_rule_version_id INTEGER PRIMARY KEY,
    fee_rule_id INTEGER NOT NULL REFERENCES settlement_fee_rules(fee_rule_id) ON DELETE CASCADE,
    rule_name TEXT NOT NULL,
    rate_ppm INTEGER NOT NULL CHECK(rate_ppm BETWEEN 0 AND 1000000),
    valid_from INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE INDEX IF NOT EXISTS idx_fee_rule_versions_rule_time
    ON settlement_fee_rule_versions(fee_rule_id, valid_from DESC, fee_rule_version_id DESC);

-- 혈맹·유저·수수료를 모두 같은 정산 객체로 취급한다.
CREATE TABLE IF NOT EXISTS settlement_payout_objects (
    payout_object_id INTEGER PRIMARY KEY,
    drop_id INTEGER NOT NULL REFERENCES settlement_drops(drop_id) ON DELETE CASCADE,
    parent_payout_object_id INTEGER REFERENCES settlement_payout_objects(payout_object_id) ON DELETE CASCADE,
    object_code INTEGER NOT NULL CHECK(object_code IN (1, 2, 3)),
    recipient_alliance_id INTEGER REFERENCES alliances(alliance_id),
    recipient_user_id INTEGER REFERENCES users(user_id),
    fee_rule_version_id INTEGER REFERENCES settlement_fee_rule_versions(fee_rule_version_id),
    amount_adena INTEGER NOT NULL CHECK(amount_adena >= 0),
    status_code INTEGER NOT NULL DEFAULT 0 CHECK(status_code IN (0, 1, 2)),
    completed_at INTEGER,
    completed_by_user_id INTEGER REFERENCES users(user_id),
    CHECK(
        (object_code = 1 AND recipient_alliance_id IS NOT NULL AND recipient_user_id IS NULL AND fee_rule_version_id IS NULL)
        OR (object_code = 2 AND recipient_user_id IS NOT NULL AND fee_rule_version_id IS NULL)
        OR (object_code = 3 AND fee_rule_version_id IS NOT NULL)
    )
) STRICT;

CREATE INDEX IF NOT EXISTS idx_payout_objects_drop_status
    ON settlement_payout_objects(drop_id, status_code);
CREATE INDEX IF NOT EXISTS idx_payout_objects_alliance_status
    ON settlement_payout_objects(recipient_alliance_id, status_code)
    WHERE recipient_alliance_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_payout_objects_user_status
    ON settlement_payout_objects(recipient_user_id, status_code)
    WHERE recipient_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_payout_objects_parent
    ON settlement_payout_objects(parent_payout_object_id)
    WHERE parent_payout_object_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS treasury_accounts (
    treasury_account_id INTEGER PRIMARY KEY,
    guild_id INTEGER NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
    alliance_id INTEGER NOT NULL REFERENCES alliances(alliance_id) ON DELETE CASCADE,
    current_balance INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
    UNIQUE(guild_id, alliance_id)
) STRICT;

CREATE TABLE IF NOT EXISTS treasury_categories (
    treasury_category_id INTEGER PRIMARY KEY,
    guild_id INTEGER NOT NULL REFERENCES guilds(guild_id) ON DELETE CASCADE,
    direction INTEGER NOT NULL CHECK(direction IN (-1, 1)),
    category_name TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0, 1)),
    UNIQUE(guild_id, direction, category_name)
) STRICT;

CREATE TABLE IF NOT EXISTS treasury_source_types (
    source_type_id INTEGER PRIMARY KEY,
    source_code TEXT NOT NULL UNIQUE
) STRICT;

CREATE TABLE IF NOT EXISTS treasury_entries (
    treasury_entry_id INTEGER PRIMARY KEY,
    treasury_account_id INTEGER NOT NULL REFERENCES treasury_accounts(treasury_account_id) ON DELETE RESTRICT,
    treasury_category_id INTEGER REFERENCES treasury_categories(treasury_category_id) ON DELETE RESTRICT,
    direction INTEGER NOT NULL CHECK(direction IN (-1, 1)),
    amount_adena INTEGER NOT NULL CHECK(amount_adena > 0),
    balance_after INTEGER NOT NULL,
    source_type_id INTEGER NOT NULL REFERENCES treasury_source_types(source_type_id),
    source_id INTEGER,
    memo TEXT,
    occurred_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    created_by_user_id INTEGER REFERENCES users(user_id) ON DELETE SET NULL,
    reversal_of_entry_id INTEGER REFERENCES treasury_entries(treasury_entry_id) ON DELETE RESTRICT,
    UNIQUE(reversal_of_entry_id)
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_treasury_source_once
    ON treasury_entries(treasury_account_id, source_type_id, source_id)
    WHERE source_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_treasury_entries_account_time
    ON treasury_entries(treasury_account_id, occurred_at DESC, treasury_entry_id DESC);
CREATE INDEX IF NOT EXISTS idx_treasury_entries_category_time
    ON treasury_entries(treasury_category_id, occurred_at DESC)
    WHERE treasury_category_id IS NOT NULL;

CREATE TRIGGER IF NOT EXISTS trg_treasury_entry_validate
BEFORE INSERT ON treasury_entries
BEGIN
    SELECT CASE
        WHEN NEW.balance_after != (
            SELECT current_balance + (NEW.direction * NEW.amount_adena)
            FROM treasury_accounts
            WHERE treasury_account_id = NEW.treasury_account_id
        )
        THEN RAISE(ABORT, 'treasury balance mismatch')
    END;
END;

CREATE TRIGGER IF NOT EXISTS trg_treasury_entry_apply
AFTER INSERT ON treasury_entries
BEGIN
    UPDATE treasury_accounts
    SET current_balance = NEW.balance_after,
        updated_at = unixepoch()
    WHERE treasury_account_id = NEW.treasury_account_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_treasury_entry_no_update
BEFORE UPDATE ON treasury_entries
BEGIN
    SELECT RAISE(ABORT, 'treasury entries are append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_treasury_entry_no_delete
BEFORE DELETE ON treasury_entries
BEGIN
    SELECT RAISE(ABORT, 'treasury entries are append-only');
END;
"""


TREASURY_SOURCE_TYPES = (
    (1, "manual"),
    (2, "member_forfeiture"),
    (3, "member_fee"),
    (4, "alliance_fee"),
    (5, "adjustment"),
    (6, "reversal"),
)


def _epoch(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        return int(datetime.now(timezone.utc).timestamp())
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _integer(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _context_from_legacy(row: sqlite3.Row) -> dict[str, int | None]:
    try:
        details = json.loads(str(row["details_json"] or "{}"))
    except (TypeError, json.JSONDecodeError):
        details = {}
    if not isinstance(details, dict):
        details = {}
    nested_item = details.get("item")
    if not isinstance(nested_item, dict):
        nested_item = {}
    action = str(row["action_type"])
    target_id = _integer(row["target_id"])
    state_code = _integer(details.get("is_completed"))
    if state_code is None and action == "bid_item":
        state_code = 1 if "수정" in str(row["summary"] or "") else 0
    amount = details.get("default_price")
    try:
        amount_value = int(float(amount)) if amount not in (None, "") else None
    except (TypeError, ValueError):
        amount_value = None
    return {
        "attendance_id": _integer(details.get("attendance_id")),
        "user_id": _integer(details.get("user_id")),
        "loot_event_id": _integer(details.get("loot_event_id"))
        or (target_id if row["target_type"] == "loot" else None),
        "item_id": _integer(details.get("item_id"))
        or (target_id if row["target_type"] == "item" else None),
        "bid_item_id": _integer(details.get("bid_item_id"))
        or _integer(nested_item.get("bid_item_id"))
        or (target_id if row["target_type"] == "bid_item" else None),
        "alliance_id": _integer(details.get("alliance_id")),
        "result_id": _integer(details.get("result_id")),
        "state_code": state_code,
        "amount_value": amount_value,
    }


def _seed_lookups(connection: sqlite3.Connection) -> None:
    connection.executemany(
        "INSERT OR IGNORE INTO audit_entity_types(entity_type_id, entity_code) VALUES (?, ?)",
        ENTITY_TYPES,
    )
    connection.executemany(
        """
        INSERT OR IGNORE INTO audit_action_types(action_type_id, action_code, entity_type_id)
        VALUES (?, ?, ?)
        """,
        ACTION_TYPES,
    )
    connection.executemany(
        "INSERT OR IGNORE INTO treasury_source_types(source_type_id, source_code) VALUES (?, ?)",
        TREASURY_SOURCE_TYPES,
    )


def _migrate_legacy_work_logs(connection: sqlite3.Connection) -> int:
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='work_logs'"
    ).fetchone()
    if exists is None:
        return 0
    connection.row_factory = sqlite3.Row
    rows = connection.execute("SELECT * FROM work_logs ORDER BY work_log_id").fetchall()
    for row in rows:
        discord_id = _integer(row["actor_discord_id"])
        actor_id = None
        if discord_id is not None:
            user_row = connection.execute(
                "SELECT user_id FROM users WHERE discord_id = ?",
                (discord_id,),
            ).fetchone()
            user_id = int(user_row["user_id"]) if user_row else None
            connection.execute(
                """
                INSERT INTO audit_actors(user_id, discord_id, fallback_name)
                VALUES (?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET
                    user_id = COALESCE(excluded.user_id, audit_actors.user_id),
                    fallback_name = excluded.fallback_name
                """,
                (user_id, discord_id, str(row["actor_display_name"] or "")),
            )
            actor_id = int(
                connection.execute(
                    "SELECT actor_id FROM audit_actors WHERE discord_id = ?",
                    (discord_id,),
                ).fetchone()["actor_id"]
            )
        action_row = connection.execute(
            "SELECT action_type_id FROM audit_action_types WHERE action_code = ?",
            (str(row["action_type"]),),
        ).fetchone()
        if action_row is None:
            entity_row = connection.execute(
                "SELECT entity_type_id FROM audit_entity_types WHERE entity_code = ?",
                (str(row["target_type"]),),
            ).fetchone()
            if entity_row is None:
                next_entity_id = int(
                    connection.execute(
                        "SELECT COALESCE(MAX(entity_type_id), 0) + 1 FROM audit_entity_types"
                    ).fetchone()[0]
                )
                connection.execute(
                    "INSERT INTO audit_entity_types(entity_type_id, entity_code) VALUES (?, ?)",
                    (next_entity_id, str(row["target_type"])),
                )
                entity_type_id = next_entity_id
            else:
                entity_type_id = int(entity_row["entity_type_id"])
            next_action_id = int(
                connection.execute(
                    "SELECT COALESCE(MAX(action_type_id), 0) + 1 FROM audit_action_types"
                ).fetchone()[0]
            )
            connection.execute(
                "INSERT INTO audit_action_types(action_type_id, action_code, entity_type_id) VALUES (?, ?, ?)",
                (next_action_id, str(row["action_type"]), entity_type_id),
            )
            action_type_id = next_action_id
        else:
            action_type_id = int(action_row["action_type_id"])
        connection.execute(
            """
            INSERT OR IGNORE INTO audit_events(
                audit_event_id, guild_id, actor_id, actor_role,
                action_type_id, target_id, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(row["work_log_id"]),
                int(row["guild_id"]),
                actor_id,
                ROLE_CODES.get(str(row["actor_role"]), 0),
                action_type_id,
                _integer(row["target_id"]),
                _epoch(row["created_at"]),
            ),
        )
        context = _context_from_legacy(row)
        connection.execute(
            """
            INSERT OR IGNORE INTO audit_event_contexts(
                audit_event_id, attendance_id, user_id, loot_event_id, item_id,
                bid_item_id, alliance_id, result_id, state_code, amount_value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(row["work_log_id"]), *context.values()),
        )
    missing = connection.execute(
        """
        SELECT COUNT(*)
        FROM work_logs wl
        LEFT JOIN audit_events ae ON ae.audit_event_id = wl.work_log_id
        WHERE ae.audit_event_id IS NULL
        """
    ).fetchone()[0]
    if int(missing) != 0:
        raise RuntimeError(f"감사 로그 전환 누락: {missing}건")
    connection.execute("DROP TABLE work_logs")
    return len(rows)


def ensure_compact_schema(database_path: str | Path) -> int:
    path = Path(database_path)
    connection = sqlite3.connect(path, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        connection.executescript("BEGIN IMMEDIATE;\n" + COMPACT_SCHEMA_SQL)
        _seed_lookups(connection)
        migrated = _migrate_legacy_work_logs(connection)
        connection.execute(
            "INSERT OR IGNORE INTO schema_migrations(version) VALUES (2)"
        )
        connection.commit()
        return migrated
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
