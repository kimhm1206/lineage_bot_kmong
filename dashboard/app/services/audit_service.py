from __future__ import annotations

import time
from contextvars import ContextVar, Token
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


ROLE_CODES = {
    "developer": 1,
    "owner": 2,
    "alliance_manager": 3,
    "clan_manager": 4,
    "clan_accountant": 5,
    "user": 6,
}


@dataclass(frozen=True)
class AuditActor:
    discord_id: int
    display_name: str
    access_role: str


_actor_context: ContextVar[AuditActor | None] = ContextVar(
    "dashboard_audit_actor",
    default=None,
)


def bind_actor(actor: AuditActor) -> Token[AuditActor | None]:
    return _actor_context.set(actor)


def reset_actor(token: Token[AuditActor | None]) -> None:
    _actor_context.reset(token)


def current_actor() -> AuditActor | None:
    return _actor_context.get()


def actor_role_code(access_role: str | None) -> int:
    return ROLE_CODES.get(
        str(access_role or "").strip().lower(),
        ROLE_CODES["user"],
    )


async def record_event(
    session: AsyncSession,
    *,
    guild_id: int,
    action_code: str,
    target_id: int | None,
    attendance_id: int | None = None,
    user_id: int | None = None,
    item_id: int | None = None,
    alliance_id: int | None = None,
    state_code: int | None = None,
    amount_value: int | None = None,
) -> None:
    action_type_id = await session.scalar(
        text(
            "SELECT action_type_id FROM audit_action_types "
            "WHERE action_code = :code"
        ),
        {"code": action_code},
    )
    if action_type_id is None:
        return

    actor = current_actor()
    actor_id: int | None = None
    actor_role = actor_role_code(actor.access_role if actor else None)
    if actor is not None and actor.discord_id > 0:
        actor_id = await session.scalar(
            text("""
                INSERT INTO audit_actors (
                    user_id, discord_id, fallback_name
                ) VALUES (
                    (
                        SELECT user_id
                        FROM users
                        WHERE discord_id = :discord_id
                        ORDER BY updated_at DESC, user_id DESC
                        LIMIT 1
                    ),
                    :discord_id,
                    :fallback_name
                )
                ON CONFLICT (discord_id) DO UPDATE SET
                    user_id = COALESCE(
                        EXCLUDED.user_id,
                        audit_actors.user_id
                    ),
                    fallback_name = EXCLUDED.fallback_name
                RETURNING actor_id
            """),
            {
                "discord_id": actor.discord_id,
                "fallback_name": actor.display_name or str(actor.discord_id),
            },
        )

    audit_event_id = await session.scalar(
        text("""
            INSERT INTO audit_events (
                guild_id, actor_id, actor_role, action_type_id,
                target_id, occurred_at
            ) VALUES (
                :guild_id, :actor_id, :actor_role, :action_type_id,
                :target_id, :occurred_at
            )
            RETURNING audit_event_id
        """),
        {
            "guild_id": guild_id,
            "actor_id": actor_id,
            "actor_role": actor_role,
            "action_type_id": int(action_type_id),
            "target_id": target_id,
            "occurred_at": int(time.time()),
        },
    )
    await session.execute(
        text("""
            INSERT INTO audit_event_contexts (
                audit_event_id, attendance_id, user_id, loot_event_id,
                item_id, alliance_id, state_code, amount_value
            ) VALUES (
                :audit_event_id, :attendance_id, :user_id, :loot_event_id,
                :item_id, :alliance_id, :state_code, :amount_value
            )
        """),
        {
            "audit_event_id": int(audit_event_id),
            "attendance_id": attendance_id,
            "user_id": user_id,
            "loot_event_id": (
                target_id
                if action_code.startswith(("loot_", "sale_"))
                else None
            ),
            "item_id": item_id,
            "alliance_id": alliance_id,
            "state_code": state_code,
            "amount_value": amount_value,
        },
    )
