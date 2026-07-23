from dashboard.app.services.audit_service import (
    AuditActor,
    actor_role_code,
    bind_actor,
    current_actor,
    reset_actor,
)


def test_actor_context_is_request_scoped() -> None:
    actor = AuditActor(
        discord_id=123,
        display_name="테스트 작업자",
        access_role="alliance_manager",
    )
    token = bind_actor(actor)
    try:
        assert current_actor() == actor
        assert actor_role_code(current_actor().access_role) == 3
    finally:
        reset_actor(token)
    assert current_actor() is None


def test_unknown_role_is_logged_as_user() -> None:
    assert actor_role_code("unknown") == 6
