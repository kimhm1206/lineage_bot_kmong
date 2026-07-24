from dashboard.app.services.workspace_store import (
    audit_target_label,
    normalize_audit_type,
)


def test_audit_type_falls_back_to_all() -> None:
    assert normalize_audit_type("attendance") == "attendance"
    assert normalize_audit_type("unknown") == "all"


def test_attendance_target_includes_user_id() -> None:
    assert audit_target_label(
        {
            "entity_code": "attendance",
            "target_id": 1574,
            "attendance_id": 1574,
            "target_user_id": 873,
            "target_user_name": "[인연]천둥산/기사",
        }
    ) == "출석 #1574 · [인연]천둥산/기사 · 유저 #873"


def test_generic_target_keeps_entity_and_id() -> None:
    assert audit_target_label(
        {
            "entity_code": "item",
            "target_id": 48,
        }
    ) == "item #48"
