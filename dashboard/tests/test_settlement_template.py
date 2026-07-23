from pathlib import Path


TEMPLATE = (
    Path(__file__).parents[1]
    / "app"
    / "templates"
    / "pages"
    / "operations"
    / "settlements.html"
)


def test_alliance_settlement_has_no_period_selector() -> None:
    source = TEMPLATE.read_text(encoding="utf-8")
    assert 'name="period"' not in source
    assert "현재 지급할 금액" in source


def test_clan_details_respect_access_mode() -> None:
    source = TEMPLATE.read_text(encoding="utf-8")
    assert "can_view_clan_details" in source
