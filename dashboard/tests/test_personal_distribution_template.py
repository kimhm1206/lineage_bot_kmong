from pathlib import Path


TEMPLATE = (
    Path(__file__).parents[1]
    / "app"
    / "templates"
    / "pages"
    / "operations"
    / "personal.html"
)


def test_estimate_is_visually_separate_from_confirmed_distributions() -> None:
    source = TEMPLATE.read_text(encoding="utf-8")
    assert "예상 분배금" in source
    assert "실제 지급 금액이 아닙니다" in source
    assert "확정 분배 내역" in source
