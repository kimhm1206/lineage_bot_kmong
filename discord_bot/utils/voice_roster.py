from __future__ import annotations

import unicodedata
from collections.abc import Iterable


CLASS_LABELS = ("요정", "법사", "기사")
UNCLASSIFIED_LABEL = "미분류"


def classify_display_name(display_name: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(display_name or ""))
    matches = [
        (normalized.find(label), index, label)
        for index, label in enumerate(CLASS_LABELS)
        if normalized.find(label) >= 0
    ]
    if not matches:
        return UNCLASSIFIED_LABEL
    return min(matches)[2]


def group_display_names(display_names: Iterable[str]) -> dict[str, list[str]]:
    groups = {label: [] for label in (*CLASS_LABELS, UNCLASSIFIED_LABEL)}
    for display_name in sorted(
        (str(name or "").strip() for name in display_names),
        key=str.casefold,
    ):
        if not display_name:
            continue
        groups[classify_display_name(display_name)].append(display_name)
    return groups


def compact_member_list(
    display_names: list[str],
    *,
    max_characters: int = 900,
) -> tuple[str, bool]:
    if not display_names:
        return "-", False

    lines: list[str] = []
    current_length = 0
    for display_name in display_names:
        line = f"• {display_name}"
        next_length = current_length + len(line) + (1 if lines else 0)
        if lines and next_length > max_characters:
            remaining = len(display_names) - len(lines)
            lines.append(f"… 외 {remaining}명")
            return "\n".join(lines), True
        lines.append(line)
        current_length = next_length
    return "\n".join(lines), False


def full_roster_text(
    guild_name: str,
    channel_names: Iterable[str],
    groups: dict[str, list[str]],
) -> str:
    lines = [
        "연합보탐 클래스 현황",
        f"서버: {guild_name}",
        f"대상 음성채널: {', '.join(channel_names) or '-'}",
        f"총 인원: {sum(len(names) for names in groups.values())}명",
        "",
    ]
    for label in (*CLASS_LABELS, UNCLASSIFIED_LABEL):
        names = groups.get(label) or []
        lines.append(f"[{label}] {len(names)}명")
        lines.extend(f"- {name}" for name in names)
        if not names:
            lines.append("-")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
