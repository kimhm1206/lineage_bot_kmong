from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NavItem:
    id: str
    label: str
    href: str
    icon: str
    description: str = ""
    badge: str = ""


@dataclass(frozen=True)
class NavGroup:
    id: str
    label: str
    icon: str
    items: tuple[NavItem, ...]


NAV_GROUPS: tuple[NavGroup, ...] = (
    NavGroup(
        id="home",
        label="홈",
        icon="layout-dashboard",
        items=(
            NavItem("home.personal", "본인 대시보드", "/", "user", "내 분배금과 마지막 활동"),
            NavItem("home.alliance", "내 혈맹 대시보드", "/#alliance-dashboard", "users", "혈맹 미정산과 혈비 요약"),
        ),
    ),
    NavGroup(
        id="drops",
        label="드랍 & 분배",
        icon="package-open",
        items=(
            NavItem("drops.personal", "분배금 현황", "#", "wallet", "개인 분배금 조회"),
            NavItem("drops.register", "드랍 등록", "#", "plus-square", "출석 회차와 아이템 연결"),
            NavItem("drops.alliance", "연합 분배", "#", "network", "혈맹별 1차 정산"),
            NavItem("drops.members", "혈맹 분배", "#", "list-check", "인원별 2차 정산"),
            NavItem("drops.settings", "분배 설정", "#", "sliders", "수수료와 분배 정책"),
        ),
    ),
    NavGroup(
        id="treasury",
        label="혈비",
        icon="landmark",
        items=(
            NavItem("treasury.summary", "혈비 현황", "#", "receipt", "잔액과 흐름"),
            NavItem("treasury.entries", "입출금 내역", "#", "rows", "수입/지출 기록"),
            NavItem("treasury.forfeits", "귀속 내역", "#", "archive", "미수령 귀속 기록"),
            NavItem("treasury.settings", "혈비 설정", "#", "settings", "카테고리와 공개 기준"),
        ),
    ),
    NavGroup(
        id="attendance",
        label="출석",
        icon="calendar-check",
        items=(
            NavItem("attendance.status", "출석 현황", "#", "activity", "회차별 출석 조회"),
            NavItem("attendance.stats", "출석 통계", "#", "bar-chart", "인원별/혈맹별 분석"),
            NavItem("attendance.alliance", "내 혈맹 통계", "#", "line-chart", "혈맹 내부 흐름"),
            NavItem("attendance.settings", "출석 설정", "#", "timer", "채널과 시간 설정"),
        ),
    ),
    NavGroup(
        id="items",
        label="아이템 & 입찰",
        icon="tag",
        items=(
            NavItem("items.prices", "아이템 시세", "#", "badge-won", "원화 기준 시세"),
            NavItem("items.bid", "입찰표", "#", "table", "혈맹별 입찰 상태"),
            NavItem("items.settings", "입찰 설정", "#", "gavel", "입찰 아이템 관리"),
        ),
    ),
    NavGroup(
        id="operations",
        label="서버 운영",
        icon="shield",
        items=(
            NavItem("operations.alliances", "혈맹 관리", "#", "users-round", "혈맹과 역할 매핑"),
            NavItem("operations.permissions", "권한 관리", "#", "key-round", "오너/경리/관리자"),
            NavItem("operations.notifications", "알림 관리", "#", "bell", "통계와 정산 알림"),
            NavItem("operations.audit", "작업 로그", "#", "clipboard-list", "관리 작업 이력"),
        ),
    ),
    NavGroup(
        id="settings",
        label="설정",
        icon="settings",
        items=(
            NavItem("settings.server", "서버 기본 설정", "#", "server", "공통 서버 값"),
            NavItem("settings.bot", "봇 설정", "#", "bot", "패널과 버전"),
            NavItem("settings.system", "시스템 설정", "#", "database", "점검과 개발자 도구"),
        ),
    ),
)


def get_navigation(active_item_id: str) -> list[dict[str, object]]:
    groups: list[dict[str, object]] = []
    for group in NAV_GROUPS:
        items = []
        group_is_active = False
        for item in group.items:
            is_active = item.id == active_item_id
            group_is_active = group_is_active or is_active
            items.append(
                {
                    "id": item.id,
                    "label": item.label,
                    "href": item.href,
                    "icon": item.icon,
                    "description": item.description,
                    "badge": item.badge,
                    "is_active": is_active,
                }
            )
        groups.append(
            {
                "id": group.id,
                "label": group.label,
                "icon": group.icon,
                "nav_items": items,
                "is_active": group_is_active,
            }
        )
    return groups
