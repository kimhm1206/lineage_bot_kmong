from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NavItem:
    id: str
    label: str
    href: str
    icon: str
    description: str = ""


@dataclass(frozen=True)
class NavGroup:
    id: str
    label: str
    icon: str
    items: tuple[NavItem, ...]
    description: str = ""
    tone: str = "common"
    developer_only: bool = False


NAV_GROUPS: tuple[NavGroup, ...] = (
    NavGroup(
        id="home",
        label="홈",
        icon="layout-dashboard",
        items=(
            NavItem("home.personal", "내 현황", "/", "user", "분배금과 최근 활동"),
        ),
        description="나의 분배금과 최근 활동",
    ),
    NavGroup(
        id="alliance-operations",
        label="연합 운영",
        icon="network",
        items=(
            NavItem("alliance.drops", "드랍 등록", "/alliance/drops", "plus-square", "출석 회차와 아이템 연결"),
            NavItem("alliance.settlement", "각혈 분배", "/alliance/settlements", "network", "혈맹별 1차 정산"),
            NavItem("alliance.treasury", "연합비 가계부", "/alliance/treasury", "receipt", "연합 전체 입출금과 잔액"),
            NavItem("alliance.bidding", "아이템 입찰", "/alliance/bidding", "gavel", "혈맹별 입찰 상태"),
            NavItem("alliance.items", "아이템 관리", "/alliance/items", "tag", "시세와 입찰 아이템"),
            NavItem("alliance.settings", "연합 분배 설정", "/alliance/settings", "sliders", "경리·연합 수수료"),
        ),
        description="드랍, 판매와 혈맹별 1차 분배",
        tone="alliance",
    ),
    NavGroup(
        id="clan-operations",
        label="내 혈맹 운영",
        icon="landmark",
        items=(
            NavItem("clan.settlement", "혈맹원 분배", "/clan/settlements", "list-check", "인원별 2차 정산"),
            NavItem("clan.treasury", "혈비 가계부", "/clan/treasury", "receipt", "잔액과 입출금 흐름"),
            NavItem("clan.forfeits", "귀속 관리", "/clan/forfeits", "archive", "미수령 분배금 귀속"),
            NavItem("clan.settings", "혈맹 분배 설정", "/clan/settings", "settings", "혈비와 내부 수수료"),
            NavItem("clan.staff", "혈맹 운영 설정", "/settings/clan", "users-round", "경리와 공개 범위"),
        ),
        description="혈맹원 분배, 혈비와 귀속 관리",
        tone="clan",
    ),
    NavGroup(
        id="attendance",
        label="출석 · 통계",
        icon="calendar-check",
        items=(
            NavItem("attendance.status", "출석 현황", "/attendance/status", "activity", "회차별 출석 조회"),
            NavItem("attendance.stats", "출석 통계", "/attendance/statistics", "bar-chart", "인원별/혈맹별 분석"),
            NavItem("attendance.alliance", "내 혈맹 통계", "/attendance/clan", "line-chart", "혈맹 내부 흐름"),
            NavItem("attendance.settings", "출석 설정", "/settings/attendance", "timer", "채널과 시간 설정"),
        ),
        description="공통 출석 기록과 분석",
    ),
    NavGroup(
        id="operations",
        label="서버 운영",
        icon="shield",
        items=(
            NavItem("operations.alliances", "혈맹 관리", "/settings/alliances", "users-round", "혈맹과 역할 매핑"),
            NavItem("operations.delegation", "운영 담당자 지정", "/settings/managers", "key-round", "관리자 유저 지정"),
            NavItem("operations.notifications", "알림 관리", "/operations/notifications", "bell", "통계와 정산 알림"),
            NavItem("operations.audit", "작업 로그", "/operations/audit", "clipboard-list", "관리 작업 이력"),
        ),
        description="혈맹, 담당자와 운영 기록",
    ),
    NavGroup(
        id="developer",
        label="개발자 도구",
        icon="code",
        items=(
            NavItem("developer.server", "서버 기본 설정", "/settings/server", "server", "서버 등록과 활성 상태"),
            NavItem("developer.bot", "봇 연동", "/developer/bot", "bot", "Discord REST 연결 상태"),
            NavItem("developer.system", "시스템 점검", "/developer/system", "database", "DB 구조와 데이터 상태"),
        ),
        description="연결 정보와 시스템 진단",
        tone="developer",
        developer_only=True,
    ),
)


def get_navigation(active_item_id: str, *, developer_access: bool = False) -> list[dict[str, object]]:
    groups: list[dict[str, object]] = []
    for group in NAV_GROUPS:
        if group.developer_only and not developer_access:
            continue
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
                "description": group.description,
                "tone": group.tone,
            }
        )
    return groups
