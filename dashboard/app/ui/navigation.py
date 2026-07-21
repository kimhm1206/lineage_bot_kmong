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
    audience: str = ""
    description: str = ""
    tone: str = "common"


NAV_GROUPS: tuple[NavGroup, ...] = (
    NavGroup(
        id="home",
        label="홈",
        icon="layout-dashboard",
        items=(
            NavItem("home.personal", "내 현황", "/", "user", "분배금과 최근 활동"),
            NavItem("home.payments", "내 분배금", "/#personal-dashboard", "wallet", "수령 및 귀속 내역"),
        ),
        description="모든 구성원이 확인하는 개인 영역",
    ),
    NavGroup(
        id="alliance-operations",
        label="연합 운영",
        icon="network",
        items=(
            NavItem("alliance.dashboard", "연합 대시보드", "/#alliance-workspace", "layout-dashboard", "판매대금과 1차 정산"),
            NavItem("alliance.drops", "드랍 등록", "#", "plus-square", "출석 회차와 아이템 연결"),
            NavItem("alliance.settlement", "각혈 분배", "#", "network", "혈맹별 1차 정산"),
            NavItem("alliance.bidding", "아이템 입찰", "#", "gavel", "혈맹별 입찰 상태"),
            NavItem("alliance.items", "아이템 관리", "#", "tag", "시세와 입찰 아이템"),
            NavItem("alliance.settings", "연합 분배 설정", "#", "sliders", "경리·연합 수수료"),
        ),
        audience="연합 관리자",
        description="연합 전체 드랍과 혈맹별 1차 분배",
        tone="alliance",
    ),
    NavGroup(
        id="clan-operations",
        label="내 혈맹 운영",
        icon="landmark",
        items=(
            NavItem("clan.dashboard", "혈맹 대시보드", "/#clan-workspace", "layout-dashboard", "내 혈맹 정산 요약"),
            NavItem("clan.settlement", "혈맹원 분배", "#", "list-check", "인원별 2차 정산"),
            NavItem("clan.treasury", "혈비 가계부", "#", "receipt", "잔액과 입출금 흐름"),
            NavItem("clan.forfeits", "귀속 관리", "#", "archive", "미수령 분배금 귀속"),
            NavItem("clan.settings", "혈맹 분배 설정", "#", "settings", "혈비와 내부 수수료"),
            NavItem("clan.staff", "혈맹 경리 관리", "/settings/clan?section=staff#staff", "users-round", "혈맹 경리 지정", "관리자"),
            NavItem("clan.visibility", "정보 공개 설정", "/settings/clan?section=visibility#visibility", "eye", "일반 유저 공개 범위", "관리자"),
            NavItem("clan.permissions", "일반 유저 권한", "/settings/clan?section=permissions#user-permissions", "key-round", "조회와 기능 허용 범위", "관리자"),
        ),
        audience="각혈 관리자 · 경리",
        description="내 혈맹의 분배, 혈비와 공개 정책",
        tone="clan",
    ),
    NavGroup(
        id="attendance",
        label="출석 · 통계",
        icon="calendar-check",
        items=(
            NavItem("attendance.status", "출석 현황", "#", "activity", "회차별 출석 조회"),
            NavItem("attendance.stats", "출석 통계", "#", "bar-chart", "인원별/혈맹별 분석"),
            NavItem("attendance.alliance", "내 혈맹 통계", "#", "line-chart", "혈맹 내부 흐름"),
            NavItem("attendance.settings", "출석 설정", "#", "timer", "채널과 시간 설정"),
        ),
        description="공통 출석 기록과 분석",
    ),
    NavGroup(
        id="operations",
        label="서버 운영",
        icon="shield",
        items=(
            NavItem("operations.alliances", "혈맹 관리", "#", "users-round", "혈맹과 역할 매핑"),
            NavItem("operations.delegation", "운영 담당자 지정", "/settings/managers", "key-round", "관리자 유저 지정", "오너"),
            NavItem("operations.notifications", "알림 관리", "#", "bell", "통계와 정산 알림"),
            NavItem("operations.audit", "작업 로그", "#", "clipboard-list", "관리 작업 이력"),
            NavItem("settings.server", "서버 기본 설정", "#", "server", "공통 서버 값"),
            NavItem("settings.bot", "봇 설정", "#", "bot", "패널과 버전"),
            NavItem("settings.system", "시스템 설정", "#", "database", "점검과 개발자 도구"),
        ),
        audience="오너",
        description="권한과 서버 공통 설정",
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
                "audience": group.audience,
                "description": group.description,
                "tone": group.tone,
            }
        )
    return groups
