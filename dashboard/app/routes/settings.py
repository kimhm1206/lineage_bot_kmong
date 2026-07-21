from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

from dashboard.app.config import BASE_DIR
from dashboard.app.ui.context import build_template_context


router = APIRouter(prefix="/settings", tags=["settings"])
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))


@router.get("/managers")
async def manager_settings(request: Request):
    context = build_template_context(
        request,
        active_nav="operations.delegation",
        page_title="운영 담당자 설정",
        page_description="서버 구성원 중 연합 관리자와 각혈 관리자를 유저 단위로 지정합니다.",
        page_kicker="User assignments",
        page_badge="OWNER",
    )
    context.update(
        {
            "manager_groups": [
                {
                    "tone": "alliance",
                    "icon": "network",
                    "title": "연합 관리자",
                    "description": "드랍 등록, 각혈 분배, 아이템 입찰과 연합 수수료를 담당합니다.",
                    "scope": ["연합 대시보드", "드랍 등록", "각혈 분배", "아이템 관리"],
                    "action": "연합 관리자 유저 지정",
                },
                {
                    "tone": "clan",
                    "icon": "landmark",
                    "title": "각혈 관리자",
                    "description": "담당 혈맹의 경리, 정보 공개 범위와 일반 유저 권한을 관리합니다.",
                    "scope": ["혈맹 경리 지정", "정보 공개 설정", "일반 유저 권한", "혈맹 운영 정책"],
                    "action": "각혈 관리자 유저 지정",
                },
            ],
        }
    )
    return templates.TemplateResponse(request, "pages/settings/managers.html", context)


@router.get("/clan")
async def clan_settings(request: Request, section: str = "staff"):
    active_nav = {
        "staff": "clan.staff",
        "visibility": "clan.visibility",
        "permissions": "clan.permissions",
    }.get(section, "clan.staff")
    context = build_template_context(
        request,
        active_nav=active_nav,
        page_title="내 혈맹 권한 설정",
        page_description="담당 혈맹의 경리와 일반 유저 공개 범위를 관리합니다.",
        page_kicker="Clan manager settings",
        page_badge="각혈 관리자",
    )
    context.update(
        {
            "accountants": [
                {"name": "미지정", "scope": "분배 · 혈비", "status": "대기"},
            ],
            "clan_policies": [
                {
                    "id": "visibility",
                    "title": "분배 정보 공개",
                    "description": "분배금과 정산 상태를 확인할 수 있는 범위",
                    "value": "혈맹원 전체",
                    "options": ["관리자만", "혈맹원 전체", "전체 공개"],
                },
                {
                    "id": "treasury-visibility",
                    "title": "혈비 가계부 공개",
                    "description": "혈비 잔액과 입출금 내역의 공개 범위",
                    "value": "전체 공개",
                    "options": ["관리자만", "혈맹원 전체", "전체 공개"],
                },
                {
                    "id": "user-permissions",
                    "title": "일반 유저 권한",
                    "description": "일반 유저에게 허용할 기본 조회 수준",
                    "value": "상세 조회",
                    "options": ["요약 조회", "상세 조회", "내 기록만"],
                },
            ],
        }
    )
    return templates.TemplateResponse(request, "pages/settings/clan.html", context)
