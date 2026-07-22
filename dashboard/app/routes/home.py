from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

from dashboard.app.config import BASE_DIR
from dashboard.app.ui.context import build_template_context


router = APIRouter()
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))


@router.get("/")
async def index(request: Request):
    context = build_template_context(
        request,
        active_nav="home.personal",
        page_title="업무 공간",
        page_description="개인 현황을 확인하고, 담당 업무에 따라 연합 운영 또는 내 혈맹 운영으로 이동합니다.",
        page_kicker="LOCAL PostgreSQL · testdb",
    )
    context.update(
        {
            "personal_cards": [
                {"label": "내 미수령 분배금", "value": "-", "meta": "분배금 현황에서 연결 예정"},
                {"label": "귀속 대기", "value": "-", "meta": "수령 기한 정책 연결 예정"},
                {"label": "최근 참여 드랍", "value": "-", "meta": "최근 한달 기준"},
                {"label": "마지막 출석", "value": "-", "meta": "출석 현황 연결 예정"},
            ],
            "workspaces": [
                {
                    "id": "alliance-workspace",
                    "tone": "alliance",
                    "eyebrow": "Alliance workspace",
                    "title": "연합 운영",
                    "role": "연합 관리자",
                    "description": "아이템 드랍 등록부터 혈맹별 1차 분배까지 담당하는 독립된 연합 업무 공간입니다.",
                    "primary": "연합 대시보드",
                    "href": "/alliance/drops",
                    "links": ["드랍 등록", "각혈 분배", "아이템 입찰", "연합 분배 설정"],
                    "flow": ["드랍 등록", "연합 수수료", "혈맹별 분배"],
                },
                {
                    "id": "clan-workspace",
                    "tone": "clan",
                    "eyebrow": "Clan workspace",
                    "title": "내 혈맹 운영",
                    "role": "각혈 관리자 · 경리",
                    "description": "혈맹원 분배와 혈비를 처리하고 경리 지정 및 공개 정책을 관리하는 독립된 혈맹 업무 공간입니다.",
                    "primary": "혈맹 대시보드",
                    "href": "/clan/settlements",
                    "links": ["혈맹원 분배", "혈비 가계부", "혈맹 경리 관리", "정보 공개 설정"],
                    "flow": ["혈맹 수령", "인원별 분배", "공개 정책"],
                },
            ],
            "common_modules": [
                {"icon": "calendar-check", "title": "출석 · 통계", "description": "회차별 출석과 인원·혈맹 통계", "href": "/attendance/status"},
                {"icon": "shield", "title": "서버 운영", "description": "혈맹, 권한, 알림과 작업 로그", "href": "/settings/alliances"},
            ],
        }
    )
    return templates.TemplateResponse(
        request,
        "pages/home/index.html",
        context,
    )
