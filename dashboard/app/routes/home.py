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
                    "role": "연합 경리",
                    "description": "아이템 드랍을 등록하고 판매대금을 혈맹별로 1차 분배하는 연합 전용 업무 공간입니다.",
                    "primary": "연합 대시보드",
                    "links": ["드랍 등록", "각혈 분배", "아이템 입찰", "연합 분배 설정"],
                    "flow": ["드랍 등록", "연합 수수료", "혈맹별 분배"],
                },
                {
                    "id": "clan-workspace",
                    "tone": "clan",
                    "eyebrow": "Clan workspace",
                    "title": "내 혈맹 운영",
                    "role": "혈맹 경리",
                    "description": "연합에서 전달받은 금액을 혈맹원에게 분배하고 혈비와 귀속 내역을 관리합니다.",
                    "primary": "혈맹 대시보드",
                    "links": ["혈맹원 분배", "혈비 가계부", "귀속 관리", "혈맹 분배 설정"],
                    "flow": ["혈맹 수령", "내부 수수료", "인원별 분배"],
                },
            ],
            "common_modules": [
                {"icon": "calendar-check", "title": "출석 · 통계", "description": "회차별 출석과 인원·혈맹 통계"},
                {"icon": "shield", "title": "서버 운영", "description": "혈맹, 권한, 알림과 작업 로그"},
            ],
        }
    )
    return templates.TemplateResponse(
        request,
        "pages/home/index.html",
        context,
    )
