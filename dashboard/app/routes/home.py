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
        page_title="운영 대시보드",
        page_description="본인과 혈맹의 정산 흐름을 먼저 확인하고, 필요한 업무로 바로 이동하는 새 운영툴 베이스입니다.",
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
            "alliance_cards": [
                {"label": "혈맹 미정산", "value": "-", "meta": "인원별 정산 연결 예정"},
                {"label": "귀속 혈비", "value": "-", "meta": "혈비 가계부 연결 예정"},
                {"label": "진행 중 드랍", "value": "-", "meta": "드랍별 상태 연결 예정"},
                {"label": "내부 수수료", "value": "-", "meta": "분배 설정 연결 예정"},
            ],
            "workspace_modules": [
                {
                    "title": "드랍 & 분배",
                    "description": "드랍 등록부터 연합/혈맹/개인 정산까지 이어지는 핵심 업무 영역입니다.",
                    "links": ["분배금 현황", "드랍 등록", "연합 분배", "혈맹 분배", "분배 설정"],
                },
                {
                    "title": "혈비",
                    "description": "귀속 혈비와 입출금 흐름을 공개 가능한 가계부 형태로 정리합니다.",
                    "links": ["혈비 현황", "입출금 내역", "귀속 내역", "혈비 설정"],
                },
                {
                    "title": "출석",
                    "description": "출석 현황과 통계는 유지하되, 설정은 출석 업무 아래에서 관리합니다.",
                    "links": ["출석 현황", "출석 통계", "내 혈맹 통계", "출석 설정"],
                },
                {
                    "title": "서버 운영",
                    "description": "혈맹, 권한, 알림, 작업 로그처럼 서버 운영에 필요한 설정을 모읍니다.",
                    "links": ["혈맹 관리", "권한 관리", "알림 관리", "작업 로그"],
                },
            ],
        }
    )
    return templates.TemplateResponse(
        request,
        "pages/home/index.html",
        context,
    )
