# Dashboard V2

FastAPI와 SQLAlchemy 2.x 기반의 PostgreSQL 전용 새 대시보드입니다.
기존 `web/`와 SQLite 코드는 건드리지 않고, 새 정산/혈비 가계부 구조만 바라봅니다.

## 실행

```bash
dashboard/venv/bin/python -m uvicorn dashboard.app.main:app --host 127.0.0.1 --port 8000 --reload
```

## 설정

기본값은 로컬 테스트 PostgreSQL입니다.

```text
postgresql+asyncpg://postgres:testest@127.0.0.1:5432/testdb
```

필요하면 `dashboard/.env`를 만들어 `dashboard/.env.example` 값을 덮어쓰면 됩니다.

## 화면 구조

새 화면은 기능별 파일이 섞이지 않도록 아래처럼 나눕니다.

```text
dashboard/app/ui/                  # 사이드바 메뉴, 공통 템플릿 컨텍스트
dashboard/app/templates/layouts/   # 전체 HTML 뼈대와 앱 셸
dashboard/app/templates/partials/  # 사이드바, 상단바 같은 반복 영역
dashboard/app/templates/components/# 아이콘, 작은 UI 조각
dashboard/app/templates/pages/     # 실제 페이지 화면
dashboard/app/static/css/          # 라이트/다크 테마와 레이아웃 CSS
dashboard/app/static/js/           # 테마 토글 같은 화면 스크립트
```
