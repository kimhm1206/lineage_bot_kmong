# Dashboard V2

FastAPI와 SQLAlchemy 2.x 기반의 PostgreSQL 전용 새 대시보드입니다.
기존 `web/`와 SQLite 코드는 건드리지 않고, 새 정산/혈비 가계부 구조만 바라봅니다.

## 실행

```bash
dashboard/venv/bin/python -m uvicorn dashboard.app.main:app --host 127.0.0.1 --port 8010 --reload
```

## 설정

기본값은 로컬 테스트 PostgreSQL입니다.

```text
postgresql+asyncpg://postgres:testest@127.0.0.1:5432/testdb
```

필요하면 `dashboard/.env`를 만들어 `dashboard/.env.example` 값을 덮어쓰면 됩니다.

