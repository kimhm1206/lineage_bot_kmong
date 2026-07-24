# New Server Workspace Handoff

현재 브랜치의 운영 구성은 `dashboard/` + `discord_bot/` + PostgreSQL이다.

## 확정된 경계

- 구형 `web/` 프로젝트는 삭제됐다.
- 봇과 웹 사이 WebSocket 브리지 및 DB 명령 큐는 사용하지 않는다.
- 즉시 설정 반영은 PostgreSQL `LISTEN/NOTIFY` 이벤트와 처리 ACK로 처리한다.
- 출석 진행 상태는 봇 메모리에서 관리하고 종료 시 PostgreSQL에 저장한다.
- 대시보드와 봇은 동일한 새 스키마를 사용한다.
- 봇은 스키마와 등록 서버를 조회할 뿐 테이블이나 서버를 자동 생성하지 않는다.

## 서버 활성화

대시보드 `/settings/server`에서 개발자가 서버 ID를 수동 등록한다.
`guilds.is_enabled=true`인 서버만 봇이 지원한다. 새 서버에 봇을 초대해도 자동
등록되지 않으며, 미등록 서버의 명령은 개발자 문의 메시지로 종료된다.

알림 저장 시 봇 APScheduler를 즉시 다시 읽고, 출석 설정 저장 시 기존 Discord
패널을 즉시 수정한다. 웹은 봇 ACK가 오지 않거나 처리 오류가 발생하면 저장은
완료됐지만 봇 반영을 확인하지 못했다는 오류를 표시한다. 주기 DB 조회는 없으며,
봇 알림 연결이 재연결될 때 전체 상태를 한 번 동기화한다.

## 실행

```bash
dashboard/venv/bin/python -m alembic \
  -c dashboard/alembic.ini upgrade head

dashboard/venv/bin/python -m uvicorn dashboard.app.main:app \
  --host 0.0.0.0 --port 8000 --reload

discord_bot/venv/bin/python -m discord_bot.main
```

운영에서는 루트 `.env`의 `BOT_DATABASE_URL`과 `dashboard/.env`의
`DATABASE_URL`이 같은 DB를 가리키는지 먼저 확인한다.

## 주의

- 운영 봇 토큰으로 로컬 봇 프로세스를 중복 실행하지 않는다.
- DB 마이그레이션 전에는 대상 DB 이름을 반드시 확인한다.
- `attendance_sessions`에는 종료 시각 컬럼이 없다.
- 출석 저장은 역할 매핑 우선, 닉네임 `[혈맹]` 파싱 차선으로 혈맹을 정한다.
