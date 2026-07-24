# Lineage Dashboard + Discord Bot

이 브랜치는 새 `dashboard/`와 Discord 봇만 사용한다. 구형 `web/`,
WebSocket 브리지, DB 명령 큐는 제거됐다.

## 구성

- `dashboard/`: FastAPI + SQLAlchemy async + PostgreSQL 운영 대시보드
- `discord_bot/`: Discord 출석 및 통계 알림 봇
- `discord_bot/storage.py`: 봇 전용 PostgreSQL 저장 계층
- `common/`: 이전 데이터 변환 스크립트가 참조하는 공통 코드

봇과 대시보드는 반드시 동일한 PostgreSQL 데이터베이스를 사용한다.

## 환경 설정

대시보드는 `dashboard/.env`, 봇은 루트 `.env`를 읽는다.

봇의 최소 설정:

```dotenv
DISCORD_BOT_TOKEN=
DEVELOPER_DISCORD_ID=238978205078388747
BOT_DATABASE_URL=postgresql://postgres:password@127.0.0.1:5432/lineage_antalas
DASHBOARD_BASE_URL=https://dashboard.example.com
REPORT_SCHEDULE_REFRESH_SECONDS=60
GUILD_REGISTRY_REFRESH_SECONDS=30
```

대시보드의 최소 설정:

```dotenv
ENVIRONMENT=production
DATABASE_URL=postgresql+asyncpg://postgres:password@127.0.0.1:5432/lineage_antalas
DISCORD_BOT_TOKEN=
DISCORD_CLIENT_ID=
DISCORD_CLIENT_SECRET=
DISCORD_REDIRECT_URI=https://dashboard.example.com/auth/discord/callback
SESSION_SECRET=
AUTH_LOCAL_BYPASS=false
```

## 설치

```bash
python3 -m venv dashboard/venv
dashboard/venv/bin/pip install -r dashboard/requirements.txt

python3 -m venv discord_bot/venv
discord_bot/venv/bin/pip install -r discord_bot/requirements.txt
```

## 실행

대시보드:

```bash
dashboard/venv/bin/python -m uvicorn dashboard.app.main:app \
  --host 0.0.0.0 --port 8000
```

봇:

```bash
discord_bot/venv/bin/python -m discord_bot.main
```

봇은 시작할 때 필요한 새 스키마가 있는지만 검증한다. 테이블을 자동 생성하거나
길드를 자동 등록하지 않는다.

## 서버 등록 정책

`guilds` 테이블이 봇의 허용 목록이다.

1. 개발자가 대시보드의 `/settings/server`에서 Discord 서버 ID를 등록한다.
2. `is_enabled=true`인 서버에서만 봇 명령과 출석 패널이 동작한다.
3. 봇을 다른 서버에 초대하는 것만으로는 DB 행이나 설정이 생성되지 않는다.
4. 미등록 서버에서 명령을 실행하면 개발자 문의 안내만 표시한다.

기존 마이그레이션으로 들어온 서버는 등록 상태를 그대로 유지한다.
서버 등록/비활성화는 DB 알림으로 즉시 반영되고, 알림 유실 시에도 기본 30초
안에 다시 동기화된다. 출석 버튼을 누를 때마다 DB를 조회하지 않는다.

## 통신 구조

- 브라우저와 대시보드: 일반 HTTP
- 봇과 Discord: Discord Gateway
- 대시보드에서 봇: PostgreSQL `LISTEN/NOTIFY` 제어 이벤트
- 공통 상태: PostgreSQL

실시간 출석 참여자는 봇 프로세스 메모리에서만 관리하고, 출석 종료 후 한 번
PostgreSQL에 저장한다. 알림/출석 설정 저장 시 대시보드가 DB 알림을 보내므로
APScheduler 재등록과 패널 수정은 즉시 처리된다. 봇 재접속이나 알림 유실에
대비해 통계 알림 설정은 기본 60초마다, 등록 서버는 30초마다 다시 확인한다.

## 점검

```bash
discord_bot/venv/bin/python -m compileall -q discord_bot
dashboard/venv/bin/python -m compileall -q dashboard/app
dashboard/venv/bin/python -m pytest -q dashboard/tests
git diff --check
```
