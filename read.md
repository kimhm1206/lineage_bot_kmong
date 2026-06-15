# Lineage Ops Console 실행 가이드

이 문서는 리눅스 운영 서버에서 현재 프로젝트를 실행하기 위한 최소 절차입니다.

중요: 운영 PostgreSQL은 이미 준비되어 있고, 기존 데이터도 운영 DB에 들어가 있습니다. DB 초기화, SQLite 마이그레이션, 테스트 데이터 생성 작업은 실행하지 마세요.

## 구성

- `web/`: FastAPI 웹 콘솔
- `discord_bot/`: Discord 봇
- `common/`: 공통 PostgreSQL DB 접근 코드
- 운영 DB: `.env`의 PostgreSQL 접속값으로 연결

## 1. 소스 준비

```bash
cd /path/to/lineage_bot_kmong
git pull
```

## 2. Python venv 준비

웹과 봇 venv를 분리해서 사용합니다.

```bash
python3 -m venv web/venv
web/venv/bin/pip install --upgrade pip
web/venv/bin/pip install -r web/requirements.txt

python3 -m venv discord_bot/venv
discord_bot/venv/bin/pip install --upgrade pip
discord_bot/venv/bin/pip install -r discord_bot/requirements.txt
```

## 3. `.env` 설정

프로젝트 루트에 `.env`를 둡니다.

운영 DB는 `LINEAGE_DB_TARGET=test` 또는 `--test` 없이도 운영 DB를 보도록 서버 환경에 맞게 정리하세요. 이 프로젝트의 DB 코드는 기본 실행 시 `LOCAL_DATABASE_URL` 또는 `PGLOCAL*`를 먼저 봅니다. 운영 서버에서 원격/운영 DB를 바로 쓰려면 아래 둘 중 하나를 선택합니다.

방법 A: `LOCAL_DATABASE_URL` 사용

```env
LOCAL_DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
```

방법 B: `PGLOCAL*` 사용

```env
PGLOCALHOST=HOST
PGLOCALPORT=5432
PGLOCALDATABASE=DBNAME
PGLOCALUSER=USER
PGLOCALPASSWORD=PASSWORD
```

Discord/OAuth 관련 값:

```env
DISCORD_BOT_TOKEN=...
DISCORD_CLIENT_ID=...
DISCORD_CLIENT_SECRET=...
DISCORD_REDIRECT_URI=https://xn--950bk80bh7an33asc.site/auth/discord/callback
WEB_SESSION_SECRET=충분히_긴_랜덤_문자열
BOT_BRIDGE_TOKEN=충분히_긴_랜덤_문자열
BOT_BRIDGE_WS_URL=ws://127.0.0.1:8000/internal/bot/ws
WEB_BASE_URL=https://xn--950bk80bh7an33asc.site
GLOBAL_DEVELOPER_DISCORD_ID=개발자_DISCORD_ID
```

`BOT_BRIDGE_TOKEN`은 웹과 봇이 내부 WebSocket으로 통신할 때 쓰는 토큰입니다. `WEB_SESSION_SECRET`과 같은 값으로 둬도 됩니다.

로컬/테스트용 `LINEAGE_DB_TARGET=test`는 운영 실행에 넣지 마세요.

## 4. 실행

웹:

```bash
cd /path/to/lineage_bot_kmong
web/venv/bin/python -m uvicorn web.app:app --host 0.0.0.0 --port 8000
```

봇:

```bash
cd /path/to/lineage_bot_kmong
discord_bot/venv/bin/python -m discord_bot.main
```

봇 프로세스에는 아래 기능이 같이 붙어 있습니다.

- 웹과 내부 WebSocket 브리지 연결
- 웹에서 보낸 출석 시작/종료 명령 처리
- Discord 관리자 패널 갱신
- 통계 알림 APScheduler Worker

## 5. Cloudflare 도메인 연결

운영 도메인:

```text
https://xn--950bk80bh7an33asc.site
```

Cloudflare Tunnel을 쓰는 경우 public hostname을 아래처럼 연결합니다.

```text
Hostname: xn--950bk80bh7an33asc.site
Service: http://localhost:8000
```

일반 Nginx/리버스 프록시를 쓰는 경우에도 외부 HTTPS 요청이 내부 웹 프로세스 `http://127.0.0.1:8000`으로 전달되게 설정합니다.

Discord Developer Portal의 OAuth2 Redirect URI에는 반드시 아래 값을 등록합니다.

```text
https://xn--950bk80bh7an33asc.site/auth/discord/callback
```

## 6. systemd 예시

경로와 사용자명은 서버에 맞게 바꾸세요.

`/etc/systemd/system/lineage-web.service`

```ini
[Unit]
Description=Lineage Ops Web
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/path/to/lineage_bot_kmong
EnvironmentFile=/path/to/lineage_bot_kmong/.env
ExecStart=/path/to/lineage_bot_kmong/web/venv/bin/python -m uvicorn web.app:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

`/etc/systemd/system/lineage-bot.service`

```ini
[Unit]
Description=Lineage Discord Bot
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/path/to/lineage_bot_kmong
EnvironmentFile=/path/to/lineage_bot_kmong/.env
ExecStart=/path/to/lineage_bot_kmong/discord_bot/venv/bin/python -m discord_bot.main
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

적용:

```bash
sudo systemctl daemon-reload
sudo systemctl enable lineage-web lineage-bot
sudo systemctl restart lineage-web lineage-bot
sudo systemctl status lineage-web lineage-bot
```

로그 확인:

```bash
journalctl -u lineage-web -f
journalctl -u lineage-bot -f
```

## 7. 실행 확인

웹:

```bash
curl -I https://xn--950bk80bh7an33asc.site/login
```

봇:

- Discord 봇이 온라인인지 확인
- 관리자 패널 메시지가 설정 채널에 갱신되는지 확인
- 웹 출석 페이지에서 실시간 연결이 되는지 확인
- 웹에서 설정 저장 시 봇 큐가 처리되는지 확인

## 8. 하면 안 되는 작업

운영 서버에서 아래 작업은 하지 마세요.

```bash
python scripts/migrate_sqlite_to_postgres.py
python scripts/migrate_sqlite_to_postgres.py --test
```

SQLite 파일(`*.sqlite3`, `data/`)은 운영 원본으로 쓰지 않습니다. 현재 운영 기준 데이터는 PostgreSQL입니다.

## 9. 자주 쓰는 중지 명령

systemd 사용 시:

```bash
sudo systemctl stop lineage-web lineage-bot
```

수동 실행 프로세스 정리 시:

```bash
pkill -f "uvicorn web.app:app"
pkill -f "discord_bot.main"
```
