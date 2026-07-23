# New Server Workspace 인수인계

작성일: 2026-07-23  
현재 작업 브랜치: `new-server-workspace`  
문서 작성 직전 기능 기준 커밋: `f5802d1 refactor: remove attendance end times`

이 문서는 다른 Mac 또는 SSH 세션에서 Codex가 현재 작업을 바로 이어가기 위한 요약이다.

## 1. 작업 목적

기존 `web/` 운영 화면을 즉시 교체하지 않고, 새 PostgreSQL 구조에 맞춘 웹을 `dashboard/`에 별도로 만들고 있다.

핵심 방향은 다음과 같다.

- FastAPI + SQLAlchemy 2.x async + PostgreSQL 전용 새 대시보드
- 연합 업무와 각 혈맹 업무를 명확하게 분리
- Discord 역할은 혈맹 소속 매핑에만 사용
- 연합 관리자, 각혈 관리자, 혈맹 경리는 Discord 유저 단위로 지정
- 혈맹/유저/수수료를 동일한 정산 객체로 관리
- 연합비와 혈비를 수정·삭제형 잔액이 아니라 입출금 원장으로 관리
- 라이트/다크 모드를 지원하는 정돈된 운영 도구 UI
- 기존 봇, 기존 `web/`, 운영 PostgreSQL은 새 대시보드 작업 중 변경하지 않음

## 2. 가장 중요한 안전 규칙

1. 현재 새 대시보드는 로컬 PostgreSQL `testdb`만 사용한다.
2. 운영 PostgreSQL에 스키마 변경, 삭제, 테스트 데이터 입력을 하지 않는다.
3. 기존 `web/`와 기존 SQLite는 별도 요청이 없는 한 수정하지 않는다.
4. `dashboard/.env`의 Discord 토큰은 조회용 REST API에만 사용한다. 운영 봇 프로세스를 실행하지 않는다.
5. DB 자동 정리 코드는 반드시 `127.0.0.1|localhost|::1` 및 DB 이름 `testdb` 조건을 유지한다.
6. `attendance_sessions.ended_at`은 의도적으로 컬럼 자체를 삭제했다. 다시 추가하거나 조회하지 않는다.
7. 비밀값이 있는 `dashboard/.env`는 커밋하지 않는다.

## 3. Git 브랜치 상태

현재 브랜치 구성은 아래와 같다.

| 브랜치 | 용도 | 현재 확인된 커밋 |
| --- | --- | --- |
| `main` | 기존 프로덕션 계열 | `b4da68e` |
| `production` | 기존 운영 웹 기준 | `966bd34` |
| `new-server` | 웹 없이 봇 중심으로 단순화한 신규 서버 실험 | `60e7e1e` |
| `new-server-workspace` | `production`을 기반으로 새 DB/새 웹을 개발하는 현재 브랜치 | `f5802d1` |

`new-server-workspace`는 현재 원격 추적 브랜치가 연결되어 있지 않다. 다른 브랜치로 이동하거나 병합하기 전에 반드시 `git status`와 최근 로그를 확인한다.

## 4. 실행 환경

### 권장 실행

VS Code 작업에서 `start:webdevelop:dashboard`를 선택한다.

- 새 대시보드: `0.0.0.0:8000`
- 기존 웹: `127.0.0.1:8010`
- `start:webdevelop`은 두 웹을 동시에 실행한다.
- 새 대시보드만 작업할 때는 `start:webdevelop:dashboard`만 실행한다.
- 봇은 실행하지 않는다.

터미널에서 직접 실행할 때:

```bash
dashboard/venv/bin/python -m uvicorn dashboard.app.main:app \
  --host 0.0.0.0 --port 8000 --reload
```

상태 확인:

```bash
curl http://127.0.0.1:8000/health
```

### Python 환경

- 가상환경: `dashboard/venv`
- 패키지 목록: `dashboard/requirements.txt`
- 현재 `pytest`는 설치되어 있지 않다.
- 기본 검증은 `compileall`, 실제 HTTP 응답, DB 읽기 조회로 진행했다.

```bash
dashboard/venv/bin/python -m compileall -q dashboard/app
git diff --check
```

## 5. 환경변수

예시는 `dashboard/.env.example`에 있다. 실제 값은 `dashboard/.env`에서 읽는다.

```dotenv
APP_NAME=Lineage Dashboard V2
ENVIRONMENT=local
DATABASE_URL=postgresql+asyncpg://postgres:testest@127.0.0.1:5432/testdb
DB_ECHO=false
DISCORD_BOT_TOKEN=
DISCORD_API_BASE=https://discord.com/api/v10
DISCORD_CACHE_TTL_SECONDS=300
```

Discord 토큰은 문서나 Git 추적 파일에 기록하지 않는다.

## 6. 로컬 PostgreSQL

현재 개발 DB:

```text
host: 127.0.0.1
port: 5432
database: testdb
user: postgres
```

초기 테스트 데이터는 기존 데이터 중 최근 3일 범위를 바탕으로 옮긴 뒤 새 정산 구조로 변환했다. 구형 복제 테이블은 삭제했고, 새 구조의 정산 상태는 테스트하기 쉽도록 미완료 중심으로 구성했다.

2026-07-23 확인 당시 주요 데이터 수:

| 데이터 | 건수 |
| --- | ---: |
| 서버 | 3 |
| 혈맹 | 25 |
| 유저 | 1,028 |
| 출석 회차 | 45 |
| 출석 참여 | 3,455 |
| 드랍 | 13 |
| 판매 상태 | 13 |
| 정산 객체 | 1,959 |
| 연합비/혈비 계정 | 9 |
| 가계부 원장 | 0 |

`schema_migrations`에는 현재 버전 `2`부터 `9`까지 기록되어 있다.

### 테이블 역할

기본 데이터:

- `guilds`: 서버와 활성 상태, Discord 메타데이터
- `guild_settings`: 출석 채널과 시간 설정
- `alliances`: 혈맹 마스터
- `users`: Discord 유저와 현재 혈맹
- `items`, `catalog_item_versions`: 아이템과 이름 버전

출석:

- `attendance_sessions`: 출석 회차와 시작 시각
- `attendance_entries`: 회차별 참여 유저
- 종료 시각은 저장하지 않는다. `attendance_sessions`에는 `attendance_id`, `guild_id`, `started_at`, `started_by_discord_id`만 있다.

정산:

- `settlement_drops`: 출석 회차와 연결된 드랍
- `settlement_drop_participants`: 드랍 당시 참여 유저/혈맹 스냅샷
- `settlement_drop_excluded_alliances`: 분배 제외 혈맹
- `settlement_drop_sales`: 판매 대기/완료, 구매 혈맹 필수, 구매 유저 선택
- `settlement_fee_rules`, `settlement_fee_rule_versions`: 연합/혈맹 수수료 규칙과 버전
- `settlement_payout_objects`: 혈맹, 유저, 수수료를 같은 구조로 저장하는 정산 객체

가계부:

- `treasury_accounts`: 연합비 또는 혈비 계정과 현재 잔액
- `treasury_categories`: 범위별 입출금 분류
- `treasury_source_types`: 수동, 귀속, 수수료 등 발생 원인
- `treasury_entries`: 금액, 거래 후 잔액, 시각, 작성자, 취소 연결을 저장하는 원장
- `account_scope_code=1`: 서버 전체 연합비, `alliance_id IS NULL`
- `account_scope_code=2`: 혈맹별 혈비, `alliance_id` 필수

설정과 권한:

- `guild_alliance_role_mappings`: Discord 역할과 혈맹 연결. 역할 저장은 이 테이블 하나만 사용한다.
- `guild_user_assignments`: 유저 단위 운영 담당자 지정
- `alliance_access_policies`: 혈맹별 공개 범위와 일반 유저 접근 정책
- `scope_code=1`: 연합 관리자
- `scope_code=2`: 각혈 관리자
- `scope_code=3`: 혈맹 경리

감사 로그:

- `audit_action_types`, `audit_entity_types`: 작업/대상 코드 마스터
- `audit_actors`: 작업자
- `audit_events`, `audit_event_contexts`: 반복 문장 대신 ID 중심으로 저장하는 작업 이력

## 7. 새 대시보드 코드 구조

```text
dashboard/
├── .env.example
├── README.md
├── requirements.txt
└── app/
    ├── main.py                    # FastAPI 생성과 lifespan
    ├── config.py                  # dashboard/.env 로딩
    ├── database.py                # async engine, 로컬 전용 스키마 마이그레이션
    ├── models.py                  # SQLAlchemy 모델
    ├── security.py                # 현재 역할 판별과 일부 권한 검사
    ├── routes/
    │   ├── home.py
    │   ├── settings.py            # Discord REST 기반 설정 저장
    │   ├── workspaces.py           # 운영/정산/출석 화면 라우트
    │   ├── developer.py
    │   └── health.py
    ├── services/
    │   ├── discord_api.py          # 봇 실행 없는 Discord REST 조회와 캐시
    │   ├── settings_store.py       # 설정 DB 접근
    │   ├── workspace_store.py      # 운영 화면 DB 조회/가계부 기록
    │   └── system_store.py         # 개발자 DB 진단
    ├── ui/
    │   ├── navigation.py           # 좌측 메뉴 정의
    │   └── context.py              # 공통 템플릿 컨텍스트
    ├── templates/
    │   ├── layouts/                # 전체 앱 셸
    │   ├── partials/               # 사이드바/상단바
    │   ├── components/             # 아이콘/출석/유저 선택기
    │   └── pages/                  # 실제 페이지
    └── static/
        ├── css/dashboard.css
        └── js/                     # 테마, 메뉴, 설정, 유저 선택기, 가계부
```

## 8. UI/UX 기준과 완료된 공통 작업

- Similarweb 계열의 정돈된 SaaS 운영 도구 분위기
- 라이트/다크 테마 지원
- 좌측 사이드바는 화면 스크롤과 무관하게 고정
- 1차 메뉴는 아이콘 레일, 2차 메뉴는 선택한 업무 분류의 페이지 목록으로 구성
- 데스크톱 사이드바는 아이콘 레일로 시작하며 펼침/접힘 상태를 브라우저에 저장
- 같은 주소의 중복 메뉴와 같은 페이지 안의 앵커 메뉴를 제거해 모든 메뉴를 페이지 단위로 통일
- 화면 제목은 상단바에 있으므로 각 페이지의 중복 `page-heading` 제거
- 연합 운영과 내 혈맹 운영을 사이드바에서 별도 업무 영역으로 구분
- 서버/혈맹/기간/검색/페이지 이동 공통 패턴 적용
- 기본 기간 조회는 최근 한 달
- Discord 유저 선택은 프로필 사진 없는 공통 모달 사용
- 공통 유저 선택기는 단일/다중 선택 옵션을 호출부에서 지정 가능
- 서버 유저가 많아도 Discord REST 멤버 페이지 조회 후 5분 캐시 사용
- Discord 정보가 정말 필요한 설정에서만 새로고침 기능 노출

## 9. 현재 구현된 화면

설정:

- `/settings/server`: 서버 등록, 활성화, Discord 서버 메타데이터
- `/settings/attendance`: 출석 패널 채널, 음성 채널, 타이머
- `/settings/alliances`: 혈맹과 Discord 역할 매핑
- `/settings/managers`: 연합 관리자/각혈 관리자 유저 지정
- `/settings/clan`: 혈맹 경리와 공개 정책
- `/developer/bot`: Discord REST 연결과 서버 동기화
- `/developer/system`: 로컬 DB 테이블/마이그레이션/데이터 진단

연합 운영:

- `/alliance/drops`: 드랍 조회
- `/alliance/settlements`: 혈맹별 1차 분배 조회
- `/alliance/treasury`: 연합비 원장 조회 및 수동 입출금 등록
- `/alliance/bidding`: 입찰 상태 조회
- `/alliance/items`: 아이템/기본 시세 조회
- `/alliance/settings`: 연합 수수료 규칙 조회

혈맹 운영:

- `/clan/settlements`: 혈맹원/내부 수수료 정산 조회
- `/clan/treasury`: 혈비 원장 조회 및 수동 입출금 등록
- `/clan/forfeits`: 귀속 기록 조회
- `/clan/settings`: 혈맹 수수료 규칙 조회

출석:

- `/attendance/status`: 회차별 출석, 혈맹별 참여자 펼치기, 10회차 단위 페이지
- `/attendance/statistics`: 유저별 순위, 혈맹 비교, 일별/시간대별 통계
- `/attendance/statistics/export`: 현재 조건 CSV 다운로드
- `/attendance/clan`: 혈맹별 참여율, 요일/시간대/추이
- 출석 관련 세 화면은 기존 운영 웹의 정보 구조를 참고하되 새 디자인에 맞게 별도 구현
- 출석 종료 시각은 DB와 모든 새 대시보드 표시에서 제거 완료

## 10. 아직 미완성인 핵심 부분

다음 작업자는 아래 항목을 완성된 기능으로 오해하면 안 된다.

1. 실제 Discord 로그인과 세션 인증은 아직 연결하지 않았다.
2. `ENVIRONMENT=local`에서는 모든 요청을 Developer로 간주한다.
3. 유저 단위 권한 데이터는 저장되지만, 모든 페이지/POST에 실제 접근 제어가 완전히 연결된 상태는 아니다.
4. 연합비/혈비 입력 외 운영 페이지 대부분은 현재 조회 중심이다.
5. 새 드랍 등록 폼, 판매 완료 처리, 구매 혈맹/구매자 입력 UI는 아직 연결 전이다.
6. 판매 완료된 드랍만 1차 분배로 넘기는 조회 규칙도 최종 UI 흐름과 함께 완성해야 한다.
7. 혈맹/유저/수수료 객체의 개별 완료·전체 완료·귀속 동작은 새 대시보드용 POST/API로 아직 완성하지 않았다.
8. 수수료 규칙 추가/수정/삭제 UI는 현재 조회 화면 이후 작업이다.
9. 가계부 자동 입금은 아직 연결 전이다. 현재 원장 수동 등록만 가능하다.
10. 자동 귀속, 수수료 입금, 정산 완료가 `treasury_entries`에 중복 없이 연결되는 흐름을 구현해야 한다.
11. 감사 로그 테이블은 준비됐지만 새 대시보드의 모든 변경 작업에 이벤트 기록이 연결되지는 않았다.
12. 자동화된 pytest 테스트 묶음과 브라우저 회귀 테스트는 아직 없다.

## 11. 권장 다음 작업 순서

1. Discord 로그인/세션을 새 대시보드에 연결한다.
2. 로그인 유저의 서버 오너 여부와 `guild_user_assignments`를 읽어 `request.state.access_role`을 설정한다.
3. 사이드바 노출, GET 조회, POST 변경 권한을 같은 정책 함수로 통일한다.
4. 드랍 등록 화면을 구현하고 출석 회차, 아이템, 수수료, 제외 혈맹을 연결한다.
5. 판매 완료 단계를 구현한다. 구매 혈맹은 필수, 구매 유저는 선택이다.
6. 판매 완료 후에만 연합의 혈맹별 1차 정산 객체가 노출되도록 한다.
7. 1차 완료 후에만 각 혈맹의 유저/수수료 2차 정산이 노출되도록 한다.
8. 정산 객체의 개별 완료, 전체 완료, 귀속을 트랜잭션으로 구현한다.
9. 수수료/귀속이 완료될 때 연합비 또는 혈비 원장을 자동 생성하고 중복 생성을 막는다.
10. 작업 로그를 ID 기반 감사 이벤트에 연결한다.
11. 핵심 서비스 단위 테스트와 권한별 라우트 테스트를 추가한다.

## 12. 최근 주요 커밋

```text
f5802d1 refactor: remove attendance end times
369ef12 feat: rebuild attendance dashboards
f808778 feat: add drop sale workflow schema
ad64d4e feat: add alliance treasury ledger
c48b9e9 fix: keep dashboard sidebar fixed
d142168 feat: connect dashboard operations to local data
306d714 feat: streamline dashboard settings and developer tools
cd0a590 fix: preserve sidebar scroll position
19877d9 feat: add reusable server user picker
505ccad docs: expose dashboard development server
ecd1180 feat: build Discord-backed settings workspace
0e7d28b feat: define user-assigned operations access
```

## 13. 작업 시작 전 빠른 점검

```bash
git branch --show-current
git status --short
git log --oneline -10

PGPASSWORD=testest psql -h 127.0.0.1 -U postgres -d testdb \
  -c "SELECT current_database(), inet_server_addr(), inet_server_port();"

dashboard/venv/bin/python -m compileall -q dashboard/app
curl http://127.0.0.1:8000/health
```

DB 변경 전에는 반드시 위 조회 결과가 로컬 `testdb`인지 확인한다. 새 마이그레이션은 `dashboard/app/database.py`의 로컬 보호 조건 안에 추가하고, 운영 DB URL을 임시로 코드에 넣지 않는다.
