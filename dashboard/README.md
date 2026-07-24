# Dashboard V2

FastAPI와 SQLAlchemy 2.x 기반의 PostgreSQL 전용 새 대시보드입니다.
Discord 봇과 같은 PostgreSQL 스키마를 사용하며, 구형 `web/` 프로젝트를 대체합니다.
출석 설정과 통계 알림 변경은 PostgreSQL `LISTEN/NOTIFY`로 봇에 전달하며,
대시보드 프로세스 자체에서는 Discord 발송 스케줄러를 실행하지 않습니다.

## 실행

```bash
dashboard/venv/bin/python -m uvicorn dashboard.app.main:app --host 0.0.0.0 --port 8000 --reload
```

## 설정

기본값은 로컬 테스트 PostgreSQL입니다.

```text
postgresql+asyncpg://postgres:testest@127.0.0.1:5432/testdb
```

필요하면 `dashboard/.env`를 만들어 `dashboard/.env.example` 값을 덮어쓰면 됩니다.

Discord 채널·역할·서버 유저 목록은 봇을 별도로 실행하지 않고 REST API로만 조회합니다. 실제 봇 토큰은 Git에 포함되는 `.env.example`에 넣지 말고, Git에서 제외되는 `dashboard/.env`에만 설정합니다.

```text
DISCORD_BOT_TOKEN=실제_봇_토큰
```

설정 페이지는 다음 경로에서 확인합니다.

```text
/settings/server       서버 등록과 사용 여부
/settings/attendance   출석 채널과 진행 시간
/settings/alliances    혈맹과 Discord 역할 매핑
/settings/managers     연합/각혈 관리자 유저 지정
/settings/clan         혈맹 경리와 공개 정책
/developer/bot         Discord REST 연동과 서버 메타데이터
/developer/system      로컬 DB 스키마와 핵심 데이터 점검
```

운영 조회 화면은 모두 로컬 PostgreSQL의 실제 데이터를 사용하며 기본 조회 기간은 최근 한 달입니다.

```text
/alliance/drops            드랍 등록 내역
/alliance/settlements      혈맹별 1차 정산
/alliance/treasury         연합 전체 입출금 가계부
/alliance/bidding          아이템 입찰 현황
/alliance/items            아이템 목록과 기본 시세
/clan/settlements          혈맹원 분배와 수수료 설정
/clan/treasury             혈비 가계부
/attendance/status         회차별 출석 현황
/attendance/statistics     유저별 출석 통계
/attendance/clan           혈맹별 출석 통계
/operations/notifications  통계 알림 현황
/operations/audit          운영 작업 로그
```

`testdb`에서만 실행되는 스키마 정리는 Discord 역할 저장을 `guild_alliance_role_mappings` 하나로 제한하고, 웹 권한은 `guild_user_assignments`의 유저 지정 방식으로 유지합니다. 사용하지 않는 이전 테이블 12개와 중복 인덱스를 제거하며, 운영 조회용 복합 인덱스를 추가합니다. 호스트가 로컬 주소가 아니거나 DB 이름이 `testdb`가 아니면 이 정리는 실행되지 않습니다.

가계부는 `treasury_accounts.account_scope_code`로 범위를 구분합니다. `1`은 서버별 연합 전체 계정이며 `alliance_id`가 비어 있고, `2`는 기존 혈맹 계정이며 `alliance_id`를 가집니다. 입출금 분류도 `treasury_categories.account_scope_code`로 분리해 연합과 혈맹 선택지가 섞이지 않습니다. 두 계정은 `treasury_entries` 원장을 함께 사용하므로 입금·출금·거래 후 잔액·발생 시각·작성자·취소 연결을 같은 방식으로 기록합니다.

드랍 등록과 각혈 분배 사이의 판매 단계는 `settlement_drop_sales`가 담당합니다. 드랍과 `drop_id`로 1:1 연결되며 `status_code=0`은 판매 대기, `status_code=1`은 판매 완료입니다. 판매 완료 상태에는 구매 혈맹과 완료 시각이 필수이고 구매 유저는 선택값입니다. 각혈 분배 화면은 이후 이 테이블의 판매 완료 행만 조회하도록 연결합니다.

## 화면 구조

새 화면은 기능별 파일이 섞이지 않도록 아래처럼 나눕니다.

업무 메뉴는 담당 주체를 기준으로 `연합 운영`과 `내 혈맹 운영`을 분리합니다. 연합 관리자는 드랍 등록과 혈맹별 1차 분배를, 각혈 관리자와 경리는 혈맹원별 2차 분배와 혈비를 담당합니다. 두 관리자 권한은 상하 관계가 아닌 독립 권한이며 `guild_id + user_id` 기준으로 직접 지정합니다. Discord 역할 매핑은 소속 혈맹 판별에만 사용합니다.

```text
홈
연합 운영       # 연합 관리자: 드랍 등록, 각혈 분배, 입찰, 연합 수수료
내 혈맹 운영    # 각혈 관리자/경리: 혈맹원 분배, 혈비, 귀속, 공개 정책
출석 · 통계     # 공통 조회와 출석 설정
서버 운영       # 오너: 혈맹, 연합/각혈 관리자 지정과 운영 기록
개발자 도구     # 개발자: 서버 등록, Discord REST 연동과 DB 진단
```

```text
dashboard/app/ui/                  # 사이드바 메뉴, 공통 템플릿 컨텍스트
dashboard/app/templates/layouts/   # 전체 HTML 뼈대와 앱 셸
dashboard/app/templates/partials/  # 사이드바, 상단바 같은 반복 영역
dashboard/app/templates/components/# 아이콘, 작은 UI 조각
dashboard/app/templates/pages/     # 실제 페이지 화면
dashboard/app/static/css/          # 라이트/다크 테마와 레이아웃 CSS
dashboard/app/static/js/           # 테마 토글 같은 화면 스크립트
```
