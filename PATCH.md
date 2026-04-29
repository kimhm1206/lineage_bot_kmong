# PATCH.md

## 변경 내용

랭커 API 전송 대상 판별 기준을 `닉네임` 기반에서 `디스코드 역할 ID` 기반으로 변경했다.

기존에는 닉네임에서 `[랭커]` 혈맹 표기를 찾아서 전송 대상을 골랐다.
이제는 역할 ID `1497949015570907196`를 가진 멤버만 전송 대상으로 판단한다.

## 수정 파일

- `utils/attendance.py`

## 변경 포인트

### 1. 상수 변경

기존:

```python
RANKER_ALLIANCE_NAME = "랭커"
```

변경:

```python
RANKER_ROLE_ID = 1497949015570907196
```

### 2. 랭커 판별 로직 변경

기존:

```python
member = guild.get_member(discord_id)
nickname = member.display_name if member is not None else str(discord_id)
if _resolve_alliance_name_from_nickname(nickname) != RANKER_ALLIANCE_NAME:
    continue
ranker_ids.append(str(discord_id))
```

변경:

```python
member = guild.get_member(discord_id)
if member is None:
    continue
if not any(role.id == RANKER_ROLE_ID for role in member.roles):
    continue
ranker_ids.append(str(discord_id))
```

## 영향 범위

- `랭커 출석 ID 전송` 대상 필터에만 영향 있음
- 일반 출석 저장, 혈맹 통계, 닉네임 기반 혈맹 분류 로직은 그대로 유지
