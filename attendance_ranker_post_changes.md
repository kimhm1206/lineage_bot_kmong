# Attendance Ranker POST Changes

이 문서는 출석 종료 후 `[랭커]` 혈맹 참가자의 Discord ID 목록을 외부 URL로 전송하도록 추가한 작업 내용을 정리한 문서입니다.

기준 파일:
- `utils/attendance.py`

## 목적

기존 출석 종료 흐름은 요약 로그를 디스코드에 남기는 역할만 했습니다.

이번 작업에서는 아래 기능을 추가했습니다.

1. 출석 참가자 중 닉네임 혈맹 태그가 `[랭커]` 인 유저만 골라냅니다.
2. 해당 유저들의 `discord_id` 를 문자열 리스트로 만듭니다.
3. Google Apps Script URL로 `POST` JSON 요청을 보냅니다.
4. 수동 종료 중 저장하는 경우와 자동 종료에서만 동작하게 연결했습니다.

요청 형태는 아래와 같습니다.

```json
{
  "ids": ["123456789012345678", "234567890123456789"]
}
```

POST body 예시:

```json
{
  "ids": [
    "123456789012345678",
    "234567890123456789",
    "345678901234567890"
  ]
}
```

## Patch 형태 요약

```diff
diff --git a/utils/attendance.py b/utils/attendance.py
--- a/utils/attendance.py
+++ b/utils/attendance.py
@@
 import asyncio
+import json
 import re
 import time
+import urllib.error
+import urllib.request
@@
 ATTENDANCE_USER_COOLDOWN_SECONDS = 3.0
+RANKER_ALLIANCE_NAME = "랭커"
+RANKER_POST_URL = "https://script.google.com/macros/s/AKfycby3I-Vo8A8WKYm9dLrexqvlaOTb4KB93C_lKsdHEkHMm-G_hMsD1Proxp03fQvXMpTc6w/exec"
```

설명:
- `json`, `urllib.request`, `urllib.error` 는 표준 라이브러리라 별도 `pip install` 이 필요 없습니다.
- `RANKER_ALLIANCE_NAME` 은 비교 대상 혈맹명 하드코딩입니다.
- `RANKER_POST_URL` 은 전송 대상 URL 하드코딩입니다.

---

```diff
@@
             await send_attendance_summary(
                 self.bot,
                 self.guild,
                 snapshot=self.snapshot,
                 reason="manual",
                 stopped_by_mention=_mention_or_system(self.snapshot.stopped_by_discord_id),
                 save_status=f"DB 저장 완료 (출석 ID: {attendance_id})",
             )
+            await send_ranker_attendance_ids(self.guild, self.snapshot)
             self.disable_all_items()
@@
             await send_attendance_summary(
                 self.bot,
                 self.guild,
                 snapshot=self.snapshot,
                 reason="manual",
                 stopped_by_mention=_mention_or_system(self.snapshot.stopped_by_discord_id),
                 save_status="기록 저장 X",
             )
             self.disable_all_items()
```

설명:
- 수동 종료 시 `예`를 눌러 DB 저장하는 경우에는 외부 전송도 실행합니다.
- 수동 종료 시 `아니요`를 눌러 DB 저장을 하지 않는 경우에는 외부 전송을 하지 않습니다.
- 즉 수동 종료에서는 `저장하는 경우만` API 호출이 일어납니다.

---

```diff
@@
         if result["ok"]:
             snapshot = result["snapshot"]
             await persist_attendance_snapshot(bot, guild, snapshot)
             await send_attendance_summary(
                 bot,
                 guild,
                 snapshot=snapshot,
                 reason="timeout",
                 stopped_by_mention="시스템",
                 save_status=None,
             )
+            await send_ranker_attendance_ids(guild, snapshot)
```

설명:
- 자동 종료 시에도 동일한 외부 전송이 동작합니다.
- 순서는 `자동 종료 -> DB 저장 -> 로그 전송 -> 랭커 ID 전송` 입니다.

---

```diff
@@
+async def send_ranker_attendance_ids(
+    guild: discord.Guild,
+    snapshot: AttendanceSnapshot,
+) -> None:
+    try:
+        ranker_ids = _get_ranker_discord_ids(guild, snapshot)
+        payload = {"ids": ranker_ids}
+        result = await asyncio.to_thread(_post_ranker_ids, payload)
+        if result["ok"]:
+            print(
+                "[attendance] ranker POST success "
+                f"guild_id={guild.id} count={len(ranker_ids)} status={result['status']}"
+            )
+        else:
+            print(
+                "[attendance] ranker POST failed "
+                f"guild_id={guild.id} count={len(ranker_ids)} error={result['error']}"
+            )
+    except Exception as exc:
+        print(
+            "[attendance] ranker POST unexpected failure "
+            f"guild_id={guild.id} error={exc!r}"
+        )
+        return
```

설명:
- 외부 전송 진입점 함수입니다.
- 참가자 목록에서 `[랭커]` 대상만 골라 payload 를 만들고, 실제 HTTP 요청은 별도 함수에 맡깁니다.
- `asyncio.to_thread(...)` 를 사용해 블로킹 네트워크 요청이 이벤트 루프를 막지 않게 했습니다.
- 현재 코드 기준으로는 성공/실패를 `print(...)` 로 콘솔에 남기도록 되어 있습니다.

---

```diff
@@
+def _get_ranker_discord_ids(
+    guild: discord.Guild,
+    snapshot: AttendanceSnapshot,
+) -> list[str]:
+    ranker_ids: list[str] = []
+    for discord_id in snapshot.participant_ids:
+        member = guild.get_member(discord_id)
+        nickname = member.display_name if member is not None else str(discord_id)
+        if _resolve_alliance_name_from_nickname(nickname) != RANKER_ALLIANCE_NAME:
+            continue
+        ranker_ids.append(str(discord_id))
+    return ranker_ids
```

설명:
- 출석 참가자 전체에서 `[랭커]` 소속만 추립니다.
- 이미 있던 `_resolve_alliance_name_from_nickname(...)` 로 닉네임의 혈맹 태그를 재사용해서 판별합니다.
- 최종 반환 타입은 `list[str]` 이고, JSON 바디에 바로 들어갈 수 있도록 문자열 ID로 변환합니다.

---

```diff
@@
-def _post_ranker_ids(payload: dict[str, list[str]]) -> None:
+def _post_ranker_ids(payload: dict[str, list[str]]) -> dict[str, object]:
+    body = json.dumps(payload).encode("utf-8")
+    request = urllib.request.Request(
+        RANKER_POST_URL,
+        data=body,
+        headers={"Content-Type": "application/json"},
+        method="POST",
+    )
+    try:
+        with urllib.request.urlopen(request, timeout=10) as response:
+            response_body = response.read().decode("utf-8", errors="replace")
+            return {
+                "ok": True,
+                "status": getattr(response, "status", None),
+                "body": response_body[:500],
+            }
+    except Exception as exc:
+        return {
+            "ok": False,
+            "error": repr(exc),
+        }
```

설명:
- 실제 `POST application/json` 요청을 보내는 함수입니다.
- `urllib.request.Request(...)` 에 `method="POST"` 와 `Content-Type: application/json` 을 명시합니다.
- 바디는 `json.dumps(payload).encode("utf-8")` 로 만듭니다.
- 성공 시 상태코드와 응답 일부를 반환하고, 실패 시 에러 문자열을 반환합니다.

## 최종 동작 흐름

### 수동 종료 + 저장

1. 출석 종료
2. DB 저장
3. 디스코드 요약 로그 전송
4. `[랭커]` 참가자 ID 외부 전송

### 수동 종료 + 미저장

1. 출석 종료
2. 디스코드 요약 로그 전송
3. 외부 API 호출 없음

### 자동 종료

1. 출석 종료
2. DB 저장
3. 디스코드 요약 로그 전송
4. `[랭커]` 참가자 ID 외부 전송

## 참고

- 현재 구현은 표준 라이브러리만 사용하므로 별도 패키지 설치가 필요 없습니다.
- `[랭커]` 판단 기준은 닉네임에 포함된 혈맹 태그입니다.
- 현재 코드 기준으로는 콘솔에 성공/실패 `print` 로그가 남습니다.
