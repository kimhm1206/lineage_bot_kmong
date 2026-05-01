from __future__ import annotations

import argparse
import json
import sqlite3
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "lineage_bot.sqlite3"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DISCORD_MESSAGE_LIMIT = 1800


@dataclass(slots=True)
class AttendanceRecord:
    attendance_id: int
    guild_id: int
    started_at: str
    ended_at: str
    started_by_discord_id: int | None
    participants_by_clan: dict[str, list[str]] = field(default_factory=dict)
    participant_count: int = 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="기존 출석 기록을 Discord webhook 로그로 백필합니다."
    )
    parser.add_argument("--webhook-url", help="Discord webhook URL")
    parser.add_argument(
        "--preview-id",
        type=int,
        default=1,
        help="미리보기할 출석 ID (기본값: 1)",
    )
    parser.add_argument(
        "--start-id",
        type=int,
        default=1,
        help="전송 시작 출석 ID (기본값: 1)",
    )
    parser.add_argument(
        "--end-id",
        type=int,
        default=None,
        help="전송 종료 출석 ID (기본값: 마지막 출석 ID)",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="실제로 webhook 전송까지 수행합니다.",
    )
    args = parser.parse_args()

    records = load_attendance_records(DB_PATH)
    if not records:
        raise SystemExit("출석 데이터가 없습니다.")

    preview_record = next(
        (record for record in records if record.attendance_id == args.preview_id),
        None,
    )
    if preview_record is None:
        raise SystemExit(f"출석 ID {args.preview_id} 데이터를 찾을 수 없습니다.")

    preview_messages = build_messages_for_record(preview_record)
    print(f"[preview] attendance_id={preview_record.attendance_id}")
    for index, message in enumerate(preview_messages, start=1):
        print(f"\n--- message {index} ---")
        print(message)

    if not args.send:
        return

    if not args.webhook_url:
        raise SystemExit("--send 사용 시 --webhook-url 이 필요합니다.")

    send_records = [
        record
        for record in records
        if record.attendance_id >= args.start_id
        and (args.end_id is None or record.attendance_id <= args.end_id)
    ]
    if not send_records:
        raise SystemExit("전송할 출석 데이터가 없습니다.")

    total_messages = 0
    for record in send_records:
        messages = build_messages_for_record(record)
        for message in messages:
            post_webhook_message(args.webhook_url, {"content": message})
            total_messages += 1
        print(
            f"[sent] attendance_id={record.attendance_id} "
            f"participants={record.participant_count} messages={len(messages)}"
        )

    print(
        f"[done] attendance_count={len(send_records)} total_messages={total_messages}"
    )


def load_attendance_records(db_path: Path) -> list[AttendanceRecord]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT
                s.attendance_id,
                s.guild_id,
                s.started_at,
                s.ended_at,
                s.started_by_discord_id,
                u.discord_nickname,
                COALESCE(a.alliance_name, '미분류') AS alliance_name
            FROM attendance_sessions s
            LEFT JOIN attendance_entries e ON e.attendance_id = s.attendance_id
            LEFT JOIN users u ON u.user_id = e.user_id
            LEFT JOIN alliances a ON a.alliance_id = u.alliance_id
            ORDER BY s.attendance_id ASC, alliance_name ASC, u.discord_nickname ASC
            """
        ).fetchall()
    finally:
        connection.close()

    records_by_id: dict[int, AttendanceRecord] = {}
    for row in rows:
        attendance_id = int(row["attendance_id"])
        record = records_by_id.get(attendance_id)
        if record is None:
            record = AttendanceRecord(
                attendance_id=attendance_id,
                guild_id=int(row["guild_id"]),
                started_at=str(row["started_at"]),
                ended_at=str(row["ended_at"]),
                started_by_discord_id=_optional_int(row["started_by_discord_id"]),
            )
            records_by_id[attendance_id] = record

        nickname = row["discord_nickname"]
        if nickname is None:
            continue

        alliance_name = str(row["alliance_name"])
        nickname_text = str(nickname)
        record.participants_by_clan.setdefault(alliance_name, []).append(nickname_text)
        record.participant_count += 1

    for record in records_by_id.values():
        for nicknames in record.participants_by_clan.values():
            nicknames.sort()

    return list(records_by_id.values())


def build_messages_for_record(record: AttendanceRecord) -> list[str]:
    header_lines = [
        f"출석 ID: {record.attendance_id})",
        f"확인 시작 시간 : {_format_summary_timestamp(record.started_at)}",
        f"참석 총인원 : {record.participant_count}명",
    ]
    return _build_summary_messages(header_lines, record.participants_by_clan)


def _build_summary_messages(
    header_lines: list[str],
    clan_members: dict[str, list[str]],
) -> list[str]:
    messages = ["\n".join(header_lines)]
    if not clan_members:
        return messages + ["[미분류] : 0명\n```없음```"]

    current_message = ""
    for clan_name, nicknames in clan_members.items():
        section = "\n".join(
            [
                f"[{clan_name}] : {len(nicknames)}명",
                f"```{chr(10).join(nicknames) if nicknames else '없음'}```",
            ]
        )
        if len(section) > DISCORD_MESSAGE_LIMIT:
            section = _truncate_clan_section(clan_name, nicknames)
        if not current_message:
            current_message = section
            continue
        if len(current_message) + 2 + len(section) > DISCORD_MESSAGE_LIMIT:
            messages.append(current_message)
            current_message = section
            continue
        current_message = f"{current_message}\n\n{section}"

    if current_message:
        messages.append(current_message)
    return messages


def _truncate_clan_section(clan_name: str, nicknames: list[str]) -> str:
    lines: list[str] = []
    current_length = len(f"[{clan_name}] : {len(nicknames)}명\n")
    for nickname in nicknames:
        extra = len(nickname) + (1 if lines else 0)
        if current_length + extra > 1750:
            lines.append("...")
            break
        lines.append(nickname)
        current_length += extra
    return "\n".join(
        [
            f"[{clan_name}] : {len(nicknames)}명",
            f"```{chr(10).join(lines) if lines else '없음'}```",
        ]
    )


def post_webhook_message(webhook_url: str, payload: dict[str, Any]) -> None:
    body = json.dumps(payload).encode("utf-8")
    while True:
        request = urllib.request.Request(
            webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                reset_after = response.headers.get("X-RateLimit-Reset-After")
                if reset_after:
                    try:
                        time.sleep(float(reset_after))
                    except ValueError:
                        pass
                return
        except urllib.error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            if exc.code == 429:
                retry_after = _extract_retry_after(response_body, exc.headers)
                time.sleep(retry_after)
                continue
            raise RuntimeError(
                f"Webhook 전송 실패: status={exc.code} body={response_body[:500]}"
            ) from exc


def _extract_retry_after(response_body: str, headers: Any) -> float:
    retry_after_header = headers.get("Retry-After")
    if retry_after_header:
        try:
            return float(retry_after_header)
        except ValueError:
            pass
    try:
        data = json.loads(response_body)
    except json.JSONDecodeError:
        return 1.0
    retry_after = data.get("retry_after")
    if isinstance(retry_after, (int, float)):
        return float(retry_after)
    return 1.0


def _format_summary_timestamp(value: str) -> str:
    try:
        dt = datetime.strptime(value, TIME_FORMAT)
        return dt.strftime("%m월%d일 %H시 %M분")
    except ValueError:
        return value


def _mention_or_unknown(discord_id: int | None) -> str:
    if discord_id is None:
        return "알 수 없음"
    return f"<@{discord_id}>"


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


if __name__ == "__main__":
    main()
