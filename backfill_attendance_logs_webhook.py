from __future__ import annotations

import argparse
import json
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "lineage_bot.sqlite3"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
EMBED_DESCRIPTION_LIMIT = 4096
EMBED_FIELD_VALUE_LIMIT = 1024
EMBED_FIELD_COUNT_LIMIT = 25
EMBED_TOTAL_TEXT_LIMIT = 6000
MESSAGE_EMBED_LIMIT = 10


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
        "--start-id",
        type=int,
        default=1,
        help="전송 시작 출석 ID (기본값: 1)",
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

    if not args.send:
        print("[ready] --send 를 붙이면 전체 출석 로그를 순서대로 전송합니다.")
        return

    if not args.webhook_url:
        raise SystemExit("--send 사용 시 --webhook-url 이 필요합니다.")

    send_records = [
        record for record in records if record.attendance_id >= args.start_id
    ]
    if not send_records:
        raise SystemExit(
            f"출석 ID {args.start_id} 이상인 전송 대상이 없습니다."
        )

    for record in send_records:
        payload = build_payload_for_record(record)
        post_webhook_message(args.webhook_url, payload)
        print(
            f"[sent] attendance_id={record.attendance_id} "
            f"participants={record.participant_count} embeds={len(payload['embeds'])}"
        )
    print(f"[done] attendance_count={len(send_records)}")


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


def build_payload_for_record(record: AttendanceRecord) -> dict[str, Any]:
    header_lines = [
        f"출석 ID: {record.attendance_id}",
        f"확인 시작 시간 : {_format_summary_timestamp(record.started_at)}",
        f"참석 총인원 : {record.participant_count}명",
    ]
    embeds = _build_embeds(record, header_lines)
    return {
        "content": "",
        "embeds": embeds,
        "allowed_mentions": {"parse": []},
    }


def _build_embeds(
    record: AttendanceRecord,
    header_lines: list[str],
) -> list[dict[str, Any]]:
    embeds: list[dict[str, Any]] = [
        {
            "title": "출석 로그",
            "description": "\n".join(header_lines),
            "color": 0x57F287,
        }
    ]

    for clan_name, nicknames in record.participants_by_clan.items():
        field_title = f"[{clan_name}] : {len(nicknames)}명"
        for chunk_index, chunk_value in enumerate(
            _split_nicknames_for_field(nicknames),
            start=1,
        ):
            name = field_title
            if chunk_index > 1:
                name = f"{field_title} ({chunk_index})"
            _append_field_to_embeds(embeds, name, f"```{chunk_value}```")

    if not record.participants_by_clan:
        _append_field_to_embeds(embeds, "[미분류] : 0명", "```없음```")

    if len(embeds) > MESSAGE_EMBED_LIMIT:
        raise RuntimeError(
            f"출석 ID {record.attendance_id} 는 Discord 한 메시지 제한을 넘습니다. "
            f"(embeds={len(embeds)})"
        )
    return embeds


def _append_field_to_embeds(
    embeds: list[dict[str, Any]],
    name: str,
    value: str,
) -> None:
    new_field_size = len(name) + len(value)
    current_embed = embeds[-1]
    current_fields = current_embed.setdefault("fields", [])
    current_size = _embed_text_size(current_embed)
    if (
        len(current_fields) >= EMBED_FIELD_COUNT_LIMIT
        or current_size + new_field_size > EMBED_TOTAL_TEXT_LIMIT
    ):
        embeds.append(
            {
                "color": 0x5865F2,
                "fields": [],
            }
        )
        current_embed = embeds[-1]
        current_fields = current_embed["fields"]

    current_fields.append(
        {
            "name": name,
            "value": value,
            "inline": False,
        }
    )


def _split_nicknames_for_field(nicknames: list[str]) -> list[str]:
    if not nicknames:
        return ["없음"]

    chunks: list[str] = []
    current_lines: list[str] = []
    current_length = 0
    available_length = EMBED_FIELD_VALUE_LIMIT - 6

    for nickname in nicknames:
        extra = len(nickname) + (1 if current_lines else 0)
        if current_lines and current_length + extra > available_length:
            chunks.append("\n".join(current_lines))
            current_lines = [nickname]
            current_length = len(nickname)
            continue

        if not current_lines and len(nickname) > available_length:
            truncated = nickname[: available_length - 3] + "..."
            chunks.append(truncated)
            current_lines = []
            current_length = 0
            continue

        current_lines.append(nickname)
        current_length += extra

    if current_lines:
        chunks.append("\n".join(current_lines))

    return chunks


def _embed_text_size(embed: dict[str, Any]) -> int:
    size = 0
    for key in ("title", "description"):
        value = embed.get(key)
        if isinstance(value, str):
            size += len(value)
    footer = embed.get("footer")
    if isinstance(footer, dict):
        text = footer.get("text")
        if isinstance(text, str):
            size += len(text)
    for field in embed.get("fields", []):
        size += len(str(field.get("name", "")))
        size += len(str(field.get("value", "")))
    return size


def post_webhook_message(webhook_url: str, payload: dict[str, Any]) -> None:
    while True:
        try:
            response = requests.post(
                webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            if response.status_code == 429:
                retry_after = _extract_retry_after(response.text, response.headers)
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            reset_after = response.headers.get("X-RateLimit-Reset-After")
            if reset_after:
                try:
                    time.sleep(float(reset_after))
                except ValueError:
                    pass
            return
        except requests.HTTPError as exc:
            response = exc.response
            status_code = response.status_code if response is not None else "unknown"
            response_body = response.text if response is not None else str(exc)
            raise RuntimeError(
                f"Webhook 전송 실패: status={status_code} body={response_body[:500]}"
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


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


if __name__ == "__main__":
    main()
