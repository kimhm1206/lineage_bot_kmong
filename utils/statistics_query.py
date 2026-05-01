from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import discord
import requests
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "lineage_bot.sqlite3"
ROOT_ENV_PATH = BASE_DIR / ".env"
TOKEN_USAGE_PATH = BASE_DIR / "data" / "groq_token_usage.json"
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
MODEL_NAME = "qwen/qwen3-32b"
DAILY_TOKEN_LIMIT = 500_000
MAX_RESULT_ROWS = 50
EMBED_FIELD_LIMIT = 25

TEST_GUILD_ID = 1493532198320603136
TEST_CHANNEL_ID = 1493532198895227045
TEST_LOG_CHANNEL_ID = 1493532243291934760
TEST_QUERY_CHANNEL_IDS = {
    TEST_CHANNEL_ID,
    TEST_LOG_CHANNEL_ID,
}
COMMAND_PREFIX = "통계-"

QUERY_TYPES = {
    "session_detail",
    "session_list",
    "user_ranking",
    "alliance_ranking",
    "stats_summary",
    "filtered_count",
}
ALLOWED_TABLES = {
    "attendance_sessions",
    "attendance_entries",
    "users",
    "alliances",
}
BANNED_SQL_TOKENS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "attach",
    "detach",
    "pragma",
    "replace",
    "truncate",
    "create",
}

SYSTEM_PROMPT = """
You generate SQLite SELECT queries for a Lineage attendance bot.
Analyze the user's request and return JSON only.

Today is {TODAY}. Yesterday is {YESTERDAY}.
If only a month is mentioned, assume year 2026.
Interpret all relative dates and times in KST (Asia/Seoul).

Schema:
- attendance_sessions(attendance_id, guild_id, started_at, ended_at, started_by_discord_id)
- attendance_entries(attendance_id, user_id)
- users(user_id, alliance_id, discord_id, discord_nickname)
- alliances(alliance_id, alliance_name)

Joins:
- attendance_sessions.attendance_id = attendance_entries.attendance_id
- attendance_entries.user_id = users.user_id
- users.alliance_id = alliances.alliance_id

Return exactly:
{{
  "query_type": "session_detail | session_list | user_ranking | alliance_ranking | stats_summary | filtered_count",
  "sql": "SELECT ...",
  "title": "Korean title, emoji allowed"
}}

Rules:
- Output valid JSON only. No markdown. No explanation.
- SQL must be a single SELECT or WITH ... SELECT query.
- No semicolon.
- No INSERT, UPDATE, DELETE, DROP, ALTER, ATTACH, DETACH, PRAGMA, CREATE, REPLACE.
- Use started_at LIKE for date filters.
- All date and time filtering must be interpreted in KST, not UTC.
- Use COALESCE(a.alliance_name, '미분류') when alliance can be null.
- Always include LIMIT <= 50.
- Do not invent guild_id filters unless the user explicitly asks for guild_id.
- If the user mentions a specific alliance, you must apply an alliance filter.
- Treat expressions like "랭커 멤버", "랭커 맴버", "랭커 혈맹", "랭커 혈맹 멤버", "[랭커] 멤버", "[랭커] 맴버", "[랭커] 유저", and "'랭커' 멤버" as the same alliance filter for alliance_name = '랭커'.
- If an alliance name appears inside square brackets, remove the brackets and use the inner text as the alliance_name filter.
- If an alliance filter is detected, use COALESCE(a.alliance_name, '미분류') = '<alliance_name>'.

Required shapes:
- session_detail columns:
  attendance_id, started_at, ended_at, session_total_count, alliance_name, alliance_count, nicknames
- session_list columns:
  attendance_id, started_at, ended_at, total_count
- user_ranking columns:
  rank, discord_nickname, alliance_name, attendance_count
- alliance_ranking columns:
  rank, alliance_name, attendance_count
- stats_summary columns:
  period, session_count, total_participants, avg_participants
- filtered_count columns:
  filter_label, total_participants

Important for session_detail:
- Return one row per alliance within exactly one chosen session.
- nicknames must use GROUP_CONCAT(u.discord_nickname, '\n').
- alliance_count must use COUNT(e.user_id) with GROUP BY attendance_id and alliance_name.
- session_total_count must use SUM(COUNT(e.user_id)) OVER (PARTITION BY s.attendance_id).
- Do not use COUNT(*) for session_total_count in session_detail.
- If the request means first / last / most recent / earliest / latest / first session of a date / last session of a date / specific hour bucket like 00시, first isolate exactly one attendance_id using a CTE or subquery, then query detail rows by that attendance_id.
- Do not LIMIT 1 on the final joined detail rows.
- Group by attendance_id and alliance_name.

Date examples:
- These date examples are KST-resolved values.
- 4월 21일 -> started_at LIKE '2026-04-21%'
- 4월 21일 00시 -> started_at LIKE '2026-04-21 00%'
- 4월 -> started_at LIKE '2026-04-%'
- 어제 -> started_at LIKE '{YESTERDAY}%'
- 오늘 -> started_at LIKE '{TODAY}%'
""".strip()


@dataclass(slots=True)
class StatisticsResult:
    title: str
    query_type: str
    sql: str
    rows: list[dict[str, Any]]
    embeds: list[discord.Embed]
    request_tokens: int
    used_tokens: int
    remaining_percent: float


def is_statistics_trigger_message(message: discord.Message) -> bool:
    if message.author.bot:
        return False
    if message.guild is None or message.guild.id != TEST_GUILD_ID:
        return False
    if message.channel.id not in TEST_QUERY_CHANNEL_IDS:
        return False
    return message.content.strip().startswith(COMMAND_PREFIX)


def extract_statistics_question(message_content: str) -> str:
    content = message_content.strip()
    if not content.startswith(COMMAND_PREFIX):
        return ""
    return content[len(COMMAND_PREFIX) :].strip()


def run_statistics_query(question: str) -> StatisticsResult:
    api_key = _load_groq_api_key()
    if not api_key:
        raise RuntimeError("GROQ API 키를 찾을 수 없습니다.")

    known_alliances = load_known_alliance_names(DB_PATH)
    enriched_question, _ = enrich_question_with_hints(question, known_alliances)
    raw_response = request_groq_response(enriched_question, api_key)
    token_usage = update_daily_token_usage(raw_response)
    parsed = extract_model_json(raw_response)
    query_type = validate_query_type(str(parsed.get("query_type", "")).strip())
    sql = validate_select_sql(str(parsed.get("sql", "")).strip())
    title = str(parsed.get("title", "")).strip() or "통계 조회 결과"
    rows = run_readonly_query(DB_PATH, sql)
    validate_result_shape(query_type, rows)
    embeds = build_discord_embeds(
        title=title,
        query_type=query_type,
        rows=rows,
        remaining_percent=token_usage["remaining_percent"],
    )
    return StatisticsResult(
        title=title,
        query_type=query_type,
        sql=sql,
        rows=rows,
        embeds=embeds,
        request_tokens=token_usage["request_tokens"],
        used_tokens=token_usage["used_tokens"],
        remaining_percent=token_usage["remaining_percent"],
    )


def _load_groq_api_key() -> str | None:
    load_dotenv(ROOT_ENV_PATH)
    return os.getenv("APIKEY")


def _today_strings() -> tuple[str, str]:
    now_kst = datetime.now(timezone(timedelta(hours=9)))
    today = now_kst.date().isoformat()
    yesterday = (now_kst.date() - timedelta(days=1)).isoformat()
    return today, yesterday


def build_groq_payload(question: str) -> dict[str, Any]:
    today, yesterday = _today_strings()
    system_prompt = SYSTEM_PROMPT.format(TODAY=today, YESTERDAY=yesterday)
    return {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }


def request_groq_response(question: str, api_key: str) -> dict[str, Any]:
    payload = build_groq_payload(question)
    try:
        return post_groq_payload(payload, api_key)
    except RuntimeError as exc:
        if "json_validate_failed" not in str(exc) and "failed_generation" not in str(exc):
            raise

    fallback_payload = {
        "model": MODEL_NAME,
        "messages": payload["messages"],
        "temperature": 0,
    }
    return post_groq_payload(fallback_payload, api_key)


def post_groq_payload(payload: dict[str, Any], api_key: str) -> dict[str, Any]:
    try:
        response = requests.post(
            GROQ_API_URL,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            timeout=60,
        )
        response.raise_for_status()
    except requests.HTTPError as exc:
        error_body = exc.response.text if exc.response is not None else str(exc)
        status_code = exc.response.status_code if exc.response is not None else "unknown"
        raise RuntimeError(
            f"Groq 요청 실패: status={status_code} body={error_body[:800]}"
        ) from exc

    return response.json()


def extract_model_json(raw_response: dict[str, Any]) -> dict[str, Any]:
    choices = raw_response.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("Groq 응답에 choices가 없습니다.")

    message = choices[0].get("message", {})
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("Groq 응답 content가 비어 있습니다.")

    try:
        return parse_json_object_from_text(content)
    except Exception as exc:
        raise RuntimeError(f"모델 content JSON 파싱 실패: raw={content[:800]}") from exc


def parse_json_object_from_text(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    for index, char in enumerate(cleaned):
        if char != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(cleaned[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            return obj
    raise ValueError("JSON object not found in model output")


def validate_query_type(query_type: str) -> str:
    if query_type not in QUERY_TYPES:
        raise RuntimeError(f"허용되지 않은 query_type입니다: {query_type}")
    return query_type


def validate_select_sql(sql: str) -> str:
    normalized = sql.strip()
    if not normalized:
        raise RuntimeError("빈 SQL입니다.")

    lowered = normalized.lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise RuntimeError("SELECT 또는 WITH SELECT 쿼리만 허용됩니다.")

    if ";" in normalized[:-1]:
        raise RuntimeError("다중 SQL은 허용되지 않습니다.")

    if normalized.endswith(";"):
        normalized = normalized[:-1].rstrip()
        lowered = normalized.lower()

    for token in BANNED_SQL_TOKENS:
        if re.search(rf"\b{re.escape(token)}\b", lowered):
            raise RuntimeError(f"금지된 SQL 키워드가 포함되었습니다: {token}")

    referenced_tables = set(
        match.group(1).lower()
        for match in re.finditer(
            r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)",
            lowered,
        )
    )
    if not referenced_tables:
        raise RuntimeError("참조 테이블을 찾을 수 없습니다.")

    cte_names = extract_cte_names(lowered)
    unknown_tables = referenced_tables - ALLOWED_TABLES - cte_names
    if unknown_tables:
        joined = ", ".join(sorted(unknown_tables))
        raise RuntimeError(f"허용되지 않은 테이블이 포함되었습니다: {joined}")

    if "limit" not in lowered:
        normalized = f"{normalized}\nLIMIT {MAX_RESULT_ROWS}"
    return normalized


def extract_cte_names(lowered_sql: str) -> set[str]:
    names = set(
        match.group(1).lower()
        for match in re.finditer(
            r"\bwith\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+as\b",
            lowered_sql,
        )
    )
    names.update(
        match.group(1).lower()
        for match in re.finditer(
            r",\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+as\b",
            lowered_sql,
        )
    )
    return names


def run_readonly_query(db_path: Path, sql: str) -> list[dict[str, Any]]:
    uri = f"file:{db_path.as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(sql).fetchall()
    finally:
        connection.close()
    return [dict(row) for row in rows]


def validate_result_shape(query_type: str, rows: list[dict[str, Any]]) -> None:
    expected_columns = {
        "session_detail": {
            "attendance_id",
            "started_at",
            "ended_at",
            "session_total_count",
            "alliance_name",
            "alliance_count",
            "nicknames",
        },
        "session_list": {"attendance_id", "started_at", "ended_at", "total_count"},
        "user_ranking": {"rank", "discord_nickname", "alliance_name", "attendance_count"},
        "alliance_ranking": {"rank", "alliance_name", "attendance_count"},
        "stats_summary": {"period", "session_count", "total_participants", "avg_participants"},
        "filtered_count": {"filter_label", "total_participants"},
    }
    if not rows:
        return
    actual = set(rows[0].keys())
    missing = expected_columns[query_type] - actual
    if missing:
        joined = ", ".join(sorted(missing))
        raise RuntimeError(f"{query_type} 결과 컬럼이 부족합니다. 누락: {joined}")


def build_discord_embeds(
    *,
    title: str,
    query_type: str,
    rows: list[dict[str, Any]],
    remaining_percent: float,
) -> list[discord.Embed]:
    embed = discord.Embed(
        title=title or "통계 조회 결과",
        color=discord.Color.green(),
    )
    fields = build_embed_fields_for_query_type(query_type, rows)
    if not fields:
        embed.description = "결과 없음"
    else:
        for field in fields[:EMBED_FIELD_LIMIT]:
            embed.add_field(
                name=field["name"],
                value=field["value"],
                inline=bool(field.get("inline", False)),
            )
    embed.set_footer(text=format_remaining_token_footer(remaining_percent))
    return [embed]


def build_embed_fields_for_query_type(
    query_type: str,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if query_type == "session_detail":
        return build_session_detail_fields(rows)
    if query_type == "session_list":
        return build_single_block_field("세션 목록", build_session_list_text(rows))
    if query_type == "user_ranking":
        return build_single_block_field("유저 랭킹", build_user_ranking_text(rows))
    if query_type == "alliance_ranking":
        return build_single_block_field("혈맹 랭킹", build_alliance_ranking_text(rows))
    if query_type == "stats_summary":
        return build_single_block_field("통계 요약", build_stats_summary_text(rows))
    if query_type == "filtered_count":
        return build_single_block_field("집계 결과", build_filtered_count_text(rows))
    return build_fallback_fields(rows)


def build_single_block_field(name: str, text: str) -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "value": f"```{(text or '결과 없음')[:1000]}```",
            "inline": False,
        }
    ]


def build_session_detail_fields(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []

    first = rows[0]
    fields = [
        {
            "name": "세션 정보",
            "value": (
                f"ID: {first.get('attendance_id', '')}\n"
                f"시작: {first.get('started_at', '')}\n"
                f"종료: {first.get('ended_at', '')}\n"
                f"참석: {_safe_int(first.get('session_total_count'))}명"
            ),
            "inline": False,
        }
    ]

    for row in rows[: EMBED_FIELD_LIMIT - 1]:
        nicknames = split_nickname_blob(str(row.get("nicknames", "") or ""))
        text = "\n".join(nicknames) if nicknames else "없음"
        fields.append(
            {
                "name": f"{row.get('alliance_name', '미분류')} ({_safe_int(row.get('alliance_count'))}명)",
                "value": f"```{text[:1000]}```",
                "inline": False,
            }
        )
    return fields[:EMBED_FIELD_LIMIT]


def build_session_list_text(rows: list[dict[str, Any]]) -> str:
    return "\n".join(
        f"ID {row.get('attendance_id', '')} | {row.get('started_at', '')} | {row.get('total_count', 0)}명"
        for row in rows[:20]
    )


def build_user_ranking_text(rows: list[dict[str, Any]]) -> str:
    return "\n".join(
        f"{row.get('rank', '')}. {row.get('discord_nickname', '')} "
        f"[{row.get('alliance_name', '미분류')}] - {row.get('attendance_count', 0)}"
        for row in rows[:20]
    )


def build_alliance_ranking_text(rows: list[dict[str, Any]]) -> str:
    return "\n".join(
        f"{row.get('rank', '')}. {row.get('alliance_name', '미분류')} - {row.get('attendance_count', 0)}"
        for row in rows[:20]
    )


def build_stats_summary_text(rows: list[dict[str, Any]]) -> str:
    return "\n".join(
        f"{row.get('period', '')} | 세션 {row.get('session_count', 0)} | "
        f"총 {row.get('total_participants', 0)} | 평균 {row.get('avg_participants', 0)}"
        for row in rows[:20]
    )


def build_filtered_count_text(rows: list[dict[str, Any]]) -> str:
    return "\n".join(
        f"{row.get('filter_label', '')} : {row.get('total_participants', 0)}"
        for row in rows[:20]
    )


def build_fallback_fields(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    preview = json.dumps(rows[:5], ensure_ascii=False, indent=2)
    return [
        {
            "name": "Raw Rows",
            "value": f"```json\n{preview[:1000]}\n```",
            "inline": False,
        }
    ]


def split_nickname_blob(value: str) -> list[str]:
    text = value.strip()
    if not text:
        return []
    if "\n" in text:
        return [line.strip() for line in text.splitlines() if line.strip()]
    return [part.strip() for part in text.split(",") if part.strip()]


def update_daily_token_usage(raw_response: dict[str, Any]) -> dict[str, Any]:
    usage = raw_response.get("usage", {})
    request_tokens = _safe_int(usage.get("total_tokens"))
    today_utc = current_utc_day()
    stored = load_token_usage_snapshot()

    used_tokens = 0
    if stored.get("date") == today_utc:
        used_tokens = _safe_int(stored.get("used_tokens"))

    used_tokens += request_tokens
    remaining_tokens = max(0, DAILY_TOKEN_LIMIT - used_tokens)
    remaining_percent = (remaining_tokens / DAILY_TOKEN_LIMIT) * 100

    save_token_usage_snapshot(
        {
            "date": today_utc,
            "used_tokens": used_tokens,
        }
    )
    return {
        "date": today_utc,
        "request_tokens": request_tokens,
        "used_tokens": used_tokens,
        "remaining_tokens": remaining_tokens,
        "remaining_percent": remaining_percent,
    }


def load_token_usage_snapshot() -> dict[str, Any]:
    if not TOKEN_USAGE_PATH.exists():
        return {}
    try:
        return json.loads(TOKEN_USAGE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_token_usage_snapshot(data: dict[str, Any]) -> None:
    TOKEN_USAGE_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_USAGE_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def current_utc_day() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def format_remaining_token_footer(remaining_percent: float) -> str:
    return f"{remaining_percent:.1f}% 남음"


def load_known_alliance_names(db_path: Path) -> list[str]:
    if not db_path.exists():
        return []
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            "SELECT alliance_name FROM alliances ORDER BY alliance_name ASC"
        ).fetchall()
    except sqlite3.DatabaseError:
        return []
    finally:
        connection.close()
    return [str(row["alliance_name"]).strip() for row in rows if row["alliance_name"]]


def enrich_question_with_hints(
    question: str,
    known_alliances: list[str],
) -> tuple[str, list[str]]:
    hint_lines: list[str] = []
    alliance_name = detect_alliance_filter(question, known_alliances)
    if alliance_name:
        hint_lines.append(f"Detected alliance filter: {alliance_name}")
    if not hint_lines:
        return question, hint_lines
    enriched = question + "\n\n" + "\n".join(hint_lines)
    return enriched, hint_lines


def detect_alliance_filter(question: str, known_alliances: list[str]) -> str | None:
    bracket_match = re.search(r"\[([^\[\]]+)\]", question)
    if bracket_match:
        return bracket_match.group(1).strip()

    quote_match = re.search(r"'([^']+)'", question)
    if quote_match:
        return quote_match.group(1).strip()

    sorted_alliances = sorted(known_alliances, key=len, reverse=True)
    for alliance_name in sorted_alliances:
        escaped = re.escape(alliance_name)
        patterns = [
            rf"{escaped}\s*혈맹",
            rf"{escaped}\s*멤버",
            rf"{escaped}\s*맴버",
            rf"{escaped}\s*유저",
            rf"{escaped}\s*중",
        ]
        if any(re.search(pattern, question) for pattern in patterns):
            return alliance_name

    return None


def _safe_int(value: object) -> int:
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0
