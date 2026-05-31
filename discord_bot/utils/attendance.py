from __future__ import annotations

import asyncio
import json
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import discord

from common import database
from discord_bot.utils.panel import (
    build_attendance_embed,
    clear_attendance_state,
    delete_attendance_message,
    get_attendance_state,
    set_attendance_state,
    update_admin_panel,
)


TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
ALLIANCE_PATTERN = re.compile(r"\[([^\[\]]{2,4})\]")
KST = timezone(timedelta(hours=9))
ATTENDANCE_USER_COOLDOWN_SECONDS = 3.0
RANKER_ROLE_IDS = {
    1497949015570907196,
    1507247503928590377,
}
RANKER_POST_URL = "https://script.google.com/macros/s/AKfycby3I-Vo8A8WKYm9dLrexqvlaOTb4KB93C_lKsdHEkHMm-G_hMsD1Proxp03fQvXMpTc6w/exec"


@dataclass(slots=True)
class AttendanceSnapshot:
    guild_id: int
    started_at: str
    ended_at: str
    started_by_discord_id: int | None
    stopped_by_discord_id: int | None
    participant_ids: list[int]


class AttendanceActionView(discord.ui.View):
    def __init__(self, bot: discord.Bot, guild_id: int):
        super().__init__(timeout=None)
        self.bot = bot
        self.guild_id = guild_id

    @discord.ui.button(
        label="출석하기",
        style=discord.ButtonStyle.success,
        custom_id="attendance:join",
    )
    async def attend_button(
        self, button: discord.ui.Button, interaction: discord.Interaction
    ) -> None:
        guild = interaction.guild
        user = interaction.user

        if guild is None or not isinstance(user, discord.Member):
            await _safe_interaction_response(
                interaction,
                "서버에서만 사용할 수 있습니다.",
            )
            return

        if not await _safe_interaction_defer(interaction):
            return

        _, message = await register_attendance(self.bot, guild.id, user)
        await _safe_interaction_response(interaction, message, ephemeral=True)


class AttendanceRecordPromptView(discord.ui.View):
    def __init__(
        self,
        bot: discord.Bot,
        guild: discord.Guild,
        snapshot: AttendanceSnapshot,
    ):
        super().__init__(timeout=300)
        self.bot = bot
        self.guild = guild
        self.snapshot = snapshot
        self.completed = False
        self.processing = False
        self._state_lock = asyncio.Lock()

    @discord.ui.button(label="예", style=discord.ButtonStyle.success)
    async def yes_button(
        self, button: discord.ui.Button, interaction: discord.Interaction
    ) -> None:
        if not await self._begin_processing(interaction):
            return

        await interaction.response.defer()
        try:
            attendance_id = await persist_attendance_snapshot(
                self.bot,
                self.guild,
                self.snapshot,
            )
            await send_attendance_summary(
                self.bot,
                self.guild,
                snapshot=self.snapshot,
                reason="manual",
                stopped_by_mention=_mention_or_system(self.snapshot.stopped_by_discord_id),
                save_status=f"DB 저장 완료 (출석 ID: {attendance_id})",
            )
            await send_ranker_attendance_ids(self.guild, self.snapshot)
            self.disable_all_items()
            await interaction.edit_original_response(
                content=(
                    f"출석 기록을 저장했습니다. "
                    f"(출석 ID: {attendance_id}, 인원: {len(self.snapshot.participant_ids)}명)"
                ),
                view=self,
            )
        except Exception:
            await self._reset_processing()
            await interaction.edit_original_response(
                content="출석 기록 저장 중 오류가 발생했습니다. 다시 시도해주세요.",
                view=self,
            )

    @discord.ui.button(label="아니요", style=discord.ButtonStyle.secondary)
    async def no_button(
        self, button: discord.ui.Button, interaction: discord.Interaction
    ) -> None:
        if not await self._begin_processing(interaction):
            return

        await interaction.response.defer()
        try:
            await send_attendance_summary(
                self.bot,
                self.guild,
                snapshot=self.snapshot,
                reason="manual",
                stopped_by_mention=_mention_or_system(self.snapshot.stopped_by_discord_id),
                save_status="기록 저장 X",
            )
            self.disable_all_items()
            await interaction.edit_original_response(
                content="이번 출석 기록은 저장하지 않습니다.",
                view=self,
            )
        except Exception:
            await self._reset_processing()
            await interaction.edit_original_response(
                content="출석 종료 처리 중 오류가 발생했습니다. 다시 시도해주세요.",
                view=self,
            )

    async def _begin_processing(self, interaction: discord.Interaction) -> bool:
        async with self._state_lock:
            if self.completed:
                await _safe_interaction_response(
                    interaction,
                    "이미 처리된 출석 기록입니다.",
                )
                return False
            if self.processing:
                await _safe_interaction_response(
                    interaction,
                    "이미 처리 중입니다. 잠시만 기다려주세요.",
                )
                return False
            self.processing = True
            self.completed = True
            return True

    async def _reset_processing(self) -> None:
        async with self._state_lock:
            self.processing = False
            self.completed = False


async def start_attendance(
    bot: discord.Bot,
    guild: discord.Guild,
    starter: discord.Member,
) -> tuple[bool, str]:
    async with _get_attendance_lock(bot, guild.id):
        state = get_attendance_state(bot, guild.id)
        if bool(state.get("active")):
            return False, "이미 출석이 진행 중입니다."

        settings = database.get_settings(guild.id)
        if settings.attendance_voice_channel_id is None:
            return False, "출석 음성채널을 먼저 설정해주세요."

        if settings.timer is None:
            return False, "타이머를 먼저 설정해주세요."

        voice_channel = guild.get_channel(settings.attendance_voice_channel_id)
        if voice_channel is None:
            return False, "설정된 출석 음성채널을 찾을 수 없습니다."

        attendance_channel_id = settings.admin_channel_id
        if attendance_channel_id is None:
            return False, "출석 채널을 먼저 설정해주세요."

        attendance_channel = guild.get_channel(attendance_channel_id)
        if not isinstance(attendance_channel, discord.TextChannel):
            return False, "설정된 출석 채널에 메시지를 보낼 수 없습니다."

        attendance_message = await attendance_channel.send(
            embed=build_attendance_embed(guild, voice_channel, settings.timer),
            view=AttendanceActionView(bot, guild.id),
        )

        started_at = _now_kst()
        started_at_text = started_at.strftime(TIME_FORMAT)
        expires_at_text = (started_at + timedelta(seconds=settings.timer)).strftime(
            TIME_FORMAT
        )

        task = asyncio.create_task(
            _expire_attendance_after_timeout(bot, guild.id, settings.timer)
        )
        set_attendance_state(
            bot,
            guild.id,
            channel_id=attendance_channel.id,
            message_id=attendance_message.id,
            task=task,
            started_by=starter.id,
            started_at=started_at_text,
            expires_at=expires_at_text,
            live_session_id=None,
            voice_channel_id=settings.attendance_voice_channel_id,
            attendance_available_timer=settings.attendance_available_timer,
        )
        _schedule_attendance_state_publish(bot, guild.id)
    await update_admin_panel(bot, guild.id)
    return True, f"출석을 시작했습니다. {attendance_channel.mention}에 안내 메시지를 보냈습니다."


async def stop_attendance(
    bot: discord.Bot,
    guild: discord.Guild,
    *,
    stopped_by: discord.Member | None,
    reason: str,
) -> dict[str, Any]:
    async with _get_attendance_lock(bot, guild.id):
        state = get_attendance_state(bot, guild.id)
        if not bool(state.get("active")):
            return {
                "ok": False,
                "message": "현재 출석이 진행 중이 아닙니다.",
            }

        snapshot = AttendanceSnapshot(
            guild_id=guild.id,
            started_at=_coerce_timestamp(state.get("started_at")),
            ended_at=_now_kst().strftime(TIME_FORMAT),
            started_by_discord_id=_optional_int(state.get("started_by")),
            stopped_by_discord_id=stopped_by.id if stopped_by is not None else None,
            participant_ids=sorted(_participant_ids(state)),
        )
        channel_id = _optional_int(state.get("channel_id"))
        message_id = _optional_int(state.get("message_id"))
        clear_attendance_state(bot, guild.id)
        _schedule_attendance_state_publish(bot, guild.id)

    await delete_attendance_message(
        bot,
        guild.id,
        channel_id=channel_id,
        message_id=message_id,
    )
    await update_admin_panel(bot, guild.id)

    return {
        "ok": True,
        "message": "출석을 종료했습니다.",
        "participant_count": len(snapshot.participant_ids),
        "snapshot": snapshot,
    }


async def register_attendance(
    bot: discord.Bot,
    guild_id: int,
    member: discord.Member,
) -> tuple[bool, str]:
    cooldown_remaining = _consume_user_attendance_cooldown(bot, guild_id, member.id)
    if cooldown_remaining is not None:
        return (
            False,
            f"출석 요청이 너무 빠릅니다. `{cooldown_remaining}`초 후 다시 시도해주세요.",
        )

    state = get_attendance_state(bot, guild_id)
    if not bool(state.get("active")):
        return False, "현재 출석이 진행 중이 아닙니다."

    voice_channel_id = _optional_int(state.get("voice_channel_id"))
    current_voice = getattr(member.voice, "channel", None)
    current_voice_id = getattr(current_voice, "id", None)
    if voice_channel_id is None or current_voice_id != voice_channel_id:
        return False, "설정된 출석 음성채널에 들어가 있어야 출석할 수 있습니다."

    available_timer = _optional_int(state.get("attendance_available_timer"))
    if available_timer is not None and available_timer > 0:
        joined_at = get_voice_entry_time(bot, guild_id, member.id)
        if joined_at is None:
            joined_at = _now_kst()
            set_voice_entry_time(bot, guild_id, member.id, joined_at)

        elapsed = (_now_kst() - joined_at).total_seconds()
        if elapsed < available_timer:
            remaining = int(available_timer - elapsed)
            return (
                False,
                f"음성채널 입장 후 `{available_timer}`초가 지나야 출석할 수 있습니다. "
                f"남은 시간: `{max(1, remaining)}`초",
            )

    async with _get_attendance_lock(bot, guild_id):
        state = get_attendance_state(bot, guild_id)
        if not bool(state.get("active")):
            return False, "현재 출석이 진행 중이 아닙니다."

        participants = _participant_ids(state)
        participant_times = _participant_times(state)
        previous_count = len(participants)
        participants.add(member.id)

        if len(participants) == previous_count:
            return True, "이미 출석이 완료되었습니다."

        attended_at = _now_kst()
        participant_times[member.id] = attended_at.strftime(TIME_FORMAT)
        set_voice_entry_time(bot, guild_id, member.id, attended_at)
        _schedule_attendance_state_publish(bot, guild_id)

    return True, "출석이 완료되었습니다."


def build_record_prompt_view(
    bot: discord.Bot,
    guild: discord.Guild,
    snapshot: AttendanceSnapshot,
) -> AttendanceRecordPromptView:
    return AttendanceRecordPromptView(bot, guild, snapshot)


async def persist_attendance_snapshot(
    bot: discord.Bot,
    guild: discord.Guild,
    snapshot: AttendanceSnapshot,
) -> int:
    participants = []
    for discord_id in snapshot.participant_ids:
        member = guild.get_member(discord_id)
        nickname = member.display_name if member is not None else str(discord_id)
        alliance_id = _resolve_alliance_id_from_nickname(nickname)
        participants.append(
            {
                "discord_id": discord_id,
                "discord_nickname": nickname,
                "alliance_id": alliance_id,
            }
        )

    return await asyncio.to_thread(
        database.save_attendance_session,
        guild_id=snapshot.guild_id,
        started_at=snapshot.started_at,
        ended_at=snapshot.ended_at,
        started_by_discord_id=snapshot.started_by_discord_id,
        participants=participants,
    )


async def send_attendance_summary(
    bot: discord.Bot,
    guild: discord.Guild,
    *,
    snapshot: AttendanceSnapshot,
    reason: str,
    stopped_by_mention: str,
    save_status: str | None,
) -> None:
    settings = database.get_settings(guild.id)
    if settings.log_channel_id is None:
        return

    log_channel = guild.get_channel(settings.log_channel_id)
    if not isinstance(log_channel, discord.TextChannel):
        return

    started_by_mention = (
        f"<@{snapshot.started_by_discord_id}>"
        if snapshot.started_by_discord_id is not None
        else "알 수 없음"
    )
    reason_text = "수동" if reason == "manual" else "자동"
    started_at_text = _format_summary_timestamp(snapshot.started_at)
    clan_members = _get_clan_members_for_snapshot(guild, snapshot)
    summary_lines = [
        f"{started_by_mention} 출석 확인 {reason_text} 종료",
        f"확인 시작 시간 : {started_at_text}",
        f"참석 총인원 : {len(snapshot.participant_ids)}명",
    ]
    if save_status:
        summary_lines.insert(2, save_status)
    messages = _build_summary_messages(summary_lines, clan_members)

    try:
        for message in messages:
            await log_channel.send(message)
    except (discord.Forbidden, discord.HTTPException):
        return


async def _expire_attendance_after_timeout(
    bot: discord.Bot,
    guild_id: int,
    timeout_seconds: int,
) -> None:
    try:
        await asyncio.sleep(timeout_seconds)
        guild = bot.get_guild(guild_id)
        if guild is None:
            clear_attendance_state(bot, guild_id)
            _schedule_attendance_state_publish(bot, guild_id)
            await update_admin_panel(bot, guild_id)
            return

        result = await stop_attendance(
            bot,
            guild,
            stopped_by=None,
            reason="timeout",
        )
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
            await send_ranker_attendance_ids(guild, snapshot)
    except asyncio.CancelledError:
        return


def _participant_ids(state: dict[str, Any]) -> set[int]:
    participants = state.get("participants")
    if isinstance(participants, set):
        return participants

    participants = set()
    state["participants"] = participants
    return participants


def _participant_times(state: dict[str, Any]) -> dict[int, str]:
    participant_times = state.get("participant_times")
    if isinstance(participant_times, dict):
        return participant_times

    participant_times = {}
    state["participant_times"] = participant_times
    return participant_times


def build_live_attendance_state(bot: discord.Bot, guild_id: int) -> dict[str, Any]:
    guild = bot.get_guild(guild_id)
    state = get_attendance_state(bot, guild_id)
    if not bool(state.get("active")):
        return {
            "active": False,
            "participant_count": 0,
            "session": None,
            "participants": [],
        }

    participant_times = _participant_times(state)
    participants: list[dict[str, Any]] = []
    for discord_id in sorted(_participant_ids(state)):
        member = guild.get_member(discord_id) if guild is not None else None
        display_name = member.display_name if member is not None else str(discord_id)
        participants.append(
            {
                "discord_id": discord_id,
                "display_name": display_name,
                "alliance_name": _resolve_alliance_name_from_nickname(display_name),
                "joined_voice_at": "",
                "attended_at": participant_times.get(discord_id, ""),
                "source": "discord",
            }
        )

    return {
        "active": True,
        "participant_count": len(participants),
        "session": {
            "live_session_id": state.get("live_session_id"),
            "started_at": state.get("started_at") or "",
            "expires_at": state.get("expires_at") or "",
            "started_by_discord_id": state.get("started_by"),
            "discord_channel_id": state.get("channel_id"),
            "discord_message_id": state.get("message_id"),
            "status": "active",
        },
        "participants": participants,
    }


def _schedule_attendance_state_publish(bot: discord.Bot, guild_id: int) -> None:
    publisher = getattr(bot, "attendance_state_publisher", None)
    if not callable(publisher):
        return

    try:
        asyncio.create_task(publisher(guild_id))
    except RuntimeError:
        return


def sync_voice_entry_time(
    bot: discord.Bot,
    guild_id: int,
    member_id: int,
    before_channel_id: int | None,
    after_channel_id: int | None,
) -> None:
    attendance_state = get_attendance_state(bot, guild_id)
    tracked_channel_id = (
        _optional_int(attendance_state.get("voice_channel_id"))
        if bool(attendance_state.get("active"))
        else None
    )
    if tracked_channel_id is None:
        settings = database.get_settings(guild_id)
        tracked_channel_id = settings.attendance_voice_channel_id
    if tracked_channel_id is None:
        clear_voice_entry_time(bot, guild_id, member_id)
        return

    if after_channel_id == tracked_channel_id:
        if before_channel_id != tracked_channel_id:
            set_voice_entry_time(bot, guild_id, member_id, _now_kst())
        return

    if before_channel_id == tracked_channel_id or after_channel_id != tracked_channel_id:
        clear_voice_entry_time(bot, guild_id, member_id)


def seed_voice_entry_times(bot: discord.Bot, guild: discord.Guild) -> None:
    settings = database.get_settings(guild.id)
    tracked_channel_id = settings.attendance_voice_channel_id
    if tracked_channel_id is None:
        return

    channel = guild.get_channel(tracked_channel_id)
    if not isinstance(channel, discord.VoiceChannel):
        return

    now = _now_kst()
    tracked_members = {member.id for member in channel.members}
    voice_entry_times = _get_voice_entry_times(bot, guild.id)
    for member_id in list(voice_entry_times.keys()):
        if member_id not in tracked_members:
            voice_entry_times.pop(member_id, None)
    for member in channel.members:
        voice_entry_times.setdefault(member.id, now)


def get_voice_entry_time(
    bot: discord.Bot,
    guild_id: int,
    member_id: int,
) -> datetime | None:
    return _get_voice_entry_times(bot, guild_id).get(member_id)


def set_voice_entry_time(
    bot: discord.Bot,
    guild_id: int,
    member_id: int,
    joined_at: datetime,
) -> None:
    _get_voice_entry_times(bot, guild_id)[member_id] = joined_at


def clear_voice_entry_time(
    bot: discord.Bot,
    guild_id: int,
    member_id: int,
) -> None:
    _get_voice_entry_times(bot, guild_id).pop(member_id, None)


def _get_voice_entry_times(
    bot: discord.Bot,
    guild_id: int,
) -> dict[int, datetime]:
    voice_entry_times_by_guild = getattr(bot, "voice_entry_times_by_guild", None)
    if voice_entry_times_by_guild is None:
        voice_entry_times_by_guild = {}
        bot.voice_entry_times_by_guild = voice_entry_times_by_guild

    state = voice_entry_times_by_guild.get(guild_id)
    if state is None:
        state = {}
        voice_entry_times_by_guild[guild_id] = state
    return state


def _consume_user_attendance_cooldown(
    bot: discord.Bot,
    guild_id: int,
    member_id: int,
) -> int | None:
    cooldowns = _get_user_attendance_cooldowns(bot, guild_id)
    now = time.monotonic()
    last_used_at = cooldowns.get(member_id)
    if last_used_at is not None:
        elapsed = now - last_used_at
        if elapsed < ATTENDANCE_USER_COOLDOWN_SECONDS:
            remaining = ATTENDANCE_USER_COOLDOWN_SECONDS - elapsed
            return max(1, int(remaining) if remaining.is_integer() else int(remaining) + 1)

    cooldowns[member_id] = now
    return None


def _get_user_attendance_cooldowns(
    bot: discord.Bot,
    guild_id: int,
) -> dict[int, float]:
    cooldowns_by_guild = getattr(bot, "attendance_user_cooldowns_by_guild", None)
    if cooldowns_by_guild is None:
        cooldowns_by_guild = {}
        bot.attendance_user_cooldowns_by_guild = cooldowns_by_guild

    state = cooldowns_by_guild.get(guild_id)
    if state is None:
        state = {}
        cooldowns_by_guild[guild_id] = state
    return state


async def send_ranker_attendance_ids(
    guild: discord.Guild,
    snapshot: AttendanceSnapshot,
) -> None:
    try:
        ranker_ids = _get_ranker_discord_ids(guild, snapshot)
        payload = {"ids": ranker_ids}
        result = await asyncio.to_thread(_post_ranker_ids, payload)
        if result["ok"]:
            print(
                "[attendance] ranker POST success "
                f"guild_id={guild.id} count={len(ranker_ids)} status={result['status']}"
            )
        else:
            print(
                "[attendance] ranker POST failed "
                f"guild_id={guild.id} count={len(ranker_ids)} error={result['error']}"
            )
    except Exception as exc:
        print(
            "[attendance] ranker POST unexpected failure "
            f"guild_id={guild.id} error={exc!r}"
        )
        return


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _coerce_timestamp(value: object) -> str:
    if isinstance(value, str) and value:
        return value
    return _now_kst().strftime(TIME_FORMAT)


def _now_kst() -> datetime:
    return datetime.now(KST)


def _resolve_alliance_id_from_nickname(nickname: str) -> int | None:
    match = ALLIANCE_PATTERN.search(nickname)
    if match is None:
        return None

    alliance_name = match.group(1).strip()
    if not alliance_name:
        return None

    alliance = database.get_or_create_alliance(alliance_name)
    return alliance.alliance_id


def _resolve_alliance_name_from_nickname(nickname: str) -> str:
    match = ALLIANCE_PATTERN.search(nickname)
    if match is None:
        return "미분류"

    alliance_name = match.group(1).strip()
    if not alliance_name:
        return "미분류"

    return alliance_name


def _get_ranker_discord_ids(
    guild: discord.Guild,
    snapshot: AttendanceSnapshot,
) -> list[str]:
    ranker_ids: list[str] = []
    for discord_id in snapshot.participant_ids:
        member = guild.get_member(discord_id)
        if member is None:
            continue
        if not any(role.id in RANKER_ROLE_IDS for role in member.roles):
            continue
        ranker_ids.append(str(discord_id))
    return ranker_ids


def _post_ranker_ids(payload: dict[str, list[str]]) -> dict[str, object]:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        RANKER_POST_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            response_body = response.read().decode("utf-8", errors="replace")
            return {
                "ok": True,
                "status": getattr(response, "status", None),
                "body": response_body[:500],
            }
    except Exception as exc:
        return {
            "ok": False,
            "error": repr(exc),
        }


def _get_clan_members_for_snapshot(
    guild: discord.Guild,
    snapshot: AttendanceSnapshot,
) -> dict[str, list[str]]:
    counts: dict[str, list[str]] = {}
    for discord_id in snapshot.participant_ids:
        member = guild.get_member(discord_id)
        nickname = member.display_name if member is not None else str(discord_id)
        alliance_name = _resolve_alliance_name_from_nickname(nickname)
        counts.setdefault(alliance_name, []).append(nickname)
    for nicknames in counts.values():
        nicknames.sort()
    return counts


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
        if len(section) > 1800:
            section = _truncate_clan_section(clan_name, nicknames)
        if not current_message:
            current_message = section
            continue
        if len(current_message) + 2 + len(section) > 1800:
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


def _mention_or_system(discord_id: int | None) -> str:
    if discord_id is None:
        return "시스템"
    return f"<@{discord_id}>"


def _get_attendance_lock(bot: discord.Bot, guild_id: int) -> asyncio.Lock:
    attendance_locks = getattr(bot, "attendance_locks", None)
    if attendance_locks is None:
        attendance_locks = {}
        bot.attendance_locks = attendance_locks

    lock = attendance_locks.get(guild_id)
    if lock is None:
        lock = asyncio.Lock()
        attendance_locks[guild_id] = lock
    return lock


def _format_summary_timestamp(value: str) -> str:
    try:
        dt = datetime.strptime(value, TIME_FORMAT)
        return dt.strftime("%m월%d일 %H시 %M분")
    except ValueError:
        return value


async def _safe_interaction_response(
    interaction: discord.Interaction,
    message: str,
    *,
    ephemeral: bool = True,
) -> None:
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(message, ephemeral=ephemeral)
    except discord.NotFound:
        return


async def _safe_interaction_defer(
    interaction: discord.Interaction,
    *,
    ephemeral: bool = True,
) -> bool:
    if interaction.response.is_done():
        return True
    try:
        await interaction.response.defer(ephemeral=ephemeral)
        return True
    except discord.NotFound:
        return False
