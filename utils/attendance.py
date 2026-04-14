from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import discord

from db import (
    get_or_create_alliance,
    get_settings,
    save_attendance_session,
)
from utils.panel import (
    build_attendance_embed,
    clear_attendance_state,
    delete_attendance_message,
    get_attendance_state,
    get_panel_message,
    set_attendance_state,
    update_admin_panel,
)


TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
ALLIANCE_PATTERN = re.compile(r"\[([^\[\]]{2,4})\]")


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

        settings = get_settings(guild.id)
        if settings.attendance_voice_channel_id is None:
            return False, "출석 음성채널을 먼저 설정해주세요."

        if settings.timer is None:
            return False, "타이머를 먼저 설정해주세요."

        voice_channel = guild.get_channel(settings.attendance_voice_channel_id)
        if voice_channel is None or not hasattr(voice_channel, "send"):
            return False, "설정된 음성채널에 메시지를 보낼 수 없습니다."

        attendance_message = await voice_channel.send(
            embed=build_attendance_embed(guild, voice_channel, settings.timer),
            view=AttendanceActionView(bot, guild.id),
        )

        task = asyncio.create_task(
            _expire_attendance_after_timeout(bot, guild.id, settings.timer)
        )
        set_attendance_state(
            bot,
            guild.id,
            channel_id=voice_channel.id,
            message_id=attendance_message.id,
            task=task,
            started_by=starter.id,
            started_at=datetime.now().strftime(TIME_FORMAT),
        )
    await update_admin_panel(bot, guild.id)
    return True, f"출석을 시작했습니다. {voice_channel.mention}에 안내 메시지를 보냈습니다."


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
            ended_at=datetime.now().strftime(TIME_FORMAT),
            started_by_discord_id=_optional_int(state.get("started_by")),
            stopped_by_discord_id=stopped_by.id if stopped_by is not None else None,
            participant_ids=sorted(_participant_ids(state)),
        )
        channel_id = _optional_int(state.get("channel_id"))
        message_id = _optional_int(state.get("message_id"))
        clear_attendance_state(bot, guild.id)

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
    settings = get_settings(guild_id)
    voice_channel_id = settings.attendance_voice_channel_id
    current_voice = getattr(member.voice, "channel", None)
    current_voice_id = getattr(current_voice, "id", None)
    if voice_channel_id is None or current_voice_id != voice_channel_id:
        return False, "설정된 출석 음성채널에 들어가 있어야 출석할 수 있습니다."

    async with _get_attendance_lock(bot, guild_id):
        state = get_attendance_state(bot, guild_id)
        if not bool(state.get("active")):
            return False, "현재 출석이 진행 중이 아닙니다."

        participants = _participant_ids(state)
        previous_count = len(participants)
        participants.add(member.id)

        if len(participants) == previous_count:
            return True, "이미 출석이 완료되었습니다."

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
        save_attendance_session,
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
    panel_message = await get_panel_message(bot, guild.id)
    if panel_message is None:
        return

    thread = getattr(panel_message, "thread", None) or guild.get_thread(panel_message.id)
    if thread is None:
        try:
            thread = await panel_message.create_thread(
                name=f"출석 기록 {snapshot.ended_at[:16]}",
                auto_archive_duration=1440,
            )
        except (discord.Forbidden, discord.HTTPException):
            thread = None

    started_by_mention = (
        f"<@{snapshot.started_by_discord_id}>"
        if snapshot.started_by_discord_id is not None
        else "알 수 없음"
    )
    reason_text = "수동" if reason == "manual" else "자동"
    started_at_text = _format_summary_timestamp(snapshot.started_at)
    alliance_counts = _get_alliance_counts_for_snapshot(guild, snapshot)
    alliance_lines = [f"[{alliance_name}] : {member_count}명" for alliance_name, member_count in alliance_counts.items()]
    if not alliance_lines:
        alliance_lines.append("[미분류] : 0명")
    summary_lines = [
        f"{started_by_mention} 출석 확인 {reason_text} 종료",
        f"확인 시작 시간 : {started_at_text}",
        f"참석 총인원 인원 : {len(snapshot.participant_ids)}명",
        "```",
        "연맹별 인원",
    ]
    if save_status:
        summary_lines.insert(2, save_status)
    summary_lines.extend(alliance_lines)
    summary_lines.append("```")
    summary = "\n".join(summary_lines)

    try:
        if thread is not None:
            await thread.send(summary)
        else:
            await panel_message.channel.send(summary)
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
    except asyncio.CancelledError:
        return


def _participant_ids(state: dict[str, Any]) -> set[int]:
    participants = state.get("participants")
    if isinstance(participants, set):
        return participants

    participants = set()
    state["participants"] = participants
    return participants


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _coerce_timestamp(value: object) -> str:
    if isinstance(value, str) and value:
        return value
    return datetime.now().strftime(TIME_FORMAT)


def _resolve_alliance_id_from_nickname(nickname: str) -> int | None:
    match = ALLIANCE_PATTERN.search(nickname)
    if match is None:
        return None

    alliance_name = match.group(1).strip()
    if not alliance_name:
        return None

    alliance = get_or_create_alliance(alliance_name)
    return alliance.alliance_id


def _resolve_alliance_name_from_nickname(nickname: str) -> str:
    match = ALLIANCE_PATTERN.search(nickname)
    if match is None:
        return "미분류"

    alliance_name = match.group(1).strip()
    if not alliance_name:
        return "미분류"

    return alliance_name


def _get_alliance_counts_for_snapshot(
    guild: discord.Guild,
    snapshot: AttendanceSnapshot,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for discord_id in snapshot.participant_ids:
        member = guild.get_member(discord_id)
        nickname = member.display_name if member is not None else str(discord_id)
        alliance_name = _resolve_alliance_name_from_nickname(nickname)
        counts[alliance_name] = counts.get(alliance_name, 0) + 1
    return counts


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
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=ephemeral)
    else:
        await interaction.response.send_message(message, ephemeral=ephemeral)
