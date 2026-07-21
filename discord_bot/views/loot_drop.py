from __future__ import annotations

import asyncio
from typing import Any

import discord

from common import database
from discord_bot.utils.guild import is_admin_member, is_supported_guild


RECENT_ATTENDANCE_LIMIT = 25


class LootDropAttendanceSelectView(discord.ui.View):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        sessions: list[dict[str, Any]],
    ):
        super().__init__(timeout=300)
        self.bot = bot
        self.guild_id = guild_id
        self.sessions = sessions
        self._build_components()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _loot_interaction_check(self.bot, interaction)

    def _build_components(self) -> None:
        if not self.sessions:
            self.add_item(
                _DisabledSelect(
                    "등록할 수 있는 출석회차가 없습니다.",
                    row=0,
                )
            )
            return

        self.add_item(
            AttendanceSessionSelect(
                self.bot,
                self.guild_id,
                self.sessions,
                row=0,
            )
        )


class AttendanceSessionSelect(discord.ui.Select):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        all_sessions: list[dict[str, Any]],
        row: int,
    ):
        self.bot = bot
        self.guild_id = guild_id
        self.session_map = {
            str(session["attendance_id"]): session
            for session in all_sessions
        }
        options = [
            discord.SelectOption(
                label=_session_label(session),
                value=str(session["attendance_id"]),
                description=_session_description(session),
            )
            for session in all_sessions
        ]
        super().__init__(
            placeholder="아이템을 연결할 출석회차를 선택해주세요.",
            min_values=1,
            max_values=1,
            options=options,
            row=row,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        session = self.session_map.get(self.values[0])
        if session is None:
            await _safe_response(interaction, "선택한 출석회차를 찾을 수 없습니다.")
            return

        await interaction.response.send_modal(
            LootDropItemModal(
                self.bot,
                self.guild_id,
                attendance_id=int(session["attendance_id"]),
                started_at=str(session["started_at"]),
            )
        )


class LootDropItemModal(discord.ui.Modal):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        *,
        attendance_id: int,
        started_at: str,
    ):
        super().__init__(title=f"드랍 등록 · 출석 #{attendance_id}")
        self.bot = bot
        self.guild_id = guild_id
        self.attendance_id = attendance_id
        self.started_at = started_at
        self.item_name_input = discord.ui.InputText(
            label="아이템 이름",
            placeholder="예: 축젤, 바람수정",
            required=True,
            min_length=1,
            max_length=100,
        )
        self.add_item(self.item_name_input)

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await _loot_interaction_check(self.bot, interaction):
            return

        item_name = self.item_name_input.value.strip()
        if not item_name:
            await interaction.response.send_message(
                "아이템 이름을 입력해주세요.",
                ephemeral=True,
            )
            return

        try:
            loot_event_id = await asyncio.to_thread(
                database.create_basic_loot_drop,
                self.guild_id,
                attendance_id=self.attendance_id,
                item_name=item_name,
                created_by_discord_id=getattr(interaction.user, "id", None),
            )
        except ValueError as exc:
            await interaction.response.send_message(
                f"드랍 등록에 실패했습니다. {exc}",
                ephemeral=True,
            )
            return
        except Exception:
            await interaction.response.send_message(
                "드랍 등록 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            (
                f"드랍을 등록했습니다.\n"
                f"출석: `#{self.attendance_id}` ({self.started_at})\n"
                f"아이템: `{item_name}`\n"
                f"드랍 ID: `#{loot_event_id}`"
            ),
            ephemeral=True,
        )


class _DisabledSelect(discord.ui.Select):
    def __init__(self, placeholder: str, row: int):
        super().__init__(
            placeholder=placeholder,
            min_values=1,
            max_values=1,
            disabled=True,
            options=[discord.SelectOption(label="선택 가능한 항목 없음", value="disabled")],
            row=row,
        )


async def build_loot_drop_select_view(
    bot: discord.Bot,
    guild_id: int,
) -> LootDropAttendanceSelectView:
    sessions = await asyncio.to_thread(
        database.get_recent_attendance_sessions,
        guild_id,
        RECENT_ATTENDANCE_LIMIT,
    )
    return LootDropAttendanceSelectView(bot, guild_id, sessions)


def select_prompt() -> str:
    return f"최근 출석회차 {RECENT_ATTENDANCE_LIMIT}개 중 드랍을 등록할 회차를 선택해주세요."


async def _loot_interaction_check(
    bot: discord.Bot,
    interaction: discord.Interaction,
) -> bool:
    guild = interaction.guild
    if guild is None or not is_supported_guild(bot, guild.id):
        await _safe_response(interaction, "권한이 없습니다.")
        return False

    if not is_admin_member(interaction.user):
        await _safe_response(interaction, "권한이 없습니다.")
        return False

    return True


async def _safe_response(interaction: discord.Interaction, message: str) -> None:
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.NotFound:
        return


def _session_label(session: dict[str, Any]) -> str:
    attendance_id = int(session["attendance_id"])
    started_at = str(session.get("started_at") or "")[:16]
    participant_count = int(session.get("participant_count") or 0)
    return _trim_text(
        f"#{attendance_id} · {started_at} · {participant_count}명",
        100,
    )


def _session_description(session: dict[str, Any]) -> str:
    ended_at = str(session.get("ended_at") or "")[:16]
    if ended_at:
        return _trim_text(f"종료: {ended_at}", 100)
    return "종료 시간 없음"


def _trim_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "…"
