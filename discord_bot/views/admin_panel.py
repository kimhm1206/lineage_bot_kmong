from __future__ import annotations

import os
from urllib.parse import urlencode

import discord

from discord_bot.utils.attendance import (
    build_record_prompt_view,
    start_attendance,
    stop_attendance,
)
from discord_bot.utils.guild import is_admin_member, is_supported_guild
from discord_bot.utils.panel import get_attendance_state


class AdminPanelView(discord.ui.View):
    def __init__(self, bot: discord.Bot, guild_id: int):
        super().__init__(timeout=None)
        self.bot = bot
        self.guild_id = guild_id
        self._apply_attendance_button_state()
        self.add_item(
            discord.ui.Button(
                label="통계",
                style=discord.ButtonStyle.link,
                url=_build_web_statistics_url(guild_id),
                row=0,
            )
        )
        self.add_item(
            discord.ui.Button(
                label="설정",
                style=discord.ButtonStyle.link,
                url=_build_web_settings_url(guild_id),
                row=0,
            )
        )

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        guild = interaction.guild
        if guild is None or not is_supported_guild(self.bot, guild.id):
            await _safe_response(interaction, "권한이 없습니다.")
            return False

        if not is_admin_member(interaction.user):
            await _safe_response(interaction, "권한이 없습니다.")
            return False

        return True

    def _apply_attendance_button_state(self) -> None:
        state = get_attendance_state(self.bot, self.guild_id)
        is_active = bool(state.get("active"))
        self.start_button.label = "출석 종료"
        self.start_button.style = discord.ButtonStyle.danger
        if not is_active:
            self.start_button.label = "출석 시작"
            self.start_button.style = discord.ButtonStyle.success

    @discord.ui.button(
        label="출석 시작",
        style=discord.ButtonStyle.success,
        custom_id="attendance:start",
        row=0,
    )
    async def start_button(
        self, button: discord.ui.Button, interaction: discord.Interaction
    ) -> None:
        guild = interaction.guild
        user = interaction.user
        if guild is None or not isinstance(user, discord.Member):
            await _safe_response(interaction, "권한이 없습니다.")
            return

        state = get_attendance_state(self.bot, self.guild_id)
        if bool(state.get("active")):
            await interaction.response.defer(ephemeral=True)
            result = await stop_attendance(
                self.bot,
                guild,
                stopped_by=user,
                reason="manual",
            )
            if not result["ok"]:
                await interaction.followup.send(result["message"], ephemeral=True)
                return

            await interaction.followup.send(
                "이번 출석 내용을 기록할까요?",
                view=build_record_prompt_view(self.bot, guild, result["snapshot"]),
                ephemeral=True,
            )
            return

        ok, message = await start_attendance(self.bot, guild, user)
        await interaction.response.send_message(message, ephemeral=True)


async def _safe_response(
    interaction: discord.Interaction, message: str
) -> None:
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)


def _build_web_statistics_url(guild_id: int) -> str:
    base_url = os.getenv("WEB_BASE_URL", "http://localhost:8000").rstrip("/")
    return f"{base_url}/dashboard?{urlencode({'guild_id': str(guild_id)})}"


def _build_web_settings_url(guild_id: int) -> str:
    base_url = os.getenv("WEB_BASE_URL", "http://localhost:8000").rstrip("/")
    return f"{base_url}/settings?{urlencode({'guild_id': str(guild_id)})}"
