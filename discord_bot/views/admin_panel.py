from __future__ import annotations

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

        if not await _safe_defer(interaction):
            return

        state = get_attendance_state(self.bot, self.guild_id)
        if bool(state.get("active")):
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
        await _safe_response(interaction, message)

    @discord.ui.button(
        label="드랍",
        style=discord.ButtonStyle.primary,
        custom_id="loot:drop:start",
        row=0,
    )
    async def loot_drop_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        from discord_bot.views.loot_drop import (
            RECENT_ATTENDANCE_LIMIT,
            build_loot_drop_select_view,
            select_prompt,
        )

        if not await _safe_defer(interaction):
            return

        view = await build_loot_drop_select_view(self.bot, self.guild_id)
        await interaction.followup.send(
            select_prompt()
            if view.sessions
            else f"최근 출석회차 {RECENT_ATTENDANCE_LIMIT}개 안에 등록할 출석 기록이 없습니다.",
            view=view,
            ephemeral=True,
        )

    @discord.ui.button(
        label="설정",
        style=discord.ButtonStyle.secondary,
        custom_id="attendance:settings",
        row=0,
    )
    async def settings_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        from discord_bot.views.settings import SettingsMenuView

        await interaction.response.send_message(
            "변경할 설정 항목을 선택해주세요.",
            view=SettingsMenuView(self.bot, self.guild_id),
            ephemeral=True,
        )


async def _safe_defer(interaction: discord.Interaction) -> bool:
    if interaction.response.is_done():
        return True
    try:
        await interaction.response.defer(ephemeral=True)
        return True
    except discord.NotFound:
        return False


async def _safe_response(
    interaction: discord.Interaction, message: str
) -> None:
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.NotFound:
        return
