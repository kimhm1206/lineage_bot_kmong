from __future__ import annotations

import asyncio
import io
import os
from urllib.parse import urlencode

import discord

from common import database
from discord_bot.utils.attendance import (
    build_record_prompt_view,
    start_attendance,
    stop_attendance,
)
from discord_bot.utils.guild import is_admin_member, is_supported_guild
from discord_bot.utils.panel import get_attendance_state
from discord_bot.utils.voice_roster import (
    CLASS_LABELS,
    UNCLASSIFIED_LABEL,
    compact_member_list,
    full_roster_text,
    group_display_names,
)


class AdminPanelView(discord.ui.View):
    def __init__(self, bot: discord.Bot, guild_id: int):
        super().__init__(timeout=None)
        self.bot = bot
        self.guild_id = guild_id
        self._apply_attendance_button_state()
        self.add_item(
            discord.ui.Button(
                label="드랍&분배",
                style=discord.ButtonStyle.link,
                url=_build_web_loot_url(guild_id),
                row=0,
            )
        )
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
        label="클래스 현황",
        style=discord.ButtonStyle.secondary,
        custom_id="attendance:voice-class-roster",
        row=0,
    )
    async def voice_class_roster_button(
        self, button: discord.ui.Button, interaction: discord.Interaction
    ) -> None:
        guild = interaction.guild
        if guild is None:
            await _safe_response(interaction, "서버에서만 사용할 수 있습니다.")
            return

        if not await _safe_defer(interaction):
            return

        try:
            settings = await asyncio.to_thread(database.get_settings, guild.id)
        except Exception:
            await interaction.followup.send(
                "음성채널 설정을 불러오지 못했습니다.",
                ephemeral=True,
            )
            return
        channel_ids = list(settings.attendance_voice_channel_ids or ())
        if not channel_ids and settings.attendance_voice_channel_id is not None:
            channel_ids = [int(settings.attendance_voice_channel_id)]

        voice_channels = []
        for channel_id in channel_ids:
            channel = guild.get_channel(int(channel_id))
            if channel is not None and hasattr(channel, "members"):
                voice_channels.append(channel)
        if not voice_channels:
            await interaction.followup.send(
                "설정된 출석 음성채널을 찾을 수 없습니다.",
                ephemeral=True,
            )
            return

        members_by_id: dict[int, discord.Member] = {}
        for channel in voice_channels:
            for member in channel.members:
                if not member.bot:
                    members_by_id[member.id] = member

        groups = group_display_names(
            member.display_name for member in members_by_id.values()
        )
        embed = discord.Embed(
            title="연합보탐 클래스 현황",
            description=(
                f"대상: {', '.join(channel.mention for channel in voice_channels)}\n"
                f"현재 접속 인원 **{len(members_by_id)}명**"
            ),
            color=discord.Color.blurple(),
        )
        has_truncated_list = False
        for label in (*CLASS_LABELS, UNCLASSIFIED_LABEL):
            names = groups[label]
            value, was_truncated = compact_member_list(names)
            has_truncated_list = has_truncated_list or was_truncated
            embed.add_field(
                name=f"{label} · {len(names)}명",
                value=value,
                inline=False,
            )
        embed.set_footer(text="닉네임의 요정 · 법사 · 기사 문구를 기준으로 분류합니다.")

        send_options: dict[str, object] = {
            "embed": embed,
            "ephemeral": True,
        }
        if has_truncated_list:
            roster_text = full_roster_text(
                guild.name,
                (channel.name for channel in voice_channels),
                groups,
            )
            send_options["file"] = discord.File(
                io.BytesIO(roster_text.encode("utf-8")),
                filename="voice_class_roster.txt",
            )
        await interaction.followup.send(**send_options)


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


def _build_web_statistics_url(guild_id: int) -> str:
    base_url = os.getenv("WEB_BASE_URL", "https://xn--950bk80bh7an33asc.site").rstrip("/")
    return f"{base_url}/dashboard?{urlencode({'guild_id': str(guild_id)})}"


def _build_web_loot_url(guild_id: int) -> str:
    base_url = os.getenv("WEB_BASE_URL", "https://xn--950bk80bh7an33asc.site").rstrip("/")
    return f"{base_url}/loot?{urlencode({'guild_id': str(guild_id)})}"


def _build_web_settings_url(guild_id: int) -> str:
    base_url = os.getenv("WEB_BASE_URL", "https://xn--950bk80bh7an33asc.site").rstrip("/")
    return f"{base_url}/settings?{urlencode({'guild_id': str(guild_id)})}#channels"
