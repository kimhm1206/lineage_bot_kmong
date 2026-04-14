from __future__ import annotations

import discord

from db import update_setting
from utils.guild import is_admin_member, is_supported_guild
from utils.panel import update_admin_panel


VOICE_CHANNELS_PER_SELECT = 25
VOICE_SELECTS_PER_PAGE = 4


class SettingsMenuView(discord.ui.View):
    def __init__(self, bot: discord.Bot, guild_id: int):
        super().__init__(timeout=300)
        self.bot = bot
        self.guild_id = guild_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        guild = interaction.guild
        if guild is None or not is_supported_guild(self.bot, guild.id):
            await _safe_response(
                interaction,
                "권한이 없습니다.",
            )
            return False

        if not is_admin_member(interaction.user):
            await _safe_response(
                interaction,
                "권한이 없습니다.",
            )
            return False

        return True

    @discord.ui.select(
        placeholder="변경할 설정 항목을 선택해주세요.",
        min_values=1,
        max_values=1,
        custom_id="attendance:settings:select",
        options=[
            discord.SelectOption(
                label="출석 음성채널 설정",
                value="attendance_voice_channel_id",
                description="출석에 사용할 음성채널을 선택합니다.",
            ),
            discord.SelectOption(
                label="타이머 설정",
                value="timer",
                description="초 단위 타이머 값을 입력합니다.",
            ),
        ],
    )
    async def settings_select(
        self, select: discord.ui.Select, interaction: discord.Interaction
    ) -> None:
        selected = select.values[0]

        if selected == "attendance_voice_channel_id":
            view = VoiceChannelSettingView(self.bot, self.guild_id)
            await interaction.response.edit_message(
                content=_voice_channel_prompt(view.page, view.page_count),
                view=view,
            )
            return

        await interaction.response.send_modal(TimerSettingModal(self.bot, self.guild_id))


class VoiceChannelSettingView(discord.ui.View):
    def __init__(self, bot: discord.Bot, guild_id: int, page: int = 0):
        super().__init__(timeout=300)
        self.bot = bot
        self.guild_id = guild_id
        self.page = page
        self.voice_channels = _get_voice_channels(bot, guild_id)
        self.page_count = max(
            1,
            (len(self.voice_channels) + _channels_per_page() - 1) // _channels_per_page(),
        )
        self.page = max(0, min(self.page, self.page_count - 1))
        self._build_components()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _settings_interaction_check(self.bot, interaction)

    def _build_components(self) -> None:
        if not self.voice_channels:
            self.add_item(
                _DisabledSelect(
                    placeholder="선택할 음성채널이 없습니다.",
                    row=0,
                )
            )
            return

        start_index = self.page * _channels_per_page()
        end_index = start_index + _channels_per_page()
        channels_on_page = self.voice_channels[start_index:end_index]

        for row_index, offset in enumerate(
            range(0, len(channels_on_page), VOICE_CHANNELS_PER_SELECT)
        ):
            chunk = channels_on_page[offset : offset + VOICE_CHANNELS_PER_SELECT]
            self.add_item(
                VoiceChannelSelect(
                    bot=self.bot,
                    guild_id=self.guild_id,
                    page=self.page,
                    row=row_index,
                    channels=chunk,
                )
            )

        self.previous_button.disabled = self.page <= 0
        self.next_button.disabled = self.page >= self.page_count - 1

    @discord.ui.button(
        label="이전",
        style=discord.ButtonStyle.secondary,
        row=4,
    )
    async def previous_button(
        self, button: discord.ui.Button, interaction: discord.Interaction
    ) -> None:
        new_page = self.page - 1
        await interaction.response.edit_message(
            content=_voice_channel_prompt(new_page, self.page_count),
            view=VoiceChannelSettingView(self.bot, self.guild_id, new_page),
        )

    @discord.ui.button(
        label="다음",
        style=discord.ButtonStyle.secondary,
        row=4,
    )
    async def next_button(
        self, button: discord.ui.Button, interaction: discord.Interaction
    ) -> None:
        new_page = self.page + 1
        await interaction.response.edit_message(
            content=_voice_channel_prompt(new_page, self.page_count),
            view=VoiceChannelSettingView(self.bot, self.guild_id, new_page),
        )

    @discord.ui.button(
        label="뒤로",
        style=discord.ButtonStyle.primary,
        row=4,
    )
    async def back_button(
        self, button: discord.ui.Button, interaction: discord.Interaction
    ) -> None:
        await interaction.response.edit_message(
            content="변경할 설정 항목을 선택해주세요.",
            view=SettingsMenuView(self.bot, self.guild_id),
        )


class VoiceChannelSelect(discord.ui.Select):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        page: int,
        row: int,
        channels: list[discord.VoiceChannel],
    ):
        self.bot = bot
        self.guild_id = guild_id
        self.page = page
        self.channel_map = {str(channel.id): channel for channel in channels}

        options = [
            discord.SelectOption(
                label=_trim_text(channel.name, 100),
                value=str(channel.id),
                description=_trim_text(_channel_description(channel), 100),
            )
            for channel in channels
        ]

        start_number = page * _channels_per_page() + row * VOICE_CHANNELS_PER_SELECT + 1
        end_number = start_number + len(channels) - 1

        super().__init__(
            placeholder=f"음성채널 선택 ({start_number}-{end_number})",
            min_values=1,
            max_values=1,
            options=options,
            row=row,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        channel = self.channel_map[self.values[0]]
        update_setting(self.guild_id, "attendance_voice_channel_id", channel.id)

        await interaction.response.edit_message(
            content=f"출석 음성채널이 {channel.mention}으로 설정되었습니다.",
            view=SettingsMenuView(self.bot, self.guild_id),
        )
        await update_admin_panel(self.bot, self.guild_id)


class _DisabledSelect(discord.ui.Select):
    def __init__(self, placeholder: str, row: int):
        super().__init__(
            placeholder=placeholder,
            min_values=1,
            max_values=1,
            disabled=True,
            options=[
                discord.SelectOption(
                    label="선택 가능한 항목 없음",
                    value="disabled",
                )
            ],
            row=row,
        )


class TimerSettingModal(discord.ui.Modal):
    def __init__(self, bot: discord.Bot, guild_id: int):
        super().__init__(title="타이머 설정")
        self.bot = bot
        self.guild_id = guild_id
        self.timer_input = discord.ui.InputText(
            label="타이머(초)",
            placeholder="예: 300 = 5분, 600 = 10분",
            required=True,
            min_length=1,
            max_length=10,
        )
        self.add_item(self.timer_input)

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await _settings_interaction_check(self.bot, interaction):
            return

        raw_value = self.timer_input.value.strip()
        if not raw_value.isdigit():
            await interaction.response.send_message(
                "타이머는 초 단위로 1 이상의 정수만 입력할 수 있습니다.",
                ephemeral=True,
            )
            return

        timer_value = int(raw_value)
        if timer_value < 1:
            await interaction.response.send_message(
                "타이머는 초 단위로 1 이상의 정수만 입력할 수 있습니다.",
                ephemeral=True,
            )
            return

        update_setting(self.guild_id, "timer", timer_value)
        await interaction.response.send_message(
            f"타이머가 `{timer_value}`초로 설정되었습니다. 예: 300 = 5분, 600 = 10분",
            ephemeral=True,
        )
        await update_admin_panel(self.bot, self.guild_id)


async def _settings_interaction_check(
    bot: discord.Bot, interaction: discord.Interaction
) -> bool:
    guild = interaction.guild
    if guild is None or not is_supported_guild(bot, guild.id):
        await _safe_response(
            interaction,
            "권한이 없습니다.",
        )
        return False

    if not is_admin_member(interaction.user):
        await _safe_response(
            interaction,
            "권한이 없습니다.",
        )
        return False

    return True


async def _safe_response(interaction: discord.Interaction, message: str) -> None:
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)


def _get_voice_channels(bot: discord.Bot, guild_id: int) -> list[discord.VoiceChannel]:
    guild = bot.get_guild(guild_id)
    if guild is None:
        return []

    return sorted(
        guild.voice_channels,
        key=lambda channel: (
            channel.category.position if channel.category else -1,
            channel.position,
            channel.id,
        ),
    )


def _channels_per_page() -> int:
    return VOICE_CHANNELS_PER_SELECT * VOICE_SELECTS_PER_PAGE


def _voice_channel_prompt(page: int, page_count: int) -> str:
    if page_count <= 1:
        return "출석에 사용할 음성채널을 선택해주세요."
    return (
        f"출석에 사용할 음성채널을 선택해주세요. "
        f"({page + 1}/{page_count}페이지)"
    )


def _channel_description(channel: discord.VoiceChannel) -> str:
    if channel.category is None:
        return "카테고리 없음"
    return f"카테고리: {channel.category.name}"


def _trim_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "…"
