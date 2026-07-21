from __future__ import annotations

import asyncio

import discord

from common import database
from discord_bot.utils.attendance import seed_voice_entry_times
from discord_bot.utils.guild import is_admin_member, is_supported_guild
from discord_bot.utils.panel import (
    clear_old_admin_panel,
    rebuild_admin_panel,
    update_admin_panel,
)


CHANNELS_PER_SELECT = 25
SELECTS_PER_PAGE = 4


class SettingsMenuView(discord.ui.View):
    def __init__(self, bot: discord.Bot, guild_id: int):
        super().__init__(timeout=300)
        self.bot = bot
        self.guild_id = guild_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _settings_interaction_check(self.bot, interaction)

    @discord.ui.select(
        placeholder="변경할 설정 항목을 선택해주세요.",
        min_values=1,
        max_values=1,
        custom_id="attendance:settings:select",
        options=[
            discord.SelectOption(
                label="출석 채널 설정",
                value="admin_channel_id",
                description="출석 패널과 출석 메시지를 보낼 채팅채널입니다.",
            ),
            discord.SelectOption(
                label="출석 음성채널 설정",
                value="attendance_voice_channel_id",
                description="출석 버튼을 누르기 위해 들어가 있어야 하는 음성채널입니다.",
            ),
            discord.SelectOption(
                label="로그채널 설정",
                value="log_channel_id",
                description="출석 종료 로그를 보낼 채팅채널입니다.",
            ),
            discord.SelectOption(
                label="출석 확인 타이머 설정",
                value="timer",
                description="출석이 열려있는 시간을 초 단위로 입력합니다.",
            ),
            discord.SelectOption(
                label="출석 가능 타이머 설정",
                value="attendance_available_timer",
                description="음성채널 입장 후 출석 가능해지는 시간을 초 단위로 입력합니다.",
            ),
            discord.SelectOption(
                label="혈맹 역할 매핑",
                value="alliance_role_mapping",
                description="Discord 역할과 혈맹 이름을 연결합니다.",
            ),
        ],
    )
    async def settings_select(
        self,
        select: discord.ui.Select,
        interaction: discord.Interaction,
    ) -> None:
        selected = select.values[0]

        if selected == "attendance_voice_channel_id":
            view = VoiceChannelSettingView(self.bot, self.guild_id)
            await interaction.response.edit_message(
                content=_voice_channel_prompt(view.page, view.page_count),
                view=view,
            )
            return

        if selected in {"admin_channel_id", "log_channel_id"}:
            view = TextChannelSettingView(
                self.bot,
                self.guild_id,
                setting_key=selected,
            )
            await interaction.response.edit_message(
                content=_text_channel_prompt(view.page, view.page_count, selected),
                view=view,
            )
            return

        if selected == "alliance_role_mapping":
            mappings = await asyncio.to_thread(
                database.get_guild_alliance_role_mappings,
                self.guild_id,
            )
            await interaction.response.edit_message(
                content=_role_mapping_content(self.bot, self.guild_id, mappings),
                view=RoleMappingSettingView(self.bot, self.guild_id, mappings),
            )
            return

        if selected == "attendance_available_timer":
            await interaction.response.send_modal(
                TimerSettingModal(
                    self.bot,
                    self.guild_id,
                    setting_key="attendance_available_timer",
                    title="출석 가능 타이머 설정",
                    success_label="출석 가능 타이머",
                )
            )
            return

        await interaction.response.send_modal(
            TimerSettingModal(
                self.bot,
                self.guild_id,
                setting_key="timer",
                title="출석 확인 타이머 설정",
                success_label="출석 확인 타이머",
            )
        )


class RoleMappingSettingView(discord.ui.View):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        mappings: list[dict[str, object]],
    ):
        super().__init__(timeout=300)
        self.bot = bot
        self.guild_id = guild_id
        self.mappings = mappings

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _settings_interaction_check(self.bot, interaction)

    @discord.ui.button(label="추가/수정", style=discord.ButtonStyle.primary, row=0)
    async def add_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        await interaction.response.edit_message(
            content="매핑할 Discord 역할을 선택해주세요.",
            view=RoleMappingRoleSelectView(self.bot, self.guild_id),
        )

    @discord.ui.button(label="삭제", style=discord.ButtonStyle.danger, row=0)
    async def delete_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        if not self.mappings:
            await interaction.response.edit_message(
                content="삭제할 혈맹 역할 매핑이 없습니다.",
                view=RoleMappingSettingView(self.bot, self.guild_id, self.mappings),
            )
            return

        await interaction.response.edit_message(
            content="삭제할 혈맹 역할 매핑을 선택해주세요.",
            view=RoleMappingDeleteView(self.bot, self.guild_id, self.mappings),
        )

    @discord.ui.button(label="뒤로", style=discord.ButtonStyle.secondary, row=0)
    async def back_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        await interaction.response.edit_message(
            content="변경할 설정 항목을 선택해주세요.",
            view=SettingsMenuView(self.bot, self.guild_id),
        )


class RoleMappingRoleSelectView(discord.ui.View):
    def __init__(self, bot: discord.Bot, guild_id: int):
        super().__init__(timeout=300)
        self.bot = bot
        self.guild_id = guild_id
        self.add_item(RoleMappingRoleSelect(bot, guild_id))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _settings_interaction_check(self.bot, interaction)

    @discord.ui.button(label="뒤로", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        mappings = await asyncio.to_thread(
            database.get_guild_alliance_role_mappings,
            self.guild_id,
        )
        await interaction.response.edit_message(
            content=_role_mapping_content(self.bot, self.guild_id, mappings),
            view=RoleMappingSettingView(self.bot, self.guild_id, mappings),
        )


class RoleMappingRoleSelect(discord.ui.Select):
    def __init__(self, bot: discord.Bot, guild_id: int):
        super().__init__(
            select_type=discord.ComponentType.role_select,
            placeholder="매핑할 역할을 선택해주세요.",
            min_values=1,
            max_values=1,
            row=0,
        )
        self.bot = bot
        self.guild_id = guild_id

    async def callback(self, interaction: discord.Interaction) -> None:
        role = self.values[0] if self.values else None
        if not isinstance(role, discord.Role):
            await _safe_response(interaction, "역할을 찾을 수 없습니다.")
            return

        await interaction.response.send_modal(
            RoleMappingAllianceModal(
                self.bot,
                self.guild_id,
                role_id=role.id,
                role_name=role.name,
            )
        )


class RoleMappingAllianceModal(discord.ui.Modal):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        *,
        role_id: int,
        role_name: str,
    ):
        super().__init__(title=f"혈맹 역할 매핑 · {role_name}")
        self.bot = bot
        self.guild_id = guild_id
        self.role_id = role_id
        self.role_name = role_name
        self.alliance_name_input = discord.ui.InputText(
            label="혈맹 이름",
            placeholder="예: 보스, 정지, 원피스",
            required=True,
            min_length=1,
            max_length=40,
        )
        self.add_item(self.alliance_name_input)

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await _settings_interaction_check(self.bot, interaction):
            return

        alliance_name = self.alliance_name_input.value.strip()
        if not alliance_name:
            await interaction.response.send_message(
                "혈맹 이름을 입력해주세요.",
                ephemeral=True,
            )
            return

        await asyncio.to_thread(
            database.upsert_guild_alliance_role_mapping,
            self.guild_id,
            self.role_id,
            self.role_name,
            alliance_name,
        )
        await interaction.response.send_message(
            f"`{self.role_name}` 역할을 `{alliance_name}` 혈맹으로 매핑했습니다.",
            ephemeral=True,
        )


class RoleMappingDeleteView(discord.ui.View):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        mappings: list[dict[str, object]],
    ):
        super().__init__(timeout=300)
        self.bot = bot
        self.guild_id = guild_id
        self.mappings = mappings[:25]
        self.add_item(RoleMappingDeleteSelect(bot, guild_id, self.mappings))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _settings_interaction_check(self.bot, interaction)

    @discord.ui.button(label="뒤로", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        mappings = await asyncio.to_thread(
            database.get_guild_alliance_role_mappings,
            self.guild_id,
        )
        await interaction.response.edit_message(
            content=_role_mapping_content(self.bot, self.guild_id, mappings),
            view=RoleMappingSettingView(self.bot, self.guild_id, mappings),
        )


class RoleMappingDeleteSelect(discord.ui.Select):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        mappings: list[dict[str, object]],
    ):
        self.bot = bot
        self.guild_id = guild_id
        self.mapping_map = {
            str(mapping["mapping_id"]): mapping
            for mapping in mappings
        }
        options = [
            discord.SelectOption(
                label=_trim_text(
                    f"{mapping['role_name']} -> {mapping['alliance_name']}",
                    100,
                ),
                value=str(mapping["mapping_id"]),
                description=f"역할 ID: {mapping['role_id']}",
            )
            for mapping in mappings
        ]
        super().__init__(
            placeholder="삭제할 매핑을 선택해주세요.",
            min_values=1,
            max_values=1,
            options=options,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        mapping = self.mapping_map.get(self.values[0])
        if mapping is None:
            await _safe_response(interaction, "선택한 매핑을 찾을 수 없습니다.")
            return

        await asyncio.to_thread(
            database.delete_guild_alliance_role_mapping,
            self.guild_id,
            int(mapping["mapping_id"]),
        )
        mappings = await asyncio.to_thread(
            database.get_guild_alliance_role_mappings,
            self.guild_id,
        )
        await interaction.response.edit_message(
            content=(
                f"`{mapping['role_name']}` 역할 매핑을 삭제했습니다.\n\n"
                f"{_role_mapping_content(self.bot, self.guild_id, mappings)}"
            ),
            view=RoleMappingSettingView(self.bot, self.guild_id, mappings),
        )


class VoiceChannelSettingView(discord.ui.View):
    def __init__(self, bot: discord.Bot, guild_id: int, page: int = 0):
        super().__init__(timeout=300)
        self.bot = bot
        self.guild_id = guild_id
        self.page = page
        self.voice_channels = _get_voice_channels(bot, guild_id)
        self.page_count = max(
            1,
            (len(self.voice_channels) + _channels_per_page() - 1)
            // _channels_per_page(),
        )
        self.page = max(0, min(self.page, self.page_count - 1))
        self._build_components()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _settings_interaction_check(self.bot, interaction)

    def _build_components(self) -> None:
        if not self.voice_channels:
            self.add_item(_DisabledSelect("선택할 음성채널이 없습니다.", row=0))
            self.previous_button.disabled = True
            self.next_button.disabled = True
            return

        start_index = self.page * _channels_per_page()
        end_index = start_index + _channels_per_page()
        channels_on_page = self.voice_channels[start_index:end_index]

        for row_index, offset in enumerate(
            range(0, len(channels_on_page), CHANNELS_PER_SELECT)
        ):
            chunk = channels_on_page[offset : offset + CHANNELS_PER_SELECT]
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

    @discord.ui.button(label="이전", style=discord.ButtonStyle.secondary, row=4)
    async def previous_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        new_page = self.page - 1
        await interaction.response.edit_message(
            content=_voice_channel_prompt(new_page, self.page_count),
            view=VoiceChannelSettingView(self.bot, self.guild_id, new_page),
        )

    @discord.ui.button(label="다음", style=discord.ButtonStyle.secondary, row=4)
    async def next_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        new_page = self.page + 1
        await interaction.response.edit_message(
            content=_voice_channel_prompt(new_page, self.page_count),
            view=VoiceChannelSettingView(self.bot, self.guild_id, new_page),
        )

    @discord.ui.button(label="뒤로", style=discord.ButtonStyle.primary, row=4)
    async def back_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
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
        self.channel_map = {str(channel.id): channel for channel in channels}

        options = [
            discord.SelectOption(
                label=_trim_text(channel.name, 100),
                value=str(channel.id),
                description=_trim_text(_channel_description(channel), 100),
            )
            for channel in channels
        ]
        start_number = page * _channels_per_page() + row * CHANNELS_PER_SELECT + 1
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
        await asyncio.to_thread(
            database.update_attendance_voice_channel_ids,
            self.guild_id,
            [channel.id],
        )
        await interaction.response.edit_message(
            content=f"출석 음성채널이 {channel.mention}으로 설정되었습니다.",
            view=SettingsMenuView(self.bot, self.guild_id),
        )
        guild = interaction.guild
        if guild is not None:
            seed_voice_entry_times(self.bot, guild)
        await update_admin_panel(self.bot, self.guild_id)


class TextChannelSettingView(discord.ui.View):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        *,
        setting_key: str,
        page: int = 0,
    ):
        super().__init__(timeout=300)
        self.bot = bot
        self.guild_id = guild_id
        self.setting_key = setting_key
        self.page = page
        self.text_channels = _get_text_channels(bot, guild_id)
        self.page_count = max(
            1,
            (len(self.text_channels) + _channels_per_page() - 1)
            // _channels_per_page(),
        )
        self.page = max(0, min(self.page, self.page_count - 1))
        self._build_components()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await _settings_interaction_check(self.bot, interaction)

    def _build_components(self) -> None:
        if not self.text_channels:
            self.add_item(_DisabledSelect("선택할 채팅채널이 없습니다.", row=0))
            self.previous_button.disabled = True
            self.next_button.disabled = True
            return

        start_index = self.page * _channels_per_page()
        end_index = start_index + _channels_per_page()
        channels_on_page = self.text_channels[start_index:end_index]

        for row_index, offset in enumerate(
            range(0, len(channels_on_page), CHANNELS_PER_SELECT)
        ):
            chunk = channels_on_page[offset : offset + CHANNELS_PER_SELECT]
            self.add_item(
                TextChannelSelect(
                    bot=self.bot,
                    guild_id=self.guild_id,
                    setting_key=self.setting_key,
                    page=self.page,
                    row=row_index,
                    channels=chunk,
                )
            )

        self.previous_button.disabled = self.page <= 0
        self.next_button.disabled = self.page >= self.page_count - 1

    @discord.ui.button(label="이전", style=discord.ButtonStyle.secondary, row=4)
    async def previous_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        new_page = self.page - 1
        await interaction.response.edit_message(
            content=_text_channel_prompt(new_page, self.page_count, self.setting_key),
            view=TextChannelSettingView(
                self.bot,
                self.guild_id,
                setting_key=self.setting_key,
                page=new_page,
            ),
        )

    @discord.ui.button(label="다음", style=discord.ButtonStyle.secondary, row=4)
    async def next_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        new_page = self.page + 1
        await interaction.response.edit_message(
            content=_text_channel_prompt(new_page, self.page_count, self.setting_key),
            view=TextChannelSettingView(
                self.bot,
                self.guild_id,
                setting_key=self.setting_key,
                page=new_page,
            ),
        )

    @discord.ui.button(label="뒤로", style=discord.ButtonStyle.primary, row=4)
    async def back_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        await interaction.response.edit_message(
            content="변경할 설정 항목을 선택해주세요.",
            view=SettingsMenuView(self.bot, self.guild_id),
        )


class TextChannelSelect(discord.ui.Select):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        setting_key: str,
        page: int,
        row: int,
        channels: list[discord.TextChannel],
    ):
        self.bot = bot
        self.guild_id = guild_id
        self.setting_key = setting_key
        self.channel_map = {str(channel.id): channel for channel in channels}

        options = [
            discord.SelectOption(
                label=_trim_text(channel.name, 100),
                value=str(channel.id),
                description=_trim_text(_text_channel_description(channel), 100),
            )
            for channel in channels
        ]
        start_number = page * _channels_per_page() + row * CHANNELS_PER_SELECT + 1
        end_number = start_number + len(channels) - 1

        super().__init__(
            placeholder=f"채팅채널 선택 ({start_number}-{end_number})",
            min_values=1,
            max_values=1,
            options=options,
            row=row,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        channel = self.channel_map[self.values[0]]
        previous_settings = await asyncio.to_thread(
            database.get_settings,
            self.guild_id,
        )
        await asyncio.to_thread(
            database.update_setting,
            self.guild_id,
            self.setting_key,
            channel.id,
        )

        label = "출석 채널" if self.setting_key == "admin_channel_id" else "로그채널"
        await interaction.response.edit_message(
            content=f"{label}이 {channel.mention}으로 설정되었습니다.",
            view=SettingsMenuView(self.bot, self.guild_id),
        )

        if self.setting_key == "admin_channel_id":
            guild = interaction.guild
            if guild is not None:
                await clear_old_admin_panel(
                    self.bot,
                    guild,
                    previous_settings.admin_channel_id,
                )
                await rebuild_admin_panel(self.bot, self.guild_id)
        else:
            await update_admin_panel(self.bot, self.guild_id)


class TimerSettingModal(discord.ui.Modal):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        *,
        setting_key: str,
        title: str,
        success_label: str,
    ):
        super().__init__(title=title)
        self.bot = bot
        self.guild_id = guild_id
        self.setting_key = setting_key
        self.success_label = success_label
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

        await asyncio.to_thread(
            database.update_setting,
            self.guild_id,
            self.setting_key,
            timer_value,
        )
        await interaction.response.send_message(
            (
                f"{self.success_label}가 `{timer_value}`초로 설정되었습니다. "
                "예: 300 = 5분, 600 = 10분"
            ),
            ephemeral=True,
        )
        await update_admin_panel(self.bot, self.guild_id)


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


async def _settings_interaction_check(
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


def _get_text_channels(bot: discord.Bot, guild_id: int) -> list[discord.TextChannel]:
    guild = bot.get_guild(guild_id)
    if guild is None:
        return []

    return sorted(
        guild.text_channels,
        key=lambda channel: (
            channel.category.position if channel.category else -1,
            channel.position,
            channel.id,
        ),
    )


def _channels_per_page() -> int:
    return CHANNELS_PER_SELECT * SELECTS_PER_PAGE


def _voice_channel_prompt(page: int, page_count: int) -> str:
    if page_count <= 1:
        return "출석에 사용할 음성채널을 선택해주세요."
    return (
        f"출석에 사용할 음성채널을 선택해주세요. "
        f"({page + 1}/{page_count}페이지)"
    )


def _text_channel_prompt(page: int, page_count: int, setting_key: str) -> str:
    label = "출석 채널" if setting_key == "admin_channel_id" else "로그채널"
    if page_count <= 1:
        return f"{label}로 사용할 채팅채널을 선택해주세요."
    return (
        f"{label}로 사용할 채팅채널을 선택해주세요. "
        f"({page + 1}/{page_count}페이지)"
    )


def _role_mapping_content(
    bot: discord.Bot,
    guild_id: int,
    mappings: list[dict[str, object]],
) -> str:
    lines = [
        "혈맹 역할 매핑을 관리합니다.",
        "출석 저장 시 역할 매핑이 닉네임 파싱보다 먼저 적용됩니다.",
        "",
        "**현재 매핑**",
    ]
    if not mappings:
        lines.append("등록된 매핑이 없습니다.")
        return "\n".join(lines)

    guild = bot.get_guild(guild_id)
    for mapping in mappings[:20]:
        role_id = int(mapping["role_id"])
        role = guild.get_role(role_id) if guild is not None else None
        role_label = role.mention if role is not None else str(mapping["role_name"])
        lines.append(f"- {role_label} -> {mapping['alliance_name']}")
    if len(mappings) > 20:
        lines.append(f"- 외 {len(mappings) - 20}개")
    return "\n".join(lines)


def _channel_description(channel: discord.VoiceChannel) -> str:
    if channel.category is None:
        return "카테고리 없음"
    return f"카테고리: {channel.category.name}"


def _text_channel_description(channel: discord.TextChannel) -> str:
    if channel.category is None:
        return "카테고리 없음"
    return f"카테고리: {channel.category.name}"


def _trim_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "…"
