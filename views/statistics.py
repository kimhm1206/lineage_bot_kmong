from __future__ import annotations

import asyncio
import csv
import io
from dataclasses import dataclass
from datetime import datetime

import discord

from db import get_alliance_names, get_attendance_export_rows, get_user_attendance_stats
from utils.guild import is_supported_guild

DATE_ONLY_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_USER_LIMIT = 20
ALLIANCE_SELECT_LIMIT = 25
MODE_NONE = "none"
MODE_USER = "user"
MODE_ALLIANCE = "alliance"
SELECT_NONE = "__none__"
SELECT_ALL = "__all__"


@dataclass(slots=True)
class StatisticsFilter:
    start_at: str | None = None
    end_at: str | None = None
    user_search: str | None = None
    alliance_name: str | None = None
    page: int = 0


class StatisticsDashboardView(discord.ui.View):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        *,
        mode: str = MODE_NONE,
        stats_filter: StatisticsFilter | None = None,
    ):
        super().__init__(timeout=300)
        self.bot = bot
        self.guild_id = guild_id
        self.mode = mode
        self.stats_filter = stats_filter or _default_stats_filter()
        self.mode_select.options = _build_mode_options(mode)
        self.alliance_select.options = _build_alliance_options(
            get_alliance_names(),
            self.stats_filter.alliance_name,
        )
        self._apply_component_state()

    def _apply_component_state(self) -> None:
        is_user_mode = self.mode == MODE_USER
        is_alliance_mode = self.mode == MODE_ALLIANCE
        has_selection = self.mode != MODE_NONE
        self.search_button.disabled = not is_user_mode
        self.clear_search_button.disabled = not is_user_mode
        self.alliance_select.disabled = not is_alliance_mode
        self.export_button.disabled = not has_selection

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        guild = interaction.guild
        if guild is None or not is_supported_guild(self.bot, guild.id):
            await _safe_response(interaction, "서버에서만 사용할 수 있습니다.")
            return False
        return True

    @discord.ui.select(
        placeholder="통계 모드 선택",
        min_values=1,
        max_values=1,
        row=0,
    )
    async def mode_select(
        self,
        select: discord.ui.Select,
        interaction: discord.Interaction,
    ) -> None:
        next_mode = select.values[0]
        if next_mode == SELECT_NONE:
            next_mode = MODE_NONE

        next_filter = StatisticsFilter(
            start_at=self.stats_filter.start_at,
            end_at=self.stats_filter.end_at,
            user_search=self.stats_filter.user_search if next_mode == MODE_USER else None,
            alliance_name=None,
            page=0,
        )
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=next_mode,
            stats_filter=next_filter,
        )

    @discord.ui.button(label="기간 설정", style=discord.ButtonStyle.primary, row=1)
    async def range_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        await interaction.response.send_modal(
            StatisticsRangeModal(
                self.bot,
                self.guild_id,
                mode=self.mode,
                stats_filter=self.stats_filter,
            )
        )

    @discord.ui.button(label="오늘", style=discord.ButtonStyle.secondary, row=1)
    async def today_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=self.mode,
            stats_filter=_default_stats_filter(
                user_search=self.stats_filter.user_search if self.mode == MODE_USER else None,
                alliance_name=self.stats_filter.alliance_name if self.mode == MODE_ALLIANCE else None,
            ),
        )

    @discord.ui.select(
        placeholder="혈맹 선택",
        min_values=1,
        max_values=1,
        row=2,
        options=[discord.SelectOption(label="혈맹 선택", value=SELECT_NONE, default=True)],
    )
    async def alliance_select(
        self,
        select: discord.ui.Select,
        interaction: discord.Interaction,
    ) -> None:
        selected = select.values[0]
        alliance_name = None if selected in {SELECT_NONE, SELECT_ALL} else selected
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=MODE_ALLIANCE,
            stats_filter=StatisticsFilter(
                start_at=self.stats_filter.start_at,
                end_at=self.stats_filter.end_at,
                user_search=None,
                alliance_name=alliance_name,
                page=0,
            ),
        )

    @discord.ui.button(label="검색", style=discord.ButtonStyle.secondary, row=3)
    async def search_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        await interaction.response.send_modal(
            StatisticsSearchModal(
                self.bot,
                self.guild_id,
                mode=self.mode,
                stats_filter=self.stats_filter,
            )
        )

    @discord.ui.button(label="검색 초기화", style=discord.ButtonStyle.secondary, row=3)
    async def clear_search_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=MODE_USER,
            stats_filter=_default_stats_filter(),
        )

    @discord.ui.button(label="CSV 내보내기", style=discord.ButtonStyle.success, row=4)
    async def export_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        guild = interaction.guild
        if guild is None:
            await _safe_response(interaction, "서버에서만 사용할 수 있습니다.")
            return
        if self.mode == MODE_NONE:
            await _safe_response(interaction, "통계 모드를 선택해주세요.")
            return
        if self.mode == MODE_ALLIANCE and not self.stats_filter.alliance_name:
            await _safe_response(interaction, "혈맹을 선택해주세요.")
            return

        rows = await asyncio.to_thread(
            get_attendance_export_rows,
            self.guild_id,
            self.stats_filter.start_at,
            self.stats_filter.end_at,
            self.stats_filter.user_search if self.mode == MODE_USER else None,
            self.stats_filter.alliance_name if self.mode == MODE_ALLIANCE else None,
        )
        file = _build_csv_file(guild.name, rows)
        message = "출석 통계 CSV 파일을 생성했습니다."
        if interaction.response.is_done():
            await interaction.followup.send(message, file=file, ephemeral=True)
        else:
            await interaction.response.send_message(message, file=file, ephemeral=True)

    @discord.ui.button(label="이전 페이지", style=discord.ButtonStyle.secondary, row=4)
    async def previous_page_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=self.mode,
            stats_filter=StatisticsFilter(
                start_at=self.stats_filter.start_at,
                end_at=self.stats_filter.end_at,
                user_search=self.stats_filter.user_search,
                alliance_name=self.stats_filter.alliance_name,
                page=max(0, self.stats_filter.page - 1),
            ),
        )

    @discord.ui.button(label="다음 페이지", style=discord.ButtonStyle.secondary, row=4)
    async def next_page_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        page_info = await _get_page_info(self.guild_id, self.mode, self.stats_filter)
        next_page = min(page_info["page_count"] - 1, self.stats_filter.page + 1)
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=self.mode,
            stats_filter=StatisticsFilter(
                start_at=self.stats_filter.start_at,
                end_at=self.stats_filter.end_at,
                user_search=self.stats_filter.user_search,
                alliance_name=self.stats_filter.alliance_name,
                page=max(0, next_page),
            ),
        )


class StatisticsRangeModal(discord.ui.Modal):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        *,
        mode: str,
        stats_filter: StatisticsFilter,
    ):
        super().__init__(title="기간 설정")
        self.bot = bot
        self.guild_id = guild_id
        self.mode = mode
        self.stats_filter = stats_filter
        self.start_input = discord.ui.InputText(
            label="시작 날짜",
            placeholder="YYYY-MM-DD 형식으로 입력",
            required=False,
            value=_date_from_datetime_string(stats_filter.start_at),
        )
        self.end_input = discord.ui.InputText(
            label="종료 날짜",
            placeholder="YYYY-MM-DD 형식으로 입력",
            required=False,
            value=_date_from_datetime_string(stats_filter.end_at),
        )
        self.add_item(self.start_input)
        self.add_item(self.end_input)

    async def callback(self, interaction: discord.Interaction) -> None:
        try:
            start_at = _date_to_datetime(self.start_input.value.strip(), end_of_day=False)
            end_at = _date_to_datetime(self.end_input.value.strip(), end_of_day=True)
        except ValueError:
            await interaction.response.send_message(
                "날짜는 YYYY-MM-DD 형식으로 입력해주세요.",
                ephemeral=True,
            )
            return

        if start_at and end_at and start_at > end_at:
            await interaction.response.send_message(
                "시작 날짜가 종료 날짜보다 늦을 수 없습니다.",
                ephemeral=True,
            )
            return

        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=self.mode,
            stats_filter=StatisticsFilter(
                start_at=start_at,
                end_at=end_at,
                user_search=self.stats_filter.user_search,
                alliance_name=self.stats_filter.alliance_name,
                page=0,
            ),
        )


class StatisticsSearchModal(discord.ui.Modal):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        *,
        mode: str,
        stats_filter: StatisticsFilter,
    ):
        super().__init__(title="검색")
        self.bot = bot
        self.guild_id = guild_id
        self.mode = mode
        self.stats_filter = stats_filter
        self.search_input = discord.ui.InputText(
            label="검색어",
            placeholder="유저 닉네임 또는 디스코드 ID",
            required=False,
            value=stats_filter.user_search or "",
        )
        self.add_item(self.search_input)

    async def callback(self, interaction: discord.Interaction) -> None:
        search = self.search_input.value.strip() or None
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=MODE_USER,
            stats_filter=StatisticsFilter(
                start_at=self.stats_filter.start_at,
                end_at=self.stats_filter.end_at,
                user_search=search,
                alliance_name=None,
                page=0,
            ),
        )


async def _render_dashboard(
    interaction: discord.Interaction,
    bot: discord.Bot,
    guild_id: int,
    *,
    mode: str,
    stats_filter: StatisticsFilter,
) -> None:
    guild = interaction.guild
    if guild is None:
        await _safe_response(interaction, "서버에서만 사용할 수 있습니다.")
        return

    embed = await _build_statistics_embed(guild_id, mode, stats_filter)
    view = StatisticsDashboardView(bot, guild_id, mode=mode, stats_filter=stats_filter)
    page_info = await _get_page_info(guild_id, mode, stats_filter)
    show_pagination = page_info["page_count"] > 1
    view.previous_page_button.disabled = not show_pagination or stats_filter.page <= 0
    view.next_page_button.disabled = not show_pagination or stats_filter.page >= page_info["page_count"] - 1

    if interaction.response.is_done():
        await interaction.edit_original_response(embed=embed, view=view, content=None)
    elif interaction.message is not None:
        await interaction.response.edit_message(embed=embed, view=view, content=None)
    else:
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


async def _build_statistics_embed(
    guild_id: int,
    mode: str,
    stats_filter: StatisticsFilter,
) -> discord.Embed:
    embed = discord.Embed(title="통계 대시보드", color=discord.Color.gold())
    embed.description = _describe_filter(mode, stats_filter)

    if mode == MODE_NONE:
        embed.add_field(name="안내", value="통계 모드를 선택하여 조회를 시작하세요.", inline=False)
        return embed

    if mode == MODE_ALLIANCE and not stats_filter.alliance_name:
        embed.add_field(name="안내", value="혈맹을 선택해야 혈맹별 통계를 볼 수 있습니다.", inline=False)
        return embed

    rows = await asyncio.to_thread(
        get_user_attendance_stats,
        guild_id,
        stats_filter.start_at,
        stats_filter.end_at,
        stats_filter.user_search if mode == MODE_USER else None,
        stats_filter.alliance_name if mode == MODE_ALLIANCE else None,
        500,
    )
    paged_rows, page_count = _slice_rows(rows, stats_filter.page)

    if mode == MODE_ALLIANCE:
        selected_alliance = stats_filter.alliance_name or "전체 혈맹"
        total_count = sum(int(row["attendance_count"]) for row in rows)
        embed.add_field(name="선택 혈맹", value=selected_alliance, inline=False)
        embed.add_field(name="총 출석 횟수", value=f"{total_count}회", inline=True)
        embed.add_field(name="참여 유저 수", value=f"{len(rows)}명", inline=True)
        embed.add_field(
            name=f"혈맹별 유저 출석 통계 ({stats_filter.page + 1}/{page_count})",
            value=_format_user_rows(paged_rows),
            inline=False,
        )
        return embed

    embed.add_field(
        name=f"유저별 출석 통계 ({stats_filter.page + 1}/{page_count})",
        value=_format_user_rows(paged_rows),
        inline=False,
    )
    return embed


def build_statistics_dashboard_embed(
    guild: discord.Guild,
    guild_id: int,
    *,
    mode: str = MODE_NONE,
    stats_filter: StatisticsFilter | None = None,
) -> discord.Embed:
    del guild_id
    filter_state = stats_filter or _default_stats_filter()
    return discord.Embed(
        title="통계 대시보드",
        description=_describe_filter(mode, filter_state),
        color=discord.Color.gold(),
    )


def _build_mode_options(current_mode: str) -> list[discord.SelectOption]:
    options = [
        (SELECT_NONE, "모드 선택", "통계 모드를 선택해주세요."),
        (MODE_USER, "유저별", "유저별 출석 통계를 봅니다."),
        (MODE_ALLIANCE, "혈맹별", "혈맹별 출석 통계를 봅니다."),
    ]
    expected_value = SELECT_NONE if current_mode == MODE_NONE else current_mode
    return [
        discord.SelectOption(
            label=label,
            value=value,
            description=description,
            default=value == expected_value,
        )
        for value, label, description in options
    ]


def _build_alliance_options(
    alliance_names: list[str],
    selected_name: str | None,
) -> list[discord.SelectOption]:
    options = [
        discord.SelectOption(
            label="혈맹 선택",
            value=SELECT_NONE,
            description="혈맹을 선택하여 필터링합니다.",
            default=selected_name is None,
        ),
    ]
    for alliance_name in alliance_names[: ALLIANCE_SELECT_LIMIT - 2]:
        options.append(
            discord.SelectOption(
                label=alliance_name,
                value=alliance_name,
                description=f"{alliance_name} 혈맹 통계를 봅니다.",
                default=alliance_name == selected_name,
            )
        )
    return options


def _default_stats_filter(
    *,
    user_search: str | None = None,
    alliance_name: str | None = None,
) -> StatisticsFilter:
    today = datetime.now().strftime(DATE_ONLY_FORMAT)
    return StatisticsFilter(
        start_at=f"{today} 00:00:00",
        end_at=f"{today} 23:59:59",
        user_search=user_search,
        alliance_name=alliance_name,
        page=0,
    )


def _describe_filter(mode: str, stats_filter: StatisticsFilter) -> str:
    start_text = _date_from_datetime_string(stats_filter.start_at) or "오늘"
    end_text = _date_from_datetime_string(stats_filter.end_at) or "오늘"
    category = "선택하세요"
    if mode == MODE_USER:
        category = "유저별"
    elif mode == MODE_ALLIANCE:
        category = "혈맹별"

    lines = [
        f"기준: {category}",
        f"기간: {start_text} ~ {end_text}",
    ]
    if mode == MODE_USER:
        lines.append(f"검색어: {stats_filter.user_search or '없음'}")
    elif mode == MODE_ALLIANCE:
        lines.append(f"선택 혈맹: {stats_filter.alliance_name or '선택하세요'}")
    return "\n".join(lines)


def _format_user_rows(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "조건에 맞는 데이터가 없습니다."
    return "\n".join(
        f"{row['discord_nickname']} [{row['alliance_name']}] | {row['attendance_count']}회"
        for row in rows
    )


def _slice_rows(
    rows: list[dict[str, object]],
    page: int,
    per_page: int = DEFAULT_USER_LIMIT,
) -> tuple[list[dict[str, object]], int]:
    page_count = max(1, (len(rows) + per_page - 1) // per_page)
    page = max(0, min(page, page_count - 1))
    start = page * per_page
    end = start + per_page
    return rows[start:end], page_count


async def _get_page_info(
    guild_id: int,
    mode: str,
    stats_filter: StatisticsFilter,
) -> dict[str, int]:
    if mode == MODE_NONE:
        return {"page_count": 1}
    if mode == MODE_ALLIANCE and not stats_filter.alliance_name:
        return {"page_count": 1}

    rows = await asyncio.to_thread(
        get_user_attendance_stats,
        guild_id,
        stats_filter.start_at,
        stats_filter.end_at,
        stats_filter.user_search if mode == MODE_USER else None,
        stats_filter.alliance_name if mode == MODE_ALLIANCE else None,
        500,
    )
    return {"page_count": max(1, (len(rows) + DEFAULT_USER_LIMIT - 1) // DEFAULT_USER_LIMIT)}


def _date_to_datetime(value: str, *, end_of_day: bool) -> str | None:
    if not value:
        return None
    dt = datetime.strptime(value, DATE_ONLY_FORMAT)
    return dt.strftime("%Y-%m-%d 23:59:59") if end_of_day else dt.strftime("%Y-%m-%d 00:00:00")


def _date_from_datetime_string(value: str | None) -> str:
    if not value:
        return ""
    try:
        return datetime.strptime(value, DATETIME_FORMAT).strftime(DATE_ONLY_FORMAT)
    except ValueError:
        return value[:10]


def _build_csv_file(guild_name: str, rows: list[dict[str, object]]) -> discord.File:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["참여 시간", "디스코드 ID", "닉네임", "혈맹"])
    for row in rows:
        writer.writerow(
            [
                row["started_at"],
                row["discord_id"] or "",
                row["discord_nickname"],
                row["alliance_name"],
            ]
        )
    data = buffer.getvalue().encode("utf-8-sig")
    file_buffer = io.BytesIO(data)
    safe_name = guild_name.replace(" ", "_")
    return discord.File(file_buffer, filename=f"{safe_name}_attendance_stats.csv")


async def _safe_response(interaction: discord.Interaction, message: str) -> None:
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)
