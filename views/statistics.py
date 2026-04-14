from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime, timedelta

import discord

from db import (
    get_alliance_attendance_stats,
    get_attendance_export_rows,
    get_attendance_overview,
    get_daily_attendance_stats,
    get_user_attendance_stats,
)
from utils.guild import is_admin_member, is_supported_guild


DATE_ONLY_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_USER_LIMIT = 20


@dataclass(slots=True)
class StatisticsFilter:
    start_at: str | None = None
    end_at: str | None = None
    user_search: str | None = None
    page: int = 0


class StatisticsDashboardView(discord.ui.View):
    def __init__(
        self,
        bot: discord.Bot,
        guild_id: int,
        *,
        mode: str = "overview",
        stats_filter: StatisticsFilter | None = None,
    ):
        super().__init__(timeout=300)
        self.bot = bot
        self.guild_id = guild_id
        self.mode = mode
        self.stats_filter = stats_filter or StatisticsFilter()
        self.mode_select.options = _build_mode_options(mode)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        guild = interaction.guild
        if guild is None or not is_supported_guild(self.bot, guild.id):
            await _safe_response(interaction, "권한이 없습니다.")
            return False
        if not is_admin_member(interaction.user):
            await _safe_response(interaction, "권한이 없습니다.")
            return False
        return True

    @discord.ui.select(
        placeholder="통계 기준을 선택해주세요.",
        min_values=1,
        max_values=1,
        row=0,
    )
    async def mode_select(
        self,
        select: discord.ui.Select,
        interaction: discord.Interaction,
    ) -> None:
        mode = select.values[0]
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=mode,
            stats_filter=self.stats_filter,
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
        today = datetime.now().strftime(DATE_ONLY_FORMAT)
        stats_filter = StatisticsFilter(
            start_at=f"{today} 00:00:00",
            end_at=f"{today} 23:59:59",
            user_search=self.stats_filter.user_search,
            page=0,
        )
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=self.mode,
            stats_filter=stats_filter,
        )

    @discord.ui.button(label="최근 7일", style=discord.ButtonStyle.secondary, row=1)
    async def week_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=6)
        stats_filter = StatisticsFilter(
            start_at=start_dt.strftime("%Y-%m-%d 00:00:00"),
            end_at=end_dt.strftime("%Y-%m-%d 23:59:59"),
            user_search=self.stats_filter.user_search,
            page=0,
        )
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=self.mode,
            stats_filter=stats_filter,
        )

    @discord.ui.button(label="검색", style=discord.ButtonStyle.secondary, row=2)
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

    @discord.ui.button(label="검색 초기화", style=discord.ButtonStyle.secondary, row=2)
    async def clear_search_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        stats_filter = StatisticsFilter(
            start_at=self.stats_filter.start_at,
            end_at=self.stats_filter.end_at,
            user_search=None,
            page=0,
        )
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=self.mode,
            stats_filter=stats_filter,
        )

    @discord.ui.button(label="CSV 다운로드", style=discord.ButtonStyle.success, row=3)
    async def export_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        guild = interaction.guild
        if guild is None:
            await _safe_response(interaction, "서버에서만 사용할 수 있습니다.")
            return

        rows = get_attendance_export_rows(
            self.guild_id,
            self.stats_filter.start_at,
            self.stats_filter.end_at,
            search=self.stats_filter.user_search,
        )
        file = _build_csv_file(guild.name, rows)
        message = "통계 CSV 파일입니다. 엑셀에서 바로 열 수 있습니다."
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
        stats_filter = StatisticsFilter(
            start_at=self.stats_filter.start_at,
            end_at=self.stats_filter.end_at,
            user_search=self.stats_filter.user_search,
            page=max(0, self.stats_filter.page - 1),
        )
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=self.mode,
            stats_filter=stats_filter,
        )

    @discord.ui.button(label="다음 페이지", style=discord.ButtonStyle.secondary, row=4)
    async def next_page_button(
        self,
        button: discord.ui.Button,
        interaction: discord.Interaction,
    ) -> None:
        page_info = _get_page_info(self.guild_id, self.mode, self.stats_filter)
        next_page = min(page_info["page_count"] - 1, self.stats_filter.page + 1)
        stats_filter = StatisticsFilter(
            start_at=self.stats_filter.start_at,
            end_at=self.stats_filter.end_at,
            user_search=self.stats_filter.user_search,
            page=max(0, next_page),
        )
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=self.mode,
            stats_filter=stats_filter,
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
        super().__init__(title="통계 기간 설정")
        self.bot = bot
        self.guild_id = guild_id
        self.mode = mode
        self.stats_filter = stats_filter

        self.start_input = discord.ui.InputText(
            label="시작 날짜",
            placeholder="YYYY-MM-DD 또는 비우기",
            required=False,
            value=_date_from_datetime_string(stats_filter.start_at),
        )
        self.end_input = discord.ui.InputText(
            label="종료 날짜",
            placeholder="YYYY-MM-DD 또는 비우기",
            required=False,
            value=_date_from_datetime_string(stats_filter.end_at),
        )
        self.add_item(self.start_input)
        self.add_item(self.end_input)

    async def callback(self, interaction: discord.Interaction) -> None:
        start_date = self.start_input.value.strip()
        end_date = self.end_input.value.strip()
        try:
            start_at = _date_to_datetime(start_date, end_of_day=False)
            end_at = _date_to_datetime(end_date, end_of_day=True)
        except ValueError:
            await interaction.response.send_message(
                "날짜는 YYYY-MM-DD 형식으로 입력해주세요.",
                ephemeral=True,
            )
            return

        if start_at and end_at and start_at > end_at:
            await interaction.response.send_message(
                "시작 날짜는 종료 날짜보다 늦을 수 없습니다.",
                ephemeral=True,
            )
            return

        stats_filter = StatisticsFilter(
            start_at=start_at,
            end_at=end_at,
            user_search=self.stats_filter.user_search,
            page=0,
        )
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=self.mode,
            stats_filter=stats_filter,
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
        super().__init__(title="통계 검색")
        self.bot = bot
        self.guild_id = guild_id
        self.mode = mode
        self.stats_filter = stats_filter
        self.search_input = discord.ui.InputText(
            label="유저/혈맹 검색",
            placeholder="닉네임, 디스코드 ID, 혈맹명",
            required=False,
            value=stats_filter.user_search or "",
        )
        self.add_item(self.search_input)

    async def callback(self, interaction: discord.Interaction) -> None:
        search = self.search_input.value.strip() or None
        stats_filter = StatisticsFilter(
            start_at=self.stats_filter.start_at,
            end_at=self.stats_filter.end_at,
            user_search=search,
            page=0,
        )
        await _render_dashboard(
            interaction,
            self.bot,
            self.guild_id,
            mode=self.mode,
            stats_filter=stats_filter,
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

    embed = _build_statistics_embed(guild, guild_id, mode, stats_filter)
    view = StatisticsDashboardView(
        bot,
        guild_id,
        mode=mode,
        stats_filter=stats_filter,
    )
    page_info = _get_page_info(guild_id, mode, stats_filter)
    show_pagination = mode in {"daily", "alliance", "user"} and page_info["page_count"] > 1
    view.previous_page_button.disabled = not show_pagination or stats_filter.page <= 0
    view.next_page_button.disabled = (
        not show_pagination or stats_filter.page >= page_info["page_count"] - 1
    )
    if interaction.response.is_done():
        await interaction.edit_original_response(embed=embed, view=view, content=None)
    elif interaction.message is not None:
        await interaction.response.edit_message(embed=embed, view=view, content=None)
    else:
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


def _build_statistics_embed(
    guild: discord.Guild,
    guild_id: int,
    mode: str,
    stats_filter: StatisticsFilter,
) -> discord.Embed:
    embed = discord.Embed(
        title="출석 통계",
        color=discord.Color.gold(),
    )
    embed.description = _describe_filter(stats_filter)
    if mode == "overview":
        overview = get_attendance_overview(
            guild_id,
            stats_filter.start_at,
            stats_filter.end_at,
        )
        embed.add_field(name="서버", value=guild.name, inline=False)
        embed.add_field(name="출석 세션 수", value=f"{overview['session_count']}회")
        embed.add_field(name="총 출석 수", value=f"{overview['total_attendance_count']}회")
        embed.add_field(name="참여 유저 수", value=f"{overview['unique_user_count']}명")
        embed.add_field(name="세션 평균 인원", value=f"{overview['average_attendance_count']}명")
        return embed

    if mode == "daily":
        rows = get_daily_attendance_stats(
            guild_id,
            stats_filter.start_at,
            stats_filter.end_at,
        )
        paged_rows, page_count = _slice_rows(rows, stats_filter.page)
        embed.add_field(
            name=f"일자별 통계 ({stats_filter.page + 1}/{page_count})",
            value=_format_daily_rows(paged_rows),
            inline=False,
        )
        return embed

    if mode == "alliance":
        rows = get_alliance_attendance_stats(
            guild_id,
            stats_filter.start_at,
            stats_filter.end_at,
            search=stats_filter.user_search,
        )
        paged_rows, page_count = _slice_rows(rows, stats_filter.page)
        embed.add_field(
            name=f"혈맹별 통계 ({stats_filter.page + 1}/{page_count})",
            value=_format_alliance_rows(paged_rows),
            inline=False,
        )
        return embed

    rows = get_user_attendance_stats(
        guild_id,
        stats_filter.start_at,
        stats_filter.end_at,
        search=stats_filter.user_search,
        limit=500,
    )
    paged_rows, page_count = _slice_rows(rows, stats_filter.page)
    embed.add_field(
        name=f"유저별 통계 ({stats_filter.page + 1}/{page_count})",
        value=_format_user_rows(paged_rows),
        inline=False,
    )
    return embed


def build_statistics_dashboard_embed(
    guild: discord.Guild,
    guild_id: int,
    *,
    mode: str = "overview",
    stats_filter: StatisticsFilter | None = None,
) -> discord.Embed:
    return _build_statistics_embed(
        guild,
        guild_id,
        mode,
        stats_filter or StatisticsFilter(),
    )


def _build_mode_options(current_mode: str) -> list[discord.SelectOption]:
    options = [
        ("overview", "전체 요약", "출석 세션, 인원, 평균을 봅니다."),
        ("daily", "일자별", "날짜별 출석 흐름을 봅니다."),
        ("alliance", "혈맹별", "혈맹별 출석 현황을 봅니다."),
        ("user", "유저별", "유저별 출석 횟수를 봅니다."),
    ]
    return [
        discord.SelectOption(
            label=label,
            value=value,
            description=description,
            default=value == current_mode,
        )
        for value, label, description in options
    ]


def _describe_filter(stats_filter: StatisticsFilter) -> str:
    range_text = "전체 기간"
    if stats_filter.start_at or stats_filter.end_at:
        start_text = _date_from_datetime_string(stats_filter.start_at) or "처음"
        end_text = _date_from_datetime_string(stats_filter.end_at) or "현재"
        range_text = f"기간: {start_text} ~ {end_text}"

    if stats_filter.user_search:
        return f"{range_text}\n검색어: {stats_filter.user_search}"
    return range_text


def _format_daily_rows(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "조회된 데이터가 없습니다."
    return "\n".join(
        f"{row['attendance_date']} | 출석 {row['session_count']}회 | 참여 유저 {row['unique_user_count']}명"
        for row in rows
    )


def _format_alliance_rows(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "조회된 데이터가 없습니다."
    return "\n".join(
        f"[{row['alliance_name']}] | 출석 {row['attendance_count']}회 | 유저 {row['unique_user_count']}명"
        for row in rows
    )


def _format_user_rows(rows: list[dict[str, object]]) -> str:
    if not rows:
        return "조회된 데이터가 없습니다."
    return "\n".join(
        f"{row['discord_nickname']} ({row['alliance_name']}) | 출석 {row['attendance_count']}회"
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


def _get_page_info(
    guild_id: int,
    mode: str,
    stats_filter: StatisticsFilter,
) -> dict[str, int]:
    if mode == "overview":
        return {"page_count": 1}
    if mode == "daily":
        rows = get_daily_attendance_stats(guild_id, stats_filter.start_at, stats_filter.end_at)
    elif mode == "alliance":
        rows = get_alliance_attendance_stats(
            guild_id,
            stats_filter.start_at,
            stats_filter.end_at,
            search=stats_filter.user_search,
        )
    else:
        rows = get_user_attendance_stats(
            guild_id,
            stats_filter.start_at,
            stats_filter.end_at,
            search=stats_filter.user_search,
            limit=500,
        )
    return {"page_count": max(1, (len(rows) + DEFAULT_USER_LIMIT - 1) // DEFAULT_USER_LIMIT)}


def _date_to_datetime(value: str, *, end_of_day: bool) -> str | None:
    if not value:
        return None
    dt = datetime.strptime(value, DATE_ONLY_FORMAT)
    if end_of_day:
        return dt.strftime("%Y-%m-%d 23:59:59")
    return dt.strftime("%Y-%m-%d 00:00:00")


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
    writer.writerow(
        [
            "started_at",
            "discord_id",
            "discord_nickname",
            "alliance_name",
        ]
    )
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
