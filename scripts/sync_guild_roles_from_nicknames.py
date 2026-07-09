from __future__ import annotations

import argparse
import asyncio
import os
import re
from dataclasses import dataclass

import discord
from dotenv import load_dotenv


DEFAULT_PREFIXES = ("반공", "원피스", "지구방위대", "정예")
NICKNAME_PREFIX_RE = re.compile(r"^\[([^\]]+)\]")


@dataclass(slots=True)
class RoleSyncResult:
    checked: int = 0
    matched: int = 0
    already_has_role: int = 0
    planned: int = 0
    added: int = 0
    missing_roles: int = 0
    errors: int = 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="닉네임 [혈맹] 접두사 기준으로 같은 이름의 Discord 역할을 부여합니다.",
    )
    parser.add_argument(
        "--guild-id",
        type=int,
        default=int(os.getenv("ROLE_SYNC_GUILD_ID") or "0"),
        help="대상 Discord 서버 ID. 생략 시 봇이 들어간 서버가 1개일 때만 자동 선택합니다.",
    )
    parser.add_argument(
        "--prefix",
        action="append",
        dest="prefixes",
        help="대상 혈맹 이름. 여러 번 지정 가능하며 생략 시 기본 4개 혈맹을 사용합니다.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="실제로 역할을 부여합니다. 생략하면 점검만 수행합니다.",
    )
    parser.add_argument(
        "--include-bots",
        action="store_true",
        help="봇 계정도 검사합니다.",
    )
    return parser.parse_args()


def _nickname_prefix(member: discord.Member) -> str | None:
    match = NICKNAME_PREFIX_RE.match(member.display_name.strip())
    if match is None:
        return None
    return match.group(1).strip()


def _role_map(guild: discord.Guild, prefixes: set[str]) -> dict[str, discord.Role]:
    roles: dict[str, discord.Role] = {}
    for prefix in prefixes:
        matches = [role for role in guild.roles if role.name == prefix]
        if matches:
            roles[prefix] = max(matches, key=lambda role: role.position)
    return roles


async def _fetch_members(guild: discord.Guild) -> list[discord.Member]:
    members: list[discord.Member] = []
    async for member in guild.fetch_members(limit=None):
        members.append(member)
    return members


async def _run() -> None:
    load_dotenv()
    args = _parse_args()
    token = os.getenv("ROLE_SYNC_BOT_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        raise SystemExit("ROLE_SYNC_BOT_TOKEN 환경변수에 봇 토큰을 넣어주세요.")

    prefixes = set(args.prefixes or DEFAULT_PREFIXES)
    intents = discord.Intents.none()
    intents.guilds = True
    intents.members = True
    client = discord.Client(intents=intents)
    result = RoleSyncResult()

    @client.event
    async def on_ready() -> None:
        try:
            guild = None
            if args.guild_id:
                guild = client.get_guild(args.guild_id)
            elif len(client.guilds) == 1:
                guild = client.guilds[0]
            else:
                print("대상 서버를 하나로 특정할 수 없습니다. --guild-id를 지정해주세요.")
                for joined_guild in client.guilds:
                    print(f"- {joined_guild.name}: {joined_guild.id}")
                return

            if guild is None:
                print(f"서버를 찾을 수 없습니다: {args.guild_id}")
                return

            roles = _role_map(guild, prefixes)
            missing_role_names = sorted(prefixes - set(roles))
            if missing_role_names:
                print("서버에 없는 역할:", ", ".join(missing_role_names))

            print(
                f"서버: {guild.name} ({guild.id}) / 모드: "
                f"{'적용' if args.apply else '점검'}"
            )
            print("대상 혈맹:", ", ".join(sorted(prefixes)))

            members = await _fetch_members(guild)
            bot_member = guild.me
            for member in sorted(members, key=lambda item: item.display_name):
                if member.bot and not args.include_bots:
                    continue
                result.checked += 1
                prefix = _nickname_prefix(member)
                if prefix not in prefixes:
                    continue
                result.matched += 1
                role = roles.get(prefix)
                if role is None:
                    result.missing_roles += 1
                    print(f"[역할없음] {member.display_name} -> {prefix}")
                    continue
                if role in member.roles:
                    result.already_has_role += 1
                    continue
                if bot_member is not None and role >= bot_member.top_role:
                    result.errors += 1
                    print(
                        f"[권한부족] {member.display_name} -> {role.name} "
                        f"(봇 역할보다 대상 역할이 높거나 같습니다)"
                    )
                    continue

                result.planned += 1
                if not args.apply:
                    print(f"[추가예정] {member.display_name} -> {role.name}")
                    continue
                try:
                    await member.add_roles(
                        role,
                        reason="닉네임 혈맹 접두사 기준 역할 동기화",
                    )
                    result.added += 1
                    print(f"[추가완료] {member.display_name} -> {role.name}")
                except discord.Forbidden:
                    result.errors += 1
                    print(f"[권한실패] {member.display_name} -> {role.name}")
                except discord.HTTPException as exc:
                    result.errors += 1
                    print(f"[API실패] {member.display_name} -> {role.name}: {exc}")

            print("--- 요약 ---")
            print(f"검사 인원: {result.checked}")
            print(f"접두사 매칭: {result.matched}")
            print(f"이미 역할 있음: {result.already_has_role}")
            print(f"추가 대상: {result.planned}")
            print(f"추가 완료: {result.added}")
            print(f"역할 없음: {result.missing_roles}")
            print(f"오류: {result.errors}")
        finally:
            await client.close()

    await client.start(token)


if __name__ == "__main__":
    asyncio.run(_run())
