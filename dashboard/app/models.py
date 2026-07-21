from __future__ import annotations

from sqlalchemy import BigInteger, Boolean, CheckConstraint, ForeignKey, Integer, SmallInteger, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Guild(Base):
    __tablename__ = "guilds"

    guild_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class Alliance(Base):
    __tablename__ = "alliances"

    alliance_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    alliance_name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    display_name: Mapped[str | None] = mapped_column(Text)
    tag_name: Mapped[str | None] = mapped_column(Text)
    color: Mapped[str | None] = mapped_column(Text)
    sort_order: Mapped[int | None] = mapped_column(Integer)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class User(Base):
    __tablename__ = "users"

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    alliance_id: Mapped[int | None] = mapped_column(ForeignKey("alliances.alliance_id"))
    discord_id: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True)
    discord_nickname: Mapped[str] = mapped_column(Text, nullable=False)
    game_nickname: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class Item(Base):
    __tablename__ = "items"

    item_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    guild_id: Mapped[int | None] = mapped_column(ForeignKey("guilds.guild_id", ondelete="CASCADE"))
    item_name: Mapped[str] = mapped_column(Text, nullable=False)
    default_price: Mapped[int | None] = mapped_column(BigInteger)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class AttendanceSession(Base):
    __tablename__ = "attendance_sessions"

    attendance_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    guild_id: Mapped[int] = mapped_column(ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)
    started_at: Mapped[str] = mapped_column(Text, nullable=False)
    ended_at: Mapped[str] = mapped_column(Text, nullable=False)
    started_by_discord_id: Mapped[int | None] = mapped_column(BigInteger)


class CatalogItemVersion(Base):
    __tablename__ = "catalog_item_versions"

    item_version_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    item_id: Mapped[int] = mapped_column(ForeignKey("items.item_id", ondelete="RESTRICT"), nullable=False)
    item_name: Mapped[str] = mapped_column(Text, nullable=False)
    valid_from: Mapped[int] = mapped_column(BigInteger, nullable=False)


class SettlementDrop(Base):
    __tablename__ = "settlement_drops"

    drop_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    guild_id: Mapped[int] = mapped_column(ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)
    attendance_id: Mapped[int] = mapped_column(ForeignKey("attendance_sessions.attendance_id", ondelete="RESTRICT"), nullable=False)
    item_version_id: Mapped[int] = mapped_column(ForeignKey("catalog_item_versions.item_version_id", ondelete="RESTRICT"), nullable=False)
    cash_price_krw: Mapped[int] = mapped_column(BigInteger, nullable=False)
    adena_market_rate: Mapped[int] = mapped_column(BigInteger, nullable=False)
    gross_adena: Mapped[int] = mapped_column(BigInteger, nullable=False)
    occurred_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.user_id", ondelete="SET NULL"))

    item_version: Mapped[CatalogItemVersion] = relationship()


class SettlementDropParticipant(Base):
    __tablename__ = "settlement_drop_participants"

    drop_id: Mapped[int] = mapped_column(ForeignKey("settlement_drops.drop_id", ondelete="CASCADE"), primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.user_id", ondelete="RESTRICT"), primary_key=True)
    alliance_id: Mapped[int | None] = mapped_column(ForeignKey("alliances.alliance_id", ondelete="RESTRICT"))


class SettlementDropExcludedAlliance(Base):
    __tablename__ = "settlement_drop_excluded_alliances"

    drop_id: Mapped[int] = mapped_column(ForeignKey("settlement_drops.drop_id", ondelete="CASCADE"), primary_key=True)
    alliance_id: Mapped[int] = mapped_column(ForeignKey("alliances.alliance_id", ondelete="RESTRICT"), primary_key=True)


class SettlementFeeRule(Base):
    __tablename__ = "settlement_fee_rules"

    fee_rule_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    guild_id: Mapped[int] = mapped_column(ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)
    alliance_id: Mapped[int | None] = mapped_column(ForeignKey("alliances.alliance_id", ondelete="CASCADE"))
    scope_code: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class SettlementFeeRuleVersion(Base):
    __tablename__ = "settlement_fee_rule_versions"

    fee_rule_version_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fee_rule_id: Mapped[int] = mapped_column(ForeignKey("settlement_fee_rules.fee_rule_id", ondelete="CASCADE"), nullable=False)
    rule_name: Mapped[str] = mapped_column(Text, nullable=False)
    rate_ppm: Mapped[int] = mapped_column(Integer, nullable=False)
    valid_from: Mapped[int] = mapped_column(BigInteger, nullable=False)


class SettlementPayoutObject(Base):
    __tablename__ = "settlement_payout_objects"
    __table_args__ = (
        CheckConstraint("object_code IN (1, 2, 3)", name="chk_settlement_payout_object_code"),
        CheckConstraint("status_code IN (0, 1, 2)", name="chk_settlement_payout_status_code"),
    )

    payout_object_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    drop_id: Mapped[int] = mapped_column(ForeignKey("settlement_drops.drop_id", ondelete="CASCADE"), nullable=False)
    parent_payout_object_id: Mapped[int | None] = mapped_column(ForeignKey("settlement_payout_objects.payout_object_id", ondelete="CASCADE"))
    object_code: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    recipient_alliance_id: Mapped[int | None] = mapped_column(ForeignKey("alliances.alliance_id"))
    recipient_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.user_id"))
    fee_rule_version_id: Mapped[int | None] = mapped_column(ForeignKey("settlement_fee_rule_versions.fee_rule_version_id"))
    amount_adena: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status_code: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    completed_at: Mapped[int | None] = mapped_column(BigInteger)
    completed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.user_id"))


class TreasuryAccount(Base):
    __tablename__ = "treasury_accounts"
    __table_args__ = (UniqueConstraint("guild_id", "alliance_id", name="uq_treasury_account_guild_alliance"),)

    treasury_account_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    guild_id: Mapped[int] = mapped_column(ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)
    alliance_id: Mapped[int] = mapped_column(ForeignKey("alliances.alliance_id", ondelete="CASCADE"), nullable=False)
    current_balance: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    updated_at: Mapped[int] = mapped_column(BigInteger, nullable=False)


class TreasuryCategory(Base):
    __tablename__ = "treasury_categories"
    __table_args__ = (UniqueConstraint("guild_id", "direction", "category_name", name="uq_treasury_category_guild_direction_name"),)

    treasury_category_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    guild_id: Mapped[int] = mapped_column(ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False)
    direction: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    category_name: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class TreasurySourceType(Base):
    __tablename__ = "treasury_source_types"

    source_type_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    source_code: Mapped[str] = mapped_column(Text, nullable=False, unique=True)


class TreasuryEntry(Base):
    __tablename__ = "treasury_entries"

    treasury_entry_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    treasury_account_id: Mapped[int] = mapped_column(ForeignKey("treasury_accounts.treasury_account_id", ondelete="RESTRICT"), nullable=False)
    treasury_category_id: Mapped[int | None] = mapped_column(ForeignKey("treasury_categories.treasury_category_id", ondelete="RESTRICT"))
    direction: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    amount_adena: Mapped[int] = mapped_column(BigInteger, nullable=False)
    balance_after: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_type_id: Mapped[int] = mapped_column(ForeignKey("treasury_source_types.source_type_id"), nullable=False)
    source_id: Mapped[int | None] = mapped_column(BigInteger)
    memo: Mapped[str | None] = mapped_column(Text)
    occurred_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.user_id", ondelete="SET NULL"))
    reversal_of_entry_id: Mapped[int | None] = mapped_column(ForeignKey("treasury_entries.treasury_entry_id", ondelete="RESTRICT"), unique=True)

