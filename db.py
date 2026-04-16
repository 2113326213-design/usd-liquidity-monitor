"""SQLite / Postgres persistence for snapshot history."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import DateTime, Float, String, Text, create_engine, delete, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


class QraSnapshot(Base):
    """Parsed QRA / Treasury financing anchors (latest row wins in service layer)."""

    __tablename__ = "qra_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ts_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    source_url: Mapped[str] = mapped_column(String(512))
    end_quarter_cash_balance_bn: Mapped[float | None] = mapped_column(Float, nullable=True)
    quarter_label: Mapped[str | None] = mapped_column(String(64), nullable=True)
    snippet: Mapped[str | None] = mapped_column(Text, nullable=True)


class LiquiditySnapshot(Base):
    __tablename__ = "liquidity_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ts_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload_json: Mapped[str] = mapped_column(Text())

    net_liquidity_bn: Mapped[float | None] = mapped_column(Float, nullable=True, index=True)
    rrp_bn: Mapped[float | None] = mapped_column(Float, nullable=True)
    reserves_bn: Mapped[float | None] = mapped_column(Float, nullable=True)
    walcl_bn: Mapped[float | None] = mapped_column(Float, nullable=True)
    liquidity_velocity_bn_per_hr: Mapped[float | None] = mapped_column(Float, nullable=True)
    velocity_24h_bn_per_day: Mapped[float | None] = mapped_column(Float, nullable=True)
    velocity_7d_bn_per_day: Mapped[float | None] = mapped_column(Float, nullable=True)

    ode_baseline_lclor_date_iso: Mapped[str | None] = mapped_column(String(16), nullable=True)
    ode_baseline_rrp_floor_date_iso: Mapped[str | None] = mapped_column(String(16), nullable=True)


def _ensure_columns(engine) -> None:
    """SQLite lightweight migrations for new columns."""
    if engine.dialect.name != "sqlite":
        return
    with engine.connect() as conn:
        cols = {row[1] for row in conn.execute(text("PRAGMA table_info(liquidity_snapshots)"))}
        alters: list[str] = []
        if "reserves_bn" not in cols:
            alters.append("ALTER TABLE liquidity_snapshots ADD COLUMN reserves_bn FLOAT")
        if "walcl_bn" not in cols:
            alters.append("ALTER TABLE liquidity_snapshots ADD COLUMN walcl_bn FLOAT")
        if "liquidity_velocity_bn_per_hr" not in cols:
            alters.append(
                "ALTER TABLE liquidity_snapshots ADD COLUMN liquidity_velocity_bn_per_hr FLOAT"
            )
        if "velocity_24h_bn_per_day" not in cols:
            alters.append("ALTER TABLE liquidity_snapshots ADD COLUMN velocity_24h_bn_per_day FLOAT")
        if "velocity_7d_bn_per_day" not in cols:
            alters.append("ALTER TABLE liquidity_snapshots ADD COLUMN velocity_7d_bn_per_day FLOAT")
        if "ode_baseline_lclor_date_iso" not in cols:
            alters.append("ALTER TABLE liquidity_snapshots ADD COLUMN ode_baseline_lclor_date_iso VARCHAR(16)")
        if "ode_baseline_rrp_floor_date_iso" not in cols:
            alters.append("ALTER TABLE liquidity_snapshots ADD COLUMN ode_baseline_rrp_floor_date_iso VARCHAR(16)")
        for stmt in alters:
            conn.execute(text(stmt))
        if alters:
            conn.commit()


def cleanup_old_snapshots(session_factory, *, retention_days: int) -> int:
    """Delete snapshots older than *retention_days* (UTC). Returns rows removed."""
    if retention_days <= 0:
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=int(retention_days))
    with session_factory() as s:
        res = s.execute(delete(LiquiditySnapshot).where(LiquiditySnapshot.ts_utc < cutoff))
        s.commit()
        return int(res.rowcount or 0)


def make_session_factory(database_url: str):
    engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False} if database_url.startswith("sqlite") else {},
    )
    Base.metadata.create_all(engine)
    _ensure_columns(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _net_at_or_before(session_factory, ts: datetime) -> float | None:
    with session_factory() as s:
        row = s.execute(
            select(LiquiditySnapshot)
            .where(LiquiditySnapshot.ts_utc <= ts)
            .order_by(LiquiditySnapshot.ts_utc.desc())
            .limit(1)
        ).scalar_one_or_none()
        if not row or row.net_liquidity_bn is None:
            return None
        return float(row.net_liquidity_bn)


def store_snapshot(session_factory, payload: dict[str, Any]) -> None:
    bal = payload.get("balances_bn") or {}
    net = payload.get("net_liquidity_bn")
    rrp = bal.get("rrp")
    reserves = bal.get("reserves")
    walcl = bal.get("walcl")
    payload_out = dict(payload)

    prev_net: float | None = None
    prev_ts: datetime | None = None
    with session_factory() as s:
        row = s.execute(
            select(LiquiditySnapshot).order_by(LiquiditySnapshot.ts_utc.desc()).limit(1)
        ).scalar_one_or_none()
        if row:
            prev_net = row.net_liquidity_bn
            prev_ts = row.ts_utc

    ts_new = datetime.fromisoformat(payload_out["timestamp_utc"])
    vel_calc: float | None = None
    if prev_net is not None and net is not None and prev_ts is not None:
        dt_h = max(1e-6, (ts_new - prev_ts).total_seconds() / 3600.0)
        vel_calc = (float(net) - float(prev_net)) / dt_h
        payload_out["liquidity_velocity_bn_per_hr"] = round(vel_calc, 6)

    net24 = _net_at_or_before(session_factory, ts_new - timedelta(hours=24))
    net7 = _net_at_or_before(session_factory, ts_new - timedelta(days=7))
    v24 = v7 = None
    if net is not None and net24 is not None:
        v24 = round((float(net) - net24) / 1.0, 6)
        payload_out["velocity_24h_bn_per_day"] = v24
    if net is not None and net7 is not None:
        v7 = round((float(net) - net7) / 7.0, 6)
        payload_out["velocity_7d_bn_per_day"] = v7

    ode_lcl_iso: str | None = None
    ode_rrp_iso: str | None = None
    ode_rw = payload_out.get("ode_runway") or {}
    if isinstance(ode_rw, dict):
        bl = (ode_rw.get("scenarios") or {}).get("baseline") or {}
        if isinstance(bl.get("lclor_expected_date"), str):
            ode_lcl_iso = bl["lclor_expected_date"]
        if isinstance(bl.get("rrp_floor_expected_date"), str):
            ode_rrp_iso = bl["rrp_floor_expected_date"]

    snap = LiquiditySnapshot(
        ts_utc=ts_new,
        payload_json=json.dumps(payload_out, default=str),
        net_liquidity_bn=net if isinstance(net, (int, float)) else None,
        rrp_bn=rrp if isinstance(rrp, (int, float)) else None,
        reserves_bn=reserves if isinstance(reserves, (int, float)) else None,
        walcl_bn=walcl if isinstance(walcl, (int, float)) else None,
        liquidity_velocity_bn_per_hr=vel_calc,
        velocity_24h_bn_per_day=v24,
        velocity_7d_bn_per_day=v7,
        ode_baseline_lclor_date_iso=ode_lcl_iso,
        ode_baseline_rrp_floor_date_iso=ode_rrp_iso,
    )
    with session_factory() as s:
        s.add(snap)
        s.commit()
