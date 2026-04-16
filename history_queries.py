"""Read prior snapshots for velocity + drawdown alerts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from db import LiquiditySnapshot


@dataclass
class NetLiquidityWindow:
    hours: float
    delta_bn: float | None
    start_net_bn: float | None
    end_net_bn: float | None


def _parse_ts(row: LiquiditySnapshot) -> datetime:
    return row.ts_utc if row.ts_utc.tzinfo else row.ts_utc.replace(tzinfo=timezone.utc)


def net_liquidity_change_over_hours(
    session_factory: sessionmaker,
    *,
    hours: float = 24.0,
) -> NetLiquidityWindow | None:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    with session_factory() as s:
        q = (
            select(LiquiditySnapshot)
            .where(LiquiditySnapshot.ts_utc >= cutoff)
            .order_by(LiquiditySnapshot.ts_utc.asc())
        )
        rows = list(s.execute(q).scalars().all())
    if len(rows) < 2:
        return None
    first, last = rows[0], rows[-1]
    t0, t1 = _parse_ts(first), _parse_ts(last)
    dt_h = max(1e-6, (t1 - t0).total_seconds() / 3600.0)
    n0, n1 = first.net_liquidity_bn, last.net_liquidity_bn
    if n0 is None or n1 is None:
        return NetLiquidityWindow(hours=dt_h, delta_bn=None, start_net_bn=n0, end_net_bn=n1)
    return NetLiquidityWindow(
        hours=dt_h,
        delta_bn=float(n1) - float(n0),
        start_net_bn=float(n0),
        end_net_bn=float(n1),
    )


def rrp_avg_daily_change_bn(
    session_factory: sessionmaker,
    *,
    lookback_calendar_days: int = 12,
) -> float | None:
    """Mean ΔRRP across days (last snapshot per UTC calendar day)."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_calendar_days)
    with session_factory() as s:
        q = (
            select(LiquiditySnapshot)
            .where(LiquiditySnapshot.ts_utc >= cutoff)
            .order_by(LiquiditySnapshot.ts_utc.asc())
        )
        rows = list(s.execute(q).scalars().all())
    by_day: dict[date, float] = {}
    for r in rows:
        if r.rrp_bn is None:
            continue
        d = _parse_ts(r).date()
        by_day[d] = float(r.rrp_bn)
    days = sorted(by_day)
    if len(days) < 3:
        return None
    diffs = [by_day[days[i]] - by_day[days[i - 1]] for i in range(1, len(days))]
    return float(sum(diffs)) / len(diffs)


def daily_last_net_liquidity(session_factory: sessionmaker, *, lookback_days: int = 140) -> pd.Series:
    """UTC calendar day → last net_liquidity_bn observed that day."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    with session_factory() as s:
        q = (
            select(LiquiditySnapshot)
            .where(LiquiditySnapshot.ts_utc >= cutoff)
            .order_by(LiquiditySnapshot.ts_utc.asc())
        )
        rows = list(s.execute(q).scalars().all())
    by_day: dict[date, float] = {}
    for r in rows:
        if r.net_liquidity_bn is None:
            continue
        d = _parse_ts(r).date()
        by_day[d] = float(r.net_liquidity_bn)
    if not by_day:
        return pd.Series(dtype=float)
    idx = pd.to_datetime(sorted(by_day.keys()))
    vals = [by_day[k] for k in sorted(by_day.keys())]
    return pd.Series(vals, index=idx, name="net_liquidity_bn")


def daily_last_stress(session_factory: sessionmaker, *, lookback_days: int = 2000) -> pd.Series:
    """Calendar day → last `stress_score_0_100` parsed from stored JSON payloads."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    with session_factory() as s:
        q = (
            select(LiquiditySnapshot)
            .where(LiquiditySnapshot.ts_utc >= cutoff)
            .order_by(LiquiditySnapshot.ts_utc.asc())
        )
        rows = list(s.execute(q).scalars().all())
    by_day: dict[date, float] = {}
    for r in rows:
        try:
            p = json.loads(r.payload_json)
            st = (p.get("heuristic") or {}).get("stress_score_0_100")
            if st is None:
                continue
            d = _parse_ts(r).date()
            by_day[d] = float(st)
        except Exception:
            continue
    if not by_day:
        return pd.Series(dtype=float)
    idx = pd.to_datetime(sorted(by_day.keys()))
    vals = [by_day[k] for k in sorted(by_day.keys())]
    return pd.Series(vals, index=idx, name="stress")


def stress_hourly_distribution(
    session_factory: sessionmaker,
    *,
    hours: int = 24,
) -> list[dict[str, float | str]]:
    """UTC hourly max/mean stress for 'frog boil' charts."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    with session_factory() as s:
        q = (
            select(LiquiditySnapshot)
            .where(LiquiditySnapshot.ts_utc >= cutoff)
            .order_by(LiquiditySnapshot.ts_utc.asc())
        )
        rows = list(s.execute(q).scalars().all())
    pts: list[tuple[datetime, float]] = []
    for r in rows:
        try:
            p = json.loads(r.payload_json)
            st = (p.get("heuristic") or {}).get("stress_score_0_100")
            if st is None:
                continue
            pts.append((_parse_ts(r), float(st)))
        except Exception:
            continue
    if not pts:
        return []
    df = pd.DataFrame(pts, columns=["ts", "stress"])
    df["hour"] = df["ts"].dt.floor("h")
    g = df.groupby("hour", as_index=False)["stress"].agg(
        max_stress=("stress", "max"),
        mean_stress=("stress", "mean"),
    )
    out: list[dict[str, float | str]] = []
    for _, row in g.iterrows():
        out.append(
            {
                "hour_utc": row["hour"].isoformat(),
                "max_stress": float(row["max_stress"]),
                "mean_stress": float(row["mean_stress"]),
            }
        )
    return out


def liquidity_daily_panel(
    session_factory: sessionmaker,
    *,
    lookback_days: int = 1200,
) -> pd.DataFrame:
    """
    Last snapshot per **UTC calendar day** with reserves, RRP, net liquidity, heuristic stress.
    Used for regime detection + conditional backtests.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(30, int(lookback_days)))
    with session_factory() as s:
        q = (
            select(LiquiditySnapshot)
            .where(LiquiditySnapshot.ts_utc >= cutoff)
            .order_by(LiquiditySnapshot.ts_utc.asc())
        )
        rows = list(s.execute(q).scalars().all())
    by_day: dict[date, dict[str, float | None]] = {}
    for r in rows:
        d = _parse_ts(r).date()
        stress = None
        try:
            p = json.loads(r.payload_json)
            stress = (p.get("heuristic") or {}).get("stress_score_0_100")
            if stress is not None:
                stress = float(stress)
        except Exception:
            pass
        by_day[d] = {
            "reserves_bn": float(r.reserves_bn) if r.reserves_bn is not None else None,
            "rrp_bn": float(r.rrp_bn) if r.rrp_bn is not None else None,
            "net_liquidity_bn": float(r.net_liquidity_bn) if r.net_liquidity_bn is not None else None,
            "stress_score": stress,
        }
    if not by_day:
        return pd.DataFrame()
    df = pd.DataFrame.from_dict(by_day, orient="index")
    df.index = pd.to_datetime(sorted(df.index))
    df = df.sort_index()
    return df


def fetch_snapshot_at_or_before(
    session_factory: sessionmaker,
    *,
    cutoff_utc: datetime,
) -> LiquiditySnapshot | None:
    """Latest snapshot with ts_utc <= *cutoff_utc* (for ODE drift vs ~24h ago)."""
    with session_factory() as s:
        return s.execute(
            select(LiquiditySnapshot)
            .where(LiquiditySnapshot.ts_utc <= cutoff_utc)
            .order_by(LiquiditySnapshot.ts_utc.desc())
            .limit(1)
        ).scalar_one_or_none()
