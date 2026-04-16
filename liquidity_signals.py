"""Higher-level signals: SOFR tail acceleration, volume spikes, SPX vs liquidity divergence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

from fred_client import FredObservation


@dataclass
class SofrTailDynamics:
    spread_bp_series: list[dict[str, Any]]
    spread_acceleration_up: bool
    detail: str


def sofr_tail_from_observations(
    sofr_obs: list[FredObservation],
    sofr99_obs: list[FredObservation],
) -> SofrTailDynamics:
    """
    Align by date (inner join) and test whether (SOFR99-SOFR) bp is **strictly** increasing
    over the last three paired observations (proxy for three trading days when FRED is daily).
    """
    if not sofr_obs or not sofr99_obs:
        return SofrTailDynamics([], False, "insufficient_history")

    by_d = {o.date: o.value_raw for o in sofr_obs}
    pairs: list[tuple[str, float, float, float]] = []
    for o99 in sofr99_obs:
        m = by_d.get(o99.date)
        if m is None:
            continue
        spr_bp = (o99.value_raw - m) * 100.0
        pairs.append((o99.date, o99.value_raw, m, spr_bp))
    pairs.sort(key=lambda x: x[0])
    series = [{"date": d, "sofr99_pct": a, "sofr_pct": b, "spread_bp": c} for d, a, b, c in pairs]
    if len(pairs) < 4:
        return SofrTailDynamics(series, False, "need>=4_aligned_days")

    last_spreads = [pairs[-1][3], pairs[-2][3], pairs[-3][3], pairs[-4][3]]
    d1 = last_spreads[0] - last_spreads[1]
    d2 = last_spreads[1] - last_spreads[2]
    d3 = last_spreads[2] - last_spreads[3]
    accel = d1 > 0 and d2 > 0 and d3 > 0 and d1 >= d2 >= d3
    return SofrTailDynamics(series, accel, "ok")


@dataclass
class VolumeAnomaly:
    abnormal: bool
    current_volume: float | None
    volume_ma: float | None
    yield_rising: bool | None
    net_liquidity_bias: float
    detail: str


def abnormal_volume_detector(
    *,
    closes: list[float],
    volumes: list[float],
    vol_ma_window: int = 20,
    spike_multiple: float = 2.0,
) -> VolumeAnomaly:
    """If latest volume > k * MA and short yield proxy rising, flag negative liquidity bias in [0,-1]."""
    if len(volumes) < vol_ma_window + 1 or len(closes) < vol_ma_window + 1:
        return VolumeAnomaly(False, None, None, None, 0.0, "short_history")

    v_win = volumes[-(vol_ma_window + 1) : -1]
    c_win = closes[-(vol_ma_window + 1) : -1]
    v_cur = volumes[-1]
    c_cur = closes[-1]
    v_ma = float(np.mean(v_win))
    c_ma = float(np.mean(c_win))
    yield_rising = c_cur > c_ma
    abnormal = v_cur > spike_multiple * max(1e-9, v_ma) and bool(yield_rising)
    bias = -0.35 if abnormal else 0.0
    return VolumeAnomaly(
        abnormal=abnormal,
        current_volume=v_cur,
        volume_ma=v_ma,
        yield_rising=yield_rising,
        net_liquidity_bias=bias,
        detail="ok",
    )


def liquidity_equity_divergence(
    *,
    net_by_day: pd.Series,
    bench: str = "SPY",
    lookback_high: int = 20,
    down_days: int = 3,
) -> dict[str, Any]:
    """
    Flag equity strength vs deteriorating net liquidity (daily last-snapshot series).
    """
    out: dict[str, Any] = {"active": False, "detail": "skipped"}
    if net_by_day is None or len(net_by_day) < down_days + 5:
        return {**out, "detail": "short_net_series"}

    try:
        px = yf.download(
            bench,
            period="120d",
            interval="1d",
            progress=False,
            auto_adjust=True,
        )
        if px is None or px.empty:
            return {**out, "detail": "bench_empty"}
        if isinstance(px.columns, pd.MultiIndex):
            px.columns = [c[0] if isinstance(c, tuple) else c for c in px.columns]
        close = px["Close"].squeeze()
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        eq = close.rename("eq").copy()
        eq.index = pd.to_datetime(eq.index).tz_localize(None).normalize()
        n = net_by_day.rename("net").copy()
        n.index = pd.to_datetime(n.index).normalize()
        m = pd.concat([n, eq], axis=1).dropna()
        if len(m) < lookback_high + down_days:
            return {**out, "detail": "short_merge"}

        m["roll_high"] = m["eq"].rolling(lookback_high).max()
        m["eq_near_high"] = m["eq"] >= (m["roll_high"] * 0.998)
        dn = m["net"].diff()
        tail = dn.iloc[-down_days:]
        net_down_streak = len(tail) == down_days and bool((tail < 0).all())
        active = bool(m["eq_near_high"].iloc[-1] and net_down_streak)
        return {
            "active": active,
            "bench": bench,
            "lookback_high": lookback_high,
            "down_days": down_days,
            "detail": "ok",
        }
    except Exception as exc:  # noqa: BLE001
        return {**out, "detail": f"error:{type(exc).__name__}"}
