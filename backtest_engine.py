"""
Signal validation / stress-score backtests (offline-friendly).

Feed a table of (timestamp, stress_score, spy_close) or let helpers pull SPY via yfinance.
This stays dependency-light: pandas + yfinance only (no scipy required for baseline stats).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
import yfinance as yf


@dataclass
class StressHitStats:
    threshold: float
    forward_days: int
    hit_rate_negative_spy: float | None
    n_events: int
    detail: str


def _spy_daily() -> pd.Series:
    d = yf.download("SPY", period="6y", interval="1d", progress=False, auto_adjust=True)
    if d is None or d.empty:
        return pd.Series(dtype=float)
    if isinstance(d.columns, pd.MultiIndex):
        d.columns = [c[0] if isinstance(c, tuple) else c for c in d.columns]
    s = d["Close"].squeeze()
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    s.index = pd.to_datetime(s.index).tz_localize(None).normalize()
    return s.rename("spy")


def stress_forward_hit_rate(
    stress_by_day: pd.Series,
    *,
    stress_threshold: float = 80.0,
    forward_days: int = 5,
) -> StressHitStats:
    """
    SNR-style primitive: P(SPY forward return < 0 | stress > threshold).
    Aligns on calendar days; stress series should be daily (e.g. last snapshot per day).
    """
    spy = _spy_daily()
    if stress_by_day.empty or spy.empty:
        return StressHitStats(stress_threshold, forward_days, None, 0, "missing_series")

    m = pd.concat(
        [
            stress_by_day.rename("stress"),
            spy,
        ],
        axis=1,
    ).dropna()
    if m.empty:
        return StressHitStats(stress_threshold, forward_days, None, 0, "empty_merge")

    m["fwd"] = m["spy"].shift(-forward_days) / m["spy"] - 1.0
    hits = m[m["stress"] >= stress_threshold].dropna(subset=["fwd"])
    if hits.empty:
        return StressHitStats(stress_threshold, forward_days, None, 0, "no_events")

    neg = (hits["fwd"] < 0).mean()
    return StressHitStats(
        threshold=stress_threshold,
        forward_days=forward_days,
        hit_rate_negative_spy=float(neg),
        n_events=int(len(hits)),
        detail="ok",
    )


def stress_to_dict(s: StressHitStats) -> dict[str, Any]:
    return {
        "stress_threshold": s.threshold,
        "forward_days": s.forward_days,
        "hit_rate_negative_spy": s.hit_rate_negative_spy,
        "n_events": s.n_events,
        "detail": s.detail,
    }


def stress_forward_hit_rate_by_regime(
    stress_by_day: pd.Series,
    regime_hard: pd.Series,
    *,
    stress_threshold: float = 80.0,
    forward_days: int = 5,
    regimes: tuple[str, ...] = ("abundant", "ample", "scarce", "crisis"),
) -> dict[str, Any]:
    """
    Conditional primitive: P(SPY fwd < 0 | stress ≥ threshold, regime = k).

    *stress_by_day* and *regime_hard* must share a **DatetimeIndex** (calendar daily).
    """
    spy = _spy_daily()
    if stress_by_day.empty or spy.empty or regime_hard.empty:
        return {
            "pooled": stress_to_dict(
                StressHitStats(stress_threshold, forward_days, None, 0, "missing_series")
            ),
            "by_regime": {},
            "detail": "missing_series",
        }

    m = pd.concat(
        [
            stress_by_day.rename("stress").astype(float),
            regime_hard.rename("regime").astype(str),
            spy,
        ],
        axis=1,
    ).dropna(subset=["stress", "regime"])
    if m.empty:
        return {
            "pooled": stress_to_dict(
                StressHitStats(stress_threshold, forward_days, None, 0, "empty_merge")
            ),
            "by_regime": {},
            "detail": "empty_merge",
        }

    m["fwd"] = m["spy"].shift(-forward_days) / m["spy"] - 1.0
    pooled = m[m["stress"] >= stress_threshold].dropna(subset=["fwd"])
    pooled_stats = StressHitStats(
        stress_threshold,
        forward_days,
        float((pooled["fwd"] < 0).mean()) if len(pooled) else None,
        int(len(pooled)),
        "ok" if len(pooled) else "no_events",
    )

    by_regime: dict[str, Any] = {}
    for reg in regimes:
        sub = m[(m["stress"] >= stress_threshold) & (m["regime"] == reg)].dropna(subset=["fwd"])
        if sub.empty:
            by_regime[reg] = stress_to_dict(
                StressHitStats(stress_threshold, forward_days, None, 0, "no_events")
            )
        else:
            st = StressHitStats(
                stress_threshold,
                forward_days,
                float((sub["fwd"] < 0).mean()),
                int(len(sub)),
                "ok",
            )
            by_regime[reg] = stress_to_dict(st)

    return {
        "pooled": stress_to_dict(pooled_stats),
        "by_regime": by_regime,
        "detail": "ok",
    }


def move_vix_ratio_event_stub() -> dict[str, str]:
    """
    Placeholder for a full event-study: flag days where MOVE/VIX ratio z-score is extreme
    while VIX is muted, then measure SPY forward returns. Wire to `hist_close_daily` when
    you extend the sample construction.
    """
    return {
        "status": "stub",
        "note": "Implement joint z-score conditioning + forward SPY returns in a dedicated notebook or job.",
    }
