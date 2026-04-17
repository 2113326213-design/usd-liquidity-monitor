"""
Liquidity regime classifier — soft assignment over 4 states.

Method
------
Reserves and ON RRP are rolling-rank normalized over a 252-day window.
The resulting (reserves_rank, rrp_rank) point is compared to four
fixed prototypes in [0,1]² feature space:

    abundant: (0.88, 0.82)   — post-2021 QE peak regime
    ample:    (0.58, 0.52)   — Fed's current operating target
    scarce:   (0.38, 0.32)   — approaching 2019 repo-crisis conditions
    crisis:   (0.12, 0.12)   — active dislocation

Softmax over negative squared distances (with a temperature parameter)
gives probabilistic class membership, plus a single hard-assigned label.

Carried over from _archive/regime_detection.py (originally written for
the now-removed service.py orchestrator). Rewired to fit the
Collector / store event pattern so main.py can subscribe.

Why it matters
--------------
Walk-forward backtest showed the system's tiered thresholds (MEDIUM /
HIGH / CRITICAL) don't have uniform signal across regimes — in ample
regime they fire noisily; in crisis regime they fire late. A regime
layer is the prerequisite for making thresholds *regime-conditional*
rather than fixed. This module just produces the regime signal; the
downstream consumers (alert logic, dashboard) are separate work.
"""
from __future__ import annotations

import hashlib
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from ..storage.parquet_store import ParquetStore


REGIME_ORDER = ("abundant", "ample", "scarce", "crisis")

_PROTOTYPES = np.array(
    [
        [0.88, 0.82],  # abundant
        [0.58, 0.52],  # ample
        [0.38, 0.32],  # scarce
        [0.12, 0.12],  # crisis
    ],
    dtype=float,
)


def _rolling_rank_norm(s: pd.Series, window: int = 252, min_periods: int = 40) -> pd.Series:
    """Per-row rolling percentile rank — proportion of prior values (in the
    trailing window) that are <= the current value. Produces a [0, 1] time
    series that's regime-invariant: a value that's "high for this era" gets
    rank close to 1 regardless of absolute scale."""
    if s.empty:
        return s
    w = max(min_periods, min(window, max(20, len(s) // 2)))

    def last_rank(x: pd.Series) -> float:
        if len(x) < 5 or x.isna().all():
            return float("nan")
        v = float(x.iloc[-1])
        arr = x.dropna().to_numpy(dtype=float)
        if arr.size == 0 or not np.isfinite(v):
            return float("nan")
        return float((arr <= v).mean())

    return s.rolling(w, min_periods=min(20, w // 2)).apply(last_rank, raw=False)


def infer_regime_probabilities_rule(
    df: pd.DataFrame,
    *,
    res_col: str = "reserves_bn",
    rrp_col: str = "rrp_bn",
    stress_col: str | None = None,
    temperature: float = 0.18,
) -> pd.DataFrame:
    """Add regime probability columns to *df*.

    Adds: p_abundant, p_ample, p_scarce, p_crisis, regime_hard,
    reserves_rank, rrp_rank. Input must be daily-indexed ascending.
    """
    out = df.copy()
    if res_col not in out.columns or rrp_col not in out.columns:
        logger.warning(f"[regime] missing columns {res_col} / {rrp_col}")
        return pd.DataFrame()

    rn = _rolling_rank_norm(out[res_col].astype(float))
    rrpn = _rolling_rank_norm(out[rrp_col].astype(float))
    out["reserves_rank"] = rn
    out["rrp_rank"] = rrpn

    stress_available = stress_col and stress_col in out.columns
    if stress_available:
        out["_sn"] = _rolling_rank_norm(out[stress_col].astype(float))
    else:
        out["_sn"] = np.nan

    probs = np.zeros((len(out), 4), dtype=float)
    for i in range(len(out)):
        x = float(rn.iloc[i]) if np.isfinite(rn.iloc[i]) else 0.5
        y = float(rrpn.iloc[i]) if np.isfinite(rrpn.iloc[i]) else 0.5
        z = float(out["_sn"].iloc[i]) if np.isfinite(out["_sn"].iloc[i]) else 0.0
        pt = np.array([x, y], dtype=float)
        d2 = np.sum((pt - _PROTOTYPES) ** 2, axis=1)
        # Crisis attractor strengthens when stress rank is high
        if z > 0.65:
            d2[3] *= 0.55
        elif z < 0.25 and z != 0.0:
            d2[3] *= 1.35
        logits = -d2 / max(1e-6, float(temperature))
        e = np.exp(logits - np.max(logits))
        probs[i, :] = e / e.sum()

    for j, name in enumerate(REGIME_ORDER):
        out[f"p_{name}"] = probs[:, j]
    out["regime_hard"] = [REGIME_ORDER[int(np.argmax(probs[k]))] for k in range(len(out))]

    return out.drop(columns=["_sn"] if "_sn" in out.columns else [])


class RegimeTracker:
    """Event subscriber: recomputes regime probabilities whenever reserves
    or RRP update, and writes the latest snapshot to data/raw/regime.parquet.

    The dashboard reads this file to show the current regime badge plus
    (future work) a regime band behind the Net Liquidity chart."""

    NAME = "regime"

    def __init__(self, store: ParquetStore) -> None:
        self.store = store

    async def recompute(self, _payload: dict | None = None) -> None:
        nl = self.store.read_all("net_liquidity")
        if len(nl) < 40:
            logger.debug("[regime] < 40 net_liquidity rows — wait for more data")
            return

        nl = nl.sort_values("as_of").reset_index(drop=True).copy()
        nl["reserves_bn"] = nl["reserves_bn"].astype(float)
        nl["rrp_bn"] = nl["rrp_bn"].astype(float)

        enriched = infer_regime_probabilities_rule(nl)
        if enriched.empty:
            return
        latest = enriched.iloc[-1]

        payload: dict[str, Any] = {
            "as_of": str(latest["as_of"]),
            "reserves_bn": float(latest["reserves_bn"]),
            "rrp_bn": float(latest["rrp_bn"]),
            "net_liquidity_bn": float(latest["net_liquidity_bn"]),
            "reserves_rank": (
                None if pd.isna(latest["reserves_rank"]) else round(float(latest["reserves_rank"]), 4)
            ),
            "rrp_rank": (
                None if pd.isna(latest["rrp_rank"]) else round(float(latest["rrp_rank"]), 4)
            ),
            "p_abundant": round(float(latest["p_abundant"]), 4),
            "p_ample": round(float(latest["p_ample"]), 4),
            "p_scarce": round(float(latest["p_scarce"]), 4),
            "p_crisis": round(float(latest["p_crisis"]), 4),
            "regime_hard": str(latest["regime_hard"]),
        }

        h = hashlib.md5(
            f"{payload['as_of']}:{payload['regime_hard']}:"
            f"{payload['p_crisis']:.4f}".encode()
        ).hexdigest()

        last_h = self.store.last_hash(self.NAME)
        if last_h == h:
            return  # no change

        self.store.write_snapshot(self.NAME, payload, h)
        logger.info(
            f"[regime] {payload['as_of']} → {payload['regime_hard']} "
            f"(abundant={payload['p_abundant']:.2f} "
            f"ample={payload['p_ample']:.2f} "
            f"scarce={payload['p_scarce']:.2f} "
            f"crisis={payload['p_crisis']:.2f})"
        )


def backfill_history(store: ParquetStore) -> int:
    """One-shot — compute regime for every day in the existing
    net_liquidity history. Writes the full series to
    data/raw/regime.parquet (overwriting, not appending). Lets the
    dashboard show the historical regime sequence."""
    nl = store.read_all("net_liquidity")
    if nl.empty:
        logger.warning("[regime] no net_liquidity data to backfill from")
        return 0

    nl = nl.sort_values("as_of").reset_index(drop=True).copy()
    enriched = infer_regime_probabilities_rule(nl)
    if enriched.empty:
        return 0

    out_cols = [
        "as_of", "reserves_bn", "rrp_bn", "net_liquidity_bn",
        "reserves_rank", "rrp_rank",
        "p_abundant", "p_ample", "p_scarce", "p_crisis",
        "regime_hard",
    ]
    out = enriched[out_cols].copy()
    out["_hash"] = [
        hashlib.md5(
            f"{row['as_of']}:{row['regime_hard']}:{row['p_crisis']:.4f}".encode()
        ).hexdigest()
        for _, row in out.iterrows()
    ]
    out.to_parquet(store.data_dir / "raw" / "regime.parquet", index=False)
    logger.info(
        f"[regime] backfilled {len(out)} rows, "
        f"{out['as_of'].iloc[0]} → {out['as_of'].iloc[-1]}"
    )
    return len(out)
