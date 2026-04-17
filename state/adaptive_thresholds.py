"""
Regime-conditional adaptive thresholds.

Walk-forward validation showed static thresholds fail in the current
SCARCE regime — they fire too often for MEDIUM (no signal) and too
rarely for CRITICAL (never hit). The fix: compute per-regime percentile
thresholds from historical data within each regime.

Methodology
-----------
For each (metric, regime) pair, take the subset of historical values
where the regime was active, then:
    MEDIUM   = 25th percentile     (noticeable stress FOR THIS REGIME)
    HIGH     = 10th percentile     (meaningful stress)
    CRITICAL =  5th percentile     (tail event)

For `reserves_bn`, `rrp_bn`, `net_liquidity_bn` the direction is
"below is worse", so lower percentiles = tighter thresholds.

This module is **observational only** at this stage. The live alerter
still uses static settings.py thresholds. The adaptive values are
written to data/raw/adaptive_thresholds.parquet and surfaced on the
dashboard so the user can judge whether to swap over.

Caveats
-------
1. Uses the ENTIRE 5y history including the regime we're currently in —
   mild look-ahead bias. A true out-of-sample adaptive scheme would use
   a trailing-only window. Deferred — current data volume is marginal
   for window-based estimation.
2. Small-N regimes (crisis has ~400 days but abundant only ~300): the
   5th-percentile estimate is noisier for the rarer regime.
3. The regime classifier itself is post-hoc (see state/regime.py) —
   errors in regime assignment propagate into threshold estimates.
"""
from __future__ import annotations

import hashlib
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from ..storage.parquet_store import ParquetStore


METRIC_COLS = {
    "reserves_bn":       "reserves",
    "rrp_bn":            "rrp",
    "net_liquidity_bn":  "net_liq",
}

REGIMES = ("abundant", "ample", "scarce", "crisis")

# Percentile per tier — lower percentile = tighter = less frequent firing
TIER_PERCENTILES = {
    "MEDIUM":   25,
    "HIGH":     10,
    "CRITICAL":  5,
}


def compute_adaptive_thresholds(
    regime_df: pd.DataFrame,
) -> dict[str, dict[str, dict[str, float | None]]]:
    """
    Returns nested dict:
        { "reserves": { "abundant": {"MEDIUM": x, "HIGH": y, "CRITICAL": z},
                        "ample":    {...},
                        "scarce":   {...},
                        "crisis":   {...} },
          "rrp":      { ... },
          "net_liq":  { ... } }

    If a (metric, regime) pair has < 20 samples, returns None for that
    pair — not enough data for stable percentiles.
    """
    if regime_df.empty:
        logger.warning("[adaptive] regime_df is empty")
        return {}

    result: dict[str, dict[str, dict[str, float | None]]] = {}

    for col, short_name in METRIC_COLS.items():
        result[short_name] = {}
        for regime in REGIMES:
            subset = regime_df.loc[regime_df["regime_hard"] == regime, col]
            subset = pd.to_numeric(subset, errors="coerce").dropna()
            if len(subset) < 20:
                result[short_name][regime] = {
                    tier: None for tier in TIER_PERCENTILES
                }
                continue
            tier_values: dict[str, float | None] = {}
            for tier, pct in TIER_PERCENTILES.items():
                tier_values[tier] = round(float(np.percentile(subset, pct)), 2)
            result[short_name][regime] = tier_values

    return result


def compute_and_store(store: ParquetStore) -> dict:
    """Read regime.parquet, compute adaptive thresholds, write result
    to data/raw/adaptive_thresholds.parquet. Returns the computed dict."""
    regime_df = store.read_all("regime")
    if regime_df.empty:
        logger.warning("[adaptive] no regime data yet — backfill regime first")
        return {}

    thresholds = compute_adaptive_thresholds(regime_df)
    if not thresholds:
        return {}

    # Flatten to a row-per-(metric, regime, tier) for parquet storage
    rows: list[dict[str, Any]] = []
    for metric, regime_map in thresholds.items():
        for regime, tier_map in regime_map.items():
            for tier, value in tier_map.items():
                rows.append({
                    "metric": metric,
                    "regime": regime,
                    "tier":   tier,
                    "value":  value,
                    "n_samples": int((regime_df["regime_hard"] == regime).sum()),
                })

    df = pd.DataFrame(rows)
    df["_hash"] = df.apply(
        lambda r: hashlib.md5(
            f"{r['metric']}:{r['regime']}:{r['tier']}:{r['value']}".encode()
        ).hexdigest(),
        axis=1,
    )
    out_path = store.data_dir / "raw" / "adaptive_thresholds.parquet"
    df.to_parquet(out_path, index=False)

    # Log a compact diff summary
    logger.info("[adaptive] computed thresholds:")
    for metric_short in thresholds:
        for regime in REGIMES:
            t = thresholds[metric_short][regime]
            if all(v is None for v in t.values()):
                continue
            logger.info(
                f"  {metric_short:>8s} / {regime:<8s} — "
                f"MED {t['MEDIUM']:.0f}  HIGH {t['HIGH']:.0f}  CRIT {t['CRITICAL']:.0f}"
            )

    return thresholds
