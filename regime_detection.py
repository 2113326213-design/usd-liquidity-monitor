"""
Liquidity **regime** layer — soft assignment over 4 stylised states.

MVP uses **rolling-normalised features** (reserves, RRP, optional stress) + Gaussian
kernels over fixed prototypes. This is deliberately dependency-light (no hmmlearn
required). Swap `infer_regime_probabilities_hmm` when you wire `hmmlearn` + a
longer feature matrix offline.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

REGIME_ORDER = ("abundant", "ample", "scarce", "crisis")

# Prototypes in [0,1] feature space: (res_norm, rrp_norm) — tuned for desk intuition, not econometric fit.
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
    """Per-day percentile rank of *s* in trailing *window* (0..1)."""
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
    stress_col: str | None = "stress_score",
    temperature: float = 0.18,
) -> pd.DataFrame:
    """
    Returns columns `p_abundant`, `p_ample`, `p_scarce`, `p_crisis`, `regime_hard`.

    *df* must be daily-indexed ascending with reserves & RRP (billions).
    """
    out = df.copy()
    if res_col not in out.columns or rrp_col not in out.columns:
        return pd.DataFrame()

    rn = _rolling_rank_norm(out[res_col].astype(float))
    rrpn = _rolling_rank_norm(out[rrp_col].astype(float))
    out["_rn"] = rn
    out["_rrpn"] = rrpn

    if stress_col and stress_col in out.columns:
        sn = _rolling_rank_norm(out[stress_col].astype(float))
        # Pull crisis corner when stress rank is high (2D → 3D collapse by max distance to crisis)
        out["_sn"] = sn
    else:
        out["_sn"] = np.nan

    probs = np.zeros((len(out), 4), dtype=float)
    for i in range(len(out)):
        x = float(out["_rn"].iloc[i]) if np.isfinite(out["_rn"].iloc[i]) else 0.5
        y = float(out["_rrpn"].iloc[i]) if np.isfinite(out["_rrpn"].iloc[i]) else 0.5
        z = float(out["_sn"].iloc[i]) if np.isfinite(out["_sn"].iloc[i]) else 0.0
        pt = np.array([x, y], dtype=float)
        d2 = np.sum((pt - _PROTOTYPES) ** 2, axis=1)
        # Crisis attractor strengthens if stress rank is high
        if z > 0.65:
            d2[3] *= 0.55
        elif z < 0.25:
            d2[3] *= 1.35
        logits = -d2 / max(1e-6, float(temperature))
        e = np.exp(logits - np.max(logits))
        probs[i, :] = e / e.sum()

    for j, name in enumerate(REGIME_ORDER):
        out[f"p_{name}"] = probs[:, j]
    out["regime_hard"] = [REGIME_ORDER[int(np.argmax(probs[k]))] for k in range(len(out))]
    return out.drop(columns=[c for c in ("_rn", "_rrpn", "_sn") if c in out.columns])


def infer_regime_probabilities_hmm(_df: pd.DataFrame) -> dict[str, Any]:
    """Placeholder for hmmlearn.GaussianHMM — train offline, serialize params, load here."""
    return {
        "status": "not_configured",
        "note": "Install hmmlearn, fit 4-state HMM on [Reserves/GDP, SRF, SOFR-IORB, RRP, PD inventory], "
        "then replace infer_regime_probabilities_rule in the regime panel job.",
    }
