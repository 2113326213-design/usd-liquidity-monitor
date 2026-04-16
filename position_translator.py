"""
Decision translation layer — map liquidity state → **actionable weight nudges** (scaffold).

Week-2 direction: wire real `portfolio` sleeves + Kelly cap from regime-conditional win rates.
This module stays pure-Python and side-effect free so it can run in the API or notebooks.
"""

from __future__ import annotations

from typing import Any


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def translate_liquidity_to_actions(
    *,
    stress_score: float | None,
    regime_hard: str | None,
    regime_probs: dict[str, float] | None,
    conditional_win_prob: dict[str, float | None] | None,
    lclor_gap_sessions: int | None = None,
    portfolio: list[dict[str, Any]] | None = None,
    max_beta_haircut_pct: float = 12.0,
    cash_floor_pct: float = 5.0,
) -> dict[str, Any]:
    """
    Return a **desk-readable** action vector (not executed trades).

    *conditional_win_prob*: optional map regime → P(SPY down | stress≥thr) from backtests.
    """
    st = float(stress_score) if stress_score is not None else 0.0
    rh = (regime_hard or "ample").lower()
    rp = regime_probs or {}
    cw = conditional_win_prob or {}

    # Stylised risk budget: higher stress + higher conditional hit rate in current regime → larger haircut
    w_hit = cw.get(rh)
    if w_hit is None:
        w_hit = 0.55
    w_hit = float(_clamp(float(w_hit), 0.35, 0.92))

    intensity = _clamp(st / 100.0, 0.0, 1.0) * (0.55 + 0.45 * w_hit)
    if lclor_gap_sessions is not None and lclor_gap_sessions <= 20:
        intensity = _clamp(intensity + 0.12, 0.0, 1.0)

    # Narrative sleeves (replace with your book: TTD, SPY put, cash, etc.)
    sleeves = portfolio or [
        {"name": "TTD_bills", "base_weight_pct": 15.0, "beta": 0.05},
        {"name": "core_equity", "base_weight_pct": 55.0, "beta": 1.0},
        {"name": "hedge_puts", "base_weight_pct": 3.0, "beta": -0.15},
        {"name": "cash_mm", "base_weight_pct": 20.0, "beta": 0.0},
    ]

    actions: list[dict[str, Any]] = []
    for h in sleeves:
        base = float(h.get("base_weight_pct", 0.0))
        beta = float(h.get("beta", 1.0))
        name = str(h.get("name", "sleeve"))
        haircut = intensity * max_beta_haircut_pct * abs(beta)
        if beta > 0.2:
            target = _clamp(base - haircut, 0.0, 100.0)
            delta = target - base
        elif beta < -0.05:
            target = _clamp(base + intensity * 4.0, 0.0, 25.0)
            delta = target - base
        else:
            target = _clamp(base + intensity * 6.0, cash_floor_pct, 100.0)
            delta = target - base
        actions.append(
            {
                "sleeve": name,
                "base_weight_pct": round(base, 2),
                "target_weight_pct": round(target, 2),
                "delta_pct": round(delta, 2),
                "beta_used": beta,
            }
        )

    return {
        "regime_hard": rh,
        "stress_score": stress_score,
        "intensity_0_1": round(intensity, 4),
        "conditional_win_prob_used": w_hit,
        "actions": actions,
        "note": "Scaffold — replace sleeves with your mandate list; add Kelly cap from regime_panel.",
    }
