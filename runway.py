"""Liquidity runway — coarse narrative days until buffers look thin (not a trading model)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class RunwayEstimate:
    runway_days: float | None
    components: dict[str, Any]
    notes: list[str]


def estimate_liquidity_runway(
    *,
    rrp_bn: float | None,
    reserves_bn: float | None,
    rrp_avg_daily_inflow_bn: float | None,
    qra_daily_tga_gap_bn: float | None,
    reserves_red_bn: float,
    rrp_floor_bn: float = 50.0,
    runway_multiplier: float = 1.0,
) -> RunwayEstimate:
    """
    Heuristic only: compares RRP stock above a floor to an implied daily TGA/QRA gap,
    using recent RRP flow as a crude pace proxy when the gap outruns refill.

    Convention: pass rrp_avg_daily_change_bn (mean ΔRRP/day); negative means RRP draining.
    qra_daily_tga_gap_bn is positive when TGA must rise per day toward the QRA target.
    """
    notes: list[str] = []
    comp: dict[str, Any] = {}

    if rrp_bn is None:
        return RunwayEstimate(None, comp, ["RRP missing — cannot score runway."])

    cushion = max(0.0, float(rrp_bn) - rrp_floor_bn)
    comp["rrp_cushion_bn"] = round(cushion, 2)

    pace: float | None = None
    if rrp_avg_daily_inflow_bn is not None:
        # Convention: negative avg change => RRP balance falling => "drain" magnitude
        pace = max(1e-6, -float(rrp_avg_daily_inflow_bn))
        comp["rrp_drain_pace_bn_per_day"] = round(pace, 4)

    gap = max(0.0, float(qra_daily_tga_gap_bn or 0.0))
    comp["qra_implied_daily_tga_gap_bn"] = round(gap, 4)

    runway_rrp: float | None = None
    if pace and pace > 0:
        runway_rrp = cushion / pace
        comp["days_rrp_vs_drain"] = round(runway_rrp, 2)

    runway_gap: float | None = None
    if pace and pace > 0 and gap > 0:
        # If QRA gap exceeds what RRP drain pace can "fund", cushion erodes faster than steady-state refill.
        excess_pressure = max(0.0, gap - pace)
        if excess_pressure > 0:
            runway_gap = cushion / excess_pressure if excess_pressure else None
            comp["days_rrp_vs_gap_minus_pace"] = None if runway_gap is None else round(runway_gap, 2)
            notes.append("QRA-implied TGA build faster than recent RRP drain pace — watch reserves.")

    reserves_headroom: float | None = None
    if reserves_bn is not None:
        reserves_headroom = max(0.0, float(reserves_bn) - reserves_red_bn)
        comp["reserves_headroom_bn"] = round(reserves_headroom, 2)

    candidates = [x for x in (runway_rrp, runway_gap) if x is not None and x > 0]
    final = min(candidates) if candidates else None

    comp["runway_multiplier"] = round(runway_multiplier, 4)
    if final is not None and runway_multiplier > 0:
        final = float(final) * float(runway_multiplier)
        comp["runway_days_adjusted"] = round(final, 3)
        if runway_multiplier < 1.0:
            notes.append("Runway tightened by auction / dealer-stress overlay.")

    if final is not None and final < 10:
        notes.append("Runway < 10d under crude buffers — liquidity 'event risk' window tight.")

    return RunwayEstimate(runway_days=final, components=comp, notes=notes)


def runway_to_dict(r: RunwayEstimate) -> dict[str, Any]:
    return {
        "runway_days": r.runway_days,
        "components": r.components,
        "notes": r.notes,
    }
