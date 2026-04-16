"""TGA / QRA dynamics, tax-season priors, and reserves shock scenarios."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from config import Settings


@dataclass
class TgaForecast:
    """All amounts billions USD unless noted."""

    anchor_bn: float | None
    calendar_adjustment_bn: float
    qra_target_bn: float | None
    scenario_bn: float | None
    notes: list[str]


_TAX_MONTHS = {4, 6, 9, 12}


def quarter_end_date(d: date) -> date:
    """Next quarter-end date on or after *d* (Mar/Jun/Sep/Dec)."""
    candidates = [
        date(d.year, 3, 31),
        date(d.year, 6, 30),
        date(d.year, 9, 30),
        date(d.year, 12, 31),
    ]
    for e in candidates:
        if e >= d:
            return e
    return date(d.year + 1, 3, 31)


def business_days_inclusive(start: date, end: date) -> int:
    """Count Mon–Fri days from *start* through *end* inclusive."""
    if end < start:
        return 0
    n = 0
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            n += 1
        cur += timedelta(days=1)
    return max(n, 1)


def qra_implied_daily_tga_gap_bn(
    *,
    qra_target_bn: float | None,
    current_tga_bn: float | None,
    as_of: date,
) -> dict[str, Any]:
    """
    ΔT_avg ≈ (T_target − T_current) / D  with D = business days to quarter-end.
    """
    out: dict[str, Any] = {
        "delta_t_avg_bn_per_day": None,
        "business_days_to_quarter_end": None,
        "quarter_end": None,
    }
    if qra_target_bn is None or current_tga_bn is None:
        return out
    qe = quarter_end_date(as_of)
    ddays = business_days_inclusive(as_of, qe)
    out["quarter_end"] = qe.isoformat()
    out["business_days_to_quarter_end"] = ddays
    gap = (float(qra_target_bn) - float(current_tga_bn)) / float(ddays)
    out["delta_t_avg_bn_per_day"] = round(gap, 4)
    return out


def tax_drain_multiplier(d: date, *, lead_days: int = 3) -> float:
    """
    Second-order tax window: a few days before / through 15th, reserves path is stressed.
    Returns a multiplier applied to **simulated** reserves (1.0 = no extra drain).
    """
    if d.month not in _TAX_MONTHS:
        return 1.0
    tax = date(d.year, d.month, 15)
    if tax - timedelta(days=lead_days) <= d <= tax + timedelta(days=1):
        return 0.90
    return 1.0


def reserves_scenario_bn(reserves_bn: float | None, mult: float) -> float | None:
    if reserves_bn is None:
        return None
    return round(float(reserves_bn) * mult, 3)


def _tax_season_bump(d: date, *, peak_bn: float, half_width_days: int = 7) -> float:
    if d.month not in _TAX_MONTHS:
        return 0.0
    anchor = date(d.year, d.month, 15)
    dist = abs((d - anchor).days)
    if dist > half_width_days:
        return 0.0
    w = 1.0 - dist / float(half_width_days)
    return peak_bn * max(0.0, w)


def build_tga_forecast(
    settings: Settings,
    as_of: date,
    latest_tga_bn: float | None,
    *,
    qra_target_override_bn: float | None = None,
) -> TgaForecast:
    notes: list[str] = []
    qra = qra_target_override_bn if qra_target_override_bn is not None else settings.qra_target_tga_bn
    if qra is not None:
        notes.append("QRA end-of-quarter cash target loaded from env (or PDF parser when enabled).")

    bump = _tax_season_bump(as_of, peak_bn=settings.tax_day_tga_peak_bump_bn)
    if bump > 0:
        notes.append(
            f"Tax-season prior: +{bump:.0f}B around month-day {as_of.month}/15 (scenario, not forecast)."
        )

    anchor = latest_tga_bn
    scenario = None
    if anchor is not None:
        scenario = anchor + bump
        if qra is not None:
            notes.append("Scenario vs QRA: compare `scenario_bn` to `qra_target_tga_bn` on the chart.")

    return TgaForecast(
        anchor_bn=anchor,
        calendar_adjustment_bn=bump,
        qra_target_bn=qra,
        scenario_bn=None if scenario is None else round(scenario, 3),
        notes=notes,
    )


def build_qra_water_stress(
    settings: Settings,
    *,
    as_of: date,
    current_tga_bn: float | None,
    rrp_avg_daily_change_bn: float | None,
    qra_target_override_bn: float | None = None,
) -> dict[str, Any]:
    """
    If ΔT_avg (TGA build) exceeds **recent average RRP refill** (here: −ΔRRP/day when RRP falls),
    flag that RRP may not fund the TGA path — reserves bear residual risk.
    """
    q_tgt = qra_target_override_bn if qra_target_override_bn is not None else settings.qra_target_tga_bn
    qra_gap = qra_implied_daily_tga_gap_bn(
        qra_target_bn=q_tgt,
        current_tga_bn=current_tga_bn,
        as_of=as_of,
    )
    delta = qra_gap.get("delta_t_avg_bn_per_day")
    notes: list[str] = []
    alarm = False
    rrp_supply = None
    if rrp_avg_daily_change_bn is not None:
        # Negative change => RRP draining => "supply" to system in bn/day
        rrp_supply = max(0.0, -float(rrp_avg_daily_change_bn))
    if delta is not None and delta > 0 and rrp_supply is not None:
        if delta > rrp_supply + settings.qra_rrp_tolerance_bn_per_day:
            alarm = True
            notes.append(
                "ΔT_avg exceeds recent RRP refill pace — TGA path may compress reserves faster than RRP buffer refills."
            )
    return {
        **qra_gap,
        "rrp_avg_daily_change_bn": rrp_avg_daily_change_bn,
        "rrp_implied_daily_supply_bn": None if rrp_supply is None else round(rrp_supply, 4),
        "qra_rrp_alarm": alarm,
        "notes": notes,
    }


def build_reserves_tax_path(
    reserves_bn: float | None,
    as_of: date,
    *,
    red_threshold_bn: float,
) -> dict[str, Any]:
    mult = tax_drain_multiplier(as_of)
    scen = reserves_scenario_bn(reserves_bn, mult)
    breach = scen is not None and scen < red_threshold_bn
    return {
        "tax_window_reserve_multiplier": mult,
        "reserves_bn": reserves_bn,
        "reserves_scenario_bn": scen,
        "breaches_red_under_scenario": breach,
    }


def forecast_to_dict(fc: TgaForecast) -> dict[str, Any]:
    return {
        "anchor_tga_bn": fc.anchor_bn,
        "calendar_adjustment_bn": fc.calendar_adjustment_bn,
        "qra_target_tga_bn": fc.qra_target_bn,
        "scenario_tga_bn": fc.scenario_bn,
        "notes": fc.notes,
    }


def tga_daily_rate_for_ode(
    qra_block: dict[str, Any],
    *,
    fallback_bn_per_day: float | None = None,
) -> float | None:
    """
    Prefer QRA-implied ΔT_avg (billions per day) for dT/dt in `runway_ode`.
    Returns None if no QRA path — caller should keep JSON `tga_target_rate_bn_per_day`.
    """
    v = qra_block.get("delta_t_avg_bn_per_day")
    if v is not None:
        return float(v)
    return fallback_bn_per_day
