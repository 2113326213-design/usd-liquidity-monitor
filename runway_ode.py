"""
Liquidity ODE — (Reserves R, RRP P, TGA T) with QT + QRA TGA pace + RRP floor non-linearity.

Optional **US business-day calendar** + **tax-window TGA pulses** (piecewise constant dT/dt multiplier).
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any, Callable

import numpy as np

try:
    from scipy.integrate import solve_ivp
except ImportError:  # pragma: no cover
    solve_ivp = None  # type: ignore[misc, assignment]


@dataclass
class FedScenario:
    qt_pace_bn_per_month: float
    """β when P > rrp_floor_bn; else 0."""
    rrp_absorption_ratio: float
    lclor_bn: float
    tga_target_rate_bn_per_day: float
    currency_drift_bn_per_month: float
    rrp_floor_bn: float = 50.0
    # Piecewise-constant QT pace ($B/month): (start_day_index, pace). Last segment with start≤t wins.
    qt_schedule: tuple[tuple[float, float], ...] | None = None


def load_scenario_from_disk(path: Path | None = None) -> FedScenario:
    p = path or Path(__file__).resolve().parent / "model_parameters.json"
    raw = json.loads(p.read_text(encoding="utf-8"))
    return FedScenario(
        qt_pace_bn_per_month=float(raw.get("qt_pace_bn_per_month", 60)),
        rrp_absorption_ratio=float(raw.get("rrp_absorption_ratio", 0.78)),
        lclor_bn=float(raw.get("lclor_bn", 3000)),
        tga_target_rate_bn_per_day=float(raw.get("tga_target_rate_bn_per_day", 2.0)),
        currency_drift_bn_per_month=float(raw.get("currency_drift_bn_per_month", 5)),
        rrp_floor_bn=float(raw.get("rrp_floor_bn", 50)),
        qt_schedule=None,
    )


def scenario_with_tga_rate(base: FedScenario, tga_rate_bn_per_day: float | None) -> FedScenario:
    if tga_rate_bn_per_day is None:
        return base
    return replace(base, tga_target_rate_bn_per_day=float(tga_rate_bn_per_day))


def _effective_qt_monthly_bn(t: float, scen: FedScenario) -> float:
    if not scen.qt_schedule:
        return float(scen.qt_pace_bn_per_month)
    pace = float(scen.qt_pace_bn_per_month)
    for start, p in sorted(scen.qt_schedule, key=lambda x: x[0]):
        if float(t) + 1e-9 >= float(start):
            pace = float(p)
    return pace


def tax_tga_pulse_multiplier(d: date) -> float:
    """
    Stylised **discrete shock**: stronger TGA build (more drain on R/RRP channel)
    around estimated corporate / quarterly tax clusters (Apr/Jun/Sep/Dec mid-month).
    """
    if d.month in (4, 6, 9, 12) and 10 <= d.day <= 18:
        return 5.0
    return 1.0


def _liquidity_rhs(
    t: float,
    y: np.ndarray,
    scen: FedScenario,
    *,
    bd_lookup: Callable[[float], date] | None = None,
    use_tax_pulses: bool,
) -> np.ndarray:
    _r, p, _tg = float(y[0]), float(y[1]), float(y[2])

    qt_monthly = _effective_qt_monthly_bn(t, scen)
    qt_per_day = qt_monthly / 21.0
    d_adt = -qt_per_day

    pulse = 1.0
    if bd_lookup is not None and use_tax_pulses:
        pulse = tax_tga_pulse_multiplier(bd_lookup(t))

    d_tdt = float(scen.tga_target_rate_bn_per_day) * pulse

    beta = float(scen.rrp_absorption_ratio) if p > scen.rrp_floor_bn else 0.0
    if p <= 1e-6:
        d_pdt = 0.0
    else:
        d_pdt = -d_tdt * beta
        if p + d_pdt < 0:
            d_pdt = -p

    d_c_dt = scen.currency_drift_bn_per_month / 21.0

    d_rdt = d_adt - (d_tdt + d_pdt + d_c_dt)
    return np.array([d_rdt, d_pdt, d_tdt], dtype=float)


def _make_bd_lookup(bd_dates: list[date]) -> Callable[[float], date]:
    n = len(bd_dates)

    def lookup(t: float) -> date:
        i = min(max(0, int(math.floor(t))), n - 1)
        return bd_dates[i]

    return lookup


def project_liquidity_path(
    *,
    initial: tuple[float, float, float],
    horizon_days: int = 90,
    scenario: FedScenario | None = None,
    as_of: date | None = None,
    use_tax_pulses: bool = True,
) -> dict[str, Any]:
    """Single-scenario projection. If *as_of* is set, RHS uses US-BD calendar + tax pulses."""
    if solve_ivp is None:
        return {"error": "scipy is not installed", "t_days": [], "reserves": []}

    scen = scenario or load_scenario_from_disk()
    y0 = np.array(initial, dtype=float)
    t_span = (0.0, float(max(horizon_days, 1)))
    t_eval = np.linspace(0.0, float(horizon_days), int(horizon_days) + 1)

    bd_lookup: Callable[[float], date] | None = None
    if as_of is not None:
        from liquidity_calendar import forward_us_business_dates, us_business_anchor

        anchor = us_business_anchor(as_of)
        bd_dates = forward_us_business_dates(anchor, int(horizon_days) + 2)
        bd_lookup = _make_bd_lookup(bd_dates)

    sol = solve_ivp(
        lambda tau, y: _liquidity_rhs(
            tau,
            y,
            scen,
            bd_lookup=bd_lookup,
            use_tax_pulses=use_tax_pulses and as_of is not None,
        ),
        t_span,
        y0,
        t_eval=t_eval,
        method="RK45",
        rtol=1e-6,
        atol=1e-6,
        max_step=1.0,
    )
    if not sol.success:
        return {"error": sol.message, "t_days": [], "reserves": []}

    rs = sol.y[0]
    breach = _first_breach_day(t_eval, rs, scen.lclor_bn)
    rrp_exhaust = _first_breach_day(t_eval, sol.y[1], scen.rrp_floor_bn)

    out: dict[str, Any] = {
        "label": "baseline",
        "t_days": t_eval.tolist(),
        "reserves": rs.tolist(),
        "rrp": sol.y[1].tolist(),
        "tga": sol.y[2].tolist(),
        "lclor_bn": scen.lclor_bn,
        "lclor_breach_day": breach,
        "rrp_floor_bn": scen.rrp_floor_bn,
        "rrp_hits_floor_day": rrp_exhaust,
        "ode_as_of": as_of.isoformat() if as_of else None,
        "tax_pulses_enabled": bool(as_of and use_tax_pulses),
        "scenario": {
            "qt_pace_bn_per_month": scen.qt_pace_bn_per_month,
            "qt_schedule": scen.qt_schedule,
            "rrp_absorption_ratio": scen.rrp_absorption_ratio,
            "tga_target_rate_bn_per_day": scen.tga_target_rate_bn_per_day,
            "rrp_floor_bn": scen.rrp_floor_bn,
        },
    }
    return out


def _first_breach_day(t_eval: np.ndarray, series: np.ndarray, threshold: float) -> float | None:
    for i, v in enumerate(series):
        if float(v) < threshold:
            return float(t_eval[i])
    return None


def project_scenario_bundle(
    *,
    initial: tuple[float, float, float],
    base: FedScenario,
    horizon_days: int = 120,
    as_of: date | None = None,
    use_tax_pulses: bool = True,
) -> dict[str, Any]:
    """
    Three stylised paths:
      - optimistic: slower TGA build, slightly higher RRP β
      - baseline:   as in `base`
      - pessimistic:  faster TGA build, lower β
    """
    h = max(10, min(int(horizon_days), 365))
    opt = replace(
        base,
        qt_schedule=None,
        tga_target_rate_bn_per_day=max(0.0, base.tga_target_rate_bn_per_day * 0.75),
        rrp_absorption_ratio=min(0.97, base.rrp_absorption_ratio + 0.08),
    )
    pes = replace(
        base,
        qt_schedule=None,
        tga_target_rate_bn_per_day=max(0.0, base.tga_target_rate_bn_per_day * 1.25),
        rrp_absorption_ratio=max(0.0, base.rrp_absorption_ratio - 0.18),
    )

    kw: dict[str, Any] = {"as_of": as_of, "use_tax_pulses": use_tax_pulses}
    out: dict[str, Any] = {
        "initial_rpt": list(initial),
        "horizon_days": h,
        "ode_as_of": as_of.isoformat() if as_of else None,
        "tax_pulses_enabled": bool(as_of and use_tax_pulses),
        "scenarios": {
            "optimistic": project_liquidity_path(initial=initial, horizon_days=h, scenario=opt, **kw),
            "baseline": project_liquidity_path(initial=initial, horizon_days=h, scenario=base, **kw),
            "pessimistic": project_liquidity_path(initial=initial, horizon_days=h, scenario=pes, **kw),
        },
    }
    return out


def project_qt_policy_stress_comparison(
    *,
    initial: tuple[float, float, float],
    base: FedScenario,
    horizon_days: int,
    as_of: date,
    use_tax_pulses: bool,
    dove_switch_day_index: float,
    dove_qt_bn_per_month: float = 30.0,
    hawk_qt_bn_per_month: float = 75.0,
) -> dict[str, Any]:
    """
    Fed **QT tapering lab** (same TGA path & β as *base*):
      - qt_baseline: constant pace from `base.qt_pace_bn_per_month`
      - qt_dove_taper_next_month: pace drops to *dove_qt* from `dove_switch_day_index` onward
      - qt_hawk_no_relief: higher constant pace (hawk / no-dovish-pivot stress)
    """
    h = max(10, min(int(horizon_days), 365))
    q0 = float(base.qt_pace_bn_per_month)
    baseline_scen = replace(base, qt_schedule=None)
    dove_scen = replace(
        base,
        qt_schedule=(
            (0.0, q0),
            (max(0.0, float(dove_switch_day_index)), float(dove_qt_bn_per_month)),
        ),
    )
    hawk_scen = replace(
        base,
        qt_schedule=None,
        qt_pace_bn_per_month=float(hawk_qt_bn_per_month),
    )
    kw: dict[str, Any] = {"as_of": as_of, "use_tax_pulses": use_tax_pulses}
    return {
        "horizon_days": h,
        "ode_as_of": as_of.isoformat(),
        "dove_switch_day_index": float(dove_switch_day_index),
        "dove_qt_bn_per_month": float(dove_qt_bn_per_month),
        "hawk_qt_bn_per_month": float(hawk_qt_bn_per_month),
        "paths": {
            "qt_baseline": project_liquidity_path(
                initial=initial, horizon_days=h, scenario=baseline_scen, **kw
            ),
            "qt_dove_taper_next_month": project_liquidity_path(
                initial=initial, horizon_days=h, scenario=dove_scen, **kw
            ),
            "qt_hawk_no_relief": project_liquidity_path(
                initial=initial, horizon_days=h, scenario=hawk_scen, **kw
            ),
        },
        "labels": {
            "qt_baseline": "Baseline QT (constant pace from model)",
            "qt_dove_taper_next_month": "Dovish taper: pace falls to dove_qt from next-month 1st US business session index",
            "qt_hawk_no_relief": "Hawkish stress: higher constant QT pace (no Fed relief)",
        },
    }


class LiquidityODE:
    """Thin wrapper around `FedScenario` + `solve_ivp` for reuse in API / notebooks."""

    def __init__(
        self,
        reserves_bn: float,
        rrp_bn: float,
        tga_bn: float,
        scenario: FedScenario | None = None,
    ) -> None:
        self.y0 = np.array([reserves_bn, rrp_bn, tga_bn], dtype=float)
        self.scenario = scenario or load_scenario_from_disk()

    def rhs(self, t: float, y: np.ndarray) -> np.ndarray:
        return _liquidity_rhs(t, y, self.scenario, bd_lookup=None, use_tax_pulses=False)

    def solve(self, horizon_days: int = 90, *, as_of: date | None = None) -> dict[str, Any]:
        return project_liquidity_path(
            initial=(float(self.y0[0]), float(self.y0[1]), float(self.y0[2])),
            horizon_days=horizon_days,
            scenario=self.scenario,
            as_of=as_of,
            use_tax_pulses=as_of is not None,
        )
