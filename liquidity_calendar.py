"""US business-day calendar for ODE index → calendar date and drift metrics."""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

_US_CBD = CustomBusinessDay(calendar=USFederalHolidayCalendar())


def first_business_day_next_calendar_month(from_date: date) -> date:
    """First US business session on/after the 1st of the next calendar month (from *from_date*'s month)."""
    d = us_business_anchor(from_date)
    y, m = d.year, d.month
    if m == 12:
        nm = date(y + 1, 1, 1)
    else:
        nm = date(y, m + 1, 1)
    return us_business_anchor(nm)


def trading_sessions_from_anchor_to(anchor_calendar: date, target_calendar: date) -> float:
    """US business-day index of *target_calendar* relative to *anchor_calendar* as day-0 (0 = same session)."""
    a = us_business_anchor(anchor_calendar)
    t = us_business_anchor(target_calendar)
    if t < a:
        return 0.0
    arr = pd.bdate_range(start=a, end=t, freq=_US_CBD, inclusive="both")
    return float(max(0, len(arr) - 1))


def us_business_anchor(d: date) -> date:
    """Next or same US business session (Mon–Fri minus federal holidays)."""
    return _US_CBD.rollforward(pd.Timestamp(d)).normalize().date()


def forward_us_business_dates(anchor: date, periods: int) -> list[date]:
    """anchor = day index 0; returns [d0, d1, …] length *periods*."""
    p = max(1, int(periods))
    rng = pd.bdate_range(start=anchor, periods=p, freq=_US_CBD)
    return [x.date() for x in rng]


def trading_day_index_to_date(anchor: date, day_index: float) -> date | None:
    """Map ODE trading-day index (0 = anchor) to calendar date."""
    if day_index < 0:
        return None
    idx = int(math.floor(float(day_index)))
    seq = forward_us_business_dates(anchor, idx + 1)
    return seq[-1] if seq else None


def federal_bd_moved_earlier(old_breach: date, new_breach: date) -> int:
    """
    When the LCLoR breach date **moves earlier** (new_breach < old_breach), return an
    approximate count of US business sessions between the two dates (exclusive of
    old_breach, inclusive of new_breach side) for drift alerts.
    """
    if new_breach >= old_breach:
        return 0
    return len(
        pd.bdate_range(
            start=new_breach + timedelta(days=1),
            end=old_breach,
            freq=_US_CBD,
            inclusive="both",
        )
    )


def attach_projection_calendar(scenario: dict[str, Any], as_of: date) -> None:
    """Mutates a single `project_liquidity_path` dict with `*_expected_date` and `t_dates_iso`."""
    if not scenario or scenario.get("error"):
        return
    anchor = us_business_anchor(as_of)

    bd = scenario.get("lclor_breach_day")
    rf = scenario.get("rrp_hits_floor_day")
    if bd is not None:
        d = trading_day_index_to_date(anchor, float(bd))
        scenario["lclor_expected_date"] = d.isoformat() if d else None
    else:
        scenario["lclor_expected_date"] = None
    if rf is not None:
        d2 = trading_day_index_to_date(anchor, float(rf))
        scenario["rrp_floor_expected_date"] = d2.isoformat() if d2 else None
    else:
        scenario["rrp_floor_expected_date"] = None
    t_days = scenario.get("t_days") or []
    if t_days and not scenario.get("t_dates_iso"):
        dates: list[str | None] = []
        for td in t_days:
            dd = trading_day_index_to_date(anchor, float(td))
            dates.append(dd.isoformat() if dd else None)
        scenario["t_dates_iso"] = dates


def enrich_ode_runway_with_calendar(ode: dict[str, Any], as_of: date) -> dict[str, Any]:
    """
    Add ISO calendar dates for breach indices on each scenario.
    Mutates *ode* in place and returns it.
    """
    if not ode or ode.get("error") or "scenarios" not in ode:
        return ode

    anchor = us_business_anchor(as_of)
    ode["calendar_anchor_us_bd"] = anchor.isoformat()
    ode["calendar_note"] = "Indices are US business days from calendar_anchor_us_bd (day 0)."

    for _name, sc in (ode.get("scenarios") or {}).items():
        if not isinstance(sc, dict) or sc.get("error"):
            continue
        attach_projection_calendar(sc, as_of)
    return ode


def us_business_sessions_countdown(from_date: date, target: date | None) -> int | None:
    """US business sessions from anchor(from_date) to anchor(target), 0 if target on or before anchor."""
    if target is None:
        return None
    a = us_business_anchor(from_date)
    t = us_business_anchor(target)
    if t <= a:
        return 0
    arr = pd.bdate_range(start=a, end=t, freq=_US_CBD, inclusive="both")
    return max(0, len(arr) - 1)


def baseline_breach_dates_for_stability(ode: dict[str, Any]) -> tuple[date | None, date | None]:
    """(lclor_date, rrp_floor_date) from baseline scenario after calendar enrich."""
    base = (ode.get("scenarios") or {}).get("baseline") or {}
    ld = base.get("lclor_expected_date")
    rd = base.get("rrp_floor_expected_date")
    lcl = date.fromisoformat(ld) if isinstance(ld, str) and ld else None
    rrp = date.fromisoformat(rd) if isinstance(rd, str) and rd else None
    return lcl, rrp
