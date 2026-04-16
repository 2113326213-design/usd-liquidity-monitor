"""Overlap checks between ODE breach dates and hand-maintained macro events."""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any


def _default_calendar_path() -> Path:
    return Path(__file__).resolve().parent / "macro_calendar.json"


def load_macro_events(path: Path | None = None) -> list[dict[str, Any]]:
    p = path or _default_calendar_path()
    if not p.is_file():
        return []
    raw = json.loads(p.read_text(encoding="utf-8"))
    ev = raw.get("events") or []
    return [e for e in ev if isinstance(e, dict) and e.get("date")]


def macro_resonance_scan(
    *,
    lclor_date: date | None,
    rrp_floor_date: date | None,
    window_days: int = 3,
    calendar_path: Path | None = None,
) -> dict[str, Any]:
    """
    If model breach dates fall within ±*window_days* of a listed FOMC / QRA session,
    emit structured warnings (non-fatal desk alerts).
    """
    events = load_macro_events(calendar_path)
    win = max(0, int(window_days))
    matches: list[dict[str, Any]] = []

    def _scan(label: str, d: date | None) -> None:
        if d is None:
            return
        for ev in events:
            try:
                ed = date.fromisoformat(str(ev["date"]))
            except ValueError:
                continue
            if abs((d - ed).days) <= win:
                matches.append(
                    {
                        "ode_anchor": label,
                        "ode_date": d.isoformat(),
                        "event_date": ed.isoformat(),
                        "event_type": ev.get("type"),
                        "event_label": ev.get("label"),
                        "gap_calendar_days": (d - ed).days,
                    }
                )

    _scan("lclor_expected", lclor_date)
    _scan("rrp_floor_expected", rrp_floor_date)

    warnings = [
        (
            f"RESONANCE_WARNING: ODE {m['ode_anchor']} ({m['ode_date']}) within {win}d of "
            f"{m.get('event_type')} {m.get('event_label')} ({m['event_date']})."
        )
        for m in matches
    ]
    return {"matches": matches, "warning_messages": warnings, "window_days": win}


def next_macro_event_after(as_of: date, *, calendar_path: Path | None = None) -> dict[str, Any] | None:
    """Optional helper: soonest calendar event on/after *as_of*."""
    events = load_macro_events(calendar_path)
    best: tuple[date, dict[str, Any]] | None = None
    for ev in events:
        try:
            ed = date.fromisoformat(str(ev["date"]))
        except ValueError:
            continue
        if ed < as_of:
            continue
        if best is None or ed < best[0]:
            best = (ed, ev)
    if best is None:
        return None
    return {"date": best[0].isoformat(), **{k: v for k, v in best[1].items() if k != "date"}}
