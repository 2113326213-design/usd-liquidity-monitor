"""
Hard-bound sanity checks for incoming data.

Each collector calls `sanity_check(name, value)` at the top of its
`on_new_data` to guard against blatantly bad data from upstream APIs
(e.g., Fiscal Data occasionally returns 0 for TGA during maintenance,
FRED has returned `.` for missing values, Yahoo's yfinance sometimes
returns NaN prices during pre-market). Without this guard a single
garbage value would crash Net Liquidity into implausible territory
and fire a flurry of false alerts.

These bounds are **hard plausibility bounds**, not statistical ones.
Values outside = near-certain data bug, not a market event:
  TGA      50  – 2,000 bn    (peaked ~1.8T 2021, floored ~50bn at debt ceiling)
  RRP       0  – 3,000 bn    (peaked ~2.5T Dec 2022)
  Reserves 500 – 10,000 bn   (plenty of headroom either side)
  Net Liq -500 – 8,000 bn    (reserves + rrp − tga can go slightly negative
                              during extreme stress; cap generous)
  Stress z  -20 – 20         (composite z > 10 implies >10σ event = bug)
  Auction  -50  – 50 bp      (historical extreme +21 bp 2011, −23 bp)

Returning False tells the caller to skip alerting + skip the parquet
write for this payload.
"""
from __future__ import annotations

from loguru import logger


# (min, max) hard bounds, inclusive on min, exclusive on max.
_BOUNDS: dict[str, tuple[float, float]] = {
    "tga_bn":             (50.0,    2_000.0),
    "rrp_bn":             (0.0,     3_000.0),
    "srp_bn":             (0.0,     500.0),  # SRP never near historical peak
    "reserves_bn":        (500.0,   10_000.0),
    "net_liquidity_bn":   (-500.0,  8_000.0),
    "composite_stress_z": (-20.0,   20.0),
    "tail_bp":            (-50.0,   50.0),
}


def sanity_check(name: str, value: float | int | None) -> bool:
    """Return True if value is within hard plausibility bounds.

    Logs an ERROR (not WARNING) on failure — this should never happen in
    normal operation, and when it does happen we want it loud in the
    launchd stderr log."""
    if value is None:
        logger.error(f"[sanity] {name} is None — upstream returned no data")
        return False
    try:
        v = float(value)
    except (TypeError, ValueError):
        logger.error(f"[sanity] {name} is non-numeric: {value!r}")
        return False
    bounds = _BOUNDS.get(name)
    if bounds is None:
        # Unknown metric — be permissive but warn once
        logger.warning(f"[sanity] no bounds registered for '{name}', allowing {v}")
        return True
    lo, hi = bounds
    if not (lo <= v <= hi):
        logger.error(
            f"[sanity] {name}={v} outside plausibility bounds [{lo}, {hi}] — "
            f"likely upstream data bug, skipping alert + write"
        )
        return False
    return True
