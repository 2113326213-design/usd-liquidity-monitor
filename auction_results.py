"""
Treasury auction tails & bid-to-cover (Primary Dealer capacity proxy).

The Fiscal Data API surface for auction statistics changes; this module exposes a
stable interface and a **stub fetcher** you can aim at the authoritative dataset
once you pin the endpoint (often announced on fiscaldata.treasury.gov).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


@dataclass
class AuctionHighlight:
    record_date: str
    security_type: str
    bid_to_cover: float | None
    """Tail in basis points when high-yield vs when-issued is available."""
    tail_bp: float | None
    raw: dict[str, Any]


KNOWN_CANDIDATES: tuple[str, ...] = (
    # Placeholders — replace when Treasury publishes a stable JSON feed you trust.
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/debt/auction_query",
)


async def fetch_recent_auction_highlights(
    client: httpx.AsyncClient,
    *,
    limit: int = 5,
) -> list[AuctionHighlight]:
    """
    Attempts lightweight JSON pulls; returns [] until a working endpoint is configured.
    Wire your internal auction scraper here without touching the rest of the stack.
    """
    for url in KNOWN_CANDIDATES:
        try:
            r = await client.get(url, params={"page[size]": str(limit), "format": "json"}, timeout=20.0)
            if r.status_code >= 400:
                continue
            data = r.json()
            rows = data.get("data") or data.get("results") or []
            out: list[AuctionHighlight] = []
            for row in rows[:limit]:
                if not isinstance(row, dict):
                    continue
                out.append(
                    AuctionHighlight(
                        record_date=str(row.get("record_date") or row.get("auction_date") or ""),
                        security_type=str(row.get("security_type") or row.get("cusip") or "unknown"),
                        bid_to_cover=_f(row.get("bid_to_cover_ratio") or row.get("bid_to_cover")),
                        tail_bp=_f(row.get("tail_bp") or row.get("tail")),
                        raw=row,
                    )
                )
            if out:
                return out
        except Exception:
            continue
    return []


def _f(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def auction_liquidity_penalty(
    highlights: list[AuctionHighlight],
    *,
    bid_to_cover_weak: float = 2.3,
    tail_stress_bp: float = 1.0,
    stress_bump_points: float = 20.0,
    runway_tighten_factor: float = 0.85,
) -> dict[str, float]:
    """
    Non-linear feedback: weak bid-to-cover + meaningful tail ⇒ lift stress and
    compress runway multiplicatively (stylised dealer-capacity shock).
    """
    bump = 0.0
    mult = 1.0
    for h in highlights:
        btc = h.bid_to_cover
        tail = h.tail_bp
        if btc is not None and tail is not None and btc < bid_to_cover_weak and tail > tail_stress_bp:
            bump += stress_bump_points
            mult *= runway_tighten_factor
    bump = min(bump, 40.0)
    return {"stress_bump": float(bump), "runway_multiplier": float(mult)}


def summarize_for_alerts(
    highlights: list[AuctionHighlight],
    *,
    tail_alert_bp: float = 0.5,
    runway_days: float | None,
) -> list[str]:
    alerts: list[str] = []
    for h in highlights:
        if h.tail_bp is not None and h.tail_bp > tail_alert_bp:
            msg = f"Auction tail {h.tail_bp:.2f}bp on {h.security_type} ({h.record_date})"
            if runway_days is not None and runway_days < 14:
                msg += f" & runway {runway_days:.1f}d — dealer balance-sheet strain plausible."
            alerts.append(msg)
    return alerts
