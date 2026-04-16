"""Polygon / Massive REST: minute bars (stocks) + daily Treasury constant-maturity yields (Fed feed)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx


@dataclass
class PolygonMinuteBar:
    as_of_utc: str
    ticker: str
    close: float
    volume: float
    source: str


@dataclass
class PolygonTreasuryYields:
    as_of_date: str
    yield_1_month_pct: float | None
    yield_3_month_pct: float | None
    source: str


async def fetch_last_minute_bar(
    client: httpx.AsyncClient,
    *,
    api_key: str,
    ticker: str,
    rest_base: str = "https://api.polygon.io",
) -> PolygonMinuteBar | None:
    """Latest 1-minute aggregate (typically needs Polygon paid plan for minute history)."""
    if not api_key or not ticker:
        return None
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=2)
    ms = lambda d: int(d.timestamp() * 1000)
    url = f"{rest_base.rstrip('/')}/v2/aggs/ticker/{ticker}/range/1/minute/{ms(start)}/{ms(end)}"
    r = await client.get(
        url,
        params={"adjusted": "true", "sort": "desc", "limit": 1, "apiKey": api_key},
        timeout=30.0,
    )
    if r.status_code >= 400:
        return None
    data = r.json()
    results = data.get("results") or []
    if not results:
        return None
    bar = results[0]
    t = int(bar.get("t", 0)) / 1000.0
    ts = datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
    return PolygonMinuteBar(
        as_of_utc=ts,
        ticker=ticker,
        close=float(bar.get("c", 0)),
        volume=float(bar.get("v", 0)),
        source=f"{rest_base}/v2/aggs",
    )


async def fetch_recent_minute_bars(
    client: httpx.AsyncClient,
    *,
    api_key: str,
    ticker: str,
    rest_base: str = "https://api.polygon.io",
    limit: int = 120,
) -> tuple[list[float], list[float]]:
    """Returns (closes, volumes) oldest → newest for rolling volume stats."""
    if not api_key or not ticker:
        return [], []
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=3)
    ms = lambda d: int(d.timestamp() * 1000)
    url = f"{rest_base.rstrip('/')}/v2/aggs/ticker/{ticker}/range/1/minute/{ms(start)}/{ms(end)}"
    r = await client.get(
        url,
        params={
            "adjusted": "true",
            "sort": "asc",
            "limit": min(limit, 5000),
            "apiKey": api_key,
        },
        timeout=45.0,
    )
    if r.status_code >= 400:
        return [], []
    results = r.json().get("results") or []
    closes: list[float] = []
    vols: list[float] = []
    for bar in results:
        closes.append(float(bar.get("c", 0)))
        vols.append(float(bar.get("v", 0)))
    return closes, vols


async def fetch_latest_treasury_yields(
    client: httpx.AsyncClient,
    *,
    api_key: str,
    fed_base: str = "https://api.massive.com",
) -> PolygonTreasuryYields | None:
    """
    Daily constant-maturity Treasury yields (1M / 3M).
    Hosted on Massive (`/fed/v1/...`); same API key as Polygon often works after their rebrand.
    """
    if not api_key:
        return None
    url = f"{fed_base.rstrip('/')}/fed/v1/treasury-yields"
    r = await client.get(
        url,
        params={"limit": 1, "sort": "date.desc", "apiKey": api_key},
        timeout=30.0,
    )
    if r.status_code >= 400:
        return None
    payload: dict[str, Any] = r.json()
    rows = payload.get("results") or []
    if not rows:
        return None
    row = rows[0]
    d = str(row.get("date") or "")
    return PolygonTreasuryYields(
        as_of_date=d,
        yield_1_month_pct=_f(row.get("yield_1_month")),
        yield_3_month_pct=_f(row.get("yield_3_month")),
        source=f"{fed_base}/fed/v1/treasury-yields",
    )


def _f(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
