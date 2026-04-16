"""Shadow market block: yfinance fallback + optional Polygon minute bars / Treasury yields."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx
import yfinance as yf

from config import Settings
from liquidity_signals import abnormal_volume_detector

from polygon_io import (
    fetch_last_minute_bar,
    fetch_latest_treasury_yields,
    fetch_recent_minute_bars,
)


@dataclass
class MarketProxy:
    as_of_utc: str
    tbill_13w_pct: float | None
    """Heuristic 0–100: higher = more pressure (steep bill / missing data neutral)."""
    stress_score: float
    detail: str


def _safe_last_close(ticker: str) -> tuple[float | None, str]:
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="5d", interval="1d")
        if hist is None or hist.empty:
            return None, "no_history"
        last = float(hist["Close"].iloc[-1])
        return last, "ok"
    except Exception as exc:  # noqa: BLE001 — best-effort proxy
        return None, f"error:{type(exc).__name__}"


def fetch_market_proxy() -> MarketProxy:
    """
    ^IRX is the 13-week T-bill index (yield % level).
    This is a **shadow** signal only; it is not TGA.
    """
    now = datetime.now(timezone.utc).isoformat()
    yld, why = _safe_last_close("^IRX")
    if yld is None:
        return MarketProxy(
            as_of_utc=now,
            tbill_13w_pct=None,
            stress_score=0.0,
            detail=f"tbill:{why}",
        )
    baseline = 5.0
    pressure = max(0.0, min(100.0, (yld - baseline) * 40.0))
    return MarketProxy(
        as_of_utc=now,
        tbill_13w_pct=yld,
        stress_score=round(pressure, 2),
        detail=f"tbill:{why}",
    )


async def build_polygon_shadow(
    client: httpx.AsyncClient,
    settings: Settings,
) -> dict[str, Any]:
    """Minute ETF bar (volume) + daily CM Treasury yields when keys/hosts permit."""
    out: dict[str, Any] = {
        "polygon_minute": None,
        "polygon_treasury_yields": None,
        "volume_anomaly": None,
        "errors": [],
    }
    if not settings.polygon_api_key:
        return out

    try:
        bar = await fetch_last_minute_bar(
            client,
            api_key=settings.polygon_api_key,
            ticker=settings.polygon_minute_ticker,
            rest_base=settings.polygon_rest_base,
        )
        if bar:
            out["polygon_minute"] = {
                "ticker": bar.ticker,
                "as_of_utc": bar.as_of_utc,
                "close": bar.close,
                "volume": bar.volume,
                "source": bar.source,
            }
    except Exception as exc:  # noqa: BLE001
        out["errors"].append(f"polygon_minute:{type(exc).__name__}")

    try:
        ylds = await fetch_latest_treasury_yields(
            client,
            api_key=settings.polygon_api_key,
            fed_base=settings.polygon_fed_base,
        )
        if ylds:
            out["polygon_treasury_yields"] = {
                "as_of_date": ylds.as_of_date,
                "yield_1_month_pct": ylds.yield_1_month_pct,
                "yield_3_month_pct": ylds.yield_3_month_pct,
                "source": ylds.source,
            }
    except Exception as exc:  # noqa: BLE001
        out["errors"].append(f"polygon_treasury:{type(exc).__name__}")

    try:
        closes, vols = await fetch_recent_minute_bars(
            client,
            api_key=settings.polygon_api_key,
            ticker=settings.polygon_minute_ticker,
            rest_base=settings.polygon_rest_base,
            limit=settings.polygon_minute_lookback_bars,
        )
        if closes and vols:
            va = abnormal_volume_detector(closes=closes, volumes=vols)
            out["volume_anomaly"] = {
                "abnormal": va.abnormal,
                "current_volume": va.current_volume,
                "volume_ma": va.volume_ma,
                "yield_rising": va.yield_rising,
                "net_liquidity_bias": va.net_liquidity_bias,
                "detail": va.detail,
            }
    except Exception as exc:  # noqa: BLE001
        out["errors"].append(f"polygon_volume:{type(exc).__name__}")

    return out
