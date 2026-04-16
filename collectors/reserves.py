"""
Bank Reserves collector.

Source: FRED series WRESBAL
        "Reserve Balances with Federal Reserve Banks" (Billions USD, weekly Wednesday)
Endpoint: https://api.stlouisfed.org/fred/series/observations

Release cadence: weekly, Thursday ~16:30 ET, reflecting prior Wednesday's level.
"""
from __future__ import annotations

import httpx
from loguru import logger

from ..config import settings
from .base import Collector


class ReservesCollector(Collector):
    name = "reserves"
    URL = "https://api.stlouisfed.org/fred/series/observations"
    SERIES_ID = "WRESBAL"  # Billions USD

    async def fetch(self) -> dict | None:
        if not settings.fred_api_key:
            logger.warning("[reserves] FRED_API_KEY not set — skipping")
            return None

        params = {
            "series_id": self.SERIES_ID,
            "api_key": settings.fred_api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": "10",
        }
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(self.URL, params=params)
            r.raise_for_status()

        obs = r.json().get("observations", [])
        # drop missing "." values
        obs = [o for o in obs if o.get("value") not in (".", "", None)]
        if not obs:
            return None

        latest = obs[0]
        try:
            value_bn = float(latest["value"]) / 1000.0  # FRED returns millions, convert to billions
        except (TypeError, ValueError):
            logger.warning(f"[reserves] unparseable value: {latest!r}")
            return None

        return {
            "observation_date": latest["date"],
            "reserves_bn": value_bn,
        }

    async def on_new_data(self, payload: dict) -> None:
        from ..alerts.playbook import format_alert, suggest_action, tier_level
        from ..config import settings

        reserves_bn = float(payload["reserves_bn"])

        # Absolute-level tiered playbook: floor-approach alert.
        level = tier_level(
            reserves_bn,
            medium=settings.reserves_medium_bn,
            high=settings.reserves_high_bn,
            critical=settings.reserves_critical_bn,
            direction="below",
        )
        if level is not None:
            threshold = {
                "MEDIUM": settings.reserves_medium_bn,
                "HIGH": settings.reserves_high_bn,
                "CRITICAL": settings.reserves_critical_bn,
            }[level]
            msg = format_alert(
                level=level,
                title="Reserves approaching Fed 'ample' floor",
                metrics={
                    "Reserves": f"${reserves_bn:,.1f} bn",
                    f"{level} threshold": f"${threshold:,.0f} bn",
                    "Observation": payload.get("observation_date", "?"),
                },
                action=suggest_action(level, hedge_ticker=settings.hedge_ticker),
            )
            await self.alerter.send(level=level, msg=msg, payload=payload)

        # Week-over-week delta notice (lower-priority, keeps running even when
        # absolute level is still comfortable).
        prev = self.store.last_snapshot(self.name, offset=1)
        if prev is not None:
            try:
                delta = reserves_bn - float(prev["reserves_bn"])
                if abs(delta) > 100:  # large weekly move
                    await self.alerter.send(
                        level="MEDIUM",
                        msg=f"Reserves Δw/w = {delta:+.1f} bn. Now {reserves_bn:.1f} bn.",
                        payload=payload,
                    )
            except Exception as e:
                logger.debug(f"[reserves] delta check failed: {e}")

        await self.store.trigger("reserves_updated", payload)
