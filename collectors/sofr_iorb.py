"""
SOFR - IORB spread collector — the most direct reserve-scarcity signal.

Why it's the clearest signal
----------------------------
IORB is what Fed pays banks for parking reserves at the Fed.
SOFR is what banks pay each other overnight in repo.

When SOFR > IORB, banks prefer lending in the market over receiving
Fed interest. They only do that when **other banks want cash badly
enough to pay a premium** — i.e., reserves are scarce.

Historical benchmarks
* 2019-09-17 (repo crisis): SOFR spiked 300 bp in one day, SOFR-IORB
  opened a ~50+ bp gap that stayed for weeks until SRF was rolled out.
* 2023-Q4: brief +3-5 bp elevations on quarter-end Treasury settlement.
* Normal: spread oscillates around 0 ± 2 bp.

Data sources (both free, both FRED)
* SOFR series_id: SOFR (daily, lagged 1 day)
* IORB series_id: IORB (daily, started 2021-07-29)

Pre-2021-07-29: IORB did not exist; use IOER (Interest on Excess
Reserves) as predecessor. Not stitched automatically in this collector
(live polling doesn't need the older series) — the backfill script
handles the stitch if you want long history.

Alert thresholds (see config.py)
* MEDIUM  > 2 bp  (persistent elevation, not just month-end noise)
* HIGH    > 5 bp  (meaningful stress)
* CRITICAL > 10 bp (2019-level — extremely rare)

Update cadence: FRED publishes both series around 8:00 AM ET for
previous business day. Poll at 15:00 ET daily to be safe.
"""
from __future__ import annotations

import httpx
from loguru import logger

from ..config import settings
from .base import Collector


class SofrIorbCollector(Collector):
    name = "sofr_iorb"
    FRED_URL = "https://api.stlouisfed.org/fred/series/observations"

    async def _latest_value(self, series_id: str) -> tuple[str | None, float | None]:
        """Return (date, value) for most recent non-missing observation."""
        if not settings.fred_api_key:
            logger.debug(f"[sofr_iorb] no FRED key, cannot fetch {series_id}")
            return None, None
        params = {
            "series_id": series_id,
            "api_key": settings.fred_api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": "10",
        }
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.get(self.FRED_URL, params=params)
                r.raise_for_status()
            obs = r.json().get("observations", [])
            for o in obs:
                v = o.get("value")
                if v and v != ".":
                    return o.get("date"), float(v)
        except Exception as e:
            logger.warning(f"[sofr_iorb] {series_id} fetch failed: {e}")
        return None, None

    async def fetch(self) -> dict | None:
        sofr_date, sofr_pct = await self._latest_value("SOFR")
        iorb_date, iorb_pct = await self._latest_value("IORB")
        if sofr_pct is None or iorb_pct is None:
            return None
        spread_bp = round((sofr_pct - iorb_pct) * 100, 3)

        # Pick the later of the two dates as the observation date.
        # SOFR usually publishes one day ahead of IORB.
        obs_date = max(
            sofr_date or "", iorb_date or ""
        )

        return {
            "observation_date": obs_date,
            "sofr_pct":   round(sofr_pct, 4),
            "iorb_pct":   round(iorb_pct, 4),
            "spread_bp":  spread_bp,
            "sofr_date":  sofr_date,
            "iorb_date":  iorb_date,
        }

    def validate(self, payload: dict) -> bool:
        from ..alerts.sanity import sanity_check
        # Spread can be ±300 bp in extreme events; anything beyond is a bug
        val = payload.get("spread_bp")
        if val is None:
            logger.warning("[sofr_iorb] spread_bp is None, skipping")
            return False
        if not (-500 < val < 500):
            logger.error(
                f"[sofr_iorb] implausible spread {val} bp — upstream data bug"
            )
            return False
        # Also sanity-check individual rates (should be 0-10%)
        for k in ("sofr_pct", "iorb_pct"):
            v = payload.get(k)
            if v is None or not (0 < v < 15):
                logger.error(f"[sofr_iorb] {k}={v} implausible")
                return False
        return True

    async def on_new_data(self, payload: dict) -> None:
        from ..alerts.playbook import format_alert, suggest_action, tier_level

        spread = float(payload["spread_bp"])

        level = tier_level(
            spread,
            medium=settings.sofr_iorb_medium_bp,
            high=settings.sofr_iorb_high_bp,
            critical=settings.sofr_iorb_critical_bp,
            direction="above",
        )
        if level is None:
            return

        msg = format_alert(
            level=level,
            title="SOFR > IORB — 准备金稀缺的直接信号",
            metrics={
                "SOFR":       f"{payload['sofr_pct']:.4f}%",
                "IORB":       f"{payload['iorb_pct']:.4f}%",
                "Spread":     f"{spread:+.2f} bp",
                "Date":       payload.get("observation_date", "?"),
                "Threshold":  (
                    f"MEDIUM>{settings.sofr_iorb_medium_bp}bp / "
                    f"HIGH>{settings.sofr_iorb_high_bp}bp / "
                    f"CRITICAL>{settings.sofr_iorb_critical_bp}bp"
                ),
            },
            action=suggest_action(level, hedge_ticker=settings.hedge_ticker),
        )
        await self.alerter.send(level=level, msg=msg, payload=payload)
