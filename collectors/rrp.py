"""
ON RRP (Overnight Reverse Repo) collector.

Source: NY Fed Markets API
Endpoint: https://markets.newyorkfed.org/api/rp/reverserepo/propositions/search.json
         (also: .../api/rp/all/all/results/lastTwoWeeks.json for combined repo+reverse)

Operation cadence: once per business day, operation window roughly 12:45–13:15 ET,
results published immediately after close (usually by 13:20 ET).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
from loguru import logger

from .base import Collector


class RRPCollector(Collector):
    name = "rrp"
    URL = "https://markets.newyorkfed.org/api/rp/reverserepo/propositions/search.json"

    async def fetch(self) -> dict | None:
        # Pull last 14 days to be safe; we take the latest overnight op
        now = datetime.now(timezone.utc)
        start = (now - timedelta(days=14)).strftime("%Y-%m-%d")
        end = now.strftime("%Y-%m-%d")
        params = {"startDate": start, "endDate": end}

        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(self.URL, params=params)
            r.raise_for_status()

        data = r.json()
        ops = data.get("repo", {}).get("operations", [])
        if not ops:
            return None

        # Filter to Overnight Reverse Repo only (exclude term or FIMA)
        on_rrp = [
            o for o in ops
            if o.get("operationType", "").upper() == "REVERSE REPO"
            and "OVERNIGHT" in o.get("operationDate", "").upper() + o.get("operationTypeMisc", "").upper()
            or o.get("operationType", "").upper() == "REVERSE REPO"  # fallback
        ]
        ops_sorted = sorted(on_rrp or ops, key=lambda x: x.get("operationDate", ""), reverse=True)
        latest = ops_sorted[0]

        # Extract award rate from details if present
        details = latest.get("details", [])
        rate = None
        if details:
            rate = details[0].get("percentAwardRate") or details[0].get("percentOfferingRate")

        try:
            total_accepted_bn = float(latest.get("totalAmtAccepted", 0)) / 1e9
        except (TypeError, ValueError):
            total_accepted_bn = 0.0

        return {
            "operation_id": latest.get("operationId"),
            "operation_date": latest.get("operationDate"),
            "operation_type": latest.get("operationType"),
            "total_accepted_bn": total_accepted_bn,
            "num_submissions": latest.get("totalAmtSubmittedPositions"),
            "rate": rate,
        }

    def validate(self, payload: dict) -> bool:
        from ..alerts.sanity import sanity_check
        return sanity_check("rrp_bn", payload.get("total_accepted_bn"))

    async def on_new_data(self, payload: dict) -> None:
        from ..alerts.playbook import format_alert, suggest_action, tier_level
        from ..config import settings

        rrp_bn = float(payload["total_accepted_bn"])

        # Absolute-level tiered playbook: cushion-exhaustion alert.
        level = tier_level(
            rrp_bn,
            medium=settings.rrp_medium_bn,
            high=settings.rrp_high_bn,
            critical=settings.rrp_critical_bn,
            direction="below",
        )
        if level is not None:
            threshold = {
                "MEDIUM": settings.rrp_medium_bn,
                "HIGH": settings.rrp_high_bn,
                "CRITICAL": settings.rrp_critical_bn,
            }[level]
            msg = format_alert(
                level=level,
                title="ON RRP cushion running low — further drains hit reserves directly",
                metrics={
                    "ON RRP": f"${rrp_bn:,.1f} bn",
                    f"{level} threshold": f"${threshold:,.0f} bn",
                    "Operation date": payload.get("operation_date", "?"),
                },
                action=suggest_action(level, hedge_ticker=settings.hedge_ticker),
            )
            await self.alerter.send(level=level, msg=msg, payload=payload)

        # Daily drain delta (existing rule).
        prev = self.store.last_snapshot(self.name, offset=1)
        if prev is not None:
            try:
                delta = rrp_bn - float(prev["total_accepted_bn"])
                if delta < -settings.rrp_daily_drain_bn:
                    await self.alerter.send(
                        level="HIGH",
                        msg=f"ON RRP drain Δ1d = {delta:+.1f} bn. Now {rrp_bn:.1f} bn.",
                        payload=payload,
                    )
            except Exception as e:
                logger.debug(f"[rrp] delta check failed: {e}")

        await self.store.trigger("rrp_updated", payload)
