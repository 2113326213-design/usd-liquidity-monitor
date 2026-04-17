"""
SRP (Standing Repo Facility) collector.

Source: NY Fed Markets API
Endpoint: https://markets.newyorkfed.org/api/rp/repo/all/results/lastTwoWeeks.json

As of 2025-12-11, SRP operates twice daily (AM + PM) with no aggregate limit.
ANY non-zero acceptance is a hard signal that we've left the Ample regime.

Note: the older `repo/propositions/search.json` endpoint with startDate/endDate
params now returns HTTP 400 on NY Fed's side. The `results/lastTwoWeeks.json`
endpoint returns the same JSON shape ({"repo": {"operations": [...]}}) and
always covers the last 14 days, so no date params are needed.
"""
from __future__ import annotations

from datetime import datetime, timezone

import httpx
from loguru import logger

from ..config import settings
from .base import Collector


class SRPCollector(Collector):
    name = "srp"
    URL = "https://markets.newyorkfed.org/api/rp/repo/all/results/lastTwoWeeks.json"

    async def fetch(self) -> dict | None:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(self.URL)
            r.raise_for_status()

        data = r.json()
        ops = data.get("repo", {}).get("operations", [])
        if not ops:
            return {
                "operation_id": None,
                "operation_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "total_accepted_bn": 0.0,
                "note": "no recent SRP operations — normal",
            }

        ops_sorted = sorted(ops, key=lambda x: x.get("operationDate", ""), reverse=True)
        latest = ops_sorted[0]

        try:
            total_accepted_bn = float(latest.get("totalAmtAccepted", 0)) / 1e9
        except (TypeError, ValueError):
            total_accepted_bn = 0.0

        details = latest.get("details", [])
        rate = details[0].get("percentAwardRate") if details else None

        return {
            "operation_id": latest.get("operationId"),
            "operation_date": latest.get("operationDate"),
            "operation_type": latest.get("operationType"),
            "total_accepted_bn": total_accepted_bn,
            "rate": rate,
        }

    def validate(self, payload: dict) -> bool:
        from ..alerts.sanity import sanity_check
        return sanity_check("srp_bn", payload.get("total_accepted_bn"))

    async def on_new_data(self, payload: dict) -> None:
        if payload.get("total_accepted_bn", 0.0) > settings.srp_alert_min_bn:
            await self.alerter.send(
                level="CRITICAL",
                msg=(
                    f"🚨 SRP ACTIVATED: {payload['total_accepted_bn']:.2f} bn accepted "
                    f"on {payload['operation_date']}. "
                    f"Scarce → Crisis regime transition likely."
                ),
                payload=payload,
            )
        await self.store.trigger("srp_updated", payload)
