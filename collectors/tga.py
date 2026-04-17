"""
TGA (Treasury General Account) collector.

Source: US Fiscal Data API — Daily Treasury Statement, Operating Cash Balance table.
Endpoint: https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/operating_cash_balance

IMPORTANT QUIRK:
As of 2022-04-18, the `close_today_bal` field is always the string "null" for the
"Treasury General Account (TGA) Closing Balance" account_type row.
The actual closing balance is stored in `open_today_bal` of that same row
(Fiscal Data's internal representation shifts the closing value into the opening field).

Update cadence: once per business day, roughly 16:00–17:00 ET, reflecting T-1 close.
"""
from __future__ import annotations

import httpx
import pandas as pd
from loguru import logger

from .base import Collector


class TGACollector(Collector):
    name = "tga"
    URL = (
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
        "/v1/accounting/dts/operating_cash_balance"
    )

    async def fetch(self) -> dict | None:
        params = {
            "sort": "-record_date",
            "page[size]": "40",
            "format": "json",
        }
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(self.URL, params=params)
            r.raise_for_status()

        data = r.json().get("data", [])
        if not data:
            return None

        df = pd.DataFrame(data)
        mask = df["account_type"] == "Treasury General Account (TGA) Closing Balance"
        df = df[mask].copy()
        if df.empty:
            logger.warning("[tga] no Closing Balance row found")
            return None

        df = df.sort_values("record_date", ascending=False)
        latest = df.iloc[0]

        # Read from open_today_bal (value in millions USD); close_today_bal is "null"
        raw_bal = latest["open_today_bal"]
        try:
            bal_mn = float(raw_bal)
        except (TypeError, ValueError):
            logger.warning(f"[tga] unparseable balance: {raw_bal!r}")
            return None

        return {
            "record_date": latest["record_date"],
            "close_bal_bn": bal_mn / 1000.0,  # mn -> bn
            "close_bal_mn": bal_mn,
        }

    def validate(self, payload: dict) -> bool:
        from ..alerts.sanity import sanity_check
        return sanity_check("tga_bn", payload.get("close_bal_bn"))

    async def on_new_data(self, payload: dict) -> None:
        from ..alerts.playbook import format_alert, suggest_action
        from ..config import settings

        tga_bn = float(payload["close_bal_bn"])

        prev = self.store.last_snapshot(self.name, offset=1)
        if prev is not None:
            try:
                delta = tga_bn - float(prev["close_bal_bn"])
                if abs(delta) > settings.tga_daily_swing_bn:
                    # TGA rising = Treasury draining liquidity (HIGH).
                    # TGA falling = Treasury spending / MEDIUM observational.
                    if delta > 0:
                        level = "HIGH"
                        title = "TGA surge — Treasury draining reserves"
                        action = suggest_action(level, hedge_ticker=settings.hedge_ticker)
                    else:
                        level = "MEDIUM"
                        title = "TGA drop — Treasury releasing cash (liquidity positive)"
                        action = None  # No hedge action on inflow — this is bullish.

                    msg = format_alert(
                        level=level,
                        title=title,
                        metrics={
                            "TGA": f"${tga_bn:,.1f} bn",
                            "Δ1d": f"{delta:+.1f} bn",
                            "Record date": payload.get("record_date", "?"),
                        },
                        action=action,
                    )
                    await self.alerter.send(level=level, msg=msg, payload=payload)
            except Exception as e:
                logger.debug(f"[tga] delta check failed: {e}")

        await self.store.trigger("tga_updated", payload)
