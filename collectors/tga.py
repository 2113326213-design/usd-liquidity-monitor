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

    async def on_new_data(self, payload: dict) -> None:
        prev = self.store.last_snapshot(self.name, offset=1)
        if prev is not None:
            try:
                delta = payload["close_bal_bn"] - float(prev["close_bal_bn"])
                from ..config import settings
                if abs(delta) > settings.tga_daily_swing_bn:
                    level = "HIGH" if delta > 0 else "MEDIUM"
                    direction = "Treasury吸水 (liquidity drain)" if delta > 0 else "Treasury放水 (liquidity inject)"
                    await self.alerter.send(
                        level=level,
                        msg=f"TGA Δ1d = {delta:+.1f} bn → {direction}. Now {payload['close_bal_bn']:.1f} bn.",
                        payload=payload,
                    )
            except Exception as e:
                logger.debug(f"[tga] delta check failed: {e}")

        await self.store.trigger("tga_updated", payload)
