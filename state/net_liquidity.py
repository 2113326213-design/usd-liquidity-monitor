"""
Net Liquidity composite: Reserves + ON_RRP − TGA.

This is the single most informative liquidity water-level. All three are Fed
liabilities in a zero-sum relationship: TGA up → reserves down; RRP down → reserves up.

Watching any one in isolation is misleading.

Derived metrics:
  net_liquidity_bn        = Reserves + RRP - TGA
  ewma7_slope_bn_per_day  = daily change of 7-day EWMA
  slope_reversal_alert    = fires when slope flips pos→neg past threshold
"""
from __future__ import annotations

import hashlib

import pandas as pd
from loguru import logger

from ..alerts.telegram import TelegramAlerter
from ..config import settings
from ..storage.parquet_store import ParquetStore


class NetLiquidityCalculator:
    NAME = "net_liquidity"

    def __init__(self, store: ParquetStore, alerter: TelegramAlerter) -> None:
        self.store = store
        self.alerter = alerter

    async def recompute(self, _payload: dict | None = None) -> None:
        tga = self.store.last_snapshot("tga")
        rrp = self.store.last_snapshot("rrp")
        reserves = self.store.last_snapshot("reserves")

        if not all([tga, rrp, reserves]):
            missing = [n for n, d in [("tga", tga), ("rrp", rrp), ("reserves", reserves)] if not d]
            logger.debug(f"[net_liquidity] missing inputs: {missing}")
            return

        try:
            net = (
                float(reserves["reserves_bn"])
                + float(rrp["total_accepted_bn"])
                - float(tga["close_bal_bn"])
            )
        except Exception as e:
            logger.exception(f"[net_liquidity] compute error: {e}")
            return

        # "as_of" = latest of the three date anchors
        as_of = max(
            str(tga.get("record_date", "")),
            str(rrp.get("operation_date", "")),
            str(reserves.get("observation_date", "")),
        )

        payload = {
            "as_of": as_of,
            "net_liquidity_bn": round(net, 2),
            "reserves_bn": float(reserves["reserves_bn"]),
            "rrp_bn": float(rrp["total_accepted_bn"]),
            "tga_bn": float(tga["close_bal_bn"]),
        }

        h = hashlib.md5(
            f"{payload['as_of']}:{payload['net_liquidity_bn']}".encode()
        ).hexdigest()

        last_h = self.store.last_hash(self.NAME)
        if last_h == h:
            return  # no change

        self.store.write_snapshot(self.NAME, payload, h)
        logger.info(f"[net_liquidity] {payload['as_of']} → {net:+.1f} bn")

        await self._check_slope_reversal()

    async def _check_slope_reversal(self) -> None:
        df = self.store.read_all(self.NAME)
        if len(df) < 10:
            return

        df = df.sort_values("as_of").reset_index(drop=True)
        series = df["net_liquidity_bn"].astype(float)
        ewma = series.ewm(span=7, adjust=False).mean()
        slopes = ewma.diff()

        if len(slopes) < 3:
            return

        # Look for: slope was positive, now strongly negative
        prev_slope = float(slopes.iloc[-2])
        cur_slope = float(slopes.iloc[-1])

        if prev_slope > 0 and cur_slope < settings.net_liquidity_slope_alert:
            await self.alerter.send(
                level="HIGH",
                msg=(
                    f"Net Liquidity 7-day EWMA slope reversal: "
                    f"{prev_slope:+.1f} → {cur_slope:+.1f} bn/day"
                ),
                payload={
                    "prev_slope_bn_per_day": prev_slope,
                    "cur_slope_bn_per_day": cur_slope,
                    "latest_net_liquidity_bn": float(series.iloc[-1]),
                    "as_of": str(df["as_of"].iloc[-1]),
                },
            )
