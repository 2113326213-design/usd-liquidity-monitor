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

        # Sanity gate — NetLiquidity is a derived metric; if any input
        # collector drifted outside plausibility bounds we'd already have
        # rejected them upstream, but double-check the composite here
        # to guard against arithmetic surprises (e.g. two bad inputs
        # cancelling out but the net still nonsensical).
        from ..alerts.sanity import sanity_check
        if not sanity_check("net_liquidity_bn", payload["net_liquidity_bn"]):
            logger.error(
                f"[net_liquidity] implausible composite {net:.1f} bn from "
                f"R={payload['reserves_bn']:.1f} + RRP={payload['rrp_bn']:.1f} "
                f"− TGA={payload['tga_bn']:.1f}; skipping write + alerts"
            )
            return

        self.store.write_snapshot(self.NAME, payload, h)
        logger.info(f"[net_liquidity] {payload['as_of']} → {net:+.1f} bn")

        await self._check_absolute_level(payload)
        await self._check_slope_reversal()

    async def _check_absolute_level(self, payload: dict) -> None:
        """Fire a playbook alert if net liquidity has crossed a tiered floor."""
        from ..alerts.playbook import format_alert, suggest_action, tier_level

        net = float(payload["net_liquidity_bn"])
        level = tier_level(
            net,
            medium=settings.net_liq_medium_bn,
            high=settings.net_liq_high_bn,
            critical=settings.net_liq_critical_bn,
            direction="below",
        )
        if level is None:
            return
        threshold = {
            "MEDIUM": settings.net_liq_medium_bn,
            "HIGH": settings.net_liq_high_bn,
            "CRITICAL": settings.net_liq_critical_bn,
        }[level]
        msg = format_alert(
            level=level,
            title="综合水位跌破结构性底线",
            metrics={
                "综合水位":      f"${net:,.1f} bn",
                f"{level} 阈值": f"${threshold:,.0f} bn",
                "准备金":        f"${float(payload['reserves_bn']):,.1f} bn",
                "RRP":           f"${float(payload['rrp_bn']):,.1f} bn",
                "TGA":           f"${float(payload['tga_bn']):,.1f} bn",
                "截至日期":      payload.get("as_of", "?"),
            },
            action=suggest_action(level, hedge_ticker=settings.hedge_ticker),
        )
        await self.alerter.send(level=level, msg=msg, payload=payload)

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
            from ..alerts.playbook import format_alert, suggest_action

            msg = format_alert(
                level="HIGH",
                title="综合水位 7 日 EWMA 斜率反转",
                metrics={
                    "前日斜率":      f"{prev_slope:+.1f} bn/day",
                    "当前斜率":      f"{cur_slope:+.1f} bn/day",
                    "最新综合水位":  f"${float(series.iloc[-1]):,.1f} bn",
                    "截至日期":      str(df["as_of"].iloc[-1]),
                },
                action=suggest_action("HIGH", hedge_ticker=settings.hedge_ticker),
            )
            await self.alerter.send(
                level="HIGH",
                msg=msg,
                payload={
                    "prev_slope_bn_per_day": prev_slope,
                    "cur_slope_bn_per_day": cur_slope,
                    "latest_net_liquidity_bn": float(series.iloc[-1]),
                    "as_of": str(df["as_of"].iloc[-1]),
                },
            )
