"""
Market stress collector — Layer-2 fast pulse (15-minute ETF / VIX probe).

While TGA / RRP / Reserves publish on daily or weekly cadence, price in
stress-sensitive ETFs and VIX reacts in real time. This collector polls a
basket via yfinance every 15 minutes during US market hours and computes
a composite stress z-score. Used both as an independent alert channel and
as a "resonance confirmer" for slow-layer structural alerts.

Signal basis
    For each ticker, z-score the latest hourly return against the rolling
    30-day distribution of hourly returns. Sign-adjust by stress direction
    (SPY down = stress; VIX up = stress; etc.) so positive composite z
    always means "market moving toward stress". Composite = mean of
    available ticker stress_z values.

Noise filter
    Requires at least half of the fetched tickers to be stress-aligned
    (stress_z > 0.5) before firing any alert. A single-ticker spike gets
    muted — we want multi-basket confirmation.

Slow-layer resonance
    If Net Liquidity is already below its MEDIUM structural floor when a
    market-stress alert fires, severity is escalated one level
    (MEDIUM→HIGH, HIGH→CRITICAL). Single-layer alerts require higher
    independent z-thresholds.

Level-transition dedup
    Alerts fire on escalation only. Repeated polls at the same severity
    stay silent. De-escalation is also silent (you already know stress
    is fading if you're watching).
"""
from __future__ import annotations

import asyncio
import statistics
from datetime import datetime, timezone
from typing import Any

import yfinance as yf
from loguru import logger

from ..alerts.playbook import format_alert, suggest_action, tier_level
from ..config import settings
from .base import Collector


# +1 means rising price = stress; -1 means falling price = stress.
# Applied to raw z-scores so stress_z > 0 always indicates stress-aligned movement.
STRESS_DIRECTION: dict[str, int] = {
    "SPY": -1,    # S&P 500 — down = stress
    "^VIX": +1,   # volatility index — up = stress
    "TLT": +1,    # 20+ yr Treasury — up = flight to quality
    "IEF": +1,    # 7-10 yr Treasury — up = flight to quality
    "LQD": -1,    # IG credit — down = credit widening
    "HYG": -1,    # HY credit — down = credit widening (most sensitive)
}


def _fetch_ticker_sync(ticker: str) -> dict | None:
    """Synchronous yfinance call — must be wrapped in asyncio.to_thread."""
    try:
        df = yf.Ticker(ticker).history(
            period="30d", interval="1h", auto_adjust=False, prepost=False
        )
        if df.empty or len(df) < 20:
            return None
        closes = df["Close"].astype(float)
        returns = closes.pct_change().dropna()
        if len(returns) < 10:
            return None
        latest_ret = float(returns.iloc[-1])
        mu = float(returns.mean())
        sigma = float(returns.std())
        if sigma <= 0:
            return None
        raw_z = (latest_ret - mu) / sigma
        return {
            "price": round(float(closes.iloc[-1]), 4),
            "ret_1h_pct": round(latest_ret * 100.0, 3),
            "z_1h": round(raw_z, 3),
        }
    except Exception as e:
        logger.warning(f"[market_stress] {ticker} fetch failed: {e}")
        return None


_LEVEL_RANK = {"MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}


def _rank(level: str | None) -> int:
    return _LEVEL_RANK.get(level or "", 0)


class MarketStressCollector(Collector):
    name = "market_stress"

    async def fetch(self) -> dict | None:
        tickers = list(STRESS_DIRECTION.keys())
        results = await asyncio.gather(
            *(asyncio.to_thread(_fetch_ticker_sync, t) for t in tickers)
        )

        tick_data: dict[str, Any] = {}
        stress_zs: list[float] = []
        for ticker, data in zip(tickers, results):
            if data is None:
                continue
            sign = STRESS_DIRECTION[ticker]
            stress_z = data["z_1h"] * sign
            tick_data[ticker] = {**data, "stress_z": round(stress_z, 3)}
            stress_zs.append(stress_z)

        if not stress_zs:
            logger.warning("[market_stress] no tickers returned data")
            return None

        composite = statistics.mean(stress_zs)
        stress_aligned = sum(1 for z in stress_zs if z > 0.5)

        return {
            "as_of_utc": datetime.now(timezone.utc).isoformat(),
            "tickers": tick_data,
            "composite_stress_z": round(composite, 3),
            "tickers_returned": len(stress_zs),
            "tickers_stress_aligned": stress_aligned,
        }

    async def on_new_data(self, payload: dict) -> None:
        composite = float(payload["composite_stress_z"])
        aligned = int(payload["tickers_stress_aligned"])
        total = int(payload["tickers_returned"])

        # Noise filter: require ≥ half of tickers to agree before firing.
        if aligned < max(2, total // 2):
            logger.debug(
                f"[market_stress] z={composite:+.2f} but only "
                f"{aligned}/{total} stress-aligned — no alert"
            )
            return

        level = tier_level(
            composite,
            medium=settings.market_stress_medium_z,
            high=settings.market_stress_high_z,
            critical=settings.market_stress_critical_z,
            direction="above",
        )

        # Resonance with slow layer: if Net Liquidity is already below the
        # MEDIUM structural floor, escalate one level.
        resonance_note = ""
        nl_snap = self.store.last_snapshot("net_liquidity")
        if nl_snap is not None and level is not None:
            try:
                net_liq = float(nl_snap["net_liquidity_bn"])
                if net_liq < settings.net_liq_medium_bn:
                    upgraded = {"MEDIUM": "HIGH", "HIGH": "CRITICAL"}.get(level, level)
                    if upgraded != level:
                        resonance_note = (
                            f" (upgraded {level}→{upgraded}: slow-layer "
                            f"resonance — Net Liq ${net_liq:,.0f} bn < "
                            f"${settings.net_liq_medium_bn:,.0f} bn MEDIUM floor)"
                        )
                        level = upgraded
            except Exception as e:
                logger.debug(f"[market_stress] resonance check failed: {e}")

        if level is None:
            return

        # Level-transition dedup: suppress if previous snapshot was at same
        # or higher severity (we already told the user).
        prev = self.store.last_snapshot(self.name, offset=1)
        if prev is not None:
            try:
                prev_z = float(prev.get("composite_stress_z", 0.0))
                prev_level = tier_level(
                    prev_z,
                    medium=settings.market_stress_medium_z,
                    high=settings.market_stress_high_z,
                    critical=settings.market_stress_critical_z,
                    direction="above",
                )
                if _rank(level) <= _rank(prev_level):
                    return
            except Exception as e:
                logger.debug(f"[market_stress] dedup check failed: {e}")

        # Rank tickers by absolute stress contribution for display.
        ticker_metrics = {
            t: f"z={d['stress_z']:+.2f}  ({d['ret_1h_pct']:+.2f}%)"
            for t, d in sorted(
                payload["tickers"].items(),
                key=lambda kv: abs(kv[1]["stress_z"]),
                reverse=True,
            )
        }

        msg = format_alert(
            level=level,
            title=f"Market stress pulse{resonance_note}",
            metrics={
                "Composite stress z": f"{composite:+.2f}",
                "Aligned tickers": f"{aligned}/{total}",
                **ticker_metrics,
            },
            action=suggest_action(level, hedge_ticker=settings.hedge_ticker),
        )
        await self.alerter.send(level=level, msg=msg, payload=payload)
