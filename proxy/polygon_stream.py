"""
Polygon.io websocket streaming client for Layer-3 proxies.

Subscribes to minute aggregates (AM.*) for a basket of Treasury ETFs that track
short-end liquidity stress. Auto-reconnects on disconnect.

Polygon plan required:
- Stocks: Starter tier or higher for realtime minute aggregates
- Futures: separate futures subscription (not wired in this MVP)

For SOFR/Fed-funds futures tick data, plug in Databento in a parallel module;
the design here is decoupled so you can add Databento without touching the ETF layer.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import websockets
from loguru import logger

from ..alerts.telegram import TelegramAlerter
from ..config import settings
from ..storage.parquet_store import ParquetStore
from .proxy_state import ProxyState


POLYGON_STOCKS_WS = "wss://socket.polygon.io/stocks"
POLYGON_DELAYED_WS = "wss://delayed.polygon.io/stocks"  # fallback for lower tiers

# Basket: short-end Treasury ETFs where liquidity stress shows first
DEFAULT_TICKERS = ["BIL", "SGOV", "SHV", "SHY", "IEI", "IEF", "TLT", "TLH"]


class PolygonStream:
    def __init__(
        self,
        store: ParquetStore,
        alerter: TelegramAlerter,
        tickers: list[str] | None = None,
        use_delayed: bool = False,
    ) -> None:
        self.store = store
        self.alerter = alerter
        self.tickers = tickers or DEFAULT_TICKERS
        self.state = ProxyState(store, alerter)
        self.url = POLYGON_DELAYED_WS if use_delayed else POLYGON_STOCKS_WS
        self._backoff_sec = 5
        self._max_backoff_sec = 300

    async def run(self) -> None:
        """Main loop — reconnects forever with exponential backoff."""
        if not settings.polygon_api_key:
            # Not an error — Polygon is a paid, optional data source.
            # Users who opt out (most retail) should not see ERROR noise
            # on every monitor start. Log once at INFO so the absence is
            # visible but doesn't pollute error scans.
            logger.info("POLYGON_API_KEY not set — Layer-3 proxy disabled (optional, $29/mo)")
            return

        backoff = self._backoff_sec
        while True:
            try:
                await self._run_once()
                backoff = self._backoff_sec  # reset on clean exit
            except Exception as e:
                logger.exception(f"Polygon stream error: {e}")
                logger.warning(f"Reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._max_backoff_sec)

    async def _run_once(self) -> None:
        async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as ws:
            # 1. Auth
            await ws.send(json.dumps({"action": "auth", "params": settings.polygon_api_key}))
            auth_resp = await ws.recv()
            logger.info(f"Polygon auth response: {auth_resp}")

            # 2. Subscribe to minute aggregates
            params = ",".join(f"AM.{t}" for t in self.tickers)
            await ws.send(json.dumps({"action": "subscribe", "params": params}))
            sub_resp = await ws.recv()
            logger.info(f"Polygon subscribe response: {sub_resp}")

            logger.info(f"Subscribed to {len(self.tickers)} minute streams: {self.tickers}")

            # 3. Consume loop
            async for msg in ws:
                try:
                    events = json.loads(msg)
                except json.JSONDecodeError:
                    continue

                if not isinstance(events, list):
                    continue

                for ev in events:
                    if ev.get("ev") == "AM":
                        await self._on_minute_bar(ev)
                    elif ev.get("ev") == "status":
                        logger.debug(f"Polygon status: {ev}")

    async def _on_minute_bar(self, ev: dict) -> None:
        """
        Polygon AM event fields:
          sym, v (volume), av (accum volume), op (open), vw (vwap),
          o (open), c (close), h (high), l (low), s (start ms), e (end ms)
        """
        try:
            start_ms = ev["s"]
            ts = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat()
            bar = {
                "symbol": ev["sym"],
                "ts": ts,
                "open": float(ev["o"]),
                "high": float(ev["h"]),
                "low": float(ev["l"]),
                "close": float(ev["c"]),
                "volume": float(ev["v"]),
                "vwap": float(ev.get("vw", ev["c"])),
            }
        except (KeyError, TypeError, ValueError) as e:
            logger.debug(f"Skipping malformed AM event: {e} / {ev}")
            return

        self.store.append_proxy_bar(bar)
        await self.state.update(bar["symbol"], bar["close"], bar["volume"], bar["ts"])
