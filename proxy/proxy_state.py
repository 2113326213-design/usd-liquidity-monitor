"""
ProxyState: rolling 60-minute state per symbol, emits z-score anomaly alerts.

Logic:
- Keep last N closes per symbol in a deque
- On each new bar, compute log returns series
- Compare last 5-minute cumulative return to rolling-window std-dev
- If |z| > threshold → emit MEDIUM alert

Extended: also track aggregate "proxy stress" composite by weighting symbols.
Short-duration Treasury ETFs (BIL, SGOV) moving sharply = money-market stress.
"""
from __future__ import annotations

from collections import defaultdict, deque

import numpy as np
from loguru import logger

from ..alerts.telegram import TelegramAlerter
from ..config import settings
from ..storage.parquet_store import ParquetStore


# Weight each symbol by its sensitivity to liquidity stress
# Short-duration money-market ETFs carry the strongest signal
SYMBOL_WEIGHTS: dict[str, float] = {
    "BIL": 3.0,   # SPDR 1-3 Month T-Bill — most liquidity-sensitive
    "SGOV": 3.0,  # iShares 0-3 Month T-Bill
    "SHV": 2.0,   # iShares Short Treasury
    "SHY": 1.5,   # iShares 1-3 Year Treasury
    "IEI": 1.0,
    "IEF": 1.0,
    "TLT": 0.8,   # long-duration — less pure liquidity, more rates
    "TLH": 0.8,
}


class ProxyState:
    def __init__(self, store: ParquetStore, alerter: TelegramAlerter) -> None:
        self.store = store
        self.alerter = alerter
        self.window = settings.proxy_window_minutes
        self.z_threshold = settings.proxy_z_threshold
        self.prices: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=self.window)
        )
        self.recent_z: dict[str, float] = {}
        # Rate-limit: don't alert same symbol more than once every 10 min
        self._last_alert_ts: dict[str, float] = {}
        self._alert_cooldown_sec = 600

    async def update(self, symbol: str, close: float, volume: float, ts_iso: str) -> None:
        import time

        series = self.prices[symbol]
        series.append(close)
        if len(series) < self.window:
            return

        arr = np.array(series, dtype=float)
        # log returns (minute-to-minute)
        returns = np.diff(np.log(arr))
        if len(returns) < 10:
            return

        sigma = returns.std()
        if sigma <= 0:
            return

        recent_5 = returns[-5:].sum()
        z = float(recent_5 / sigma)
        self.recent_z[symbol] = z

        if abs(z) > self.z_threshold:
            last_alert = self._last_alert_ts.get(symbol, 0)
            if time.time() - last_alert > self._alert_cooldown_sec:
                self._last_alert_ts[symbol] = time.time()
                await self.alerter.send(
                    level="MEDIUM",
                    msg=(
                        f"Layer-3 anomaly: {symbol} 5-min cumulative return = "
                        f"{recent_5*100:+.2f}% (z={z:+.2f}σ over {self.window}-min window)"
                    ),
                    payload={
                        "symbol": symbol,
                        "close": close,
                        "volume": volume,
                        "ts": ts_iso,
                        "z_score": z,
                        "return_5min_pct": recent_5 * 100,
                    },
                )

    def composite_stress(self) -> float | None:
        """
        Weighted sum of |z| across symbols → composite proxy-stress score.
        Range roughly 0-20+ in extreme stress; >6 is elevated.
        Returns None if not enough symbols have full window.
        """
        if not self.recent_z:
            return None
        num = 0.0
        denom = 0.0
        for sym, z in self.recent_z.items():
            w = SYMBOL_WEIGHTS.get(sym, 1.0)
            num += w * abs(z)
            denom += w
        return num / denom if denom else None
