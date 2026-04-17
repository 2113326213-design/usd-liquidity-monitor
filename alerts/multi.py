"""Fan-out alerter with level-based throttling.

The MultiAlerter dispatches each alert to every configured channel
concurrently. It also suppresses near-duplicate alerts that fire within
a short time window — important because structural liquidity metrics are
correlated (Net Liquidity = Reserves + RRP − TGA) and a single real
event typically trips 2-3 thresholds at once. Without throttling you'd
get a pile of near-identical Bark pushes within seconds of each other.
"""
from __future__ import annotations

import asyncio
import time
from typing import Protocol

from loguru import logger


class _Alerter(Protocol):
    async def send(self, level: str, msg: str, payload: dict | None = None) -> None: ...


# Per-level throttle window in seconds. A new alert with the same
# (level, short-title) key within this window is suppressed.
# CRITICAL has the shortest window (you still want spam protection, but
# crisis events deserve faster re-firing); INFO heartbeats can wait.
_DEFAULT_THROTTLE_SECONDS: dict[str, int] = {
    "CRITICAL": 60,     # 1 min — crisis can re-fire after spam window
    "HIGH":     300,    # 5 min
    "MEDIUM":   600,    # 10 min
    "INFO":     3600,   # 1 hour — heartbeats don't need redundancy
}


class MultiAlerter:
    """Duck-type compatible with TelegramAlerter. Holds N sub-alerters and
    fans `send` out to all of them concurrently. Suppresses near-duplicate
    alerts via a (level, short-title) → last-sent-timestamp cache.

    Duplicate detection key = (level, first 80 chars of message). This
    catches the common case of "Reserves below floor" + "Net Liq below
    floor" firing seconds apart — they have different titles so both
    still fire (the dedup is per-title, not per-level). What it prevents
    is the exact same alert being re-emitted within a short window.

    A failure in one channel does not block the others (each sub-alerter
    already logs its own errors)."""

    def __init__(
        self,
        alerters: list[_Alerter],
        throttle_seconds: dict[str, int] | None = None,
    ) -> None:
        self.alerters = alerters
        self.throttle = dict(_DEFAULT_THROTTLE_SECONDS)
        if throttle_seconds:
            self.throttle.update(throttle_seconds)
        # (level, first-line-of-msg) → unix timestamp of last send
        self._last_sent: dict[tuple[str, str], float] = {}
        # Stats for diagnostics
        self._sent_count: int = 0
        self._throttled_count: int = 0

    def _throttle_key(self, level: str, msg: str) -> tuple[str, str]:
        """Keys on level + first line truncated to 80 chars.

        The first line of alert messages (built by alerts/playbook.py) is
        always of the form `<emoji> <LEVEL>: <human title>`, which uniquely
        identifies the *kind* of alert. Payload bodies vary hit-to-hit
        (current value, timestamps) so hashing the whole message would
        defeat dedup."""
        first_line = msg.split("\n", 1)[0][:80]
        return (level, first_line)

    def _should_throttle(self, level: str, msg: str) -> bool:
        window = self.throttle.get(level, 300)
        if window <= 0:
            return False
        key = self._throttle_key(level, msg)
        last = self._last_sent.get(key)
        if last is None:
            return False
        return (time.time() - last) < window

    async def send(self, level: str, msg: str, payload: dict | None = None) -> None:
        if not self.alerters:
            return
        if self._should_throttle(level, msg):
            self._throttled_count += 1
            logger.debug(
                f"[multialerter] throttled {level}: "
                f"{msg.split(chr(10), 1)[0][:60]}"
            )
            return

        await asyncio.gather(
            *(a.send(level, msg, payload) for a in self.alerters),
            return_exceptions=True,
        )
        self._last_sent[self._throttle_key(level, msg)] = time.time()
        self._sent_count += 1

    def stats(self) -> dict[str, int]:
        """Diagnostic counters. Useful in the weekly heartbeat."""
        return {
            "sent": self._sent_count,
            "throttled": self._throttled_count,
            "cached_keys": len(self._last_sent),
        }
