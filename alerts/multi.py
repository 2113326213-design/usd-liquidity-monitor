"""Fan-out alerter. Dispatches each alert to every configured channel concurrently."""
from __future__ import annotations

import asyncio
from typing import Protocol


class _Alerter(Protocol):
    async def send(self, level: str, msg: str, payload: dict | None = None) -> None: ...


class MultiAlerter:
    """Duck-type compatible with TelegramAlerter. Holds N sub-alerters and
    fans `send` out to all of them concurrently. A failure in one channel
    does not block the others (each sub-alerter already logs its own errors)."""

    def __init__(self, alerters: list[_Alerter]) -> None:
        self.alerters = alerters

    async def send(self, level: str, msg: str, payload: dict | None = None) -> None:
        if not self.alerters:
            return
        await asyncio.gather(
            *(a.send(level, msg, payload) for a in self.alerters),
            return_exceptions=True,
        )
