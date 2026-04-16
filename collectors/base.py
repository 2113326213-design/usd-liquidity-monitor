"""Base collector with hash-dedup polling loop."""
from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from loguru import logger

from ..alerts.telegram import TelegramAlerter
from ..storage.parquet_store import ParquetStore


class Collector(ABC):
    name: str = ""

    def __init__(self, store: ParquetStore, alerter: TelegramAlerter) -> None:
        self.store = store
        self.alerter = alerter

    @abstractmethod
    async def fetch(self) -> dict | None:
        """Return fresh payload dict, or None if no data available."""
        ...

    def _hash(self, payload: dict) -> str:
        """Hash the meaningful fields only (exclude poll_ts)."""
        content = {k: v for k, v in payload.items() if k != "poll_ts"}
        serialized = json.dumps(content, sort_keys=True, default=str)
        return hashlib.md5(serialized.encode()).hexdigest()

    async def poll(self) -> None:
        """Scheduled entry point: fetch → dedup → store → trigger."""
        try:
            payload = await self.fetch()
        except Exception as e:
            logger.exception(f"[{self.name}] fetch error: {e}")
            return

        if payload is None:
            logger.debug(f"[{self.name}] no data returned")
            return

        payload["poll_ts"] = datetime.now(timezone.utc).isoformat()
        h = self._hash(payload)
        last_h = self.store.last_hash(self.name)

        if last_h == h:
            logger.debug(f"[{self.name}] unchanged")
            return

        self.store.write_snapshot(self.name, payload, h)
        logger.info(f"[{self.name}] updated: {payload}")

        try:
            await self.on_new_data(payload)
        except Exception as e:
            logger.exception(f"[{self.name}] on_new_data hook failed: {e}")

    async def on_new_data(self, payload: dict) -> None:
        """Override in subclass to fire alerts or trigger downstream."""
        pass
