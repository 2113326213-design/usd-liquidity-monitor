"""Bark (iOS) alerter. Falls back to log-only if device key not configured."""
from __future__ import annotations

import json
from typing import Any

import httpx
from loguru import logger

from ..config import settings


LEVEL_EMOJI = {
    "CRITICAL": "🚨🔴",
    "HIGH": "🟠",
    "MEDIUM": "🟡",
    "INFO": "🔵",
}

# Map our severity to Bark's interruption level.
# critical      -> breaks through Focus / silent mode (iOS 15+, requires permission)
# timeSensitive -> elevated, surfaces in Focus summary
# active        -> default banner + sound
# passive       -> delivered silently to notification list
LEVEL_BARK = {
    "CRITICAL": "critical",
    "HIGH": "timeSensitive",
    "MEDIUM": "active",
    "INFO": "passive",
}

LEVEL_LOG = {
    "CRITICAL": "CRITICAL",
    "HIGH": "WARNING",
    "MEDIUM": "WARNING",
    "INFO": "INFO",
}


class BarkAlerter:
    def __init__(self) -> None:
        self.device_key = settings.bark_device_key
        self.server_url = settings.bark_server_url.rstrip("/")
        self.enabled = bool(self.device_key and self.server_url)
        if not self.enabled:
            logger.warning("Bark disabled (missing device_key/server_url). Alerts will be logged only.")

    async def send(self, level: str, msg: str, payload: dict | None = None) -> None:
        emoji = LEVEL_EMOJI.get(level, "")
        title = f"{emoji} [{level}] USD Liquidity"
        body = msg
        if payload:
            snippet = json.dumps(payload, indent=2, default=str, ensure_ascii=False)
            if len(snippet) > 1500:
                snippet = snippet[:1500] + "\n... (truncated)"
            body += f"\n{snippet}"

        log_level = LEVEL_LOG.get(level, "INFO")
        logger.log(log_level, f"ALERT {level}: {msg}")

        if not self.enabled:
            return

        url = f"{self.server_url}/push"
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.post(url, json={
                    "device_key": self.device_key,
                    "title": title,
                    "body": body,
                    "level": LEVEL_BARK.get(level, "active"),
                    "group": "usd-liquidity",
                })
                if r.status_code != 200:
                    logger.error(f"Bark send failed {r.status_code}: {r.text}")
        except Exception as e:
            logger.exception(f"Bark send exception: {e}")
