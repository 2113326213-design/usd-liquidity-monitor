"""Telegram alerter. Falls back to log-only if token not configured."""
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

LEVEL_LOG = {
    "CRITICAL": "CRITICAL",
    "HIGH": "WARNING",
    "MEDIUM": "WARNING",
    "INFO": "INFO",
}


class TelegramAlerter:
    def __init__(self) -> None:
        self.token = settings.telegram_bot_token
        self.chat_id = settings.telegram_chat_id
        self.enabled = bool(self.token and self.chat_id)
        if not self.enabled:
            logger.warning("Telegram disabled (missing token/chat_id). Alerts will be logged only.")

    async def send(self, level: str, msg: str, payload: dict | None = None) -> None:
        emoji = LEVEL_EMOJI.get(level, "")
        text = f"{emoji} *[{level}]* {msg}"
        if payload:
            snippet = json.dumps(payload, indent=2, default=str, ensure_ascii=False)
            if len(snippet) > 1500:
                snippet = snippet[:1500] + "\n... (truncated)"
            text += f"\n```\n{snippet}\n```"

        log_level = LEVEL_LOG.get(level, "INFO")
        logger.log(log_level, f"ALERT {level}: {msg}")

        if not self.enabled:
            return

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.post(url, json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                })
                if r.status_code != 200:
                    logger.error(f"Telegram send failed {r.status_code}: {r.text}")
        except Exception as e:
            logger.exception(f"Telegram send exception: {e}")
