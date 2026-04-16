"""Webhook + Telegram push for alert fan-out (best-effort, non-blocking for core refresh)."""

from __future__ import annotations

import hashlib
import json
from typing import Any

import httpx


def _fingerprint(alerts: list[str], extras: dict[str, Any] | None = None) -> str:
    raw = json.dumps({"a": alerts, "e": extras or {}}, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


async def dispatch_alerts(
    *,
    alerts: list[str],
    payload_summary: dict[str, Any],
    webhook_url: str | None,
    telegram_bot_token: str | None,
    telegram_chat_id: str | None,
    last_fp: str | None,
) -> tuple[str | None, list[str]]:
    """
    Returns (fingerprint_state, outbound_errors).
    Skips network when there are no alerts, or when the fingerprint matches `last_fp` (dedupe).
    """
    errs: list[str] = []
    if not alerts:
        return last_fp, errs
    fp = _fingerprint(alerts, {"net": payload_summary.get("net_liquidity_bn")})
    if fp == last_fp:
        return fp, errs

    body = {
        "text": "\n".join(alerts),
        "summary": payload_summary,
    }

    sent = False
    async with httpx.AsyncClient() as client:
        if webhook_url:
            try:
                r = await client.post(webhook_url, json=body, timeout=15.0)
                if r.status_code < 400:
                    sent = True
                else:
                    errs.append(f"webhook:{r.status_code}")
            except Exception as exc:  # noqa: BLE001
                errs.append(f"webhook:{type(exc).__name__}")

        if telegram_bot_token and telegram_chat_id:
            msg = "USD Liquidity Alerts\n" + "\n".join(f"- {a}" for a in alerts)
            tg_url = f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage"
            try:
                r = await client.post(
                    tg_url,
                    json={"chat_id": telegram_chat_id, "text": msg},
                    timeout=15.0,
                )
                if r.status_code < 400:
                    sent = True
                else:
                    errs.append(f"telegram:{r.status_code}")
            except Exception as exc:  # noqa: BLE001
                errs.append(f"telegram:{type(exc).__name__}")

    configured = bool(webhook_url or (telegram_bot_token and telegram_chat_id))
    if not configured:
        return fp, errs
    if not sent:
        return last_fp, errs
    return fp, errs
