"""QRA PDF refresh: download, parse End-of-quarter cash balance, persist snapshot."""

from __future__ import annotations

import io
import re
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from config import Settings
from db import QraSnapshot


def parse_qra_pdf_bytes(data: bytes) -> dict[str, Any]:
    """
    Extract Treasury QRA **end-of-quarter cash balance** (billions) from raw PDF bytes.
    Shared by `refresh_qra_from_pdf` and `scripts/fetch_qra_cash_balance.py`.
    """
    try:
        import pdfplumber
    except ImportError:
        return {
            "status": "error",
            "reason": "pdfplumber_not_installed",
            "parsed_end_quarter_cash_bn": None,
            "snippet": None,
        }

    text = ""
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for page in pdf.pages:
            text += (page.extract_text() or "") + "\n"

    patterns = [
        re.compile(
            r"End[-\s]+of[-\s]+quarter[-\s]+cash[-\s]+balance.*?\$?\s*([\d,\.]+)\s*billion",
            re.I | re.S,
        ),
        re.compile(r"end[-\s]+of[-\s]+quarter.*?(\d[\d,\.]*)\s*billion", re.I | re.S),
    ]
    m = None
    for pat in patterns:
        m = pat.search(text)
        if m:
            break

    val: float | None = None
    snippet: str | None = None
    if m:
        try:
            val = float(m.group(1).replace(",", ""))
            snippet = text[max(0, m.start() - 120) : m.end() + 120]
        except (ValueError, IndexError):
            val = None

    return {
        "status": "ok" if val is not None else "parse_failed",
        "parsed_end_quarter_cash_bn": val,
        "snippet": snippet,
    }


def fetch_latest_qra_snapshot(session_factory: sessionmaker) -> QraSnapshot | None:
    with session_factory() as s:
        return s.execute(select(QraSnapshot).order_by(QraSnapshot.ts_utc.desc()).limit(1)).scalar_one_or_none()


async def refresh_qra_from_pdf(session_factory: sessionmaker, settings: Settings) -> dict[str, Any]:
    url = (settings.qra_pdf_url or "").strip()
    if not url:
        return {"status": "skipped", "reason": "QRA_PDF_URL not set"}

    async with httpx.AsyncClient() as client:
        r = await client.get(url, timeout=120.0, follow_redirects=True)
        r.raise_for_status()
        data = r.content

    parsed = parse_qra_pdf_bytes(data)
    if parsed.get("status") == "error" and parsed.get("reason") == "pdfplumber_not_installed":
        return {"status": "error", "reason": "pdfplumber_not_installed"}
    val = parsed.get("parsed_end_quarter_cash_bn")
    snippet = parsed.get("snippet")
    if not isinstance(val, (int, float)):
        val = None

    prev = fetch_latest_qra_snapshot(session_factory)
    critical = False
    if val is not None:
        if prev is None or prev.end_quarter_cash_balance_bn is None:
            critical = True
        else:
            critical = abs(val - float(prev.end_quarter_cash_balance_bn)) > 0.05

    if val is not None:
        snap = QraSnapshot(
            ts_utc=datetime.now(timezone.utc),
            source_url=url[:500],
            end_quarter_cash_balance_bn=val,
            quarter_label=None,
            snippet=snippet,
        )
        with session_factory() as s:
            s.add(snap)
            s.commit()

    out = {
        "status": "ok" if val is not None else parsed.get("status", "parse_failed"),
        "parsed_end_quarter_cash_bn": val,
        "critical_config_update": critical and val is not None,
        "snippet": snippet,
    }
    if parsed.get("status") == "error":
        out["status"] = "error"
        out["reason"] = parsed.get("reason")
    return out
