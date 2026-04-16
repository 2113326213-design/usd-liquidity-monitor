"""Treasury Fiscal Data — Daily Treasury Statement operating cash (TGA)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

# Opening TGA balance (millions USD in API integer/string form).
OPERATING_CASH = (
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
    "v1/accounting/dts/operating_cash_balance"
)


@dataclass
class TgaTreasuryRow:
    record_date: str
    """TGA opening balance, billions USD."""
    open_today_bal_bn: float | None
    close_today_bal_bn: float | None
    account_type: str
    raw: dict[str, Any]


def _parse_millions_usd_to_bn(val: Any) -> float | None:
    if val is None or val == "null":
        return None
    try:
        millions = float(str(val).replace(",", ""))
    except ValueError:
        return None
    return millions / 1000.0


async def fetch_latest_tga(client: httpx.AsyncClient) -> TgaTreasuryRow | None:
    filt = (
        "account_type:eq:Treasury General Account (TGA) Opening Balance"
    )
    r = await client.get(
        OPERATING_CASH,
        params={
            "filter": filt,
            "sort": "-record_date",
            "page[size]": "1",
            "format": "json",
        },
        timeout=30.0,
    )
    r.raise_for_status()
    rows = r.json().get("data") or []
    if not rows:
        return None
    row = rows[0]
    return TgaTreasuryRow(
        record_date=str(row.get("record_date") or ""),
        open_today_bal_bn=_parse_millions_usd_to_bn(row.get("open_today_bal")),
        close_today_bal_bn=_parse_millions_usd_to_bn(row.get("close_today_bal")),
        account_type=str(row.get("account_type") or ""),
        raw=row,
    )
