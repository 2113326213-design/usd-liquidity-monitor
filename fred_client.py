"""FRED API: latest observations with normalization to billions USD where applicable."""

from __future__ import annotations

import math
from dataclasses import dataclass

import httpx

FRED_BASE = "https://api.stlouisfed.org/fred"

# Default units for common series (override via /series when unknown).
_SERIES_UNITS: dict[str, str] = {
    "WALCL": "millions_usd",
    "WTREGEN": "millions_usd",
    "RRPONTTLD": "millions_usd",
    "TOTRESNS": "millions_usd",
    "CURRCIR": "billions_usd",
    "SOFR": "percent",
    "DFF": "percent",
    "SOFR99": "percent",
    "WLRRAFOIAL": "millions_usd",
    "RPONTSYD": "millions_usd",
    "VIXCLS": "index",
}


def _to_billions_usd(series_id: str, raw: float, units: str) -> float | None:
    if units in ("percent", "index"):
        return None
    if units == "billions_usd":
        return raw
    if units == "millions_usd":
        return raw / 1000.0
    return raw / 1000.0


@dataclass
class FredObservation:
    series_id: str
    date: str
    value_raw: float
    """Billions USD for balance-sheet style series; None for rates."""
    value_bn: float | None


class FredClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    async def series_units(self, client: httpx.AsyncClient, series_id: str) -> str:
        if series_id == "VIXCLS":
            return "index"
        r = await client.get(
            f"{FRED_BASE}/series",
            params={"series_id": series_id, "api_key": self.api_key, "file_type": "json"},
            timeout=30.0,
        )
        r.raise_for_status()
        data = r.json()
        ser = (data.get("seriess") or [{}])[0]
        units = (ser.get("units") or "").lower()
        if "billions" in units and "dollar" in units:
            return "billions_usd"
        if "millions" in units and "dollar" in units:
            return "millions_usd"
        if "percent" in units:
            return "percent"
        if units.strip() == "index":
            return "index"
        return "millions_usd"

    async def latest_observation(
        self,
        client: httpx.AsyncClient,
        series_id: str,
        *,
        units_hint: str | None = None,
    ) -> FredObservation | None:
        r = await client.get(
            f"{FRED_BASE}/series/observations",
            params={
                "series_id": series_id,
                "api_key": self.api_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 30,
            },
            timeout=30.0,
        )
        r.raise_for_status()
        obs = r.json().get("observations") or []
        units = units_hint or _SERIES_UNITS.get(series_id)
        if units is None:
            units = await self.series_units(client, series_id)

        for row in obs:
            v = row.get("value")
            if v in (None, ".", ""):
                continue
            try:
                val = float(v)
            except ValueError:
                continue
            if math.isnan(val):
                continue
            bn = _to_billions_usd(series_id, val, units)
            return FredObservation(
                series_id=series_id,
                date=str(row.get("date") or ""),
                value_raw=val,
                value_bn=bn,
            )
        return None

    async def recent_observations(
        self,
        client: httpx.AsyncClient,
        series_id: str,
        *,
        limit: int = 12,
        units_hint: str | None = None,
    ) -> list[FredObservation]:
        """Chronological observations (oldest → newest), skipping missing points."""
        r = await client.get(
            f"{FRED_BASE}/series/observations",
            params={
                "series_id": series_id,
                "api_key": self.api_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": max(limit, 5) + 10,
            },
            timeout=30.0,
        )
        r.raise_for_status()
        obs = r.json().get("observations") or []
        units = units_hint or _SERIES_UNITS.get(series_id)
        if units is None:
            units = await self.series_units(client, series_id)

        tmp: list[FredObservation] = []
        for row in obs:
            v = row.get("value")
            if v in (None, ".", ""):
                continue
            try:
                val = float(v)
            except ValueError:
                continue
            if math.isnan(val):
                continue
            bn = _to_billions_usd(series_id, val, units)
            tmp.append(
                FredObservation(
                    series_id=series_id,
                    date=str(row.get("date") or ""),
                    value_raw=val,
                    value_bn=bn,
                )
            )
            if len(tmp) >= limit:
                break
        tmp.reverse()
        return tmp
