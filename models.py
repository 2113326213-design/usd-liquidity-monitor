"""Pydantic models for API responses."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class LiquidityInstant(BaseModel):
    model_config = ConfigDict(extra="allow")

    timestamp_utc: str
    last_updated_official: dict[str, str | None] = Field(default_factory=dict)
    balances_bn: dict[str, Any]
    net_liquidity_bn: float | None
    rates: dict[str, float | None]
    sofr_minus_dff_bp: float | None
    heuristic: dict[str, Any]
    alerts: list[str]
    meta: dict[str, Any] = Field(default_factory=dict)


class Health(BaseModel):
    ok: bool
    fred_configured: bool
    message: str
