"""
Smoke tests — hit the real external APIs and verify collector.fetch() returns
a sane payload shape.

Why these exist: base.Collector.poll() catches all exceptions from fetch() and
logs them, so API endpoint drift (e.g. NY Fed deprecating a URL) fails
silently in production. These tests bypass the swallow by calling fetch()
directly and assert on payload fields, so CI (or a local `pytest` run) will
fail loudly when an upstream endpoint changes.

Run:
    cd ~/Desktop/usd_liquidity_monitor
    python3 -m pytest tests/ -v

Skip the network-dependent ones:
    python3 -m pytest tests/ -v -m "not smoke"

Notes:
- Reserves requires FRED_API_KEY in .env; the test skips if missing.
- Payload shape assertions match the columns the dashboard and
  net_liquidity calculator expect — if you change the shape, update both.
"""
from __future__ import annotations

import pytest

from ..collectors.reserves import ReservesCollector
from ..collectors.rrp import RRPCollector
from ..collectors.srp import SRPCollector
from ..collectors.tga import TGACollector
from ..config import settings


class _StoreStub:
    """Stand-in for ParquetStore. Collectors accept it but fetch() never uses it."""

    def last_hash(self, name):
        return None

    def last_snapshot(self, name, offset=0):
        return None

    def write_snapshot(self, *a, **kw):
        pass

    async def trigger(self, *a, **kw):
        pass

    def on(self, *a, **kw):
        pass


class _AlerterStub:
    async def send(self, *a, **kw):
        pass


@pytest.fixture
def store():
    return _StoreStub()


@pytest.fixture
def alerter():
    return _AlerterStub()


# ───────────────────────── TGA ─────────────────────────

@pytest.mark.smoke
@pytest.mark.asyncio
async def test_tga_fetch_returns_positive_balance(store, alerter):
    """Treasury Fiscal Data DTS — operating cash balance."""
    payload = await TGACollector(store, alerter).fetch()
    assert payload is not None, "TGA fetch returned None — endpoint/parse issue"
    assert "close_bal_bn" in payload
    # TGA runs a few hundred billion USD typically.
    assert 10 < payload["close_bal_bn"] < 3000, (
        f"TGA close_bal_bn looks wrong: {payload['close_bal_bn']}"
    )
    assert "record_date" in payload


# ───────────────────────── RRP ─────────────────────────

@pytest.mark.smoke
@pytest.mark.asyncio
async def test_rrp_fetch_returns_overnight_op(store, alerter):
    """NY Fed reverserepo/propositions/search."""
    payload = await RRPCollector(store, alerter).fetch()
    # On quiet days there may be no overnight ops — fetch may return None.
    # But the HTTP call itself must succeed; if the endpoint is broken the
    # raise_for_status() call inside fetch() would raise and escape here.
    if payload is None:
        return  # no ops in last 14 days is valid, but rare
    assert "total_accepted_bn" in payload
    assert payload["total_accepted_bn"] >= 0


# ───────────────────────── SRP ─────────────────────────

@pytest.mark.smoke
@pytest.mark.asyncio
async def test_srp_fetch_returns_latest_repo_op(store, alerter):
    """NY Fed repo/all/results/lastTwoWeeks — the post-2026 endpoint
    (the old propositions/search.json returns 400 as of 2026-04)."""
    payload = await SRPCollector(store, alerter).fetch()
    assert payload is not None, "SRP fetch returned None"
    assert "total_accepted_bn" in payload
    assert payload["total_accepted_bn"] >= 0
    # operation_date may be None on the "no recent ops" fallback, but the
    # key must be present so downstream code can branch on it.
    assert "operation_date" in payload


# ───────────────────────── Reserves ─────────────────────────

@pytest.mark.smoke
@pytest.mark.asyncio
async def test_reserves_fetch_returns_wresbal(store, alerter):
    """FRED WRESBAL — weekly bank reserves."""
    if not settings.fred_api_key:
        pytest.skip("FRED_API_KEY not set")
    payload = await ReservesCollector(store, alerter).fetch()
    assert payload is not None, "Reserves fetch returned None with a key configured"
    assert "reserves_bn" in payload
    # Reserves run ~$3T these days; lower bound generous for past values.
    assert 1000 < payload["reserves_bn"] < 10000, (
        f"Reserves value looks wrong: {payload['reserves_bn']}"
    )
    assert "observation_date" in payload
