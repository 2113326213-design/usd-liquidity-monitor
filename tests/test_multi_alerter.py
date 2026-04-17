"""Unit tests for MultiAlerter throttle/dedup logic and sanity bounds."""
from __future__ import annotations

import time

import pytest

from ..alerts.multi import MultiAlerter
from ..alerts.sanity import sanity_check


# ───────────────────────── Stub alerter ─────────────────────────

class _Recording:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    async def send(self, level: str, msg: str, payload=None):
        self.calls.append((level, msg))


# ───────────────────────── MultiAlerter.send ─────────────────────────

@pytest.mark.asyncio
async def test_send_fans_out_to_all_alerters():
    a, b = _Recording(), _Recording()
    multi = MultiAlerter([a, b])
    await multi.send("HIGH", "🟠 HIGH: Test")
    assert len(a.calls) == 1
    assert len(b.calls) == 1


@pytest.mark.asyncio
async def test_no_alerters_is_noop():
    multi = MultiAlerter([])
    await multi.send("HIGH", "test")  # must not raise


# ───────────────────────── Throttle by (level, title) ─────────────────

@pytest.mark.asyncio
async def test_throttle_suppresses_duplicate_within_window():
    a = _Recording()
    multi = MultiAlerter([a], throttle_seconds={"HIGH": 300})
    await multi.send("HIGH", "🟠 HIGH: Reserves approaching floor\nline 2")
    await multi.send("HIGH", "🟠 HIGH: Reserves approaching floor\nline 2 different")
    assert len(a.calls) == 1  # second call suppressed (same title)


@pytest.mark.asyncio
async def test_different_titles_both_fire():
    a = _Recording()
    multi = MultiAlerter([a])
    await multi.send("HIGH", "🟠 HIGH: Reserves approaching floor")
    await multi.send("HIGH", "🟠 HIGH: ON RRP drain")
    assert len(a.calls) == 2  # different titles = not duplicates


@pytest.mark.asyncio
async def test_different_levels_both_fire():
    a = _Recording()
    multi = MultiAlerter([a])
    # Same title but different level — both fire
    await multi.send("MEDIUM", "Reserves approaching floor")
    await multi.send("HIGH", "Reserves approaching floor")
    assert len(a.calls) == 2


@pytest.mark.asyncio
async def test_throttle_zero_disables():
    a = _Recording()
    multi = MultiAlerter([a], throttle_seconds={"HIGH": 0})
    await multi.send("HIGH", "test")
    await multi.send("HIGH", "test")
    assert len(a.calls) == 2


@pytest.mark.asyncio
async def test_throttle_respects_time_window():
    a = _Recording()
    multi = MultiAlerter([a], throttle_seconds={"HIGH": 1})
    await multi.send("HIGH", "test")
    assert len(a.calls) == 1
    # Fake time advance by back-dating the cache entry
    key = multi._throttle_key("HIGH", "test")
    multi._last_sent[key] = time.time() - 10  # 10 s ago, > 1 s window
    await multi.send("HIGH", "test")
    assert len(a.calls) == 2  # window expired, fires again


@pytest.mark.asyncio
async def test_stats_counter():
    a = _Recording()
    multi = MultiAlerter([a])
    await multi.send("HIGH", "fire 1")
    await multi.send("HIGH", "fire 1")  # throttled
    await multi.send("HIGH", "fire 2")
    s = multi.stats()
    assert s["sent"] == 2
    assert s["throttled"] == 1


# ───────────────────────── sanity_check ─────────────────────────

def test_sanity_within_bounds():
    assert sanity_check("tga_bn", 780) is True
    assert sanity_check("reserves_bn", 3100) is True
    assert sanity_check("rrp_bn", 0.2) is True
    assert sanity_check("composite_stress_z", -0.5) is True
    assert sanity_check("tail_bp", 2.5) is True


def test_sanity_rejects_none():
    assert sanity_check("tga_bn", None) is False


def test_sanity_rejects_non_numeric():
    assert sanity_check("tga_bn", "abc") is False


def test_sanity_rejects_below_min():
    assert sanity_check("tga_bn", 0) is False  # below 50
    assert sanity_check("reserves_bn", 100) is False  # below 500


def test_sanity_rejects_above_max():
    assert sanity_check("tga_bn", 10_000) is False  # above 2000
    assert sanity_check("composite_stress_z", 50) is False  # above 20


def test_sanity_permissive_for_unknown_metric():
    # Unknown metric name — warns but doesn't reject
    assert sanity_check("foo_bar", 42) is True
