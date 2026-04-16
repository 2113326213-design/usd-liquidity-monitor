"""Unit tests for alerts.playbook — pure functions, no network."""
from __future__ import annotations

from ..alerts.playbook import ACTIONS, format_alert, suggest_action, tier_level


# ───────────────────────── tier_level ─────────────────────────

def test_tier_level_below_direction():
    # value above all thresholds → None
    assert tier_level(3500, medium=3200, high=3000, critical=2800, direction="below") is None
    # value crosses only MEDIUM
    assert tier_level(3100, medium=3200, high=3000, critical=2800, direction="below") == "MEDIUM"
    # value crosses HIGH
    assert tier_level(2900, medium=3200, high=3000, critical=2800, direction="below") == "HIGH"
    # value crosses CRITICAL
    assert tier_level(2700, medium=3200, high=3000, critical=2800, direction="below") == "CRITICAL"


def test_tier_level_above_direction():
    assert tier_level(50, medium=150, high=200, critical=300, direction="above") is None
    assert tier_level(160, medium=150, high=200, critical=300, direction="above") == "MEDIUM"
    assert tier_level(210, medium=150, high=200, critical=300, direction="above") == "HIGH"
    assert tier_level(350, medium=150, high=200, critical=300, direction="above") == "CRITICAL"


def test_tier_level_missing_thresholds():
    # Only critical defined — medium / high skipped
    assert tier_level(100, critical=50, direction="below") is None
    assert tier_level(40, critical=50, direction="below") == "CRITICAL"


def test_tier_level_invalid_direction():
    import pytest
    with pytest.raises(ValueError):
        tier_level(1.0, medium=0.5, direction="sideways")


# ───────────────────────── suggest_action ─────────────────────────

def test_suggest_action_returns_none_for_info_or_unknown():
    assert suggest_action("INFO") is None
    assert suggest_action("NORMAL") is None
    assert suggest_action("") is None


def test_suggest_action_has_required_fields():
    for level in ("MEDIUM", "HIGH", "CRITICAL"):
        a = suggest_action(level)
        assert a is not None
        for field in (
            "reduce_equity_pct",
            "hedge_notional_pct",
            "put_dte_days",
            "put_strike",
            "review_horizon_days",
            "hedge_ticker",
            "caveat",
        ):
            assert field in a, f"{level} missing {field}"


def test_suggest_action_escalates_with_severity():
    m = suggest_action("MEDIUM")
    h = suggest_action("HIGH")
    c = suggest_action("CRITICAL")
    assert m["reduce_equity_pct"] < h["reduce_equity_pct"] < c["reduce_equity_pct"]
    assert m["hedge_notional_pct"] < h["hedge_notional_pct"] < c["hedge_notional_pct"]
    # Review horizon should get shorter as severity rises
    assert m["review_horizon_days"] > h["review_horizon_days"] >= c["review_horizon_days"]


def test_suggest_action_respects_hedge_ticker():
    a = suggest_action("HIGH", hedge_ticker="QQQ")
    assert a["hedge_ticker"] == "QQQ"
    assert "QQQ" in a["caveat"]


# ───────────────────────── format_alert ─────────────────────────

def test_format_alert_without_action():
    msg = format_alert(
        level="MEDIUM",
        title="Test title",
        metrics={"Foo": "1", "Bar": "2"},
        action=None,
    )
    assert "🟡 MEDIUM: Test title" in msg
    assert "├─ Foo: 1" in msg
    assert "└─ Bar: 2" in msg  # last metric uses └
    assert "Suggested action" not in msg


def test_format_alert_with_action():
    msg = format_alert(
        level="HIGH",
        title="Floor breach",
        metrics={"Reserves": "$2,950 bn"},
        action=suggest_action("HIGH"),
    )
    assert "🟠 HIGH: Floor breach" in msg
    assert "Reserves: $2,950 bn" in msg
    assert "Suggested action:" in msg
    assert "Reduce equity exposure: −30%" in msg
    assert "SPY put hedge" in msg
    assert "Caveat:" in msg


def test_format_alert_critical_emoji():
    msg = format_alert(
        level="CRITICAL",
        title="Crash mode",
        metrics={"x": "y"},
        action=suggest_action("CRITICAL"),
    )
    assert "🚨🔴 CRITICAL:" in msg


# ───────────────────────── ACTIONS table sanity ─────────────────────────

def test_actions_table_covers_three_levels():
    assert set(ACTIONS) == {"MEDIUM", "HIGH", "CRITICAL"}
