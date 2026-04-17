"""Unit tests for state.fed_reaction — pure rule evaluation, no network."""
from __future__ import annotations

from datetime import date

from ..state.fed_reaction import (
    FOMC_DATES,
    compute_intervention_probability,
    days_until_next_fomc,
)


# ───────────────────────── FOMC calendar ─────────────────────────

def test_fomc_dates_are_sorted():
    assert list(FOMC_DATES) == sorted(FOMC_DATES)


def test_days_until_next_fomc_future():
    # Use a known-in-calendar date
    today = date(2025, 1, 1)
    days = days_until_next_fomc(today)
    assert days is not None
    assert days == (date(2025, 1, 29) - today).days


def test_days_until_next_fomc_on_fomc_day():
    # If today IS a FOMC day, days = 0
    today = date(2025, 1, 29)
    assert days_until_next_fomc(today) == 0


def test_days_until_next_fomc_beyond_calendar():
    # A date past our last hardcoded FOMC → None
    past_cal = date(2030, 1, 1)
    assert days_until_next_fomc(past_cal) is None


# ───────────────────────── Rule-based probability ─────────────────

def test_srp_active_dominates():
    """SRP non-zero should set P(5d) ~= 0.95 regardless of other signals."""
    state = {"srp_active": True}
    r = compute_intervention_probability(state)
    assert r["top_rule"] == "SRP_ACTIVE"
    assert r["p_5d"] >= 0.9


def test_baseline_when_nothing_triggered():
    """No rule matches → baseline low probability."""
    state = {
        "srp_active": False,
        "sofr_iorb_bp": -5,
        "reserves_bn": 3500,
        "net_liquidity_bn": 3000,
        "auction_tail_bp": -1,
        "market_stress_z": 0.1,
        "days_to_fomc": 30,
    }
    r = compute_intervention_probability(state)
    assert r["top_rule"] == "BASELINE"
    assert r["p_5d"] <= 0.10


def test_compound_severe_beats_isolated_funding_stress():
    """SOFR-IORB >20bp + reserves <$3T ranks higher than SOFR-IORB >10bp alone."""
    compound_state = {"sofr_iorb_bp": 25, "reserves_bn": 2900}
    r = compute_intervention_probability(compound_state)
    assert r["top_rule"] == "SEVERE_COMPOUND"
    assert r["p_5d"] >= 0.80


def test_isolated_funding_stress():
    state = {"sofr_iorb_bp": 12, "reserves_bn": 3500}
    r = compute_intervention_probability(state)
    assert r["top_rule"] == "FUNDING_STRESS"
    assert 0.5 <= r["p_5d"] <= 0.7


def test_net_liquidity_crisis():
    state = {
        "srp_active": False,
        "sofr_iorb_bp": -5,  # normal
        "net_liquidity_bn": 1900,  # below CRITICAL
    }
    r = compute_intervention_probability(state)
    assert r["top_rule"] == "NETLIQ_CRITICAL"
    assert r["p_5d"] >= 0.6


def test_tail_plus_scarce():
    state = {
        "srp_active": False,
        "sofr_iorb_bp": -5,
        "reserves_bn": 2900,
        "net_liquidity_bn": 2500,  # above NetLiq CRITICAL
        "auction_tail_bp": 8,
    }
    r = compute_intervention_probability(state)
    assert r["top_rule"] == "TAIL_PLUS_SCARCE"


def test_pre_fomc_market_stress():
    state = {
        "srp_active": False,
        "sofr_iorb_bp": 0,
        "reserves_bn": 3200,
        "net_liquidity_bn": 2500,
        "auction_tail_bp": 0,
        "market_stress_z": 4.5,
        "days_to_fomc": 5,
    }
    r = compute_intervention_probability(state)
    assert r["top_rule"] == "PRE_FOMC_MARKET_STRESS"


def test_reserves_critical_standalone():
    state = {
        "srp_active": False,
        "sofr_iorb_bp": -3,     # normal
        "reserves_bn": 2700,    # below CRITICAL
        "net_liquidity_bn": 2200,  # above NetLiq CRITICAL
        "auction_tail_bp": 0,
        "market_stress_z": 1,
        "days_to_fomc": 30,
    }
    r = compute_intervention_probability(state)
    assert r["top_rule"] == "RESERVES_CRITICAL"
    assert 0.4 <= r["p_5d"] <= 0.55


def test_probabilities_ordered_by_horizon():
    """For every triggered rule, P(5d) <= P(10d) <= P(30d)."""
    scenarios = [
        {"srp_active": True},
        {"sofr_iorb_bp": 25, "reserves_bn": 2900},
        {"sofr_iorb_bp": 12},
        {"net_liquidity_bn": 1950},
        {"auction_tail_bp": 8, "reserves_bn": 2950},
        {"market_stress_z": 5, "days_to_fomc": 7},
        {"reserves_bn": 2700},
        {},  # baseline
    ]
    for state in scenarios:
        r = compute_intervention_probability(state)
        assert r["p_5d"] <= r["p_10d"] <= r["p_30d"] + 1e-6, (
            f"monotonicity failed for state={state}: {r}"
        )


def test_none_values_dont_crash():
    """All inputs None should fall through to baseline without error."""
    state = {
        "srp_active": None,
        "sofr_iorb_bp": None,
        "reserves_bn": None,
        "net_liquidity_bn": None,
        "auction_tail_bp": None,
        "market_stress_z": None,
        "days_to_fomc": None,
    }
    r = compute_intervention_probability(state)
    # srp_active None is treated as falsy, rest are None and gt/lt returns False
    assert r["top_rule"] == "BASELINE"


def test_rule_has_historical_anchor():
    """Every rule should carry a non-empty historical calibration string."""
    # Trigger all rules across many scenarios and verify `top_rule_historical`
    states = [
        {"srp_active": True},
        {"sofr_iorb_bp": 25, "reserves_bn": 2900},
        {"sofr_iorb_bp": 12},
        {"net_liquidity_bn": 1950},
        {"auction_tail_bp": 8, "reserves_bn": 2950},
        {"market_stress_z": 5, "days_to_fomc": 7},
        {"reserves_bn": 2700},
        {},
    ]
    for state in states:
        r = compute_intervention_probability(state)
        assert r["top_rule_historical"], f"rule {r['top_rule']} has empty historical"
