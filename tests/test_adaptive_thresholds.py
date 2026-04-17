"""Unit tests for state.adaptive_thresholds — pure computation, no network."""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..state.adaptive_thresholds import (
    REGIMES,
    TIER_PERCENTILES,
    compute_adaptive_thresholds,
)


def _make_regime_df(
    n_per_regime: int = 100,
    seed: int = 42,
) -> pd.DataFrame:
    """Synthetic regime-labeled series. Each regime has a distinct
    reserves distribution so we can predict its percentile thresholds."""
    rng = np.random.default_rng(seed)
    rows = []
    # Mean Reserves per regime (billions)
    mean_by_regime = {
        "abundant": 3600,
        "ample":    3200,
        "scarce":   2900,
        "crisis":   2700,
    }
    for regime, mean in mean_by_regime.items():
        for _ in range(n_per_regime):
            rows.append({
                "as_of": "2024-01-01",  # doesn't matter for threshold calc
                "reserves_bn": float(rng.normal(mean, 50)),
                "rrp_bn": float(max(0, rng.normal(100, 30))),
                "net_liquidity_bn": float(rng.normal(mean + 100, 80)),
                "regime_hard": regime,
            })
    return pd.DataFrame(rows)


def test_compute_returns_all_metrics_all_regimes():
    df = _make_regime_df()
    out = compute_adaptive_thresholds(df)
    # Three metrics × four regimes × three tiers
    assert set(out.keys()) == {"reserves", "rrp", "net_liq"}
    for metric in out:
        assert set(out[metric].keys()) == set(REGIMES)
        for regime in REGIMES:
            assert set(out[metric][regime].keys()) == set(TIER_PERCENTILES.keys())


def test_tiers_ordered_by_percentile_within_regime():
    """MEDIUM > HIGH > CRITICAL numerically for 'below is worse' metrics."""
    df = _make_regime_df()
    out = compute_adaptive_thresholds(df)
    for metric in ("reserves", "net_liq"):
        for regime in REGIMES:
            t = out[metric][regime]
            if any(v is None for v in t.values()):
                continue
            assert t["MEDIUM"] >= t["HIGH"] >= t["CRITICAL"], (
                f"{metric}/{regime} tiers out of order: {t}"
            )


def test_abundant_thresholds_higher_than_crisis():
    """Same tier: abundant Reserves > crisis Reserves (because abundant
    regime historically had higher Reserves)."""
    df = _make_regime_df()
    out = compute_adaptive_thresholds(df)
    for tier in TIER_PERCENTILES:
        assert out["reserves"]["abundant"][tier] > out["reserves"]["crisis"][tier]
        assert out["reserves"]["ample"][tier] > out["reserves"]["scarce"][tier]


def test_small_sample_returns_none():
    """< 20 samples for a (metric, regime) → None for all tiers."""
    small = pd.DataFrame({
        "reserves_bn": [3200.0] * 15,
        "rrp_bn":      [100.0] * 15,
        "net_liquidity_bn": [2500.0] * 15,
        "regime_hard": ["abundant"] * 15,
    })
    out = compute_adaptive_thresholds(small)
    # abundant has only 15 samples → all tiers should be None
    for tier in TIER_PERCENTILES:
        assert out["reserves"]["abundant"][tier] is None
    # other regimes have 0 samples → also None
    for regime in ("ample", "scarce", "crisis"):
        for tier in TIER_PERCENTILES:
            assert out["reserves"][regime][tier] is None


def test_empty_df_returns_empty_dict():
    assert compute_adaptive_thresholds(pd.DataFrame()) == {}


def test_percentile_values_make_sense():
    """For a known distribution, verify percentiles are in expected range."""
    # Construct a pure uniform[0, 100] for one regime
    df = pd.DataFrame({
        "reserves_bn": np.linspace(0, 100, 500),
        "rrp_bn":      [50.0] * 500,
        "net_liquidity_bn": [2500.0] * 500,
        "regime_hard": ["ample"] * 500,
    })
    out = compute_adaptive_thresholds(df)
    r = out["reserves"]["ample"]
    # 25th percentile of uniform(0,100) ≈ 25
    assert 20 <= r["MEDIUM"] <= 30
    # 10th ≈ 10
    assert 5 <= r["HIGH"] <= 15
    # 5th ≈ 5
    assert 2 <= r["CRITICAL"] <= 10
