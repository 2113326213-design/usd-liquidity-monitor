"""Unit tests for state.regime — pure functions, no network."""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..state.regime import (
    REGIME_ORDER,
    _rolling_rank_norm,
    infer_regime_probabilities_rule,
)


def _synthetic_series(reserves_path: list[float], rrp_path: list[float]) -> pd.DataFrame:
    """Build a daily-indexed DataFrame suitable for the classifier."""
    dates = pd.date_range("2020-01-01", periods=len(reserves_path), freq="D")
    return pd.DataFrame({
        "as_of": dates.astype(str),
        "reserves_bn": reserves_path,
        "rrp_bn": rrp_path,
        "net_liquidity_bn": np.array(reserves_path) + np.array(rrp_path),
    })


# ───────────────────────── _rolling_rank_norm ─────────────────────────

def test_rank_norm_monotone():
    # Strictly increasing values → last row rank == 1.0
    s = pd.Series(range(1, 101), dtype=float)
    r = _rolling_rank_norm(s, window=50)
    assert abs(r.iloc[-1] - 1.0) < 1e-9


def test_rank_norm_decreasing_ends_at_low_rank():
    s = pd.Series(range(100, 0, -1), dtype=float)
    r = _rolling_rank_norm(s, window=50)
    assert r.iloc[-1] < 0.1


def test_rank_norm_empty_safe():
    r = _rolling_rank_norm(pd.Series([], dtype=float))
    assert r.empty


# ───────────────────────── infer_regime_probabilities_rule ─────────────

def _make_regime_test_series(reserves_level: float, rrp_level: float) -> pd.DataFrame:
    """Build a 300-day DataFrame where the last row sits at the target
    (reserves_level, rrp_level) inside the rolling-rank distribution."""
    n = 300
    np.random.seed(42)
    # Build distributions such that the last value has the target rank
    def tail_at_rank(target_rank: float) -> list[float]:
        # Generate 299 i.i.d. N(0, 1), then set the last value to the
        # `target_rank` percentile of the distribution.
        baseline = np.random.randn(n - 1)
        tail_value = np.quantile(baseline, target_rank)
        return list(baseline) + [float(tail_value)]

    res = tail_at_rank(reserves_level)
    rrp = tail_at_rank(rrp_level)
    return _synthetic_series(res, rrp)


def test_abundant_corner():
    # Both reserves and rrp at high rank → abundant
    df = _make_regime_test_series(reserves_level=0.90, rrp_level=0.85)
    out = infer_regime_probabilities_rule(df)
    assert not out.empty
    latest = out.iloc[-1]
    assert latest["regime_hard"] == "abundant", (
        f"expected abundant, got {latest['regime_hard']} "
        f"(p_abundant={latest['p_abundant']:.3f}, p_ample={latest['p_ample']:.3f})"
    )


def test_crisis_corner():
    df = _make_regime_test_series(reserves_level=0.10, rrp_level=0.10)
    out = infer_regime_probabilities_rule(df)
    latest = out.iloc[-1]
    assert latest["regime_hard"] == "crisis"


def test_scarce_corner():
    df = _make_regime_test_series(reserves_level=0.35, rrp_level=0.30)
    out = infer_regime_probabilities_rule(df)
    latest = out.iloc[-1]
    assert latest["regime_hard"] == "scarce"


def test_ample_corner():
    df = _make_regime_test_series(reserves_level=0.60, rrp_level=0.55)
    out = infer_regime_probabilities_rule(df)
    latest = out.iloc[-1]
    assert latest["regime_hard"] == "ample"


def test_probabilities_sum_to_one():
    df = _make_regime_test_series(reserves_level=0.55, rrp_level=0.50)
    out = infer_regime_probabilities_rule(df)
    row = out.iloc[-1]
    total = sum(row[f"p_{name}"] for name in REGIME_ORDER)
    assert abs(total - 1.0) < 1e-6


def test_missing_columns_returns_empty():
    bad = pd.DataFrame({"as_of": ["2024-01-01"], "foo": [1.0]})
    out = infer_regime_probabilities_rule(bad)
    assert out.empty


def test_regime_order_is_stable():
    assert REGIME_ORDER == ("abundant", "ample", "scarce", "crisis")
