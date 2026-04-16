"""Assemble regime probabilities + conditional stress→SPY hit matrix for API / UI."""

from __future__ import annotations

from typing import Any

import pandas as pd

from backtest_engine import stress_forward_hit_rate_by_regime
from history_queries import liquidity_daily_panel
from regime_detection import REGIME_ORDER, infer_regime_probabilities_rule


def build_regime_panel(
    session_factory,
    *,
    lookback_days: int = 1400,
    chart_days: int = 90,
    stress_threshold: float = 80.0,
    forward_days: int = 5,
) -> dict[str, Any]:
    df = liquidity_daily_panel(session_factory, lookback_days=lookback_days)
    if df.empty or len(df) < 50:
        return {"status": "insufficient_history", "n_rows": int(len(df))}

    df = df.dropna(subset=["reserves_bn", "rrp_bn"], how="any")
    if len(df) < 50:
        return {"status": "insufficient_reserves_rrp_history", "n_rows": int(len(df))}

    enriched = infer_regime_probabilities_rule(df)
    if "stress_score" in enriched.columns:
        stress = enriched["stress_score"].astype(float)
    else:
        stress = pd.Series(50.0, index=enriched.index)
    stress = stress.ffill().bfill()

    hits = stress_forward_hit_rate_by_regime(
        stress,
        enriched["regime_hard"].astype(str),
        stress_threshold=stress_threshold,
        forward_days=forward_days,
    )

    tail = enriched.tail(max(10, int(chart_days)))
    chart_rows: list[dict[str, Any]] = []
    for ts, row in tail.iterrows():
        d = ts.date().isoformat() if hasattr(ts, "date") else str(ts)[:10]
        chart_rows.append(
            {
                "date": d,
                **{f"p_{k}": float(row.get(f"p_{k}", 0.0)) for k in REGIME_ORDER},
                "regime_hard": str(row.get("regime_hard")),
            }
        )

    cur = enriched.iloc[-1]
    probs_now = {f"p_{k}": float(cur.get(f"p_{k}", 0.0)) for k in REGIME_ORDER}
    last_ts = enriched.index[-1]
    last_d = last_ts.date().isoformat() if hasattr(last_ts, "date") else str(last_ts)[:10]

    return {
        "status": "ok",
        "method": "rolling_rank_softmax_prototypes",
        "n_daily_rows": int(len(enriched)),
        "current": {
            "date": last_d,
            "regime_hard": str(cur.get("regime_hard")),
            **probs_now,
        },
        "conditional_hit_matrix": hits,
        "chart_series_last_days": chart_rows,
        "forward_days_used": int(forward_days),
        "stress_threshold_used": float(stress_threshold),
    }
