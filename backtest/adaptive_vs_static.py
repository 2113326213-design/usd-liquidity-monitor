"""
Adaptive-vs-static walk-forward validation.

Tests the hypothesis: do regime-conditional (adaptive) alert thresholds
produce a better signal than fixed (static) thresholds?

Method
------
1. Split the 5y of daily Net Liquidity history into TRAIN (first 60%)
   and TEST (last 40%). TRAIN provides the per-regime percentiles;
   TEST is where we measure out-of-sample signal quality.
2. On TEST, walk each day forward. For each metric and alert tier:
     - STATIC: fire when value crosses below settings.<metric>_<tier>_bn
     - ADAPTIVE: fire when value crosses below the TRAIN-derived
       percentile for that date's regime
3. For each fired alert, pull SPY forward return (5 / 10 / 20 days).
4. Report both conditional hit rates + difference.

Why TRAIN/TEST split
--------------------
Without it, we'd be computing adaptive thresholds from the same data we
test on — pure look-ahead bias. A 60/40 time-ordered split is the
simplest honest protocol given data volume (5y ≈ 1260 trading days →
test has ~500 days).

What this does NOT prove
------------------------
* Does not tell you whether adaptive would have fired on real future
  events — only on the last 40% of the observed window.
* 500-day test is still small for CRITICAL tier (typical 0-2 events).
* Regime classifier is itself fit post-hoc (prototypes chosen by hand).

Output
------
backtest/adaptive_vs_static_report.md
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

from ..config import settings
from ..state.adaptive_thresholds import (
    REGIMES,
    TIER_PERCENTILES,
    compute_adaptive_thresholds,
)
from .walk_forward import (
    FORWARD_HORIZONS_DAYS,
    event_return_stats,
    load_spy,
)


DATA_DIR = settings.data_dir / "raw"
REPORT_PATH = Path(__file__).parent / "adaptive_vs_static_report.md"
TRAIN_FRACTION = 0.60


# ───────────────────────── Dynamic-threshold crossing ──────────────────

def first_crossings_below_dynamic(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    regime_col: str,
    thresholds_by_regime: dict[str, float | None],
) -> list[pd.Timestamp]:
    """Detect first-crossings where the threshold depends on the regime
    at each date.

    An event fires on date D when:
        - regime(D) has a defined threshold (not None)
        - value(D) < threshold(regime(D))
        - AND the previous day either (a) did not cross, or (b) had a
          different regime whose threshold was higher and wasn't crossed

    Simpler formulation: fire when "value below its current regime's
    threshold" goes from False to True day-over-day. Handles the case
    where a regime change alone triggers a fire (moving from a loose
    threshold to a tight one with the same value)."""
    if df.empty:
        return []
    s = df.sort_values(date_col).reset_index(drop=True).copy()
    s["_threshold"] = s[regime_col].map(thresholds_by_regime)
    s["_below"] = (s[value_col].astype(float) < s["_threshold"].astype(float))
    # treat None threshold rows as "not below" for crossing purposes
    s.loc[s["_threshold"].isna(), "_below"] = False

    crossings: list[pd.Timestamp] = []
    was_below = False
    for i in range(len(s)):
        is_below = bool(s["_below"].iloc[i])
        if is_below and not was_below:
            crossings.append(pd.Timestamp(s[date_col].iloc[i]))
        was_below = is_below
    return crossings


def first_crossings_below_static(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    threshold: float,
) -> list[pd.Timestamp]:
    """Mirror of the dynamic version, with a single fixed threshold."""
    if df.empty:
        return []
    s = df.sort_values(date_col).reset_index(drop=True)
    below = (s[value_col].astype(float) < threshold).to_numpy()
    dates = pd.to_datetime(s[date_col]).dt.tz_localize(None).dt.normalize().to_numpy()
    crossings: list[pd.Timestamp] = []
    for i in range(1, len(below)):
        if below[i] and not below[i - 1]:
            crossings.append(pd.Timestamp(dates[i]))
    return crossings


# ───────────────────────── Main ─────────────────────────

STATIC_TIERS = {
    "reserves": {
        "MEDIUM":   settings.reserves_medium_bn,
        "HIGH":     settings.reserves_high_bn,
        "CRITICAL": settings.reserves_critical_bn,
    },
    "rrp": {
        "MEDIUM":   settings.rrp_medium_bn,
        "HIGH":     settings.rrp_high_bn,
        "CRITICAL": settings.rrp_critical_bn,
    },
    "net_liq": {
        "MEDIUM":   settings.net_liq_medium_bn,
        "HIGH":     settings.net_liq_high_bn,
        "CRITICAL": settings.net_liq_critical_bn,
    },
}


METRIC_TO_COL = {
    "reserves": "reserves_bn",
    "rrp":      "rrp_bn",
    "net_liq":  "net_liquidity_bn",
}


def run() -> dict:
    """Compute static vs adaptive hit-rate comparison on test window."""
    regime_df = pd.read_parquet(DATA_DIR / "regime.parquet").sort_values("as_of").reset_index(drop=True)
    if len(regime_df) < 100:
        logger.error("[adaptive_vs_static] need at least 100 regime rows")
        return {}

    spy = load_spy()

    # 60/40 time-ordered split
    split_idx = int(len(regime_df) * TRAIN_FRACTION)
    train = regime_df.iloc[:split_idx].copy()
    test = regime_df.iloc[split_idx:].copy()
    train_start = train["as_of"].iloc[0]
    train_end = train["as_of"].iloc[-1]
    test_start = test["as_of"].iloc[0]
    test_end = test["as_of"].iloc[-1]
    logger.info(f"[adaptive_vs_static] TRAIN {train_start} → {train_end} ({len(train)} days)")
    logger.info(f"[adaptive_vs_static] TEST  {test_start} → {test_end} ({len(test)} days)")

    # Fit adaptive thresholds on TRAIN only
    adaptive = compute_adaptive_thresholds(train)

    results: dict = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "train_window": {"start": str(train_start), "end": str(train_end), "days": len(train)},
        "test_window":  {"start": str(test_start),  "end": str(test_end),  "days": len(test)},
        "adaptive_thresholds_from_train": adaptive,
        "comparison": {},
    }

    # For every metric × tier: compute events on TEST with both methods
    for metric_short, metric_col in METRIC_TO_COL.items():
        for tier in TIER_PERCENTILES:
            key = f"{metric_short}_{tier}"

            # STATIC
            static_events = first_crossings_below_static(
                test, "as_of", metric_col,
                STATIC_TIERS[metric_short][tier],
            )
            static_stats = event_return_stats(static_events, spy)

            # ADAPTIVE (threshold depends on regime at each date)
            adaptive_map = {
                regime: (adaptive[metric_short][regime][tier]
                          if adaptive[metric_short][regime][tier] is not None else None)
                for regime in REGIMES
            }
            adaptive_events = first_crossings_below_dynamic(
                test, "as_of", metric_col, "regime_hard", adaptive_map,
            )
            adaptive_stats = event_return_stats(adaptive_events, spy)

            results["comparison"][key] = {
                "static": {
                    "threshold": STATIC_TIERS[metric_short][tier],
                    "events": len(static_events),
                    "stats": static_stats,
                },
                "adaptive": {
                    "thresholds_by_regime": adaptive_map,
                    "events": len(adaptive_events),
                    "stats": adaptive_stats,
                },
            }

    return results


def build_report(results: dict) -> str:
    lines: list[str] = []
    lines.append("# Adaptive vs Static Threshold Comparison")
    lines.append("")
    lines.append(f"_Generated: {results['generated_utc']}_")
    lines.append("")
    lines.append(f"**TRAIN**: {results['train_window']['start']} → {results['train_window']['end']} ({results['train_window']['days']} days)")
    lines.append(f"**TEST**:  {results['test_window']['start']} → {results['test_window']['end']} ({results['test_window']['days']} days)")
    lines.append("")
    lines.append("Adaptive thresholds fitted on TRAIN only — tested out-of-sample on TEST.")
    lines.append("")
    lines.append("## Comparison: SPY 10-day forward hit rate per alert")
    lines.append("")
    lines.append("| Alert | Static thresh | Static N | Static P(down) | Adaptive N | Adaptive P(down) | Δ P(down) | Verdict |")
    lines.append("|---|---|---|---|---|---|---|---|")

    comparison = results["comparison"]
    for key, row in comparison.items():
        static_10d = row["static"]["stats"]["10d"]
        adaptive_10d = row["adaptive"]["stats"]["10d"]
        s_hit = static_10d["hit_rate_down"]
        a_hit = adaptive_10d["hit_rate_down"]
        s_n = static_10d["n"]
        a_n = adaptive_10d["n"]

        # Verdict
        if s_n == 0 and a_n == 0:
            verdict = "⚪ neither fired"
        elif a_n == 0:
            verdict = "⚫ adaptive silent"
        elif s_n == 0:
            verdict = "🟢 adaptive catches events static missed"
        else:
            delta = (a_hit or 0) - (s_hit or 0)
            if abs(delta) < 0.05:
                verdict = "🟡 no meaningful difference"
            elif delta > 0.05:
                verdict = f"🟢 adaptive +{delta*100:.1f} pp better"
            else:
                verdict = f"🔴 adaptive {delta*100:.1f} pp worse"

        s_hit_s = f"{s_hit:.1%}" if s_hit is not None else "—"
        a_hit_s = f"{a_hit:.1%}" if a_hit is not None else "—"
        delta_s = "—"
        if s_hit is not None and a_hit is not None:
            delta_s = f"{(a_hit - s_hit) * 100:+.1f} pp"
        lines.append(
            f"| {key} | {row['static']['threshold']:.0f} | {s_n} | {s_hit_s} "
            f"| {a_n} | {a_hit_s} | {delta_s} | {verdict} |"
        )

    # Aggregate verdict
    lines.append("")
    lines.append("## Aggregate verdict across structural alerts")
    lines.append("")

    def _hit_mean(method: str) -> float | None:
        vals: list[float] = []
        for row in comparison.values():
            h = row[method]["stats"]["10d"]["hit_rate_down"]
            if h is not None and row[method]["stats"]["10d"]["n"] > 0:
                vals.append(h)
        return float(np.mean(vals)) if vals else None

    static_mean = _hit_mean("static")
    adaptive_mean = _hit_mean("adaptive")

    lines.append(f"- Static avg 10d P(down) across fired alerts:   {static_mean:.1%}" if static_mean is not None else "- Static: no events")
    lines.append(f"- Adaptive avg 10d P(down) across fired alerts: {adaptive_mean:.1%}" if adaptive_mean is not None else "- Adaptive: no events")

    if static_mean is not None and adaptive_mean is not None:
        delta = adaptive_mean - static_mean
        if abs(delta) < 0.03:
            lines.append(f"- **Δ = {delta*100:+.1f} pp — no edge to swap.** Current static thresholds are fine.")
        elif delta > 0.03:
            lines.append(f"- **Δ = {delta*100:+.1f} pp — adaptive has edge.** Consider swapping live alerter.")
        else:
            lines.append(f"- **Δ = {delta*100:+.1f} pp — adaptive is worse.** Keep static.")

    lines.append("")
    lines.append("## Thresholds used")
    lines.append("")
    lines.append("### Adaptive (fitted on TRAIN only)")
    adaptive_t = results["adaptive_thresholds_from_train"]
    for metric in ["reserves", "rrp", "net_liq"]:
        lines.append(f"\n**{metric}**")
        lines.append("| Regime | MEDIUM | HIGH | CRITICAL |")
        lines.append("|---|---|---|---|")
        for regime in REGIMES:
            t = adaptive_t[metric][regime]
            med = f"{t['MEDIUM']:.0f}" if t['MEDIUM'] is not None else "—"
            hi  = f"{t['HIGH']:.0f}"   if t['HIGH']   is not None else "—"
            cr  = f"{t['CRITICAL']:.0f}" if t['CRITICAL'] is not None else "—"
            lines.append(f"| {regime} | {med} | {hi} | {cr} |")

    lines.append("")
    lines.append("### Static (from .env)")
    for metric, tiers in STATIC_TIERS.items():
        lines.append(f"- {metric}: MEDIUM {tiers['MEDIUM']}, HIGH {tiers['HIGH']}, CRITICAL {tiers['CRITICAL']}")

    return "\n".join(lines)


def main() -> None:
    results = run()
    if not results:
        return
    md = build_report(results)
    REPORT_PATH.write_text(md)
    logger.info(f"[adaptive_vs_static] report written to {REPORT_PATH}")
    print(md)


if __name__ == "__main__":
    main()
