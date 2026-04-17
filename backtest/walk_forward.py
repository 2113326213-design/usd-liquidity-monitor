"""
Walk-forward validation for alert thresholds.

Replays 5 years of historical collector data against the CURRENT alert
thresholds and measures the forward SPY return distribution conditional
on each alert level firing. Answers the question reviewers flagged:

    "Does the playbook 60% reduction at CRITICAL have any historical
     data supporting it, or did the author pick the number from vibes?"

Method
------
For each alert type (Reserves tier, RRP tier, NetLiq tier, market stress
z-score, TGA daily swing, 30Y auction tail), detect first-crossing
events in the historical parquet data. Each event = one day where the
system WOULD have fired given that day's data and current thresholds.
Pull SPY forward return over 5 / 10 / 20 trading days from that date.
Compare against the unconditional base rate over the same window.

Event definition
----------------
For level-threshold alerts (Reserves floor, RRP floor, NetLiq floor):
  an event = first day the value crosses from >= threshold to < threshold.
  Subsequent days while still below DO NOT count. This avoids temporal
  autocorrelation inflating the statistics.

For delta-based (TGA daily swing): daily check, |Δ1d| > threshold.
For rate-based (market stress z, auction tail): first crossing upward.

Known caveats — surface them, don't hide them
----------------------------------------------
1. Look-ahead bias: thresholds were hand-calibrated AFTER seeing the
   2019 / 2020 / 2023 events. A rigorous test would re-calibrate
   thresholds on the early half of the data and test on the late half.
   We don't do that here (too little data for CRITICAL tier). The
   numbers below indicate whether the threshold *picks out* events,
   not whether it would *predict* new ones.
2. Small N: CRITICAL-tier alerts may fire 0-2 times in 5 years. Those
   rows are flagged and treated as anecdotal, not statistical.
3. SPY only: forward returns use SPY. If your portfolio is tech-heavy
   or small-cap, the actual drawdown is typically 1.5-2.5x larger.
4. No transaction costs, no slippage, no tax.

Output
------
Markdown report at backtest/latest_report.md.
Stats dict also returned for programmatic use.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf
from loguru import logger

from ..config import settings


DATA_DIR = settings.data_dir / "raw"
REPORT_PATH = Path(__file__).parent / "latest_report.md"
SPY_CACHE = DATA_DIR / "spy_daily.parquet"

FORWARD_HORIZONS_DAYS = [5, 10, 20]


# ───────────────────────── Data loading ─────────────────────────

def load_spy(refresh: bool = False) -> pd.DataFrame:
    """Daily SPY close, 8y back. Cached to parquet."""
    if not refresh and SPY_CACHE.exists():
        df = pd.read_parquet(SPY_CACHE)
        if len(df) > 0:
            return df
    logger.info("[backtest] fetching SPY from yfinance (8y daily)")
    raw = yf.Ticker("SPY").history(period="8y", interval="1d", auto_adjust=False)
    raw = raw.reset_index()
    raw["date"] = pd.to_datetime(raw["Date"]).dt.tz_localize(None).dt.normalize()
    df = raw[["date", "Close"]].rename(columns={"Close": "close"}).copy()
    df["close"] = df["close"].astype(float)
    df = df.dropna().sort_values("date").reset_index(drop=True)
    df.to_parquet(SPY_CACHE, index=False)
    logger.info(f"[backtest] SPY cached: {len(df)} rows, {df['date'].min()} → {df['date'].max()}")
    return df


def spy_forward_return(
    spy: pd.DataFrame, from_date: pd.Timestamp, horizon_days: int
) -> float | None:
    """Return total SPY percentage return from `from_date` to
    `from_date + horizon` trading days. None if insufficient future data."""
    from_date = pd.Timestamp(from_date).normalize()
    # index of first trading day >= from_date
    idx = spy["date"].searchsorted(from_date)
    if idx >= len(spy) - horizon_days:
        return None
    start_px = spy["close"].iloc[idx]
    end_px = spy["close"].iloc[idx + horizon_days]
    return float((end_px / start_px - 1.0) * 100.0)


# ───────────────────────── Event detection ─────────────────────────

def first_crossings_below(
    df: pd.DataFrame, date_col: str, value_col: str, threshold: float
) -> list[pd.Timestamp]:
    """Return dates where value crosses from >= threshold to < threshold.

    Handles series that go back above the threshold and then below again
    (counts as a second event). Weekly data (Reserves) handled naturally
    since first-crossing is order-of-observation independent."""
    if df.empty:
        return []
    s = df.sort_values(date_col).reset_index(drop=True)
    below = (s[value_col].astype(float) < threshold).to_numpy()
    dates = pd.to_datetime(s[date_col]).dt.tz_localize(None).dt.normalize().to_numpy()
    crossings: list[pd.Timestamp] = []
    for i in range(1, len(below)):
        if below[i] and not below[i - 1]:
            crossings.append(pd.Timestamp(dates[i]))
    # Edge case: series starts already below threshold
    if len(below) > 0 and below[0]:
        crossings.insert(0, pd.Timestamp(dates[0]))
    return crossings


def first_crossings_above(
    df: pd.DataFrame, date_col: str, value_col: str, threshold: float
) -> list[pd.Timestamp]:
    """Mirror of first_crossings_below — for z-score / tail metrics where
    rising above threshold is the alert."""
    if df.empty:
        return []
    s = df.sort_values(date_col).reset_index(drop=True)
    above = (s[value_col].astype(float) > threshold).to_numpy()
    dates = pd.to_datetime(s[date_col]).dt.tz_localize(None).dt.normalize().to_numpy()
    crossings: list[pd.Timestamp] = []
    for i in range(1, len(above)):
        if above[i] and not above[i - 1]:
            crossings.append(pd.Timestamp(dates[i]))
    if len(above) > 0 and above[0]:
        crossings.insert(0, pd.Timestamp(dates[0]))
    return crossings


def tga_delta_events(
    df: pd.DataFrame, threshold_bn: float = 50.0
) -> list[pd.Timestamp]:
    """Days where |Δ1d TGA| exceeds threshold."""
    if df.empty or len(df) < 2:
        return []
    s = df.sort_values("record_date").reset_index(drop=True)
    s["delta"] = s["close_bal_bn"].astype(float).diff()
    events_mask = s["delta"].abs() > threshold_bn
    return list(pd.to_datetime(s.loc[events_mask, "record_date"]).dt.tz_localize(None).dt.normalize())


# ───────────────────────── Stats ─────────────────────────

def event_return_stats(
    events: list[pd.Timestamp], spy: pd.DataFrame
) -> dict[str, dict[str, float | int | None]]:
    """For every forward horizon, compute distribution of SPY fwd returns
    conditional on the event dates."""
    out: dict[str, dict[str, float | int | None]] = {}
    for h in FORWARD_HORIZONS_DAYS:
        rets = [r for r in (spy_forward_return(spy, e, h) for e in events) if r is not None]
        if not rets:
            out[f"{h}d"] = {"n": 0, "mean": None, "median": None, "p10": None, "hit_rate_down": None}
            continue
        arr = np.array(rets)
        out[f"{h}d"] = {
            "n": len(arr),
            "mean": round(float(arr.mean()), 2),
            "median": round(float(np.median(arr)), 2),
            "p10": round(float(np.percentile(arr, 10)), 2),
            "hit_rate_down": round(float((arr < 0).mean()), 3),
        }
    return out


def baseline_stats(spy: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, dict]:
    """Unconditional forward-return distribution over the same window.
    Comparator for event-conditional stats."""
    spy = spy[(spy["date"] >= start) & (spy["date"] <= end)].reset_index(drop=True)
    out: dict[str, dict] = {}
    for h in FORWARD_HORIZONS_DAYS:
        rets: list[float] = []
        for i in range(len(spy) - h):
            start_px = spy["close"].iloc[i]
            end_px = spy["close"].iloc[i + h]
            rets.append(float((end_px / start_px - 1.0) * 100.0))
        if not rets:
            out[f"{h}d"] = {"n": 0, "mean": None, "median": None, "p10": None, "hit_rate_down": None}
            continue
        arr = np.array(rets)
        out[f"{h}d"] = {
            "n": len(arr),
            "mean": round(float(arr.mean()), 2),
            "median": round(float(np.median(arr)), 2),
            "p10": round(float(np.percentile(arr, 10)), 2),
            "hit_rate_down": round(float((arr < 0).mean()), 3),
        }
    return out


# ───────────────────────── Report builder ─────────────────────────

def _load_parquet(name: str) -> pd.DataFrame:
    p = DATA_DIR / f"{name}.parquet"
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


def run_all() -> dict[str, Any]:
    """Execute the full backtest, return a structured results dict."""
    spy = load_spy()
    window_start = spy["date"].min()
    window_end = spy["date"].max()

    # Unconditional base rate over the same window
    baseline = baseline_stats(spy, window_start, window_end)

    tga = _load_parquet("tga")
    rrp = _load_parquet("rrp")
    reserves = _load_parquet("reserves")
    nl = _load_parquet("net_liquidity")
    ms = _load_parquet("market_stress")
    tail = _load_parquet("auction_tail")

    results: dict[str, Any] = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "window": {"start": str(window_start.date()), "end": str(window_end.date())},
        "baseline": baseline,
        "alerts": {},
    }

    # ── Reserves (below thresholds) ──
    for tier, key in [("MEDIUM", "reserves_medium_bn"),
                      ("HIGH",   "reserves_high_bn"),
                      ("CRITICAL", "reserves_critical_bn")]:
        events = first_crossings_below(reserves, "observation_date", "reserves_bn",
                                        getattr(settings, key))
        results["alerts"][f"reserves_{tier}"] = {
            "threshold": f"reserves < ${getattr(settings, key):.0f}bn",
            "events": len(events),
            "event_dates": [str(e.date()) for e in events],
            "stats": event_return_stats(events, spy),
        }

    # ── RRP (below thresholds) ──
    for tier, key in [("MEDIUM", "rrp_medium_bn"),
                      ("HIGH",   "rrp_high_bn"),
                      ("CRITICAL", "rrp_critical_bn")]:
        events = first_crossings_below(rrp, "operation_date", "total_accepted_bn",
                                        getattr(settings, key))
        results["alerts"][f"rrp_{tier}"] = {
            "threshold": f"rrp < ${getattr(settings, key):.0f}bn",
            "events": len(events),
            "event_dates": [str(e.date()) for e in events[-10:]],
            "stats": event_return_stats(events, spy),
        }

    # ── Net Liquidity (below thresholds) ──
    for tier, key in [("MEDIUM", "net_liq_medium_bn"),
                      ("HIGH",   "net_liq_high_bn"),
                      ("CRITICAL", "net_liq_critical_bn")]:
        events = first_crossings_below(nl, "as_of", "net_liquidity_bn",
                                        getattr(settings, key))
        results["alerts"][f"netliq_{tier}"] = {
            "threshold": f"netliq < ${getattr(settings, key):.0f}bn",
            "events": len(events),
            "event_dates": [str(e.date()) for e in events[-10:]],
            "stats": event_return_stats(events, spy),
        }

    # ── TGA daily swing ──
    events = tga_delta_events(tga, threshold_bn=settings.tga_daily_swing_bn)
    results["alerts"]["tga_daily_swing"] = {
        "threshold": f"|Δ1d TGA| > ${settings.tga_daily_swing_bn:.0f}bn",
        "events": len(events),
        "event_dates": [str(e.date()) for e in events[-10:]],
        "stats": event_return_stats(events, spy),
    }

    # ── Market stress z (above thresholds) ──
    if not ms.empty:
        # Collapse hourly to daily max for a cleaner per-day event view
        ms_daily = ms.copy()
        ms_daily["date"] = pd.to_datetime(ms_daily["as_of_utc"], format="ISO8601") \
                              .dt.tz_convert(None).dt.normalize()
        daily_z = ms_daily.groupby("date", as_index=False)["composite_stress_z"].max()
        for tier, key in [("MEDIUM", "market_stress_medium_z"),
                          ("HIGH",   "market_stress_high_z"),
                          ("CRITICAL", "market_stress_critical_z")]:
            events = first_crossings_above(daily_z, "date", "composite_stress_z",
                                            getattr(settings, key))
            results["alerts"][f"market_stress_{tier}"] = {
                "threshold": f"daily max composite_z > {getattr(settings, key):.1f}",
                "events": len(events),
                "event_dates": [str(e.date()) for e in events[-10:]],
                "stats": event_return_stats(events, spy),
            }

    # ── 30Y auction tail (above thresholds) ──
    if not tail.empty:
        for tier, key in [("MEDIUM", "auction_tail_medium_bp"),
                          ("HIGH",   "auction_tail_high_bp"),
                          ("CRITICAL", "auction_tail_critical_bp")]:
            events = first_crossings_above(
                tail.dropna(subset=["tail_bp"]),
                "auction_date", "tail_bp",
                getattr(settings, key),
            )
            results["alerts"][f"auction_tail_{tier}"] = {
                "threshold": f"tail_bp > {getattr(settings, key):.1f}",
                "events": len(events),
                "event_dates": [str(e.date()) for e in events[-10:]],
                "stats": event_return_stats(events, spy),
            }

    # ── SOFR − IORB spread (above thresholds) ──
    # Most direct reserve-scarcity signal — added after reviewer flagged it
    # as the highest-value free indicator. Test whether empirical hit rate
    # matches the theoretical appeal.
    sofr_iorb = _load_parquet("sofr_iorb")
    if not sofr_iorb.empty:
        for tier, key in [("MEDIUM", "sofr_iorb_medium_bp"),
                          ("HIGH",   "sofr_iorb_high_bp"),
                          ("CRITICAL", "sofr_iorb_critical_bp")]:
            events = first_crossings_above(
                sofr_iorb, "observation_date", "spread_bp",
                getattr(settings, key),
            )
            results["alerts"][f"sofr_iorb_{tier}"] = {
                "threshold": f"spread_bp > {getattr(settings, key):.1f}",
                "events": len(events),
                "event_dates": [str(e.date()) for e in events[-10:]],
                "stats": event_return_stats(events, spy),
            }

    return results


def build_report(results: dict[str, Any]) -> str:
    """Format the results dict as a human-readable markdown report."""
    lines: list[str] = []
    b = results["baseline"]
    lines.append("# Walk-forward Validation Report")
    lines.append("")
    lines.append(f"_Generated: {results['generated_utc']}_")
    lines.append(f"_SPY window: {results['window']['start']} → {results['window']['end']}_")
    lines.append("")
    lines.append("## ⚠️ Read this first")
    lines.append("")
    lines.append("**Look-ahead bias**: the thresholds below were hand-calibrated")
    lines.append("*after* seeing the 2019 / 2020 / 2023 crises. So these numbers")
    lines.append("tell you whether the thresholds *pick out* historical events,")
    lines.append("**not** whether they would predict new ones. True out-of-sample")
    lines.append("validation requires re-fitting thresholds on the early half and")
    lines.append("testing on the late half — defer until more data accumulates.")
    lines.append("")
    lines.append("**Small N warning**: CRITICAL-tier alerts typically fire 0-2 times")
    lines.append("in 5 years. Statistical power at that tier is weak; treat as")
    lines.append("anecdote, not evidence.")
    lines.append("")
    lines.append("**SPY only**: forward returns use SPY. Tech-heavy / small-cap")
    lines.append("portfolios typically see 1.5-2.5× the drawdown.")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Baseline
    lines.append("## Baseline: unconditional SPY forward return over the same window")
    lines.append("")
    lines.append("| Horizon | N | Mean % | Median % | 10th pct % | P(down) |")
    lines.append("|---|---|---|---|---|---|")
    for h in FORWARD_HORIZONS_DAYS:
        s = b[f"{h}d"]
        if s["n"] == 0:
            continue
        lines.append(
            f"| {h}d | {s['n']:,} | {s['mean']:+.2f} | {s['median']:+.2f} | "
            f"{s['p10']:+.2f} | {s['hit_rate_down']:.1%} |"
        )
    lines.append("")
    lines.append(
        "*Any alert's conditional hit_rate_down must significantly exceed the baseline "
        "P(down) to carry information.*"
    )
    lines.append("")

    # Per-alert tables
    lines.append("## Per-alert results")
    lines.append("")
    for name, a in results["alerts"].items():
        lines.append(f"### {name}")
        lines.append(f"- **Threshold**: `{a['threshold']}`")
        lines.append(f"- **Historical events**: **{a['events']}**")
        if a["events"] == 0:
            lines.append(
                "- ⚪ _No triggers in sample window. Cannot validate or invalidate._"
            )
            lines.append("")
            continue
        if a["event_dates"]:
            dates_preview = ", ".join(a["event_dates"])
            lines.append(f"- Event dates: `{dates_preview}`")
        lines.append("")
        lines.append("| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |")
        lines.append("|---|---|---|---|---|---|---|")
        for h in FORWARD_HORIZONS_DAYS:
            s = a["stats"][f"{h}d"]
            if s["n"] == 0:
                continue
            base_p = b[f"{h}d"]["hit_rate_down"] or 0
            lift = (s["hit_rate_down"] - base_p) * 100 if s["hit_rate_down"] is not None else None
            lift_str = f"{lift:+.1f} pp" if lift is not None else "—"
            lines.append(
                f"| {h}d | {s['n']} | {s['mean']:+.2f} | {s['median']:+.2f} | "
                f"{s['p10']:+.2f} | {s['hit_rate_down']:.1%} | {lift_str} |"
            )
        lines.append("")
        # CRITICAL tier specific commentary
        if name.endswith("_CRITICAL"):
            if a["events"] < 3:
                lines.append(
                    f"> ⚠️ **CRITICAL tier has only {a['events']} historical event(s) "
                    f"in the sample — statistically weak. Playbook's 60% equity "
                    f"reduction at this level is based on judgment, not data.**"
                )
                lines.append("")

    # Playbook-specific key question answer
    lines.append("## 🎯 Key question: does the playbook have data support?")
    lines.append("")
    for level in ["MEDIUM", "HIGH", "CRITICAL"]:
        # Aggregate all structural alerts at this level
        keys = [k for k in results["alerts"] if k.endswith(f"_{level}") and not k.startswith(("market_stress", "auction_tail"))]
        total_events = sum(results["alerts"][k]["events"] for k in keys)
        sample_hit_rates = []
        for k in keys:
            s = results["alerts"][k]["stats"].get("10d")
            if s and s["hit_rate_down"] is not None:
                sample_hit_rates.append(s["hit_rate_down"])
        avg_hit = np.mean(sample_hit_rates) * 100 if sample_hit_rates else None
        base_10d = (b["10d"]["hit_rate_down"] or 0) * 100
        lift = (avg_hit - base_10d) if avg_hit is not None else None

        playbook_pct = {"MEDIUM": 15, "HIGH": 30, "CRITICAL": 60}[level]
        verdict = "❓ insufficient data" if total_events < 3 else (
            "✅ signal present" if lift and lift > 5 else
            "🟡 marginal" if lift and lift > 0 else "❌ no edge"
        )
        lines.append(
            f"- **{level}** (playbook: 减仓 {playbook_pct}%) — "
            f"total events across structural alerts: {total_events}. "
            f"Avg 10d P(SPY down): {f'{avg_hit:.1f}%' if avg_hit is not None else '—'} "
            f"(baseline: {base_10d:.1f}%). Verdict: {verdict}"
        )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(
        "## How to interpret"
    )
    lines.append("")
    lines.append(
        "- **P(down)** significantly above baseline = the threshold has signal"
    )
    lines.append("- **Mean / median forward return** more negative than baseline = actionable")
    lines.append("- **10th percentile** tells you the bad-case drawdown after the alert fires — this is what hedging is priced against")
    lines.append(
        "- If a tier has 0 events: expected for CRITICAL in a 5y sample that doesn't contain a real crisis. Not a bug."
    )
    lines.append(
        "- If a tier has many events with **no lift over baseline**: threshold is noise. Tighten or remove."
    )

    return "\n".join(lines)


def main() -> None:
    logger.info("[backtest] running walk-forward validation")
    results = run_all()

    # Markdown report
    md = build_report(results)
    REPORT_PATH.write_text(md)
    logger.info(f"[backtest] report written to {REPORT_PATH}")

    # Machine-readable JSON for dashboard
    json_path = REPORT_PATH.with_suffix(".json")
    json_path.write_text(json.dumps(results, indent=2, default=str))
    logger.info(f"[backtest] json written to {json_path}")

    # Print summary to stdout
    print(md[:3000])
    print("\n... (truncated — full report at backtest/latest_report.md)\n")


if __name__ == "__main__":
    main()
