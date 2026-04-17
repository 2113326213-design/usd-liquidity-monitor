"""
Data completeness audit — catch silent coverage gaps in every collector.

Motivation
----------
The RRP bug we found via cross-source comparison (propositions/search.json
silently missing 5 recent ops that last/500.json had) is probably not
unique. The purpose of this script is to systematically scan every
parquet for two warning signs:

1. **Internal gap anomalies** — unusually large time gaps between
   consecutive observations that don't match the data source's
   publication cadence. For example: TGA should publish every
   business day, so a 5-day gap mid-week is a coverage gap.

2. **Cross-source spot check** — where a second free endpoint exists
   for the same series, diff them for the overlapping window and
   report missing IDs or values.

Output
------
backtest/data_audit_report.md — human-readable report per collector.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd
from loguru import logger

from ..config import settings


DATA_DIR = settings.data_dir / "raw"
REPORT_PATH = Path(__file__).parent / "data_audit_report.md"


# Expected cadence per collector (business-day-aware where relevant)
CADENCE_SPEC: dict[str, dict] = {
    "tga":          {"date_col": "record_date",      "expected_max_gap_days": 5,  "business_days_only": True,  "series_kind": "daily"},
    "rrp":          {"date_col": "operation_date",   "expected_max_gap_days": 5,  "business_days_only": True,  "series_kind": "daily"},
    "srp":          {"date_col": "operation_date",   "expected_max_gap_days": 5,  "business_days_only": True,  "series_kind": "daily"},
    "reserves":     {"date_col": "observation_date", "expected_max_gap_days": 14, "business_days_only": False, "series_kind": "weekly"},
    "sofr_iorb":    {"date_col": "observation_date", "expected_max_gap_days": 5,  "business_days_only": True,  "series_kind": "daily"},
    "auction_tail": {"date_col": "auction_date",     "expected_max_gap_days": 90, "business_days_only": False, "series_kind": "monthly"},
    "net_liquidity":{"date_col": "as_of",            "expected_max_gap_days": 3,  "business_days_only": False, "series_kind": "daily-forward-fill"},
}


def analyse_gaps(df: pd.DataFrame, date_col: str) -> dict:
    """Return gap statistics: rows, range, max gap, large gap count."""
    if df.empty or date_col not in df.columns:
        return {"rows": 0}
    s = pd.to_datetime(df[date_col].astype(str)).sort_values().reset_index(drop=True)
    if len(s) < 2:
        return {"rows": len(s)}
    deltas_days = (s.diff().dt.days.dropna()).astype(int)
    return {
        "rows":          len(s),
        "first_date":    s.iloc[0].date().isoformat(),
        "last_date":     s.iloc[-1].date().isoformat(),
        "mean_gap_days": round(float(deltas_days.mean()), 2),
        "median_gap_days": int(deltas_days.median()),
        "max_gap_days":  int(deltas_days.max()),
    }


def find_large_gaps(
    df: pd.DataFrame, date_col: str, threshold_days: int, top_n: int = 10
) -> list[dict]:
    """Return top-N largest gaps with the dates that bracket them."""
    if df.empty or date_col not in df.columns:
        return []
    s = pd.to_datetime(df[date_col].astype(str)).sort_values().reset_index(drop=True)
    if len(s) < 2:
        return []
    gap_days = s.diff().dt.days.fillna(0).astype(int)
    big_ix = gap_days[gap_days > threshold_days].sort_values(ascending=False).index
    out = []
    for i in big_ix[:top_n]:
        out.append({
            "gap_days": int(gap_days.iloc[i]),
            "after":   s.iloc[i - 1].date().isoformat(),
            "before":  s.iloc[i].date().isoformat(),
        })
    return out


# ───────────────── TGA cross-source check ─────────────────

async def tga_cross_source_diff() -> dict:
    """Compare the deployed `operating_cash_balance` endpoint against
    Fiscal Data's `dts_table_1` for the last 60 days. Count dates present
    in one but not the other."""
    if not (DATA_DIR / "tga.parquet").exists():
        return {"status": "skipped", "reason": "no tga.parquet"}

    end = datetime.now(timezone.utc).date()
    start = end.replace(day=1)  # last ~30-60 days
    from datetime import timedelta
    start = end - timedelta(days=60)

    url = (
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
        "/v1/accounting/dts/dts_table_1"
    )
    params = {
        "filter": f"record_date:gte:{start.isoformat()},record_date:lte:{end.isoformat()}",
        "fields": "record_date,account_type,close_today_bal",
        "page[size]": "10000",
        "format": "json",
    }
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(url, params=params)
            r.raise_for_status()
    except Exception as e:
        return {"status": "error", "reason": str(e)}

    data = r.json().get("data", [])
    # Pull the Treasury General Account row type if present
    tga_rows = [
        d for d in data
        if "treasury general account" in str(d.get("account_type", "")).lower()
        or "federal reserve account" in str(d.get("account_type", "")).lower()
    ]
    alt_dates = {d["record_date"] for d in tga_rows}

    # Compare to deployed parquet
    deployed = pd.read_parquet(DATA_DIR / "tga.parquet")
    deployed_dates = set(deployed["record_date"].astype(str).tolist())

    window_alt = {d for d in alt_dates if start.isoformat() <= d <= end.isoformat()}
    window_dep = {d for d in deployed_dates if start.isoformat() <= d <= end.isoformat()}

    only_in_alt = sorted(window_alt - window_dep)
    only_in_dep = sorted(window_dep - window_alt)

    return {
        "status": "ok",
        "window": f"{start.isoformat()} → {end.isoformat()}",
        "endpoint_a_rows": len(window_dep),
        "endpoint_b_rows": len(window_alt),
        "only_in_operating_cash_balance": only_in_dep[:20],
        "only_in_dts_table_1": only_in_alt[:20],
        "agreement": (
            1.0 if not (only_in_alt or only_in_dep)
            else round(len(window_alt & window_dep) / max(1, len(window_alt | window_dep)), 3)
        ),
    }


# ───────────────── RRP cross-source check (already known good) ─────────────────

async def rrp_cross_source_diff() -> dict:
    """Re-check: does the live endpoint still match last/500.json?
    Re-runs the comparison that originally exposed the 5-op gap."""
    deployed = pd.read_parquet(DATA_DIR / "rrp.parquet")
    deployed_ids = set(deployed["operation_id"].dropna().astype(str).tolist())

    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(
                "https://markets.newyorkfed.org/api/rp/reverserepo/all/results/last/500.json"
            )
            r.raise_for_status()
    except Exception as e:
        return {"status": "error", "reason": str(e)}

    ops = r.json().get("repo", {}).get("operations", [])
    ops = [o for o in ops if o.get("term", "").upper() == "OVERNIGHT"]
    rich_ids = {o["operationId"] for o in ops}

    only_in_rich = sorted(rich_ids - deployed_ids)[:20]
    return {
        "status": "ok",
        "deployed_rows":    len(deployed_ids),
        "rich_endpoint_rows": len(rich_ids),
        "only_in_rich":     only_in_rich,
        "agreement": round(len(rich_ids & deployed_ids) / max(1, len(rich_ids | deployed_ids)), 3),
    }


# ───────────────── Main ─────────────────

def main() -> None:
    lines: list[str] = []
    lines.append("# Data completeness audit")
    lines.append("")
    lines.append(f"_Generated: {datetime.now(timezone.utc).isoformat()}_")
    lines.append("")
    lines.append("## Per-collector gap analysis")
    lines.append("")
    lines.append(
        "A row in the table is one parquet file. `max_gap_days` > "
        "`expected_max_gap_days` is the warning signal."
    )
    lines.append("")
    lines.append(
        "| Collector | Rows | Range | Median gap | Max gap | Expected | Flag |"
    )
    lines.append("|---|---|---|---|---|---|---|")

    findings: dict = {}
    for name, spec in CADENCE_SPEC.items():
        p = DATA_DIR / f"{name}.parquet"
        if not p.exists():
            lines.append(f"| {name} | — | _no file_ | — | — | — | ⚪ |")
            continue
        df = pd.read_parquet(p)
        stats = analyse_gaps(df, spec["date_col"])
        if stats["rows"] < 2:
            lines.append(f"| {name} | {stats['rows']} | — | — | — | — | ⚪ |")
            continue
        flag = "🟢"
        if stats["max_gap_days"] > spec["expected_max_gap_days"]:
            flag = "🟡" if stats["max_gap_days"] < spec["expected_max_gap_days"] * 2 else "🔴"
        lines.append(
            f"| {name} | {stats['rows']:,} | {stats['first_date']} → {stats['last_date']} "
            f"| {stats['median_gap_days']}d | {stats['max_gap_days']}d "
            f"| ≤ {spec['expected_max_gap_days']}d | {flag} |"
        )
        findings[name] = {"stats": stats, "flag": flag}

    # Drill into large gaps for flagged collectors
    lines.append("")
    lines.append("## Top 10 largest gaps per collector")
    lines.append("")
    for name, spec in CADENCE_SPEC.items():
        p = DATA_DIR / f"{name}.parquet"
        if not p.exists():
            continue
        df = pd.read_parquet(p)
        gaps = find_large_gaps(df, spec["date_col"], spec["expected_max_gap_days"], top_n=5)
        if not gaps:
            continue
        lines.append(f"\n### {name}")
        lines.append("| gap | between | and |")
        lines.append("|---|---|---|")
        for g in gaps:
            lines.append(f"| {g['gap_days']}d | {g['after']} | {g['before']} |")

    # Cross-source checks
    lines.append("")
    lines.append("## Cross-source checks (diff deployed endpoint vs. alternative)")
    lines.append("")

    tga_res = asyncio.run(tga_cross_source_diff())
    lines.append("### TGA: `operating_cash_balance` vs `dts_table_1`")
    if tga_res.get("status") == "ok":
        agreement_pct = tga_res["agreement"] * 100
        lines.append(f"- Window: {tga_res['window']}")
        lines.append(f"- Deployed endpoint rows: {tga_res['endpoint_a_rows']}")
        lines.append(f"- Alternative endpoint rows: {tga_res['endpoint_b_rows']}")
        lines.append(f"- Agreement: **{agreement_pct:.1f}%**")
        if tga_res["only_in_dts_table_1"]:
            lines.append(
                f"- ⚠️ Dates ONLY in dts_table_1 (deployed missing): "
                f"`{', '.join(tga_res['only_in_dts_table_1'])}`"
            )
        if tga_res["only_in_operating_cash_balance"]:
            lines.append(
                f"- Dates only in deployed (fine, just different schema exposure): "
                f"`{', '.join(tga_res['only_in_operating_cash_balance'])}`"
            )
    else:
        lines.append(f"- Status: {tga_res.get('status')}")
        lines.append(f"- Reason: {tga_res.get('reason')}")

    lines.append("")
    rrp_res = asyncio.run(rrp_cross_source_diff())
    lines.append("### RRP: deployed parquet vs `all/results/last/500.json` (regression check)")
    if rrp_res.get("status") == "ok":
        lines.append(f"- Deployed rows: {rrp_res['deployed_rows']}")
        lines.append(f"- Rich endpoint rows: {rrp_res['rich_endpoint_rows']}")
        lines.append(f"- Agreement: **{rrp_res['agreement'] * 100:.1f}%**")
        if rrp_res["only_in_rich"]:
            lines.append(
                f"- ⚠️ Operation IDs ONLY in rich endpoint (deployed missing): "
                f"`{', '.join(rrp_res['only_in_rich'])}`"
            )
        else:
            lines.append("- ✅ no new missing ops since last re-ingest")
    else:
        lines.append(f"- Status: {rrp_res.get('status')}")
        lines.append(f"- Reason: {rrp_res.get('reason')}")

    # Footer
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## How to interpret")
    lines.append("")
    lines.append(
        "- 🟢 max gap within expected cadence → no evidence of missing data"
    )
    lines.append(
        "- 🟡 max gap 1-2× expected → suspicious, investigate (might be holiday, might be real gap)"
    )
    lines.append(
        "- 🔴 max gap >2× expected → likely real coverage gap, re-run backfill"
    )
    lines.append(
        "- For cross-source checks: agreement < 100% → some dates/IDs missing "
        "from one endpoint. Which endpoint is 'right' depends on the data source. "
        "For NY Fed RRP, the rich `all/results/last/N` endpoint is the superset."
    )

    REPORT_PATH.write_text("\n".join(lines))
    logger.info(f"[audit] report written to {REPORT_PATH}")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
