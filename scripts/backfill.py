"""
One-off 5-year historical backfill.

Fetches TGA / RRP / Reserves / SRP from their public APIs and stores them
directly to data/raw/*.parquet, bypassing the collectors' on_new_data hooks.
No alerts fire for past events. Then recomputes the full Net Liquidity series.
Also backfills ~2y of hourly market_stress z-scores via yfinance.

Usage:
    # Stop the live monitor first to avoid write races:
    tmux kill-window -t liquidity:monitor

    # Run backfill:
    cd ~/Desktop
    python3 -m usd_liquidity_monitor.scripts.backfill

    # Restart the live monitor:
    (re-attach via tmux new-window)

Idempotent: re-running overwrites the existing parquet files for the backfill
window. Live monitor will then dedup on its next poll.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import date, datetime, timedelta, timezone

import httpx
import pandas as pd
import yfinance as yf

from ..collectors.market_stress import STRESS_DIRECTION
from ..config import settings


# ───────────────────────── SOFR − IORB spread ─────────────────────────

async def backfill_sofr_iorb() -> None:
    """Daily SOFR + IORB (+ IOER predecessor) from FRED, compute spread."""
    if not settings.fred_api_key:
        print("[sofr_iorb] SKIP — no FRED_API_KEY")
        return

    url = "https://api.stlouisfed.org/fred/series/observations"

    async def _series(series_id: str) -> dict[str, float]:
        params = {
            "series_id": series_id,
            "api_key": settings.fred_api_key,
            "file_type": "json",
            "observation_start": START_DATE.isoformat(),
            "observation_end": END_DATE.isoformat(),
            "limit": "100000",
        }
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.get(url, params=params)
            r.raise_for_status()
        obs = r.json().get("observations", [])
        return {
            o["date"]: float(o["value"])
            for o in obs
            if o.get("value") and o["value"] != "."
        }

    sofr = await _series("SOFR")
    iorb = await _series("IORB")
    # IOER is the pre-2021 predecessor — stitch for dates before IORB
    ioer = await _series("IOER")

    # Build merged rate series
    rates: list[dict] = []
    all_dates = sorted(set(sofr.keys()) | set(iorb.keys()) | set(ioer.keys()))
    for d in all_dates:
        sofr_pct = sofr.get(d)
        iorb_pct = iorb.get(d) or ioer.get(d)  # IOER only used when IORB missing
        if sofr_pct is None or iorb_pct is None:
            continue
        spread_bp = round((sofr_pct - iorb_pct) * 100, 3)
        rates.append({
            "observation_date": d,
            "sofr_pct":  round(sofr_pct, 4),
            "iorb_pct":  round(iorb_pct, 4),
            "spread_bp": spread_bp,
            "poll_ts":   f"{d}T20:00:00+00:00",
        })

    if not rates:
        print("[sofr_iorb] no data")
        return
    df = pd.DataFrame(rates)
    df["_hash"] = df.apply(
        lambda r: hashlib.md5(
            f"{r['observation_date']}:{r['spread_bp']}".encode()
        ).hexdigest(),
        axis=1,
    )
    df = df.sort_values("observation_date").reset_index(drop=True)
    df.to_parquet(DATA_DIR / "sofr_iorb.parquet", index=False)
    print(
        f"[sofr_iorb] {len(df)} rows, "
        f"{df['observation_date'].iloc[0]} → {df['observation_date'].iloc[-1]}, "
        f"max spread = {df['spread_bp'].max():.2f} bp on "
        f"{df.loc[df['spread_bp'].idxmax(), 'observation_date']}"
    )


YEARS = 5
END_DATE: date = datetime.now(timezone.utc).date()
START_DATE: date = END_DATE - timedelta(days=365 * YEARS)
DATA_DIR = settings.data_dir / "raw"


def _hash_row(row: dict) -> str:
    content = {k: v for k, v in row.items() if k not in ("poll_ts", "_hash")}
    s = json.dumps(content, sort_keys=True, default=str)
    return hashlib.md5(s.encode()).hexdigest()


# ───────────────────────── TGA ─────────────────────────

async def backfill_tga() -> None:
    # Fiscal Data imposes an implicit ~5000-row cap per filtered query.
    # Chunk by year to stay under it (each year has ~1500 rows × 4 types).
    url = (
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
        "/v1/accounting/dts/operating_cash_balance"
    )
    all_rows: list[dict] = []
    async with httpx.AsyncClient(timeout=120) as c:
        for year in range(START_DATE.year, END_DATE.year + 1):
            s = max(date(year, 1, 1), START_DATE)
            e = min(date(year, 12, 31), END_DATE)
            if s > e:
                continue
            params = {
                "filter": f"record_date:gte:{s},record_date:lte:{e}",
                "sort": "record_date",
                "page[size]": "10000",
                "format": "json",
            }
            r = await c.get(url, params=params)
            r.raise_for_status()
            all_rows.extend(r.json().get("data", []))
    df = pd.DataFrame(all_rows)
    if df.empty:
        print("[tga] no data returned")
        return
    # Fiscal Data has renamed this account_type twice:
    #   pre-2021-10        : "Federal Reserve Account" (held at Fed = TGA)
    #   2021-10 → 2022-04 : "Treasury General Account (TGA)"
    #   2022-04 onward     : "Treasury General Account (TGA) Closing Balance"
    # Treat all three as the same series for backfill.
    tga_labels = {
        "Federal Reserve Account",
        "Treasury General Account (TGA)",
        "Treasury General Account (TGA) Closing Balance",
    }
    df = df[df["account_type"].isin(tga_labels)].copy()
    rows: list[dict] = []
    for _, d in df.iterrows():
        try:
            mn = float(d["open_today_bal"])
        except (TypeError, ValueError):
            continue
        row = {
            "record_date": str(d["record_date"]),
            "close_bal_bn": mn / 1000.0,
            "close_bal_mn": mn,
            "poll_ts": f"{d['record_date']}T23:00:00+00:00",
        }
        row["_hash"] = _hash_row(row)
        rows.append(row)
    out = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["record_date"], keep="last")
        .sort_values("record_date")
        .reset_index(drop=True)
    )
    out.to_parquet(DATA_DIR / "tga.parquet", index=False)
    print(
        f"[tga] {len(out)} rows, "
        f"{out['record_date'].iloc[0]} → {out['record_date'].iloc[-1]}"
    )


# ───────────────────────── Reserves ─────────────────────────

async def backfill_reserves() -> None:
    if not settings.fred_api_key:
        print("[reserves] SKIP — no FRED_API_KEY")
        return
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "WRESBAL",
        "api_key": settings.fred_api_key,
        "file_type": "json",
        "observation_start": START_DATE.isoformat(),
        "observation_end": END_DATE.isoformat(),
        "limit": "100000",
    }
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.get(url, params=params)
        r.raise_for_status()
    obs = r.json().get("observations", [])
    obs = [o for o in obs if o.get("value") not in (".", "", None)]
    rows: list[dict] = []
    for o in obs:
        try:
            val_bn = float(o["value"]) / 1000.0  # FRED mn → bn
        except (TypeError, ValueError):
            continue
        row = {
            "observation_date": o["date"],
            "reserves_bn": val_bn,
            "poll_ts": f"{o['date']}T16:30:00+00:00",
        }
        row["_hash"] = _hash_row(row)
        rows.append(row)
    out = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["observation_date"], keep="last")
        .sort_values("observation_date")
        .reset_index(drop=True)
    )
    out.to_parquet(DATA_DIR / "reserves.parquet", index=False)
    print(
        f"[reserves] {len(out)} rows, "
        f"{out['observation_date'].iloc[0]} → {out['observation_date'].iloc[-1]}"
    )


# ───────────────────────── RRP ─────────────────────────

async def _ny_fed_rp_chunked(
    endpoint: str, start: date, end: date, op_type_filter: str
) -> list[dict]:
    """Chunk by year to avoid hitting any undocumented range limits."""
    out: list[dict] = []
    for year in range(start.year, end.year + 1):
        s = max(date(year, 1, 1), start)
        e = min(date(year, 12, 31), end)
        if s > e:
            continue
        url = f"https://markets.newyorkfed.org/api/rp/{endpoint}"
        params = {"startDate": s.isoformat(), "endDate": e.isoformat()}
        try:
            async with httpx.AsyncClient(timeout=60) as c:
                r = await c.get(url, params=params)
                r.raise_for_status()
            ops = r.json().get("repo", {}).get("operations", [])
        except Exception as exc:
            print(f"[{endpoint}] {year} failed: {exc}")
            continue
        for op in ops:
            if op.get("operationType", "").upper() != op_type_filter.upper():
                continue
            out.append(op)
    return out


async def backfill_rrp() -> None:
    # Two-endpoint merge strategy for maximum data quality:
    #   1. reverserepo/all/results/last/500.json — rich schema including
    #      `term` field, covers most recent ~500 ops (≈ 2 years). Lets us
    #      explicitly verify each op is Overnight. NY Fed caps N at 500
    #      (1000 and 1500 return HTTP 400).
    #   2. reverserepo/propositions/search.json?startDate=..&endDate=.. —
    #      only endpoint supporting multi-year date ranges, but minimal
    #      schema (no `term`). Used for the older tail. Defensive filter
    #      treats missing `term` as Overnight — matches historical
    #      reality (Fed did not conduct Term RRP in 2021-2026).
    # Dedup by operationId; rich endpoint wins on overlap. Empirically
    # the rich endpoint also catches ~5 recent ops that propositions/
    # search.json misses, so the merge strictly dominates either alone.
    from ..collectors.rrp import filter_on_rrp

    # (1) rich endpoint — verified Overnight
    rich_ops: list[dict] = []
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.get(
                "https://markets.newyorkfed.org/api/rp/reverserepo/all/results/last/500.json"
            )
            r.raise_for_status()
        rich_ops = r.json().get("repo", {}).get("operations", [])
    except Exception as exc:
        print(f"[rrp] rich endpoint failed: {exc}")

    # (2) date-range endpoint for older data
    date_ops = await _ny_fed_rp_chunked(
        "reverserepo/propositions/search.json",
        START_DATE,
        END_DATE,
        "REVERSE REPO",
    )

    rich_filtered = filter_on_rrp(rich_ops)
    date_filtered = filter_on_rrp(date_ops)

    # Dedup by operationId; rich takes precedence for overlapping IDs.
    by_id: dict[str, dict] = {}
    for op in date_filtered:
        oid = op.get("operationId")
        if oid:
            by_id[oid] = op
    for op in rich_filtered:
        oid = op.get("operationId")
        if oid:
            by_id[oid] = op  # overwrite with rich-schema version
    ops = list(by_id.values())
    print(
        f"[rrp]   rich verified-Overnight: {len(rich_filtered)}, "
        f"date-range: {len(date_filtered)}, merged unique: {len(ops)}"
    )
    rows: list[dict] = []
    for op in ops:
        try:
            accepted_bn = float(op.get("totalAmtAccepted", 0)) / 1e9
        except (TypeError, ValueError):
            accepted_bn = 0.0
        details = op.get("details", [])
        rate = None
        if details:
            rate = details[0].get("percentAwardRate") or details[0].get(
                "percentOfferingRate"
            )
        row = {
            "operation_id": op.get("operationId"),
            "operation_date": op.get("operationDate"),
            "operation_type": op.get("operationType"),
            "total_accepted_bn": accepted_bn,
            "num_submissions": op.get("totalAmtSubmittedPositions"),
            "rate": rate,
            "poll_ts": f"{op.get('operationDate', '')}T13:30:00+00:00",
        }
        row["_hash"] = _hash_row(row)
        rows.append(row)
    if not rows:
        print("[rrp] no data")
        return
    # Some dates have MULTIPLE Reverse Repo ops (standard ON RRP for MMFs
    # + FIMA RRP for foreign central banks; both satisfy term==Overnight).
    # The previous dedup `keep="last"` was order-dependent and silently
    # picked the smaller FIMA op on some dates, understating ON RRP by
    # hundreds of billions. Fix: sort by amount descending, keep LARGEST
    # per date — which is always the standard ON RRP we actually want
    # for liquidity monitoring.
    out = (
        pd.DataFrame(rows)
        .sort_values(["operation_date", "total_accepted_bn"], ascending=[True, False])
        .drop_duplicates(subset=["operation_date"], keep="first")
        .sort_values("operation_date")
        .reset_index(drop=True)
    )
    out.to_parquet(DATA_DIR / "rrp.parquet", index=False)
    print(
        f"[rrp] {len(out)} rows, "
        f"{out['operation_date'].iloc[0]} → {out['operation_date'].iloc[-1]}"
    )


# ───────────────────────── SRP ─────────────────────────

async def backfill_srp() -> None:
    # NY Fed has no date-range endpoint that works for Repo ops; use
    # `last/N.json` with a large N. N=5000 gives ~2000 unique ops because
    # it includes separate AM/PM + security-type rollups.
    ops: list[dict] = []
    try:
        # NY Fed caps `last/N` at N=500; 500 is enough for ~1 year of SRP
        # operations (there are ~2/day when active, plus many 0-volume days).
        url = "https://markets.newyorkfed.org/api/rp/repo/all/results/last/500.json"
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.get(url)
            r.raise_for_status()
        all_ops = r.json().get("repo", {}).get("operations", [])
        ops = [o for o in all_ops if o.get("operationType", "").upper() == "REPO"]
    except Exception as e:
        print(f"[srp] fetch failed: {e}")
    rows: list[dict] = []
    for op in ops:
        try:
            accepted_bn = float(op.get("totalAmtAccepted", 0)) / 1e9
        except (TypeError, ValueError):
            accepted_bn = 0.0
        details = op.get("details", [])
        rate = None
        if details:
            rate = (
                details[0].get("percentHighRate")
                or details[0].get("percentWeightedAverageRate")
            )
        row = {
            "operation_id": op.get("operationId"),
            "operation_date": op.get("operationDate"),
            "operation_type": op.get("operationType"),
            "total_accepted_bn": accepted_bn,
            "rate": rate,
            "poll_ts": f"{op.get('operationDate', '')}T08:30:00+00:00",
        }
        row["_hash"] = _hash_row(row)
        rows.append(row)
    if not rows:
        print("[srp] no data — leaving existing parquet untouched")
        return
    out = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["operation_id"], keep="last")
        .sort_values("operation_date")
        .reset_index(drop=True)
    )
    out.to_parquet(DATA_DIR / "srp.parquet", index=False)
    print(
        f"[srp] {len(out)} rows, "
        f"{out['operation_date'].iloc[0]} → {out['operation_date'].iloc[-1]}"
    )


# ───────────────────────── Net Liquidity (derived) ─────────────────────────

def backfill_net_liquidity() -> None:
    tga = pd.read_parquet(DATA_DIR / "tga.parquet")
    rrp = pd.read_parquet(DATA_DIR / "rrp.parquet")
    reserves = pd.read_parquet(DATA_DIR / "reserves.parquet")

    tga_d = tga[["record_date", "close_bal_bn"]].rename(
        columns={"record_date": "date", "close_bal_bn": "tga_bn"}
    )
    rrp_d = rrp[["operation_date", "total_accepted_bn"]].rename(
        columns={"operation_date": "date", "total_accepted_bn": "rrp_bn"}
    )
    res_w = reserves[["observation_date", "reserves_bn"]].rename(
        columns={"observation_date": "date"}
    )

    for df in (tga_d, rrp_d, res_w):
        df["date"] = pd.to_datetime(df["date"])

    start = min(tga_d["date"].min(), rrp_d["date"].min(), res_w["date"].min())
    end = max(tga_d["date"].max(), rrp_d["date"].max(), res_w["date"].max())
    cal = pd.DataFrame({"date": pd.date_range(start, end, freq="D")})

    m = (
        cal.merge(tga_d, on="date", how="left")
        .merge(rrp_d, on="date", how="left")
        .merge(res_w, on="date", how="left")
    )
    # Forward-fill daily/weekly series; RRP missing = 0 (pre-facility days)
    m["tga_bn"] = m["tga_bn"].ffill()
    m["rrp_bn"] = m["rrp_bn"].ffill().fillna(0.0)
    m["reserves_bn"] = m["reserves_bn"].ffill()

    m = m.dropna(subset=["tga_bn", "reserves_bn"]).reset_index(drop=True)
    m["net_liquidity_bn"] = (
        m["reserves_bn"] + m["rrp_bn"] - m["tga_bn"]
    ).round(2)
    m["as_of"] = m["date"].dt.strftime("%Y-%m-%d")
    out = m[["as_of", "net_liquidity_bn", "reserves_bn", "rrp_bn", "tga_bn"]].copy()

    def _h(r):
        return hashlib.md5(
            f"{r['as_of']}:{r['net_liquidity_bn']}".encode()
        ).hexdigest()
    out["_hash"] = out.apply(_h, axis=1)
    out.to_parquet(DATA_DIR / "net_liquidity.parquet", index=False)
    print(
        f"[net_liquidity] {len(out)} rows, "
        f"{out['as_of'].iloc[0]} → {out['as_of'].iloc[-1]}"
    )


# ───────────────────────── Market stress (yfinance) ─────────────────────────

def backfill_market_stress() -> None:
    closes: dict[str, pd.Series] = {}
    for ticker in STRESS_DIRECTION:
        try:
            df = yf.Ticker(ticker).history(
                period="2y", interval="1h", auto_adjust=False, prepost=False
            )
            if df.empty or len(df) < 100:
                print(f"[market_stress] {ticker}: insufficient data")
                continue
            # Normalise to UTC, then floor to the hour so SPY/ETFs (half-past
            # NY hours) align with ^VIX (on-the-hour Chicago). Without this,
            # the union DataFrame has zero fully-populated rows because SPY
            # bars are at 13:30/14:30 UTC while VIX bars are at 14:00/15:00
            # UTC. Floor rounds both to the same hour bucket (keep last if
            # multiple bars collapse to the same hour).
            s = df["Close"].astype(float)
            s.index = s.index.tz_convert("UTC").floor("h")
            s = s[~s.index.duplicated(keep="last")]
            closes[ticker] = s
        except Exception as e:
            print(f"[market_stress] {ticker}: {e}")

    if not closes:
        print("[market_stress] no tickers fetched")
        return

    # Align on union index; each ticker's returns computed separately.
    prices = pd.DataFrame(closes).sort_index()
    rets = prices.pct_change()

    # 30 trading-day window ≈ 210 hourly bars (regular trading hours).
    window = 210
    mu = rets.rolling(window).mean()
    sig = rets.rolling(window).std()
    z = (rets - mu) / sig

    # Sign-adjust so stress_z > 0 = stress-aligned.
    for t, s in STRESS_DIRECTION.items():
        if t in z.columns:
            z[t] = z[t] * s

    z = z.dropna(how="all")

    rows: list[dict] = []
    for ts, zrow in z.iterrows():
        tick_detail = {}
        stress_values: list[float] = []
        for t in STRESS_DIRECTION:
            if t not in z.columns:
                continue
            sz = zrow.get(t)
            if pd.isna(sz):
                continue
            sz_f = float(sz)
            raw_z = sz_f / STRESS_DIRECTION[t]
            r = rets.loc[ts, t] if pd.notna(rets.loc[ts, t]) else 0.0
            p = prices.loc[ts, t] if pd.notna(prices.loc[ts, t]) else 0.0
            tick_detail[t] = {
                "price": round(float(p), 4),
                "ret_1h_pct": round(float(r) * 100, 3),
                "z_1h": round(raw_z, 3),
                "stress_z": round(sz_f, 3),
            }
            stress_values.append(sz_f)
        if not stress_values:
            continue
        composite = sum(stress_values) / len(stress_values)
        aligned = sum(1 for v in stress_values if v > 0.5)
        iso = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
        row = {
            "as_of_utc": iso,
            "tickers": tick_detail,
            "composite_stress_z": round(composite, 3),
            "tickers_returned": len(stress_values),
            "tickers_stress_aligned": aligned,
            "poll_ts": iso,
        }
        row["_hash"] = _hash_row(row)
        rows.append(row)

    if not rows:
        print("[market_stress] no rows computed")
        return
    out = pd.DataFrame(rows)
    out.to_parquet(DATA_DIR / "market_stress.parquet", index=False)
    print(
        f"[market_stress] {len(out)} rows, "
        f"{out['as_of_utc'].iloc[0]} → {out['as_of_utc'].iloc[-1]}"
    )


# ───────────────────────── driver ─────────────────────────

async def _async_sources() -> None:
    await asyncio.gather(
        backfill_tga(),
        backfill_reserves(),
        backfill_rrp(),
        backfill_srp(),
        backfill_sofr_iorb(),
    )


def main() -> None:
    print(f"=== Backfill {START_DATE} → {END_DATE} ({YEARS}y) ===")
    asyncio.run(_async_sources())
    backfill_net_liquidity()
    backfill_market_stress()
    print("=== DONE ===")


if __name__ == "__main__":
    main()
