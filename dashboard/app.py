"""
Streamlit dashboard.

Run with:
    streamlit run usd_liquidity_monitor/dashboard/app.py

Auto-refreshes every 60s. Reads parquet files directly, no coupling to
the background monitor process.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

DATA_DIR = (Path(__file__).parent.parent / "data" / "raw").resolve()


st.set_page_config(
    page_title="Fed Liquidity Monitor",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def _load(name: str) -> pd.DataFrame:
    p = DATA_DIR / f"{name}.parquet"
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


def _latest_delta(df: pd.DataFrame, col: str) -> tuple[float | None, float | None]:
    if df.empty:
        return None, None
    latest = float(df[col].iloc[-1])
    if len(df) < 2:
        return latest, 0.0
    prev = float(df[col].iloc[-2])
    return latest, latest - prev


# ═══════════════════════ Header ═══════════════════════════════════
st_autorefresh(interval=60_000, key="auto_refresh")

st.title("💧 Fed Liquidity Monitor")

now_utc = datetime.now(timezone.utc)
st.caption(
    f"Last refresh: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')} "
    f"| Auto-refresh: 60s"
)

# ═══════════════════════ Data load ════════════════════════════════
tga = _load("tga").sort_values("poll_ts") if not _load("tga").empty else _load("tga")
rrp = _load("rrp").sort_values("poll_ts") if not _load("rrp").empty else _load("rrp")
srp = _load("srp").sort_values("poll_ts") if not _load("srp").empty else _load("srp")
reserves = _load("reserves").sort_values("poll_ts") if not _load("reserves").empty else _load("reserves")
nl = _load("net_liquidity").sort_values("as_of") if not _load("net_liquidity").empty else _load("net_liquidity")

# ═══════════════════════ Top row: KPI cards ═══════════════════════
c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    val, delta = _latest_delta(tga, "close_bal_bn")
    if val is not None:
        st.metric("TGA", f"${val:,.1f} bn",
                  f"{delta:+.1f} bn" if delta is not None else None,
                  delta_color="inverse",  # TGA up = liquidity drain = bad
                  help="Treasury General Account. Up = Treasury吸水.")
    else:
        st.metric("TGA", "no data")

with c2:
    val, delta = _latest_delta(rrp, "total_accepted_bn")
    if val is not None:
        st.metric("ON RRP", f"${val:,.1f} bn",
                  f"{delta:+.1f} bn" if delta is not None else None,
                  help="Overnight Reverse Repo. Down = RRP draining → reserves.")
    else:
        st.metric("ON RRP", "no data")

with c3:
    val, delta = _latest_delta(reserves, "reserves_bn")
    if val is not None:
        st.metric("Reserves (WRESBAL)", f"${val:,.1f} bn",
                  f"{delta:+.1f} bn" if delta is not None else None,
                  help="Bank reserves at the Fed (weekly).")
    else:
        st.metric("Reserves", "no data")

with c4:
    if not srp.empty:
        latest = float(srp["total_accepted_bn"].iloc[-1])
        if latest > 0:
            st.metric("🚨 SRP", f"${latest:,.2f} bn", "CRISIS",
                      delta_color="inverse",
                      help="SRP non-zero = Scarce→Crisis regime")
        else:
            st.metric("SRP", "$0.00 bn", "Normal",
                      help="Standing Repo Facility usage (should be zero).")
    else:
        st.metric("SRP", "no data")

with c5:
    val, delta = _latest_delta(nl, "net_liquidity_bn")
    if val is not None:
        st.metric("Net Liquidity", f"${val:,.1f} bn",
                  f"{delta:+.1f} bn" if delta is not None else None,
                  help="Reserves + RRP − TGA. The composite water level.")
    else:
        st.metric("Net Liquidity", "not yet computed")

st.divider()

# ═══════════════════════ Net Liquidity chart ══════════════════════
st.subheader("Net Liquidity = Reserves + RRP − TGA")

if not nl.empty:
    nl_plot = nl.drop_duplicates(subset=["as_of"], keep="last").sort_values("as_of")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=nl_plot["as_of"],
            y=nl_plot["net_liquidity_bn"],
            mode="lines+markers",
            name="Net Liquidity",
            line=dict(width=3, color="#1f77b4"),
            marker=dict(size=6),
        )
    )
    # 7-day EWMA
    nl_plot["ewma7"] = nl_plot["net_liquidity_bn"].ewm(span=7, adjust=False).mean()
    fig.add_trace(
        go.Scatter(
            x=nl_plot["as_of"],
            y=nl_plot["ewma7"],
            mode="lines",
            name="7-day EWMA",
            line=dict(width=2, dash="dash", color="#ff7f0e"),
        )
    )
    fig.update_layout(
        height=420,
        xaxis_title="As-of date",
        yaxis_title="Billion USD",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Net liquidity not yet computed (need TGA + RRP + Reserves snapshots).")

# ═══════════════════════ Components stacked ═══════════════════════
st.subheader("Components")

if not nl.empty:
    nl_plot = nl.drop_duplicates(subset=["as_of"], keep="last").sort_values("as_of")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=nl_plot["as_of"], y=nl_plot["reserves_bn"],
        name="Reserves", stackgroup="pos", fill="tonexty",
        line=dict(color="#2ca02c")))
    fig.add_trace(go.Scatter(
        x=nl_plot["as_of"], y=nl_plot["rrp_bn"],
        name="ON RRP", stackgroup="pos", fill="tonexty",
        line=dict(color="#9467bd")))
    fig.add_trace(go.Scatter(
        x=nl_plot["as_of"], y=-nl_plot["tga_bn"],
        name="−TGA (drain)", stackgroup="neg", fill="tonexty",
        line=dict(color="#d62728")))

    fig.update_layout(
        height=420,
        xaxis_title="As-of date",
        yaxis_title="Billion USD (signed)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════ Layer-3 proxy (if data exists) ═══════════
st.subheader("Layer-3 Proxy Stress (minute bars)")

proxy_dir = DATA_DIR / "proxy"
proxy_files = sorted(proxy_dir.glob("*.parquet"))[-3:]  # last 3 days

if proxy_files:
    proxy_df = pd.concat([pd.read_parquet(f) for f in proxy_files], ignore_index=True)
    proxy_df["ts"] = pd.to_datetime(proxy_df["ts"])
    latest_ts = proxy_df["ts"].max()
    cutoff = latest_ts - pd.Timedelta(hours=6)
    recent = proxy_df[proxy_df["ts"] > cutoff]

    if not recent.empty:
        fig = go.Figure()
        for sym in sorted(recent["symbol"].unique()):
            sub = recent[recent["symbol"] == sym].sort_values("ts")
            # normalize to 0 at start of window for visual comparison
            first = sub["close"].iloc[0]
            sub["norm_pct"] = (sub["close"] / first - 1) * 100
            fig.add_trace(go.Scatter(
                x=sub["ts"], y=sub["norm_pct"],
                mode="lines", name=sym,
            ))
        fig.update_layout(
            height=380, xaxis_title="Time",
            yaxis_title="% change (normalized to window start)",
            legend=dict(orientation="h"),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"Showing last 6h across {len(recent['symbol'].unique())} symbols, "
                   f"{len(recent):,} bars.")
else:
    st.info("No Layer-3 proxy data yet. Polygon stream may still be warming up, "
            "or you're outside market hours.")

st.divider()

# ═══════════════════════ Raw tables ═══════════════════════════════
with st.expander("📋 Raw snapshot history"):
    tabs = st.tabs(["TGA", "RRP", "SRP", "Reserves", "Net Liquidity"])
    for tab, df in zip(tabs, [tga, rrp, srp, reserves, nl]):
        with tab:
            if df.empty:
                st.write("_No data yet._")
            else:
                display_cols = [c for c in df.columns if c != "_hash"]
                st.dataframe(
                    df[display_cols].tail(100).iloc[::-1],
                    use_container_width=True,
                    height=400,
                )
