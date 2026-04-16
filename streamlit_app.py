"""Streamlit dashboard — run alongside FastAPI or set LIQUIDITY_API_URL."""

from __future__ import annotations

import os
from datetime import date

import httpx
import pandas as pd
import streamlit as st
import yfinance as yf

from liquidity_calendar import us_business_sessions_countdown

API = os.environ.get("LIQUIDITY_API_URL", "http://127.0.0.1:8765").rstrip("/")


@st.cache_data(ttl=30)
def _fetch_instant():
    r = httpx.get(f"{API}/liquidity/instant", timeout=60.0)
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=120)
def _fetch_history():
    r = httpx.get(f"{API}/liquidity/history?limit=800", timeout=60.0)
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=120)
def _fetch_stress_history():
    r = httpx.get(f"{API}/liquidity/stress_history?hours=24", timeout=60.0)
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=300)
def _fetch_regime_panel():
    r = httpx.get(f"{API}/liquidity/regime_panel?chart_days=90", timeout=90.0)
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=120)
def _fetch_decision_sketch():
    r = httpx.get(f"{API}/liquidity/decision_sketch", timeout=90.0)
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=3600)
def _bench_90d():
    d = yf.download("^GSPC", period="120d", interval="1d", progress=False, auto_adjust=True)
    if d is None or d.empty:
        return None
    if isinstance(d.columns, pd.MultiIndex):
        d.columns = [c[0] if isinstance(c, tuple) else c for c in d.columns]
    s = d["Close"].squeeze()
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    return s.rename("spx_close")


def main():
    st.set_page_config(page_title="USD Liquidity", layout="wide")
    st.title("USD system liquidity (TGA · ON RRP · reserves · WALCL)")
    st.caption(
        "Official series are daily / weekly; Polygon/yfinance blocks are shadow signals only."
    )

    try:
        data = _fetch_instant()
    except Exception as exc:
        st.error(f"Could not reach API at {API}: {exc}")
        st.info("Start the API: `uvicorn main:app --host 127.0.0.1 --port 8765` from `usd_liquidity_monitor/`.")
        return

    bal = data.get("balances_bn") or {}
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("WALCL (Fed assets)", _fmt_bn(bal.get("walcl")))
    c2.metric("TGA", _fmt_bn(bal.get("tga")), bal.get("tga_source") or "")
    c3.metric("ON RRP", _fmt_bn(bal.get("rrp")))
    c4.metric("Reserves (TOTRESNS)", _fmt_bn(bal.get("reserves")))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Currency in circulation", _fmt_bn(bal.get("currency_in_circulation")))
    c6.metric("Net liquidity (WALCL−TGA−RRP−CIC)", _fmt_bn(data.get("net_liquidity_bn")))
    c7.metric("SOFR − DFF", _fmt_bp(data.get("sofr_minus_dff_bp")))
    if data.get("net_liquidity_bn_volume_adjusted") is not None:
        st.caption(f"Volume-adjusted net (ETF spike bias): {_fmt_bn(data.get('net_liquidity_bn_volume_adjusted'))}")
    vel = data.get("liquidity_velocity_bn_per_hr")
    c8.metric("Velocity (B/h, last step)", "—" if vel is None else f"{float(vel):+.2f}")

    st.subheader("TGA / QRA dynamics (includes ΔT_avg vs RRP)")
    st.json(data.get("tga_forecast") or {})
    st.subheader("Reserves tax-window scenario")
    st.json(data.get("reserves_tax_path") or {})
    st.subheader("Liquidity runway (crude)")
    st.json(data.get("liquidity_runway") or {})
    _ode_runway_chart(data)
    _qt_policy_stress_panel(data)
    _macro_resonance_panel(data)
    _qra_vs_ode_table(data)
    st.subheader("Equity vs liquidity divergence")
    st.json(data.get("equity_liquidity_divergence") or {})

    st.subheader("Shadow stack (FRED + Polygon)")
    st.json(data.get("shadow_indicators") or {})

    st.subheader("Heuristic layer (yfinance ^IRX baseline)")
    st.write(data.get("heuristic") or {})

    st.subheader("MOVE vs VIX (Treasury vol vs equity vol)")
    st.json(data.get("volatility_linkage") or {})
    st.write(data.get("vol_control_hint") or {})

    st.subheader("Stress — last 24h hourly max (frog-boil view)")
    try:
        panel = data.get("stress_panel_24h") or _fetch_stress_history()
        if panel:
            sdf = pd.DataFrame(panel)
            if not sdf.empty and "hour_utc" in sdf.columns:
                sdf["hour_utc"] = pd.to_datetime(sdf["hour_utc"], utc=True)
                sdf = sdf.set_index("hour_utc")
                st.bar_chart(sdf[["max_stress"]])
    except Exception as exc:
        st.caption(f"No stress distribution: {exc}")

    st.subheader("Signed contributions to net liquidity (single snapshot)")
    heat = _contribution_row(bal)
    if heat is not None:
        try:
            st.dataframe(
                heat.style.format("{:,.1f}").background_gradient(cmap="RdYlGn", axis=1),
                width="stretch",
            )
        except Exception:
            st.bar_chart(heat.T)

    st.subheader("Rolling correlation: daily ΔNet vs daily ΔSPX")
    _rolling_corr_panel()

    st.subheader("Liquidity beta panel (XLK / IWM vs daily ΔNet)")
    _liquidity_beta_panel()

    _regime_and_decision_panel()

    st.subheader("Rates")
    rates = data.get("rates") or {}
    st.write(
        {
            "SOFR (%)": rates.get("sofr_pct"),
            "Effective Fed Funds (%)": rates.get("dff_effective_pct"),
        }
    )

    alerts = data.get("alerts") or []
    if alerts:
        st.subheader("Alerts")
        for a in alerts:
            st.warning(a)

    st.subheader("History (stored snapshots)")
    try:
        hist = _fetch_history()
        if hist:
            df = pd.DataFrame(hist)
            df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
            df = df.sort_values("ts_utc")
            cols = [
                c
                for c in (
                    "net_liquidity_bn",
                    "rrp_bn",
                    "liquidity_velocity_bn_per_hr",
                    "velocity_24h_bn_per_day",
                    "velocity_7d_bn_per_day",
                )
                if c in df.columns
            ]
            if cols:
                st.line_chart(df.set_index("ts_utc")[cols])
    except Exception as exc:
        st.caption(f"No history chart: {exc}")


def _regime_and_decision_panel() -> None:
    st.subheader("Regime layer + conditional backtest (stress → SPY)")
    try:
        rp = _fetch_regime_panel()
    except Exception as exc:
        st.caption(f"Regime panel unavailable ({exc}). Need DB history with reserves/RRP.")
        return
    if rp.get("status") != "ok":
        st.caption(rp.get("status", "unavailable"))
        return
    cur = rp.get("current") or {}
    st.write(
        f"**Today (model):** `{cur.get('regime_hard')}` — "
        f"p_abundant={cur.get('p_abundant', 0):.2f}, p_ample={cur.get('p_ample', 0):.2f}, "
        f"p_scarce={cur.get('p_scarce', 0):.2f}, p_crisis={cur.get('p_crisis', 0):.2f}"
    )
    rows = rp.get("chart_series_last_days") or []
    if rows:
        df = pd.DataFrame(rows)
        if "date" in df.columns:
            df = df.set_index(pd.to_datetime(df["date"]))
            pc = [c for c in df.columns if c.startswith("p_")]
            if pc:
                st.caption("Stacked soft regime weights (rolling rank → prototype distance, not HMM yet).")
                st.area_chart(df[pc])
    cm = (rp.get("conditional_hit_matrix") or {}).get("by_regime") or {}
    if cm:
        st.markdown("**P(SPY forward < 0 | stress ≥ threshold), by hard regime**")
        st.dataframe(pd.DataFrame(cm).T, width="stretch")

    st.subheader("Decision sketch (position translator scaffold)")
    try:
        ds = _fetch_decision_sketch()
        sk = (ds or {}).get("decision_sketch")
        if sk:
            st.dataframe(pd.DataFrame(sk.get("actions") or []), width="stretch")
            st.caption(sk.get("note") or "")
        else:
            st.caption("No decision sketch returned.")
    except Exception as exc:
        st.caption(f"Decision sketch: {exc}")


def _ode_runway_chart(data: dict) -> None:
    st.subheader("Runway projection — ODE (QT + QRA dT/dt + RRP floor + tax pulses)")
    ode = data.get("ode_runway")
    if not ode:
        st.caption("No `ode_runway` in API payload (needs reserves, RRP, TGA).")
        return
    if ode.get("error"):
        st.warning(str(ode["error"]))
        return
    sc = ode.get("scenarios") or {}
    base = sc.get("baseline") or {}
    t_days = base.get("t_days") or []
    if not t_days or base.get("error"):
        st.caption("ODE baseline unavailable (install scipy, check logs).")
        return

    stab = data.get("ode_stability") or {}
    st.caption(
        f"US business-day anchor: **{ode.get('calendar_anchor_us_bd') or '—'}** · "
        f"tax pulses: **{ode.get('tax_pulses_enabled', '—')}**"
    )

    d0 = date.today()
    lcl_iso = base.get("lclor_expected_date")
    rrp_iso = base.get("rrp_floor_expected_date")
    n_lcl = None
    n_rrp = None
    try:
        if isinstance(lcl_iso, str):
            n_lcl = us_business_sessions_countdown(d0, date.fromisoformat(lcl_iso))
        if isinstance(rrp_iso, str):
            n_rrp = us_business_sessions_countdown(d0, date.fromisoformat(rrp_iso))
    except ValueError:
        pass
    c1, c2, c3 = st.columns(3)
    c1.metric(
        "Sessions to LCLoR (US BD)",
        "—" if n_lcl is None else str(n_lcl),
        help=f"Calendar breach anchor **{lcl_iso}** (model)." if lcl_iso else None,
    )
    c2.metric(
        "Sessions to RRP floor (US BD)",
        "—" if n_rrp is None else str(n_rrp),
        help=f"RRP floor touch **{rrp_iso}** (model)." if rrp_iso else None,
    )
    prev_l = stab.get("baseline_lclor_date_24h_ago")
    drift = stab.get("lclor_acceleration_bd_vs_24h")
    c3.metric(
        "Prior baseline LCLoR (~24h)",
        prev_l if prev_l else "—",
        help=(
            f"Breach moved earlier by **{drift}** US business sessions vs this field."
            if drift is not None
            else "No comparable snapshot yet."
        ),
    )

    cols: dict[str, list[float]] = {}
    meta_lines: list[str] = []
    for name, label in (
        ("optimistic", "optimistic (slower TGA / higher β)"),
        ("baseline", "baseline"),
        ("pessimistic", "pessimistic (faster TGA / lower β)"),
    ):
        pr = sc.get(name) or {}
        if pr.get("error"):
            meta_lines.append(f"{name}: {pr['error']}")
            continue
        rs = pr.get("reserves") or []
        if len(rs) != len(t_days):
            meta_lines.append(f"{name}: length mismatch")
            continue
        cols[label] = [float(x) for x in rs]
        bd = pr.get("lclor_breach_day")
        lcl = pr.get("lclor_bn")
        rf = pr.get("rrp_hits_floor_day")
        if name == "baseline":
            dcal = pr.get("lclor_expected_date")
            drrp = pr.get("rrp_floor_expected_date")
            if bd is not None:
                tail = f" → **{dcal}**" if isinstance(dcal, str) else ""
                meta_lines.append(
                    f"Baseline — reserves cross ~{float(lcl or 0):,.0f}B near day {float(bd):.0f}{tail}."
                )
            if rf is not None:
                t2 = f" → **{drrp}**" if isinstance(drrp, str) else ""
                meta_lines.append(f"Baseline — RRP at floor near day {float(rf):.0f}{t2} (β→0).")
    if not cols:
        for m in meta_lines:
            st.caption(m)
        return
    df = pd.DataFrame(cols)
    df.index = pd.Index([float(x) for x in t_days], name="day")
    st.line_chart(df)
    for m in meta_lines:
        st.caption(m)
    with st.expander("ODE bundle JSON"):
        st.json(ode)
    if stab:
        with st.expander("ODE stability / drift"):
            st.json(stab)


def _qt_policy_stress_panel(data: dict) -> None:
    st.subheader("Fed QT tapering lab (same TGA / β — pace stress only)")
    pol = data.get("ode_qt_policy")
    if not pol or pol.get("error"):
        st.caption("No `ode_qt_policy` bundle (needs balances + scipy).")
        if isinstance(pol, dict) and pol.get("error"):
            st.warning(str(pol["error"]))
        return
    rows = []
    labels = pol.get("labels") or {}
    for key, pr in (pol.get("paths") or {}).items():
        if not isinstance(pr, dict) or pr.get("error"):
            rows.append({"scenario": labels.get(key, key), "lclor_day_idx": None, "lclor_date": None})
            continue
        rows.append(
            {
                "scenario": labels.get(key, key),
                "lclor_day_idx": pr.get("lclor_breach_day"),
                "lclor_date": pr.get("lclor_expected_date"),
                "rrp_floor_date": pr.get("rrp_floor_expected_date"),
            }
        )
    st.dataframe(pd.DataFrame(rows), width="stretch")
    st.caption(
        "Dove path: QT pace falls to **ode_dove_qt_bn_per_month** from the first US business day of the **next** "
        "calendar month. Hawk path: constant **ode_hawk_qt_bn_per_month** (configure in Settings / env)."
    )
    with st.expander("QT policy paths JSON"):
        st.json(pol)


def _macro_resonance_panel(data: dict) -> None:
    st.subheader("Macro resonance (FOMC / QRA overlap)")
    mr = data.get("macro_resonance") or {}
    matches = mr.get("matches") or []
    if not matches:
        st.caption("No overlap between ODE breach dates and `macro_calendar.json` within the window.")
        return
    st.warning("**Resonance_Warning** — model breach dates stack against scheduled macro events.")
    st.dataframe(pd.DataFrame(matches), width="stretch")
    with st.expander("Resonance JSON"):
        st.json(mr)


def _qra_vs_ode_table(data: dict) -> None:
    st.subheader("Official QRA anchor vs ODE baseline (desk alpha lens)")
    tga = data.get("tga_forecast") or {}
    ode = data.get("ode_runway") or {}
    base = (ode.get("scenarios") or {}).get("baseline") or {}
    rows = [
        {
            "Metric": "QRA end-quarter cash target (Bn)",
            "Treasury / QRA": tga.get("qra_effective_target_bn") or tga.get("qra_target_tga_bn"),
            "ODE (uses same ΔT_avg when set)": "—",
        },
        {
            "Metric": "Implied ΔT_avg (Bn/day)",
            "Treasury / QRA": tga.get("delta_t_avg_bn_per_day"),
            "ODE (same driver)": tga.get("delta_t_avg_bn_per_day"),
        },
        {
            "Metric": "Baseline LCLoR breach (calendar)",
            "Treasury / QRA": "Not in PDF — model only",
            "ODE (same driver)": base.get("lclor_expected_date"),
        },
        {
            "Metric": "RRP hits floor (calendar)",
            "Treasury / QRA": "Not stated — watch bill supply",
            "ODE (same driver)": base.get("rrp_floor_expected_date"),
        },
    ]
    st.dataframe(pd.DataFrame(rows), width="stretch")
    st.caption(
        "If **ODE** RRP / reserve stress runs **ahead** of where a static QRA cash target would imply, "
        "the gap is a stylised **alpha** flag (market absorbing debt faster than the official financing baseline)."
    )


def _contribution_row(bal: dict) -> pd.DataFrame | None:
    w, t, r, c = bal.get("walcl"), bal.get("tga"), bal.get("rrp"), bal.get("currency_in_circulation")
    if any(x is None for x in (w, t, r, c)):
        return None
    row = {
        "WALCL (+)": float(w),
        "−TGA": -float(t),
        "−ON RRP": -float(r),
        "−CIC": -float(c),
    }
    return pd.DataFrame([row])


def _liquidity_beta_panel() -> None:
    try:
        hist = _fetch_history()
        if not hist:
            st.caption("No history for beta panel.")
            return
        df = pd.DataFrame(hist)
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
        df["d"] = df["ts_utc"].dt.normalize()
        daily = df.groupby("d", as_index=True)["net_liquidity_bn"].last().sort_index()
        dnet = daily.diff().dropna()
        dnet.index = pd.to_datetime(dnet.index).tz_localize(None)
        if len(dnet) < 25:
            st.caption("Need more snapshot history to estimate rolling betas.")
            return

        def _close(sym: str) -> pd.Series | None:
            d = yf.download(sym, period="240d", interval="1d", progress=False, auto_adjust=True)
            if d is None or d.empty:
                return None
            if isinstance(d.columns, pd.MultiIndex):
                d.columns = [c[0] if isinstance(c, tuple) else c for c in d.columns]
            s = d["Close"].squeeze()
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0]
            s = s.rename(sym)
            s.index = pd.to_datetime(s.index).tz_localize(None).normalize()
            return s

        xlk = _close("XLK")
        iwm = _close("IWM")
        if xlk is None or iwm is None:
            st.caption("Could not download XLK/IWM.")
            return
        m = pd.concat([xlk, iwm, dnet.rename("d_net")], axis=1).dropna()
        if len(m) < 30:
            st.caption("Insufficient overlap between ETF history and net-liquidity changes.")
            return
        win = min(60, max(20, len(m) // 2))
        out: dict[str, float | None] = {}
        for sym in ("XLK", "IWM"):
            er = m[sym].pct_change()
            cov = er.rolling(win).cov(m["d_net"])
            var = m["d_net"].rolling(win).var()
            beta = (cov / var.replace(0, float("nan"))).iloc[-1]
            out[sym] = None if pd.isna(beta) else float(beta)
        st.dataframe(pd.DataFrame([out]), width="stretch")
        st.caption(
            f"Rolling beta of daily ETF returns to daily ΔNet (last window ≈ {win} sessions). "
            "XLK ≈ large-cap tech factor; IWM ≈ small-cap liquidity stock."
        )
    except Exception as exc:
        st.caption(f"Beta panel skipped: {exc}")


def _rolling_corr_panel() -> None:
    try:
        hist = _fetch_history()
        spx = _bench_90d()
        if not hist or spx is None:
            st.caption("Need API history and ^GSPC download for correlation panel.")
            return
        h = pd.DataFrame(hist)
        h["ts_utc"] = pd.to_datetime(h["ts_utc"], utc=True)
        h["date"] = h["ts_utc"].dt.normalize()
        daily = h.groupby("date", as_index=False)["net_liquidity_bn"].last()
        daily["d_net"] = daily["net_liquidity_bn"].diff()
        bx = spx.reset_index()
        bx.columns = ["date", "spx_close"]
        if not bx.empty:
            bx["date"] = pd.to_datetime(bx["date"], utc=True).dt.normalize()
        bx["d_spx"] = bx["spx_close"].pct_change()
        m = pd.merge(daily, bx, on="date", how="inner").dropna()
        if len(m) < 15:
            st.caption("Not enough overlapping days yet for a stable rolling correlation.")
            return
        win = min(40, max(10, len(m) // 3))
        m = m.sort_values("date")
        m["roll_corr"] = m["d_net"].rolling(win).corr(m["d_spx"])
        st.line_chart(m.set_index("date")[["roll_corr"]])
        st.caption(f"Window ≈ {win} trading days; uses last net snapshot per UTC calendar day.")
    except Exception as exc:
        st.caption(f"Correlation panel skipped: {exc}")


def _fmt_bn(v) -> str:
    if v is None:
        return "—"
    return f"{float(v):,.1f}B"


def _fmt_bp(v) -> str:
    if v is None:
        return "—"
    return f"{float(v):,.1f} bp"


main()
