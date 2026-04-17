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


# Pull tier thresholds from the same config the monitor / alerter use,
# so .env overrides stay in sync with dashboard badges. The package root
# is one level up from dashboard/.
import sys as _sys
_PKG_ROOT = Path(__file__).parent.parent.parent
if str(_PKG_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_PKG_ROOT))
from usd_liquidity_monitor.config import settings as _s

RESERVES_TIERS = {
    "medium": _s.reserves_medium_bn,
    "high":   _s.reserves_high_bn,
    "critical": _s.reserves_critical_bn,
}
RRP_TIERS = {
    "medium": _s.rrp_medium_bn,
    "high":   _s.rrp_high_bn,
    "critical": _s.rrp_critical_bn,
}
NET_LIQ_TIERS = {
    "medium": _s.net_liq_medium_bn,
    "high":   _s.net_liq_high_bn,
    "critical": _s.net_liq_critical_bn,
}
MARKET_Z_TIERS = {
    "medium": _s.market_stress_medium_z,
    "high":   _s.market_stress_high_z,
    "critical": _s.market_stress_critical_z,
}


def regime_emoji(value, tiers, direction="below"):
    """Traffic-light emoji for a metric against its tiered thresholds.

    direction="below": lower is worse (reserves, RRP, net liquidity).
    direction="above": higher is worse (stress z, auction tail)."""
    if value is None:
        return "⚪"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "⚪"
    if direction == "below":
        if v < tiers["critical"]: return "🔴"
        if v < tiers["high"]:     return "🟠"
        if v < tiers["medium"]:   return "🟡"
        return "🟢"
    else:  # above
        if v > tiers["critical"]: return "🔴"
        if v > tiers["high"]:     return "🟠"
        if v > tiers["medium"]:   return "🟡"
        return "🟢"


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

# ═══════════════════════ 📖 Reading guide (collapsible) ═══════════
with st.expander("📖 怎么读这些数据 · How to read this dashboard", expanded=False):
    st.markdown(
        """
### 一句话总览
**Net Liquidity = Reserves + RRP − TGA** 是综合水位。水位降到阈值以下，系统推告警。

| 指标 | 意思 | 关注方向 | 阈值（从 .env） |
|---|---|---|---|
| **TGA** 🏦 | 美国财政部账户 | ↑=Treasury 抽银行的钱（**坏**）；↓=Treasury 花钱回流（**好**） | 看日变化 > 50bn 就留意 |
| **ON RRP** 🛋 | 货币基金的缓冲垫 | ↓=缓冲被消耗，下次抽水将直接命中银行准备金 | 🟡<200 🟠<100 🔴<50 bn |
| **Reserves** 🏛 | 银行放在 Fed 的现金（真·流动性） | ↓=银行开始缺钱 | 🟡<3.2T 🟠<3.0T 🔴<2.8T |
| **SRP** 🚨 | 银行向 Fed 紧急借款窗口 | 平时 = 0；**任何非零 = 危机** | **>0 直接 CRITICAL** |
| **Net Liquidity** 💧 | 综合水位 | 趋势向下 + 绝对水位低 = 风险升高 | 🟡<2.4T 🟠<2.2T 🔴<2.0T |
| **Market Stress z** ⚡ | ETF + VIX 综合压力分数（15 分钟刷新） | >0 = 股市开始不安；> 2σ = 警觉 | 🟡>2 🟠>3 🔴>4 |
| **30Y 拍卖 tail** 📉 | 拍卖需求强弱（领先 2-4 周） | <0 强需求；>0 dealer 勉强 | 🟡>2 🟠>4 🔴>6 bp |

### 一天 1 分钟读法
1. 扫一眼 5 张 KPI 卡片，看右上角 emoji
2. 全 🟢 → 今天没事，关掉就行
3. 出现 🟡 → 留意这个指标，明天继续看
4. 出现 🟠 → 慢层已压力，快层（Market z）会告诉你市场开始反应没有
5. 出现 🔴 → 真事件，看 iPhone Bark 推送的行动建议

### 两个"共振"时最有价值
- **慢层 + 快层同时异常**：结构性压力 + 市场开始 price it in → 历史上这是危机前 3-7 天的特征
- **30Y tail 扩大 + 准备金慢慢下跌**：2-4 周级的领先——最珍贵的预警窗口

### ⚠ 注意
这不是投资建议。阈值基于 2019 repo、2020 COVID、2023 SVB 三次危机校准，但**没有经过完整回测**（下一步工作）。用作决策**起点**，不是终点。
"""
    )

# ═══════════════════════ Data load ════════════════════════════════
tga = _load("tga").sort_values("poll_ts") if not _load("tga").empty else _load("tga")
rrp = _load("rrp").sort_values("poll_ts") if not _load("rrp").empty else _load("rrp")
srp = _load("srp").sort_values("poll_ts") if not _load("srp").empty else _load("srp")
reserves = _load("reserves").sort_values("poll_ts") if not _load("reserves").empty else _load("reserves")
nl = _load("net_liquidity").sort_values("as_of") if not _load("net_liquidity").empty else _load("net_liquidity")
ms = _load("market_stress").sort_values("as_of_utc") if not _load("market_stress").empty else _load("market_stress")
regime_df = _load("regime").sort_values("as_of") if not _load("regime").empty else _load("regime")

# ═══════════════════════ 🏷 Regime status strip ═══════════════════
# Full-width regime indicator — appears above KPIs so users see
# "what regime am I in right now" before any specific number.
_REGIME_VISUAL = {
    "abundant": {"emoji": "🟢", "label_cn": "充裕", "color": "#2ca02c"},
    "ample":    {"emoji": "🟡", "label_cn": "充足", "color": "#FFD700"},
    "scarce":   {"emoji": "🟠", "label_cn": "稀缺", "color": "#FF8C00"},
    "crisis":   {"emoji": "🔴", "label_cn": "危机", "color": "#DC143C"},
}

if not regime_df.empty:
    latest_regime = regime_df.iloc[-1]
    reg = str(latest_regime["regime_hard"])
    viz = _REGIME_VISUAL.get(reg, {"emoji": "⚪", "label_cn": "未知", "color": "gray"})
    st.markdown(
        f"### {viz['emoji']} 当前 regime：**{reg.upper()}** · {viz['label_cn']}"
    )
    # 4-bar probability distribution
    rc1, rc2, rc3, rc4 = st.columns(4)
    for col, name, cn in [
        (rc1, "abundant", "充裕"),
        (rc2, "ample",    "充足"),
        (rc3, "scarce",   "稀缺"),
        (rc4, "crisis",   "危机"),
    ]:
        p = float(latest_regime[f"p_{name}"])
        highlight = "**" if name == reg else ""
        col.markdown(
            f"<div style='text-align:center; padding:4px; "
            f"background:{_REGIME_VISUAL[name]['color']}22; "
            f"border-radius:6px;'>"
            f"<div style='font-size:0.8em; color:gray;'>{_REGIME_VISUAL[name]['emoji']} {cn}</div>"
            f"<div style='font-size:1.3em;'>{highlight}{p:.1%}{highlight}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
    st.caption(
        f"基于过去 252 天 Reserves + RRP 的滚动分位数，"
        f"当前点坐标 = (reserves_rank={latest_regime['reserves_rank']:.2f}, "
        f"rrp_rank={latest_regime['rrp_rank']:.2f})，"
        f"最接近 **{reg}** 原型"
    )
    st.divider()

# ═══════════════════════ Top row: KPI cards ═══════════════════════
c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    # TGA has no tiered floor — watch the daily delta, not the level.
    val, delta = _latest_delta(tga, "close_bal_bn")
    if val is not None:
        st.metric(
            "🏦 TGA · 财政部账户",
            f"${val:,.0f} bn",
            f"{delta:+.0f} bn" if delta is not None else None,
            delta_color="inverse",
            help=(
                "美国财政部在 Fed 的账户。\n"
                "↑ 上涨 = Treasury 抽银行的钱（流动性↓，坏）\n"
                "↓ 下跌 = Treasury 花钱进市场（流动性↑，好）\n"
                "看日变化：|Δ1d| > 50bn 即告警"
            ),
        )
    else:
        st.metric("TGA", "no data")

with c2:
    val, delta = _latest_delta(rrp, "total_accepted_bn")
    if val is not None:
        emo = regime_emoji(val, RRP_TIERS, "below")
        st.metric(
            f"🛋 ON RRP · 缓冲垫 {emo}",
            f"${val:,.1f} bn",
            f"{delta:+.1f} bn" if delta is not None else None,
            help=(
                "货币基金过夜存 Fed 的缓冲垫。\n"
                "🟢 >200bn 缓冲充足\n"
                "🟡 <200bn 开始紧\n"
                "🟠 <100bn 基本耗尽\n"
                "🔴 <50bn 下次抽水直接命中银行准备金"
            ),
        )
    else:
        st.metric("ON RRP", "no data")

with c3:
    val, delta = _latest_delta(reserves, "reserves_bn")
    if val is not None:
        emo = regime_emoji(val, RESERVES_TIERS, "below")
        st.metric(
            f"🏛 Reserves · 银行准备金 {emo}",
            f"${val:,.0f} bn",
            f"{delta:+.0f} bn" if delta is not None else None,
            help=(
                "银行放在 Fed 的现金 = 真·流动性。\n"
                "🟢 >3.2T 充裕（Abundant）\n"
                "🟡 <3.2T 充足下沿（Ample）\n"
                "🟠 <3.0T 接近 Fed 警戒线\n"
                "🔴 <2.8T 2019 repo 危机级别"
            ),
        )
    else:
        st.metric("Reserves", "no data")

with c4:
    if not srp.empty:
        latest = float(srp["total_accepted_bn"].iloc[-1])
        if latest > 0:
            st.metric(
                "🚨 SRP · 紧急借款",
                f"${latest:,.2f} bn",
                "CRISIS",
                delta_color="inverse",
                help="银行向 Fed 紧急借款的量。任何非零 = 有银行在抢钱 = 危机模式",
            )
        else:
            st.metric(
                "🚨 SRP · 紧急借款 🟢",
                "$0.00 bn",
                "正常",
                help="平时应为 0。一旦非零 = CRITICAL 告警立即发。2019 repo 危机时爆过。",
            )
    else:
        st.metric("SRP", "no data")

with c5:
    val, delta = _latest_delta(nl, "net_liquidity_bn")
    if val is not None:
        emo = regime_emoji(val, NET_LIQ_TIERS, "below")
        st.metric(
            f"💧 Net Liquidity · 综合水位 {emo}",
            f"${val:,.0f} bn",
            f"{delta:+.0f} bn" if delta is not None else None,
            help=(
                "Reserves + RRP − TGA = 给股市的那碗汤还剩多少。\n"
                "🟢 >2.4T 安全\n"
                "🟡 <2.4T 压力累积中（MEDIUM）\n"
                "🟠 <2.2T 明显紧张（HIGH）\n"
                "🔴 <2.0T 结构性危险（CRITICAL）"
            ),
        )
    else:
        st.metric("Net Liquidity", "not yet computed")

# ═══════════════════════ 🧭 现状解读 (auto-generated) ═════════════
# Auto-built narrative so the user gets a plain-language read of where
# the system stands right now, without having to interpret 5 KPIs.
def _build_current_state() -> str:
    lines: list[str] = []

    # Reserves — explicit per-tier handling; ⚪ (null/unparseable) falls
    # through to the "数据缺失" branch rather than being mis-reported as
    # CRITICAL.
    r_val, _ = _latest_delta(reserves, "reserves_bn")
    if r_val is not None:
        emo = regime_emoji(r_val, RESERVES_TIERS, "below")
        if emo == "🟢":
            lines.append(f"{emo} **银行准备金** 在充裕区（${r_val:,.0f} bn > ${RESERVES_TIERS['medium']:,.0f}bn）")
        elif emo == "🟡":
            lines.append(
                f"{emo} **银行准备金** 已跌破 MEDIUM 线（${r_val:,.0f} bn < ${RESERVES_TIERS['medium']:,.0f}bn）"
                " — 压力累积阶段"
            )
        elif emo == "🟠":
            lines.append(
                f"{emo} **银行准备金** 接近 Fed 警戒线（${r_val:,.0f} bn < ${RESERVES_TIERS['high']:,.0f}bn）"
                " — HIGH 告警触发"
            )
        elif emo == "🔴":
            lines.append(
                f"{emo} **银行准备金** 危机级别（${r_val:,.0f} bn < ${RESERVES_TIERS['critical']:,.0f}bn）"
                " — 2019 repo 复刻风险"
            )
        else:  # ⚪ — null / unparseable
            lines.append(f"⚪ **银行准备金** 数据缺失")

    # ON RRP cushion
    rrp_val, _ = _latest_delta(rrp, "total_accepted_bn")
    if rrp_val is not None:
        emo = regime_emoji(rrp_val, RRP_TIERS, "below")
        if emo == "🔴":
            lines.append(
                f"{emo} **RRP 缓冲垫** 基本耗尽（${rrp_val:,.1f} bn）"
                " — 下次 Treasury 抽水直接命中准备金"
            )
        elif emo == "🟠":
            lines.append(
                f"{emo} **RRP 缓冲垫** 非常薄（${rrp_val:,.1f} bn < ${RRP_TIERS['high']:,.0f}bn）"
            )
        elif emo == "🟡":
            lines.append(f"{emo} **RRP 缓冲垫** 薄（${rrp_val:,.0f} bn）")
        elif emo == "🟢":
            lines.append(f"{emo} **RRP 缓冲垫** 充足（${rrp_val:,.0f} bn）")
        else:  # ⚪
            lines.append(f"⚪ **RRP 缓冲垫** 数据缺失")

    # Net Liquidity
    nl_val, nl_delta = _latest_delta(nl, "net_liquidity_bn")
    if nl_val is not None:
        emo = regime_emoji(nl_val, NET_LIQ_TIERS, "below")
        delta_s = (
            f"（w/w {nl_delta:+.0f} bn）" if nl_delta else ""
        )
        lines.append(f"{emo} **综合水位** ${nl_val:,.0f} bn {delta_s}")

    # SRP — the alarm bell
    if not srp.empty:
        srp_val = float(srp["total_accepted_bn"].iloc[-1])
        if srp_val > 0:
            lines.append(f"🚨 **SRP 非零** — ${srp_val:,.2f} bn 被接受 — **CRISIS 模式**")
        else:
            lines.append("🟢 **SRP 为 0**（紧急借款窗口未启用）")

    # Market stress z
    if not ms.empty:
        z = float(ms["composite_stress_z"].iloc[-1])
        if abs(z) < 1:
            lines.append(f"🟢 **市场情绪** 平静（z = {z:+.2f}）")
        elif z > 2:
            emo = regime_emoji(z, MARKET_Z_TIERS, "above")
            lines.append(f"{emo} **市场情绪** 紧张（z = {z:+.2f}）— 快层已开始反应")
        else:
            lines.append(f"🟢 **市场情绪** 无异动（z = {z:+.2f}）")

    return "\n\n".join(f"- {ln}" for ln in lines)


with st.container():
    st.markdown("### 🧭 现状解读")
    st.markdown(_build_current_state() or "_等待首次数据抓取..._")

st.divider()

# ═══════════════════════ Net Liquidity chart ══════════════════════
st.subheader("💧 Net Liquidity = Reserves + RRP − TGA")
st.caption(
    "蓝色实线 = 综合水位；橙色虚线 = 7 日指数移动平均；"
    "黄/橙/红水平线 = MEDIUM/HIGH/CRITICAL 阈值（跌破即告警）。"
)

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
            marker=dict(size=4),
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
    # Threshold hlines so user sees how close the current value is to firing
    for tier_name, color in [
        ("medium",   "#FFD700"),
        ("high",     "#FF8C00"),
        ("critical", "#DC143C"),
    ]:
        fig.add_hline(
            y=NET_LIQ_TIERS[tier_name],
            line_dash="dot",
            line_color=color,
            line_width=1,
            annotation_text=f"{tier_name.upper()} ${NET_LIQ_TIERS[tier_name]:.0f}bn",
            annotation_position="right",
            annotation_font_color=color,
            annotation_font_size=10,
        )
    fig.update_layout(
        height=440,
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

# ═══════════════════════ Layer-2 Market Stress (yfinance) ═════════
st.subheader("Layer-2 Market Stress (ETF + VIX basket, 1h z-score)")

if not ms.empty:
    ms_plot = ms.copy()
    # format="ISO8601" handles mixed-precision timestamps (backfilled rows
    # have no microseconds, live-poll rows do). Without it pandas 2.2+
    # picks a format from the first row and rejects the rest.
    ms_plot["ts"] = pd.to_datetime(ms_plot["as_of_utc"], format="ISO8601")
    # Show last 30 days by default — full 2y history is too dense
    cutoff = ms_plot["ts"].max() - pd.Timedelta(days=30)
    ms_recent = ms_plot[ms_plot["ts"] > cutoff].copy()

    # Current composite z
    latest_z = float(ms_recent["composite_stress_z"].iloc[-1])
    aligned = int(ms_recent["tickers_stress_aligned"].iloc[-1])
    total = int(ms_recent["tickers_returned"].iloc[-1])
    badge_color = "🟢" if abs(latest_z) < 1 else ("🟡" if latest_z < 2 else ("🟠" if latest_z < 3 else "🚨🔴"))
    st.caption(
        f"**Latest composite stress z:** {badge_color} `{latest_z:+.2f}` "
        f"(aligned tickers: {aligned}/{total}) · "
        f"Thresholds: MEDIUM 2.0 / HIGH 3.0 / CRITICAL 4.0"
    )

    fig = go.Figure()
    # threshold bands
    fig.add_hrect(y0=2.0, y1=3.0, fillcolor="yellow", opacity=0.08, layer="below", line_width=0)
    fig.add_hrect(y0=3.0, y1=4.0, fillcolor="orange", opacity=0.10, layer="below", line_width=0)
    fig.add_hrect(y0=4.0, y1=10, fillcolor="red", opacity=0.12, layer="below", line_width=0)
    fig.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.4)

    # composite line
    fig.add_trace(go.Scatter(
        x=ms_recent["ts"],
        y=ms_recent["composite_stress_z"],
        mode="lines",
        name="Composite stress z",
        line=dict(width=2, color="#d62728"),
    ))
    fig.update_layout(
        height=380,
        xaxis_title="UTC",
        yaxis_title="Composite stress z (> 0 = stress-aligned)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        hovermode="x unified",
        yaxis=dict(range=[min(-3, ms_recent["composite_stress_z"].min() - 0.5),
                          max(5, ms_recent["composite_stress_z"].max() + 0.5)]),
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        f"Last 30 days · {len(ms_recent):,} hourly bars · "
        f"Basket: SPY / ^VIX / TLT / IEF / LQD / HYG (sign-adjusted so positive = stress)"
    )
else:
    st.info("No market_stress data yet. Scheduler polls every 15 min during US market hours.")

st.divider()

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
