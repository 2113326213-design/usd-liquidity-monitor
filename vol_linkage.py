"""MOVE vs VIX — vol-control stack, z-scores, and 48h passive-flow toy estimates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf


@dataclass
class MoveVixSnapshot:
    move: float | None
    vix_yahoo: float | None
    vix_fred: float | None
    move_over_vix_yahoo: float | None
    as_of_utc: str
    detail: str


def _last_close(ticker: str) -> tuple[float | None, str]:
    try:
        h = yf.Ticker(ticker).history(period="10d", interval="1d")
        if h is None or h.empty:
            return None, "empty"
        v = float(h["Close"].iloc[-1])
        return v, "ok"
    except Exception as exc:  # noqa: BLE001
        return None, type(exc).__name__


def _hist_close(ticker: str, *, period: str = "730d") -> pd.Series:
    d = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=True)
    if d is None or d.empty:
        return pd.Series(dtype=float)
    if isinstance(d.columns, pd.MultiIndex):
        d.columns = [c[0] if isinstance(c, tuple) else c for c in d.columns]
    s = d["Close"].squeeze()
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    s.index = pd.to_datetime(s.index).tz_localize(None)
    return s.astype(float)


def _hist_ohlc(ticker: str, *, period: str = "800d") -> pd.DataFrame:
    d = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=True)
    if d is None or d.empty:
        return pd.DataFrame()
    if isinstance(d.columns, pd.MultiIndex):
        d.columns = [c[0] if isinstance(c, tuple) else c for c in d.columns]
    for col in ("High", "Low", "Close"):
        if col not in d.columns:
            return pd.DataFrame()
    out = d[["High", "Low", "Close"]].copy()
    out.index = pd.to_datetime(out.index).tz_localize(None)
    return out.astype(float)


def _atr14(ohlc: pd.DataFrame) -> pd.Series:
    if ohlc.empty or len(ohlc) < 15:
        return pd.Series(dtype=float)
    h, l, c = ohlc["High"], ohlc["Low"], ohlc["Close"]
    pc = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.rolling(14, min_periods=5).mean()


def synthetic_move_from_bond_etfs() -> dict[str, Any]:
    """
    Rough MOVE proxy: regress historical ^MOVE on IEF/SHY 14d ATR (same calendar index).
    Used when Yahoo last print is missing or clearly spiked vs a 5d median.
    """
    try:
        move = _hist_close("^MOVE", period="800d")
        if move.empty or len(move) < 80:
            return {"level": None, "detail": "move_hist_short"}
        ief = _hist_ohlc("IEF", period="800d")
        shy = _hist_ohlc("SHY", period="800d")
        if ief.empty or shy.empty:
            return {"level": None, "detail": "ief_shy_empty"}
        a1 = _atr14(ief).rename("atr_ief")
        a2 = _atr14(shy).rename("atr_shy")
        df = pd.concat([move.rename("MOVE"), a1, a2], axis=1).dropna()
        if len(df) < 80:
            return {"level": None, "detail": "insufficient_overlap"}
        tail = df.iloc[-220:]
        y = tail["MOVE"].to_numpy(dtype=float)
        x1 = tail["atr_ief"].to_numpy(dtype=float)
        x2 = tail["atr_shy"].to_numpy(dtype=float)
        X = np.column_stack([x1, x2, np.ones(len(tail))])
        coef, _, rank, _ = np.linalg.lstsq(X, y, rcond=None)
        if rank < 3:
            return {"level": None, "detail": "rank_deficient"}
        last = np.array([[float(df["atr_ief"].iloc[-1]), float(df["atr_shy"].iloc[-1]), 1.0]])
        pred = float(last @ coef)
        return {
            "level": round(max(0.0, pred), 2),
            "coef_ief_shy_const": [round(float(c), 6) for c in coef.tolist()],
            "detail": "ols_move_on_atr14_ief_shy",
        }
    except Exception as exc:  # noqa: BLE001
        return {"level": None, "detail": type(exc).__name__}


def _zscore_level(series: pd.Series, *, window: int = 252) -> float | None:
    if series is None or len(series) < max(30, window // 4):
        return None
    w = series.dropna().iloc[-window:]
    if len(w) < 30:
        return None
    mu = float(w.mean())
    sd = float(w.std(ddof=1))
    if sd <= 1e-9:
        return None
    return float((float(w.iloc[-1]) - mu) / sd)


def _move_quality_score(move_hist: pd.Series) -> dict[str, Any]:
    """Heuristic 0–1: 1 = clean series, lower if many missing/stale prints."""
    if move_hist.empty:
        return {"score": None, "detail": "empty"}
    miss = move_hist.isna().rolling(5).sum().iloc[-20:].max()
    score = max(0.0, min(1.0, 1.0 - float(miss) / 5.0))
    return {"score": round(score, 3), "detail": "recent_missing_bars_proxy"}


def build_move_vix_block(*, vix_fred_level: float | None) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    move_raw, mwhy = _last_close("^MOVE")
    move_hist = _hist_close("^MOVE", period="800d")
    move_smooth = None
    if not move_hist.empty:
        med5 = move_hist.rolling(5, min_periods=1).median()
        if pd.notna(med5.iloc[-1]):
            move_smooth = float(med5.iloc[-1])
    synth = synthetic_move_from_bond_etfs()

    move_effective = move_raw
    move_source = "yahoo_raw"
    if move_raw is None:
        if synth.get("level") is not None:
            move_effective = float(synth["level"])
            move_source = "synthetic_ief_shy"
        elif move_smooth is not None:
            move_effective = move_smooth
            move_source = "median5_history_tail"
    elif move_smooth is not None and move_smooth > 1.0:
        rel = abs(float(move_raw) - move_smooth) / move_smooth
        if rel > 0.35:
            move_effective = move_smooth
            move_source = "median5_spike_trim"

    vix_y, vwhy = _last_close("^VIX")
    ratio = None
    if move_effective is not None and vix_y and vix_y > 0:
        ratio = round(float(move_effective) / vix_y, 4)
    return {
        "move": move_effective,
        "move_yahoo_raw": move_raw,
        "move_median5d": move_smooth,
        "move_synthetic_bonds": synth.get("level"),
        "move_source": move_source,
        "vix_yahoo": vix_y,
        "vix_fred": vix_fred_level,
        "move_over_vix_yahoo": ratio,
        "as_of_utc": now,
        "detail": f"move_raw:{mwhy};vix_yahoo:{vwhy};synth:{synth.get('detail')}",
    }


def build_vol_control_stack(
    *,
    vix_fred_level: float | None,
    spy_passive_aum_bn: float,
    qqq_passive_aum_bn: float,
    move_z_threshold: float = 2.0,
    trading_days_48h: float = 2.0,
) -> dict[str, Any]:
    """
    Risk-parity style **toy**:
      ΔPos ≈ 1 − σ_baseline/σ_current  when MOVE z-score is hot,
      mapped to SPY+QQQ passive sleeve AUM × (48h / 252) trading-day slice.
    """
    base = build_move_vix_block(vix_fred_level=vix_fred_level)
    move_hist_raw = _hist_close("^MOVE")
    move_hist = move_hist_raw.rolling(5, min_periods=1).median()
    vix_hist = _hist_close("^VIX")
    ratio_hist = (move_hist / vix_hist.replace(0, np.nan)).dropna()

    move_z = _zscore_level(move_hist, window=252)
    vix_z = _zscore_level(vix_hist, window=252)
    ratio_z = _zscore_level(ratio_hist, window=252)

    mq = _move_quality_score(move_hist_raw)

    move = base.get("move")
    vix_y = base.get("vix_yahoo")
    delta_pos = 0.0
    if move is not None and move_z is not None and move_z > move_z_threshold:
        baseline = float(move_hist.dropna().iloc[-60:].median()) if len(move_hist) >= 60 else float(move_hist.median())
        sigma_cur = max(float(move), 1e-6)
        sigma_base = max(baseline, 1e-6)
        raw = 1.0 - (sigma_base / sigma_cur)
        delta_pos = float(max(0.0, min(0.85, raw)))

    aum = max(0.0, float(spy_passive_aum_bn) + float(qqq_passive_aum_bn))
    frac = trading_days_48h / 252.0
    est_48h = aum * delta_pos * frac

    diverge = ratio_z is not None and ratio_z > 2.0 and (vix_z is None or vix_z < 0.5)

    return {
        **base,
        "move_zscore": move_z,
        "vix_zscore": vix_z,
        "move_over_vix_zscore": ratio_z,
        "move_quality": mq,
        "vol_target_rebalance_fraction": round(delta_pos, 4),
        "estimated_passive_selling_48h_bn": None if est_48h <= 0 else round(est_48h, 2),
        "aum_proxy_bn": round(aum, 1),
        "move_vix_ratio_divergence": diverge,
        "note": "Vol-control estimate is illustrative (AUM sleeves are configurable).",
    }


def passive_selling_hint(
    *,
    delta_net_bn: float | None,
    move: float | None,
    vix: float | None,
    vol_target_gamma: float = 35.0,
    gamma_pct: float | None = None,
    move_vix_cap: float = 6.0,
) -> dict[str, Any]:
    """
    Stylised passive-flow pressure: max(0, −ΔNet) × (γ/100) × min(cap, MOVE/VIX).

    *gamma_pct* overrides *vol_target_gamma* when set (keeps older kw name working).
    """
    if delta_net_bn is None or move is None or vix is None or vix <= 0:
        return {"expected_selling_bn": None, "note": "insufficient_inputs"}
    g = float(gamma_pct) if gamma_pct is not None else float(vol_target_gamma)
    ratio = float(move) / float(vix)
    ratio_w = min(float(move_vix_cap), max(0.0, ratio))
    selling = max(0.0, -float(delta_net_bn)) * (g / 100.0) * ratio_w
    return {
        "expected_selling_bn": round(selling, 2),
        "inputs": {
            "delta_net_bn": delta_net_bn,
            "move": move,
            "vix": vix,
            "ratio": round(ratio, 4),
            "ratio_weighted": round(ratio_w, 4),
            "gamma_pct_used": g,
        },
        "note": "MOVE/VIX-weighted heuristic — prefer build_vol_control_stack for vol-target AUM toy.",
    }


hist_close_daily = _hist_close
