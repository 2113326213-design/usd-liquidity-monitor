"""Orchestrates FRED + Treasury + shadow stack + QRA/runway + alerts."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx
from sqlalchemy.orm import sessionmaker

from auction_results import (
    auction_liquidity_penalty,
    fetch_recent_auction_highlights,
    summarize_for_alerts,
)
from config import Settings, get_settings
from forecast import (
    build_qra_water_stress,
    build_reserves_tax_path,
    build_tga_forecast,
    forecast_to_dict,
    tga_daily_rate_for_ode,
)
from fred_client import FredClient
from heuristic import build_polygon_shadow, fetch_market_proxy
from history_queries import (
    daily_last_net_liquidity,
    fetch_snapshot_at_or_before,
    net_liquidity_change_over_hours,
    rrp_avg_daily_change_bn,
    stress_hourly_distribution,
)
from liquidity_calendar import (
    attach_projection_calendar,
    baseline_breach_dates_for_stability,
    enrich_ode_runway_with_calendar,
    federal_bd_moved_earlier,
    first_business_day_next_calendar_month,
    trading_sessions_from_anchor_to,
)
from macro_resonance import macro_resonance_scan
from liquidity_signals import liquidity_equity_divergence, sofr_tail_from_observations
from qra_refresher import fetch_latest_qra_snapshot
from runway import estimate_liquidity_runway, runway_to_dict
from runway_ode import (
    load_scenario_from_disk,
    project_qt_policy_stress_comparison,
    project_scenario_bundle,
    scenario_with_tga_rate,
)
from treasury_fiscal import fetch_latest_tga
from vol_linkage import build_vol_control_stack, passive_selling_hint


@dataclass
class ServiceState:
    settings: Settings
    last_payload: dict[str, Any] | None = None
    last_error: str | None = None


def _bp_spread(sofr_pct: float | None, dff_pct: float | None) -> float | None:
    if sofr_pct is None or dff_pct is None:
        return None
    return round((sofr_pct - dff_pct) * 100.0, 2)


def _obs_bn(obs) -> float | None:
    return obs.value_bn if obs else None


async def build_snapshot(
    settings: Settings,
    session_factory: sessionmaker | None = None,
) -> dict[str, Any]:
    if not settings.fred_api_key:
        raise ValueError("FRED_API_KEY is not set")

    fred = FredClient(settings.fred_api_key)
    ts_dt = datetime.now(timezone.utc)
    ts = ts_dt.isoformat()

    vix_obs = None
    auction_highlights: list = []

    async with httpx.AsyncClient() as client:
        walcl = await fred.latest_observation(client, settings.fred_walcl)
        tga_fred = await fred.latest_observation(client, settings.fred_tga)
        rrp = await fred.latest_observation(client, settings.fred_rrp)
        reserves = await fred.latest_observation(client, settings.fred_reserves)
        currcir = await fred.latest_observation(client, settings.fred_currcir)
        sofr = await fred.latest_observation(client, settings.fred_sofr)
        dff = await fred.latest_observation(client, settings.fred_dff)
        sofr99 = await fred.latest_observation(client, settings.fred_sofr_99)
        sofr_vol = await fred.latest_observation(client, settings.fred_sofr_vol)
        fima_rr = await fred.latest_observation(client, settings.fred_fima_rr)
        repo_stress = await fred.latest_observation(client, settings.fred_overnight_repo_stress)
        vix_obs = await fred.latest_observation(client, settings.fred_vix)

        sofr_hist = await fred.recent_observations(client, settings.fred_sofr, limit=8)
        sofr99_hist = await fred.recent_observations(client, settings.fred_sofr_99, limit=8)

        tga_row = None
        if settings.use_treasury_fiscal_api:
            try:
                tga_row = await fetch_latest_tga(client)
            except Exception:
                tga_row = None

        polygon_shadow = await build_polygon_shadow(client, settings)
        auction_highlights = await fetch_recent_auction_highlights(client, limit=8)

    tga_bn = tga_fred.value_bn if tga_fred else None
    tga_date = tga_fred.date if tga_fred else None
    tga_source: str = "fred_wtregen"
    if tga_row and tga_row.open_today_bal_bn is not None:
        tga_bn = tga_row.open_today_bal_bn
        tga_date = tga_row.record_date
        tga_source = "treasury_dts"

    walcl_bn = _obs_bn(walcl)
    rrp_bn = _obs_bn(rrp)
    reserves_bn = _obs_bn(reserves)
    cic_bn = _obs_bn(currcir)

    net: float | None = None
    if walcl_bn is not None and tga_bn is not None and rrp_bn is not None and cic_bn is not None:
        net = walcl_bn - (tga_bn + rrp_bn + cic_bn)

    sofr_pct = sofr.value_raw if sofr else None
    dff_pct = dff.value_raw if dff else None
    sofr99_pct = sofr99.value_raw if sofr99 else None
    sofr_tail_bp = None
    if sofr99_pct is not None and sofr_pct is not None:
        sofr_tail_bp = round((sofr99_pct - sofr_pct) * 100.0, 2)

    tail_dyn = sofr_tail_from_observations(sofr_hist, sofr99_hist)

    yf_proxy = fetch_market_proxy()
    stress_base = float(yf_proxy.stress_score)
    if tail_dyn.spread_acceleration_up:
        stress_base = min(100.0, stress_base * 2.0)
    vol_block = (polygon_shadow.get("volume_anomaly") or {}) if polygon_shadow else {}
    if vol_block.get("abnormal"):
        stress_base = min(100.0, stress_base + 15.0)
    vol_bias = float(vol_block.get("net_liquidity_bias") or 0.0)
    net_adjusted = None if net is None else round(float(net) * (1.0 + vol_bias), 3)

    rrp_avg_change = None
    net_daily = None
    win24 = None
    stress_panel: list[dict[str, float | str]] = []
    if session_factory is not None:
        rrp_avg_change = rrp_avg_daily_change_bn(session_factory, lookback_calendar_days=12)
        net_daily = daily_last_net_liquidity(session_factory, lookback_days=140)
        win24 = net_liquidity_change_over_hours(session_factory, hours=24.0)
        stress_panel = stress_hourly_distribution(session_factory, hours=24)

    effective_qra_bn = settings.qra_target_tga_bn
    if session_factory is not None:
        last_q = fetch_latest_qra_snapshot(session_factory)
        if last_q and last_q.end_quarter_cash_balance_bn is not None:
            effective_qra_bn = float(last_q.end_quarter_cash_balance_bn)

    fc = build_tga_forecast(
        settings,
        date.today(),
        tga_bn,
        qra_target_override_bn=effective_qra_bn,
    )

    vol_stack = build_vol_control_stack(
        vix_fred_level=vix_obs.value_raw if vix_obs else None,
        spy_passive_aum_bn=settings.spy_passive_aum_bn,
        qqq_passive_aum_bn=settings.qqq_passive_aum_bn,
        move_z_threshold=settings.move_z_threshold,
        trading_days_48h=settings.vol_control_horizon_days,
    )
    vol_hint = passive_selling_hint(
        delta_net_bn=win24.delta_bn if win24 and win24.delta_bn is not None else None,
        move=vol_stack.get("move"),
        vix=vol_stack.get("vix_yahoo"),
        gamma_pct=float(settings.liq_selling_gamma_pct),
        move_vix_cap=float(settings.liq_selling_move_vix_cap),
    )

    qra_block = build_qra_water_stress(
        settings,
        as_of=date.today(),
        current_tga_bn=tga_bn,
        rrp_avg_daily_change_bn=rrp_avg_change,
        qra_target_override_bn=effective_qra_bn,
    )
    reserves_path = build_reserves_tax_path(
        reserves_bn,
        date.today(),
        red_threshold_bn=settings.reserves_red_threshold_bn,
    )

    ap = auction_liquidity_penalty(
        auction_highlights,
        bid_to_cover_weak=settings.auction_btc_weak,
        tail_stress_bp=settings.auction_tail_stress_bp,
        stress_bump_points=settings.auction_stress_bump_points,
        runway_tighten_factor=settings.auction_runway_tighten_factor,
    )

    runway = estimate_liquidity_runway(
        rrp_bn=rrp_bn,
        reserves_bn=reserves_bn,
        rrp_avg_daily_inflow_bn=rrp_avg_change,
        qra_daily_tga_gap_bn=qra_block.get("delta_t_avg_bn_per_day"),
        reserves_red_bn=settings.reserves_red_threshold_bn,
        rrp_floor_bn=settings.runway_rrp_floor_bn,
        runway_multiplier=ap["runway_multiplier"],
    )

    stress = min(100.0, stress_base + ap["stress_bump"])

    ode_runway: dict[str, Any] | None = None
    ode_stability: dict[str, Any] = {}
    ode_qt_policy: dict[str, Any] | None = None
    macro_resonance: dict[str, Any] = {}
    if reserves_bn is not None and rrp_bn is not None and tga_bn is not None:
        try:
            base_ode = load_scenario_from_disk()
            base_ode = replace(base_ode, rrp_floor_bn=max(0.0, float(settings.runway_rrp_floor_bn)))
            tga_rate = tga_daily_rate_for_ode(qra_block, fallback_bn_per_day=None)
            if tga_rate is not None:
                base_ode = scenario_with_tga_rate(base_ode, tga_rate)
            mz = vol_stack.get("move_zscore")
            if mz is not None and float(mz) > float(settings.ode_move_z_for_beta):
                base_ode = replace(
                    base_ode,
                    rrp_absorption_ratio=max(
                        0.04,
                        float(base_ode.rrp_absorption_ratio) - float(settings.ode_move_beta_shrink),
                    ),
                )
            as_of_d = date.today()
            ode_runway = project_scenario_bundle(
                initial=(float(reserves_bn), float(rrp_bn), float(tga_bn)),
                base=base_ode,
                horizon_days=max(10, min(int(settings.ode_horizon_days), 365)),
                as_of=as_of_d,
                use_tax_pulses=settings.ode_use_tax_pulses,
            )
            enrich_ode_runway_with_calendar(ode_runway, as_of_d)

            lcl_new, rrp_new = baseline_breach_dates_for_stability(ode_runway)
            macro_resonance = macro_resonance_scan(
                lclor_date=lcl_new,
                rrp_floor_date=rrp_new,
                window_days=int(settings.macro_resonance_window_days),
            )
            ode_stability = {
                "baseline_lclor_date_current": lcl_new.isoformat() if lcl_new else None,
                "baseline_rrp_floor_date_current": rrp_new.isoformat() if rrp_new else None,
                "move_zscore_used_for_beta_cut": mz,
                "liquidity_acceleration_warning": False,
                "buffer_depletion_acceleration_warning": False,
            }
            if session_factory is not None:
                prev = fetch_snapshot_at_or_before(
                    session_factory,
                    cutoff_utc=ts_dt - timedelta(hours=24),
                )
                prev_lcl = prev.ode_baseline_lclor_date_iso if prev else None
                prev_rrp = prev.ode_baseline_rrp_floor_date_iso if prev else None
                ode_stability["baseline_lclor_date_24h_ago"] = prev_lcl
                ode_stability["baseline_rrp_floor_date_24h_ago"] = prev_rrp
                if lcl_new and prev_lcl:
                    try:
                        po = date.fromisoformat(prev_lcl)
                        if lcl_new < po:
                            drift_bd = federal_bd_moved_earlier(po, lcl_new)
                            ode_stability["lclor_acceleration_bd_vs_24h"] = drift_bd
                            if drift_bd >= int(settings.ode_drift_alert_bd):
                                ode_stability["liquidity_acceleration_warning"] = True
                    except ValueError:
                        pass
                if rrp_new and prev_rrp:
                    try:
                        pr = date.fromisoformat(prev_rrp)
                        if rrp_new < pr:
                            cal_days = (pr - rrp_new).days
                            ode_stability["rrp_floor_acceleration_calendar_days_vs_24h"] = cal_days
                            ode_stability["rrp_floor_acceleration_bd_vs_24h"] = federal_bd_moved_earlier(
                                pr, rrp_new
                            )
                            if cal_days >= int(settings.ode_rrp_drift_alert_calendar_days):
                                ode_stability["buffer_depletion_acceleration_warning"] = True
                    except ValueError:
                        pass

            try:
                nm = first_business_day_next_calendar_month(as_of_d)
                sw = trading_sessions_from_anchor_to(as_of_d, nm)
                if sw < 1.0:
                    sw = 1.0
                ode_qt_policy = project_qt_policy_stress_comparison(
                    initial=(float(reserves_bn), float(rrp_bn), float(tga_bn)),
                    base=base_ode,
                    horizon_days=max(10, min(int(settings.ode_horizon_days), 365)),
                    as_of=as_of_d,
                    use_tax_pulses=settings.ode_use_tax_pulses,
                    dove_switch_day_index=float(sw),
                    dove_qt_bn_per_month=float(settings.ode_dove_qt_bn_per_month),
                    hawk_qt_bn_per_month=float(settings.ode_hawk_qt_bn_per_month),
                )
                for _name, proj in (ode_qt_policy.get("paths") or {}).items():
                    if isinstance(proj, dict) and not proj.get("error"):
                        attach_projection_calendar(proj, as_of_d)
            except Exception:
                ode_qt_policy = {"error": "qt_policy_bundle_failed"}
        except Exception as exc:  # noqa: BLE001
            ode_runway = {"error": str(exc)}

    div = (
        liquidity_equity_divergence(net_by_day=net_daily, bench="SPY")
        if net_daily is not None and len(net_daily) > 0
        else {"active": False, "detail": "no_net_history"}
    )

    shadow_indicators: dict[str, Any] = {
        "sofr_99th_pct": sofr99_pct,
        "sofr_median_pct": sofr_pct,
        "sofr99_minus_sofr_bp": sofr_tail_bp,
        "sofr_tail_spread_series": (tail_dyn.spread_bp_series[-12:] if tail_dyn.spread_bp_series else []),
        "spread_acceleration_up": tail_dyn.spread_acceleration_up,
        "tail_dynamics_detail": tail_dyn.detail,
        "sofr_volume_bn": sofr_vol.value_bn if sofr_vol else None,
        "sofr_volume_asof": sofr_vol.date if sofr_vol else None,
        "fima_reverse_repo_bn": _obs_bn(fima_rr),
        "fima_reverse_repo_asof": fima_rr.date if fima_rr else None,
        "overnight_repo_purchased_bn": _obs_bn(repo_stress),
        "overnight_repo_purchased_asof": repo_stress.date if repo_stress else None,
        "polygon": polygon_shadow,
    }

    alerts: list[str] = []

    srf_bn = _obs_bn(repo_stress)
    if srf_bn is not None and srf_bn >= settings.srf_alert_min_bn:
        alerts.append(
            f"RED ALERT: Standing / overnight RP uptake (RPONTSYD proxy) "
            f"{srf_bn:.3f}B ≥ {settings.srf_alert_min_bn:.3f}B — private market may be rationing."
        )

    if rrp_bn is not None and rrp_bn < settings.rrp_critical_threshold_bn:
        alerts.append(
            f"CRITICAL: ON RRP < {settings.rrp_critical_threshold_bn:.0f}B "
            f"({rrp_bn:.1f}B) — liquidity buffer largely gone."
        )
    elif rrp_bn is not None and rrp_bn < settings.rrp_warning_threshold_bn:
        alerts.append(
            f"ON RRP below {settings.rrp_warning_threshold_bn:.0f}B USD "
            f"(reading {rrp_bn:.1f}B) — watch money-market capacity."
        )

    if reserves_bn is not None and reserves_bn < settings.reserves_red_threshold_bn:
        alerts.append(
            f"Reserves (TOTRESNS) {reserves_bn:.1f}B < {settings.reserves_red_threshold_bn:.0f}B — red-zone cushion."
        )

    spread_bp = _bp_spread(sofr_pct, dff_pct)
    if spread_bp is not None and spread_bp > settings.sofr_minus_dff_alert_bp:
        alerts.append(
            f"SOFR minus DFF {spread_bp} bp (> {settings.sofr_minus_dff_alert_bp:g} bp) — repo pressure."
        )

    if sofr_tail_bp is not None and sofr_tail_bp > 25:
        alerts.append(
            f"SOFR tail wide: 99th minus median = {sofr_tail_bp} bp — tail funding heat."
        )

    if tail_dyn.spread_acceleration_up:
        alerts.append(
            "SOFR tail accelerating (99th minus median widening) — edge banks likely paying up."
        )

    if vol_block.get("abnormal"):
        alerts.append(
            "Minute ETF volume spike with rising short yield — possible dealer bill/off‑run inventory pressure."
        )

    if qra_block.get("qra_rrp_alarm"):
        alerts.append("QRA-implied TGA build is faster than recent RRP refill — reserves are likely residual shock absorber.")

    if reserves_path.get("breaches_red_under_scenario"):
        alerts.append(
            "Tax-window reserves scenario drops TOTRESNS below red threshold — watch settlement / TT&L dynamics."
        )

    if div.get("active"):
        alerts.append("Divergence: equity near rolling highs while net liquidity falls several days straight.")

    if vol_stack.get("move_vix_ratio_divergence"):
        alerts.append(
            "MOVE/VIX ratio z-score hot vs calm VIX — Treasury vol leading; watch SPY catch-up risk."
        )

    if isinstance(ode_runway, dict) and ode_runway.get("scenarios"):
        bl = ode_runway["scenarios"].get("baseline") or {}
        breach = bl.get("lclor_breach_day")
        lcl = bl.get("lclor_bn")
        dcal = bl.get("lclor_expected_date")
        if breach is not None and isinstance(breach, (int, float)) and breach < 120:
            tail = f" (~{dcal})" if isinstance(dcal, str) else ""
            alerts.append(
                f"ODE baseline: TOTRESNS crosses ~{float(lcl or 0):.0f}B (LCLoR proxy) near day "
                f"{breach:.0f}{tail} on the stylised QT+QRA grid — watch RRP floor non-linearity."
            )

    if ode_stability.get("liquidity_acceleration_warning"):
        bd = ode_stability.get("lclor_acceleration_bd_vs_24h")
        alerts.append(
            "LIQUIDITY_ACCELERATION_WARNING: baseline LCLoR breach date moved earlier vs ~24h ago by "
            f"{bd} US business sessions (threshold "
            f"{int(settings.ode_drift_alert_bd)}) — path depletion accelerated."
        )

    if ode_stability.get("buffer_depletion_acceleration_warning"):
        cd = ode_stability.get("rrp_floor_acceleration_calendar_days_vs_24h")
        alerts.append(
            "BUFFER_DEPLETION_ACCELERATION: RRP floor expected date moved earlier by ≥ "
            f"{int(settings.ode_rrp_drift_alert_calendar_days)} calendar days vs ~24h ago "
            f"(actual Δ {cd}d) — first-line liquidity buffer may be eroding faster than yesterday's path."
        )

    for msg in macro_resonance.get("warning_messages") or []:
        alerts.append(msg)

    if session_factory is not None:
        win = net_liquidity_change_over_hours(
            session_factory,
            hours=settings.net_liquidity_drop_window_hours,
        )
        if win and win.delta_bn is not None and win.delta_bn <= -settings.net_liquidity_drop_alert_bn:
            alerts.append(
                f"Net liquidity down ~{-win.delta_bn:.0f}B over {win.hours:.1f}h "
                f"(>{settings.net_liquidity_drop_alert_bn:.0f}B / {settings.net_liquidity_drop_window_hours:.0f}h rule)."
            )

    if runway.runway_days is not None:
        for n in runway.notes:
            alerts.append(f"Runway: {n}")

    alerts.extend(
        summarize_for_alerts(
            auction_highlights,
            tail_alert_bp=settings.auction_tail_alert_bp,
            runway_days=runway.runway_days,
        )
    )

    mv = vol_stack.get("move")
    if (
        mv is not None
        and float(mv) >= settings.move_index_alert
        and runway.runway_days is not None
        and runway.runway_days < settings.move_runway_combo_days
    ):
        alerts.append(
            f"MOVE {float(mv):.1f} ≥ {settings.move_index_alert:.0f} with runway "
            f"{runway.runway_days:.1f}d < {settings.move_runway_combo_days:.0f}d — vol-control / RV shock risk."
        )

    payload: dict[str, Any] = {
        "timestamp_utc": ts,
        "last_updated_official": {
            "walcl": walcl.date if walcl else None,
            "tga": tga_date,
            "rrp": rrp.date if rrp else None,
            "reserves": reserves.date if reserves else None,
            "currcir": currcir.date if currcir else None,
            "sofr": sofr.date if sofr else None,
            "dff": dff.date if dff else None,
            "sofr99": sofr99.date if sofr99 else None,
            "sofrvol": sofr_vol.date if sofr_vol else None,
            "fima_rr": fima_rr.date if fima_rr else None,
            "overnight_repo": repo_stress.date if repo_stress else None,
            "vix": vix_obs.date if vix_obs else None,
        },
        "balances_bn": {
            "walcl": walcl_bn,
            "tga": tga_bn,
            "tga_source": tga_source,
            "rrp": rrp_bn,
            "reserves": reserves_bn,
            "currency_in_circulation": cic_bn,
        },
        "net_liquidity_bn": None if net is None else round(net, 3),
        "net_liquidity_bn_volume_adjusted": net_adjusted,
        "rates": {
            "sofr_pct": sofr_pct,
            "dff_effective_pct": dff_pct,
        },
        "sofr_minus_dff_bp": spread_bp,
        "shadow_indicators": shadow_indicators,
        "tga_forecast": {
            **forecast_to_dict(fc),
            **qra_block,
            "qra_effective_target_bn": effective_qra_bn,
        },
        "reserves_tax_path": reserves_path,
        "liquidity_runway": runway_to_dict(runway),
        "ode_runway": ode_runway,
        "ode_stability": ode_stability,
        "ode_qt_policy": ode_qt_policy,
        "macro_resonance": macro_resonance,
        "equity_liquidity_divergence": div,
        "volatility_linkage": vol_stack,
        "vol_control_hint": vol_hint,
        "auction_highlights": [asdict(h) for h in auction_highlights],
        "stress_panel_24h": stress_panel,
        "heuristic": {
            "tbill_13w_pct": yf_proxy.tbill_13w_pct,
            "stress_score_0_100": round(stress, 2),
            "as_of_utc": yf_proxy.as_of_utc,
            "detail": yf_proxy.detail,
        },
        "alerts": alerts,
        "meta": {
            "formula": "net_liquidity_bn = walcl - (tga + rrp + currcir); units billions USD",
            "rrp_warning_threshold_bn": settings.rrp_warning_threshold_bn,
            "rrp_critical_threshold_bn": settings.rrp_critical_threshold_bn,
            "reserves_red_threshold_bn": settings.reserves_red_threshold_bn,
            "sofr_minus_dff_alert_bp": settings.sofr_minus_dff_alert_bp,
            "net_liquidity_drop_alert_bn": settings.net_liquidity_drop_alert_bn,
            "srf_alert_min_bn": settings.srf_alert_min_bn,
        },
    }
    return payload


async def refresh_state(
    state: ServiceState,
    session_factory: sessionmaker | None = None,
) -> None:
    try:
        state.last_payload = await build_snapshot(state.settings, session_factory)
        state.last_error = None
    except Exception as exc:  # noqa: BLE001
        state.last_error = str(exc)
        raise


def default_state() -> ServiceState:
    return ServiceState(settings=get_settings())
