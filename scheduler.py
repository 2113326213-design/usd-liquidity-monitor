"""
Scheduler: polls each data source in its actual release window.

Key insight: we DON'T poll every minute because these APIs don't update
every minute. We poll aggressively only during each source's known release
window, then once a day otherwise as a safety net.
"""
from __future__ import annotations

from typing import Callable

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

ET = pytz.timezone("America/New_York")


def build_scheduler(collectors: dict) -> AsyncIOScheduler:
    """
    Build the production scheduler.

    Release window cheat-sheet (all ET):
      TGA        : ~16:00-17:00 business days (T-1 balance in DTS)
      ON RRP     : ~13:15-13:30 business days (after op closes)
      SRP (AM)   : ~09:30-10:00 business days (after op closes)
      SRP (PM)   : ~13:30-14:00 business days (after op closes)
      Reserves   : ~16:30-17:30 Thursday (H.4.1 weekly)
    """
    sched = AsyncIOScheduler(timezone=ET)

    # ── TGA (Daily Treasury Statement) ──────────────────────────────
    # Primary window: 16:00-18:00 ET mon-fri, every 2 minutes
    sched.add_job(
        collectors["tga"].poll,
        CronTrigger(day_of_week="mon-fri", hour="16-17", minute="*/2", timezone=ET),
        id="tga_primary", replace_existing=True,
    )
    # Morning safety poll (in case of late release)
    sched.add_job(
        collectors["tga"].poll,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=5, timezone=ET),
        id="tga_morning", replace_existing=True,
    )

    # ── ON RRP ──────────────────────────────────────────────────────
    # Primary window: 13:10-13:45 ET mon-fri, every 30 seconds
    sched.add_job(
        collectors["rrp"].poll,
        CronTrigger(
            day_of_week="mon-fri", hour=13, minute="10-45", second="*/30", timezone=ET
        ),
        id="rrp_primary", replace_existing=True,
    )
    # Afternoon safety poll
    sched.add_job(
        collectors["rrp"].poll,
        CronTrigger(day_of_week="mon-fri", hour=15, minute=0, timezone=ET),
        id="rrp_afternoon", replace_existing=True,
    )

    # ── SRP (Standing Repo Facility) AM window ──────────────────────
    sched.add_job(
        collectors["srp"].poll,
        CronTrigger(
            day_of_week="mon-fri", hour=9, minute="30-55", second="*/30", timezone=ET
        ),
        id="srp_am", replace_existing=True,
    )
    # ── SRP PM window ───────────────────────────────────────────────
    sched.add_job(
        collectors["srp"].poll,
        CronTrigger(
            day_of_week="mon-fri", hour=13, minute="30-55", second="*/30", timezone=ET
        ),
        id="srp_pm", replace_existing=True,
    )
    # Extra safety check every hour throughout business day
    sched.add_job(
        collectors["srp"].poll,
        CronTrigger(day_of_week="mon-fri", hour="10-15", minute=15, timezone=ET),
        id="srp_hourly", replace_existing=True,
    )

    # ── Reserves (H.4.1 weekly release) ─────────────────────────────
    sched.add_job(
        collectors["reserves"].poll,
        CronTrigger(day_of_week="thu", hour="16-17", minute="*/5", timezone=ET),
        id="reserves_thu", replace_existing=True,
    )
    # Friday morning safety poll for late Thursday releases
    sched.add_job(
        collectors["reserves"].poll,
        CronTrigger(day_of_week="fri", hour=9, minute=0, timezone=ET),
        id="reserves_fri", replace_existing=True,
    )

    # ── Auction tail (30Y Treasury auction leading indicator) ───────
    # Poll daily at 17:00 ET — auction results post ~13:00 ET on
    # auction days, and the endpoint is idempotent so daily is fine.
    if "auction_tail" in collectors:
        sched.add_job(
            collectors["auction_tail"].poll,
            CronTrigger(day_of_week="mon-fri", hour=17, minute=0, timezone=ET),
            id="auction_tail_daily", replace_existing=True,
        )

    # ── SOFR − IORB spread (direct reserve-scarcity signal) ────────
    # FRED publishes both series ~8 AM ET for prior business day. Poll
    # at 15:00 ET to be safely past publication.
    if "sofr_iorb" in collectors:
        sched.add_job(
            collectors["sofr_iorb"].poll,
            CronTrigger(day_of_week="mon-fri", hour=15, minute=30, timezone=ET),
            id="sofr_iorb_daily", replace_existing=True,
        )

    # ── Market stress (Layer-2 fast pulse via yfinance) ─────────────
    # Every 15 min during US market hours (9:30-16:00 ET, mon-fri).
    # yfinance gives us ~15-min delayed data — cheaper than Polygon, adequate
    # for the "stress pulse confirms structural alert" use case.
    if "market_stress" in collectors:
        sched.add_job(
            collectors["market_stress"].poll,
            CronTrigger(
                day_of_week="mon-fri", hour="9-16", minute="*/15", timezone=ET
            ),
            id="market_stress_15min", replace_existing=True,
        )

    # ── Proxy buffer flush ──────────────────────────────────────────
    # Flush Layer-3 minute-bar buffer to disk every 2 minutes during market hours
    store = collectors["tga"].store
    sched.add_job(
        store.flush_proxy_buffer,
        CronTrigger(day_of_week="mon-fri", hour="9-16", minute="*/2", timezone=ET),
        id="proxy_flush", replace_existing=True,
    )

    # ── Heartbeat (hourly log, silent) ──────────────────────────────
    sched.add_job(
        lambda: logger.info("Scheduler heartbeat"),
        CronTrigger(minute=0, timezone=ET),
        id="heartbeat", replace_existing=True,
    )

    # ── Weekly health push (goes to Bark/Telegram) ──────────────────
    # Every Monday 09:00 ET — sends an INFO-level alert with latest
    # values so the user has proof the system is still alive. If the
    # user stops receiving these pings, the system (or the push channel)
    # has died — that's the whole point.
    alerter = collectors["tga"].alerter  # same MultiAlerter shared across
    sched.add_job(
        _build_heartbeat_job(collectors["tga"].store, alerter),
        CronTrigger(day_of_week="mon", hour=9, minute=0, timezone=ET),
        id="weekly_heartbeat_push", replace_existing=True,
    )

    return sched


def _build_heartbeat_job(store, alerter):
    """Closure capturing store + alerter for the weekly health push."""
    async def run() -> None:
        tga = store.last_snapshot("tga") or {}
        rrp = store.last_snapshot("rrp") or {}
        reserves = store.last_snapshot("reserves") or {}
        nl = store.last_snapshot("net_liquidity") or {}
        stress = store.last_snapshot("market_stress") or {}

        def _fmt(v):
            try:
                return f"${float(v):,.1f} bn"
            except (TypeError, ValueError):
                return "—"

        msg_lines = [
            "💧 USD Liquidity Monitor — weekly health ping",
            f"├─ TGA:          {_fmt(tga.get('close_bal_bn'))}",
            f"├─ ON RRP:       {_fmt(rrp.get('total_accepted_bn'))}",
            f"├─ Reserves:     {_fmt(reserves.get('reserves_bn'))}",
            f"├─ Net Liquidity:{_fmt(nl.get('net_liquidity_bn'))}",
            f"├─ Market z:     {stress.get('composite_stress_z', '—')}",
            "└─ If you stop getting this every Monday, the system is down.",
        ]
        # Append throttle stats if available
        stats = getattr(alerter, "stats", lambda: None)()
        if stats:
            msg_lines.insert(
                -1,
                f"├─ Alerts sent/throttled: "
                f"{stats['sent']}/{stats['throttled']}",
            )
        await alerter.send(level="INFO", msg="\n".join(msg_lines))
    return run
