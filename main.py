"""
Entry point.

Run with:
    python -m usd_liquidity_monitor.main

Wires together: store, alerter, collectors, scheduler, proxy stream,
net-liquidity calculator. Graceful shutdown on SIGINT / SIGTERM.
"""
from __future__ import annotations

import asyncio
import signal
import sys

from loguru import logger

from .alerts.bark import BarkAlerter
from .alerts.multi import MultiAlerter
from .alerts.telegram import TelegramAlerter
from .collectors.market_stress import MarketStressCollector
from .collectors.reserves import ReservesCollector
from .collectors.rrp import RRPCollector
from .collectors.srp import SRPCollector
from .collectors.tga import TGACollector
from .config import settings
from .proxy.polygon_stream import PolygonStream
from .scheduler import build_scheduler
from .state.net_liquidity import NetLiquidityCalculator
from .storage.parquet_store import ParquetStore


def _configure_logging() -> None:
    logger.remove()
    logger.add(sys.stderr, level=settings.log_level, enqueue=True,
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                      "<level>{level: <8}</level> | "
                      "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                      "<level>{message}</level>")
    logger.add(
        settings.data_dir / "logs" / "monitor_{time:YYYY-MM-DD}.log",
        rotation="1 day", retention="30 days", level="DEBUG", enqueue=True,
    )


async def main() -> None:
    _configure_logging()
    logger.info("=" * 60)
    logger.info("Fed Liquidity Monitor starting")
    logger.info(f"Data dir: {settings.data_dir.absolute()}")
    logger.info("=" * 60)

    # ── Core services ─────────────────────────────────────────────
    store = ParquetStore(settings.data_dir)
    alerter = MultiAlerter([TelegramAlerter(), BarkAlerter()])

    # ── Derived state ─────────────────────────────────────────────
    nl_calc = NetLiquidityCalculator(store, alerter)

    # ── Collectors ────────────────────────────────────────────────
    collectors = {
        "tga": TGACollector(store, alerter),
        "rrp": RRPCollector(store, alerter),
        "srp": SRPCollector(store, alerter),
        "reserves": ReservesCollector(store, alerter),
        "market_stress": MarketStressCollector(store, alerter),
    }

    # ── Wire event subscribers ────────────────────────────────────
    store.on("tga_updated", nl_calc.recompute)
    store.on("rrp_updated", nl_calc.recompute)
    store.on("reserves_updated", nl_calc.recompute)

    # ── Initial poll of all collectors ────────────────────────────
    if settings.initial_poll_on_start:
        logger.info("Running initial poll of all collectors...")
        for c in collectors.values():
            await c.poll()
        # Force net-liquidity recompute once all inputs are present
        await nl_calc.recompute()

    # ── Scheduler ─────────────────────────────────────────────────
    sched = build_scheduler(collectors)
    sched.start()
    logger.info(f"Scheduler started with {len(sched.get_jobs())} jobs:")
    for job in sched.get_jobs():
        logger.info(f"  • {job.id}: next run = {job.next_run_time}")

    # ── Layer-3 proxy stream (concurrent task) ────────────────────
    proxy = PolygonStream(store, alerter)
    proxy_task = asyncio.create_task(proxy.run(), name="polygon_stream")

    # ── Shutdown handler ──────────────────────────────────────────
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _on_signal() -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            # Windows fallback
            pass

    logger.info("System running. Press Ctrl+C to stop.")
    await stop_event.wait()

    logger.info("Shutting down...")
    sched.shutdown(wait=False)
    proxy_task.cancel()
    try:
        await asyncio.wait_for(proxy_task, timeout=5)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass
    store.flush_proxy_buffer()
    logger.info("Goodbye.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
