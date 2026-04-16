"""
ParquetStore: append-only snapshot store with in-memory hash cache + event pub/sub.

Design:
- One parquet per series (tga.parquet, rrp.parquet, ...) under data/raw/
- Each write appends a row; hash of content is stored for dedup
- Proxy minute bars are partitioned by day under data/raw/proxy/YYYY-MM-DD.parquet
- trigger(event) fires async callbacks registered via on(event, cb)
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from pathlib import Path
from typing import Awaitable, Callable

import pandas as pd
from loguru import logger


Callback = Callable[[dict], Awaitable[None]]


class ParquetStore:
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw"
        self.derived_dir = self.data_dir / "derived"
        self.proxy_dir = self.raw_dir / "proxy"
        for d in (self.raw_dir, self.derived_dir, self.proxy_dir):
            d.mkdir(parents=True, exist_ok=True)

        # hash cache: avoid re-reading parquet every poll
        self._hash_cache: dict[str, str] = {}

        # in-memory buffer for high-frequency proxy bars
        self._proxy_buffer: dict[str, list[dict]] = defaultdict(list)
        self._last_flush: float = time.time()
        self._flush_interval_sec: int = 60

        # event callbacks
        self._callbacks: dict[str, list[Callback]] = defaultdict(list)

    # ── snapshot ops (low-frequency series) ─────────────────────────────
    def _path(self, name: str) -> Path:
        return self.raw_dir / f"{name}.parquet"

    def write_snapshot(self, name: str, payload: dict, h: str) -> None:
        path = self._path(name)
        row = {**payload, "_hash": h}
        df_new = pd.DataFrame([row])
        if path.exists():
            df_old = pd.read_parquet(path)
            df = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df = df_new
        df.to_parquet(path, index=False)
        self._hash_cache[name] = h

    def last_hash(self, name: str) -> str | None:
        if name in self._hash_cache:
            return self._hash_cache[name]
        path = self._path(name)
        if not path.exists():
            return None
        df = pd.read_parquet(path)
        if df.empty or "_hash" not in df.columns:
            return None
        h = str(df["_hash"].iloc[-1])
        self._hash_cache[name] = h
        return h

    def last_snapshot(self, name: str, offset: int = 0) -> dict | None:
        """offset=0 returns latest row; offset=1 returns second-to-latest."""
        path = self._path(name)
        if not path.exists():
            return None
        df = pd.read_parquet(path)
        if len(df) <= offset:
            return None
        return df.iloc[-(offset + 1)].to_dict()

    def read_all(self, name: str) -> pd.DataFrame:
        path = self._path(name)
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    # ── proxy minute bars (high-frequency, buffered) ────────────────────
    def append_proxy_bar(self, bar: dict) -> None:
        """Buffered append. Flushes every flush_interval seconds or >300 rows."""
        day = bar["ts"][:10]
        self._proxy_buffer[day].append(bar)

        total_buffered = sum(len(v) for v in self._proxy_buffer.values())
        if (time.time() - self._last_flush > self._flush_interval_sec
                or total_buffered > 300):
            self.flush_proxy_buffer()

    def flush_proxy_buffer(self) -> None:
        if not self._proxy_buffer:
            return
        for day, rows in self._proxy_buffer.items():
            if not rows:
                continue
            path = self.proxy_dir / f"{day}.parquet"
            df_new = pd.DataFrame(rows)
            if path.exists():
                df_old = pd.read_parquet(path)
                df = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df = df_new
            df.to_parquet(path, index=False)
        self._proxy_buffer.clear()
        self._last_flush = time.time()

    def read_proxy_bars(self, day: str | None = None) -> pd.DataFrame:
        if day:
            path = self.proxy_dir / f"{day}.parquet"
            if not path.exists():
                return pd.DataFrame()
            return pd.read_parquet(path)
        # read all
        files = sorted(self.proxy_dir.glob("*.parquet"))
        if not files:
            return pd.DataFrame()
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    # ── event pub/sub ───────────────────────────────────────────────────
    def on(self, event: str, callback: Callback) -> None:
        self._callbacks[event].append(callback)

    async def trigger(self, event: str, payload: dict) -> None:
        for cb in self._callbacks.get(event, []):
            try:
                await cb(payload)
            except Exception as e:
                logger.exception(f"Trigger '{event}' callback failed: {e}")
