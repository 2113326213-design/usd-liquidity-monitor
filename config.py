"""
Central config. All thresholds, API keys, paths live here.
Load from .env at repo root.
"""
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=_ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    # ─── API keys ──────────────────────────────────────
    polygon_api_key: str = ""
    databento_api_key: str = ""
    fred_api_key: str = ""
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Bark (iOS push). device_key is your per-device key from the Bark app.
    # server_url defaults to the official public server; set to a self-hosted
    # URL (e.g. https://bark.example.com) if you run your own.
    bark_device_key: str = ""
    bark_server_url: str = "https://api.day.app"

    # ─── Paths ─────────────────────────────────────────
    data_dir: Path = Path("./data")

    # ─── Alert thresholds ──────────────────────────────
    # SRP (Standing Repo Facility) — any non-zero acceptance is CRITICAL
    srp_alert_min_bn: float = 0.0

    # TGA daily swing (absolute billion USD) that triggers HIGH alert
    tga_daily_swing_bn: float = 50.0

    # ON RRP daily drain threshold (negative billion USD delta)
    rrp_daily_drain_bn: float = 50.0

    # Net liquidity 7-day EWMA slope reversal (billion USD / day)
    net_liquidity_slope_alert: float = -20.0

    # Layer-3 proxy z-score anomaly threshold (standard deviations over 60-min window)
    proxy_z_threshold: float = 3.0
    proxy_window_minutes: int = 60

    # ─── Operational ───────────────────────────────────
    # If True, run a one-off initial poll at startup for every collector
    initial_poll_on_start: bool = True

    # Log level
    log_level: str = "INFO"


settings = Settings()

# Normalise data_dir: if user supplied a relative path (e.g. "./data" in .env),
# resolve it relative to the package directory so the project runs correctly
# no matter which cwd it is launched from.
if not settings.data_dir.is_absolute():
    settings.data_dir = (Path(__file__).parent / settings.data_dir).resolve()

# Ensure dirs exist
(settings.data_dir / "raw").mkdir(parents=True, exist_ok=True)
(settings.data_dir / "derived").mkdir(parents=True, exist_ok=True)
(settings.data_dir / "logs").mkdir(parents=True, exist_ok=True)
(settings.data_dir / "raw" / "proxy").mkdir(parents=True, exist_ok=True)
