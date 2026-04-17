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

    # ─── Tiered structural thresholds (for playbook) ───
    # Fire ACTION playbook at MEDIUM / HIGH / CRITICAL levels.
    #
    # Bank reserves level — approaching Fed's "ample" floor.
    # Historical context: Fed's own guidance around LCLoR (Lowest Comfortable
    # Level of Reserves) puts concern zone roughly around $3T; 2019 repo crisis
    # was preceded by reserves falling to ~$1.4T (pre-QE era floor was lower).
    # For post-2022 regime, treat $3.2T/3.0T/2.8T as medium/high/critical.
    reserves_medium_bn: float = 3200.0
    reserves_high_bn: float = 3000.0
    reserves_critical_bn: float = 2800.0

    # ON RRP level — the "cushion" absorbing liquidity drains before
    # reserves themselves start to fall. When RRP is exhausted, every TGA
    # rebuild bn comes directly out of bank reserves.
    rrp_medium_bn: float = 200.0
    rrp_high_bn: float = 100.0
    rrp_critical_bn: float = 50.0

    # Net Liquidity — Reserves + RRP − TGA (billion USD).
    # Post-QT regime baseline runs ~$2.3T (Reserves ~$3T + RRP near zero
    # − TGA ~$0.7T). The pre-QT peak was ~$5.8T (2021). These thresholds
    # are calibrated for the CURRENT regime, not historical. Revisit if
    # Fed resumes QE (operating range shifts up ~$2T).
    net_liq_medium_bn: float = 2400.0
    net_liq_high_bn: float = 2200.0
    net_liq_critical_bn: float = 2000.0

    # Default hedge ticker used by alerts/playbook.py suggestions.
    # Switch to QQQ or IWM if your book skews tech / small-cap.
    hedge_ticker: str = "SPY"

    # ─── Market stress (Layer-2 fast pulse) ──────────────
    # Composite stress z-score thresholds for the yfinance-based
    # ETF + VIX basket. Positive z = stress-aligned movement.
    market_stress_medium_z: float = 2.0
    market_stress_high_z: float = 3.0
    market_stress_critical_z: float = 4.0

    # ─── 30Y auction tail (leading indicator) ────────────
    # Tail (bp) = clearing high yield − prior-day DGS30 close. Normal
    # auctions clear within 0-1 bp. Historical "serious stress"
    # threshold is ~6 bp (2018 Q4, 2023 Oct 30y stress).
    auction_tail_medium_bp: float = 2.0
    auction_tail_high_bp: float = 4.0
    auction_tail_critical_bp: float = 6.0

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
