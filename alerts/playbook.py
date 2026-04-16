"""
Translate stress severity into concrete action suggestions.

Given a stress level (MEDIUM / HIGH / CRITICAL) plus a short narrative reason,
build a structured action recommendation (equity reduction %, SPY put hedge
parameters) and format it as a multi-line Markdown-friendly alert body.

Design principles:
- Output is **percentages / ratios**, never absolute dollars or share counts.
  Consumer decides absolute sizing against their own portfolio.
- Default hedge ticker is SPY (broad, liquid). Caveat surfaces the
  under-hedging risk for high-beta / small-cap concentration.
- No state: each call is a pure function of (level, metrics).

Action magnitudes are deliberately conservative. Tune in ACTIONS if you want
a more aggressive playbook.
"""
from __future__ import annotations


ACTIONS: dict[str, dict[str, object]] = {
    "MEDIUM": {
        "reduce_equity_pct": 15,
        "hedge_notional_pct": 10,
        "put_dte_days": 45,
        "put_strike": "ATM",
        "review_horizon_days": 5,
    },
    "HIGH": {
        "reduce_equity_pct": 30,
        "hedge_notional_pct": 20,
        "put_dte_days": 30,
        "put_strike": "ATM",
        "review_horizon_days": 3,
    },
    "CRITICAL": {
        "reduce_equity_pct": 60,
        "hedge_notional_pct": 40,
        "put_dte_days": 30,
        "put_strike": "5% OTM",
        "review_horizon_days": 1,
    },
}

_LEVEL_EMOJI = {
    "MEDIUM": "🟡",
    "HIGH": "🟠",
    "CRITICAL": "🚨🔴",
    "INFO": "🔵",
}


def suggest_action(level: str, hedge_ticker: str = "SPY") -> dict | None:
    """Return an action template for a severity level, or None for levels
    below MEDIUM."""
    template = ACTIONS.get(level)
    if template is None:
        return None
    plan = dict(template)
    plan["hedge_ticker"] = hedge_ticker
    plan["caveat"] = (
        f"{hedge_ticker} puts under-hedge high-beta / small-cap / tech "
        "concentration. Size up or switch to QQQ/IWM puts if your book tilts there."
    )
    return plan


def format_alert(
    *,
    level: str,
    title: str,
    metrics: dict[str, str],
    action: dict | None,
) -> str:
    """Build a multi-line alert body.

    Example output:
        🟠 HIGH: Reserves approaching Fed 'ample' floor
        ├─ Reserves: $2,985 bn (below $3.0T HIGH threshold)
        ├─ Threshold: $3,000 bn
        └─ Suggested action:
            ├─ Reduce equity exposure: −30%
            ├─ SPY put hedge: 20% notional, 30 DTE, ATM
            ├─ Review horizon: 3 days
            └─ Caveat: SPY under-hedges high-beta ...
    """
    emoji = _LEVEL_EMOJI.get(level, "🔵")
    lines = [f"{emoji} {level}: {title}"]

    metric_items = list(metrics.items())
    for i, (k, v) in enumerate(metric_items):
        is_last_metric = (i == len(metric_items) - 1) and action is None
        prefix = "└─" if is_last_metric else "├─"
        lines.append(f"{prefix} {k}: {v}")

    if action:
        lines.append("└─ Suggested action:")
        lines.append(
            f"    ├─ Reduce equity exposure: −{action['reduce_equity_pct']}%"
        )
        lines.append(
            f"    ├─ {action['hedge_ticker']} put hedge: "
            f"{action['hedge_notional_pct']}% notional, "
            f"{action['put_dte_days']} DTE, {action['put_strike']}"
        )
        lines.append(
            f"    ├─ Review horizon: {action['review_horizon_days']} day(s)"
        )
        lines.append(f"    └─ Caveat: {action['caveat']}")

    return "\n".join(lines)


def tier_level(
    value: float,
    *,
    medium: float | None = None,
    high: float | None = None,
    critical: float | None = None,
    direction: str = "below",
) -> str | None:
    """Return the severity level for a metric crossing its thresholds.

    direction: "below" means value < threshold triggers (e.g. reserves, rrp).
               "above" means value > threshold triggers (e.g. TGA swing).

    Returns None if no threshold is crossed, else the highest matching level.
    """
    if direction not in ("below", "above"):
        raise ValueError(f"direction must be below or above, got {direction!r}")

    def crosses(v: float, threshold: float | None) -> bool:
        if threshold is None:
            return False
        return (v < threshold) if direction == "below" else (v > threshold)

    if crosses(value, critical):
        return "CRITICAL"
    if crosses(value, high):
        return "HIGH"
    if crosses(value, medium):
        return "MEDIUM"
    return None
