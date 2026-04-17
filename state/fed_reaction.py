"""
Fed reaction function — rule-based probability that the Fed intervenes
within the next 5 / 10 / 30 days.

Why rule-based not ML
---------------------
Reviewer explicitly flagged that training a logistic regression on ~30
historical Fed interventions across 3 different chairs (Bernanke /
Yellen / Powell) with different reaction functions would severely
overfit. A hand-coded decision tree encoding known interventions keeps
the model transparent, lets us track WHY the probability is what it is,
and makes the regime-dependence explicit (can add new rules for new
chairs without retraining).

Method
------
Rules are an ordered list. First rule that matches sets the
probabilities. This mirrors how a trader thinks: "if SRP is on, Fed is
already intervening, stop checking". Each rule carries (5d, 10d, 30d)
probabilities calibrated against historical observations.

What this does NOT do
---------------------
* Modify playbook action magnitudes (kept as observation-only for now;
  swap later if walk-forward validates it helps).
* Predict Fed policy rate moves (different question entirely).
* Capture BOE/ECB/BOJ interventions (only US Fed).

Historical calibration references
---------------------------------
* 2019-09-17: SRP spike → SRF announced 2019-10-11 (24 days); initial
  ad-hoc repo ops within 48h. Gives ~0.95 for "SRP active".
* 2020-03-15: Emergency rate cut + unlimited QE restart after MOVE
  spiked + credit froze. ~10 days from first stress to action.
* 2023-03-12: BTFP created 48h after SVB fail. Reserves were < $3T
  AND funding stress had just shown up → rapid action.
* 2024-Q4: Mild stress but no Fed action — auction tails > 10bp
  multiple times without intervention. Informs the "tail alone
  probably won't cause Fed to move" calibration.
* Post-2021 SRF era: standing facility absorbs many scarcity events
  silently, reducing the "visible intervention" rate. Encoded via
  lower base-rate.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
from loguru import logger

from ..storage.parquet_store import ParquetStore


# ───────────────────────── FOMC calendar ─────────────────────────
# Scheduled FOMC meetings. Update this yearly. Source: Fed's published
# calendar at https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm

FOMC_DATES: list[date] = [
    # 2024
    date(2024, 1, 31), date(2024, 3, 20), date(2024, 5, 1),
    date(2024, 6, 12), date(2024, 7, 31), date(2024, 9, 18),
    date(2024, 11, 7), date(2024, 12, 18),
    # 2025
    date(2025, 1, 29), date(2025, 3, 19), date(2025, 5, 7),
    date(2025, 6, 18), date(2025, 7, 30), date(2025, 9, 17),
    date(2025, 10, 29), date(2025, 12, 10),
    # 2026 (projected — verify against Fed calendar publication)
    date(2026, 1, 28), date(2026, 3, 18), date(2026, 4, 29),
    date(2026, 6, 17), date(2026, 7, 29), date(2026, 9, 16),
    date(2026, 10, 28), date(2026, 12, 9),
]


def days_until_next_fomc(today: date | None = None) -> int | None:
    """Return days until the next scheduled FOMC, or None if our calendar
    is exhausted (past 2026-12)."""
    today = today or datetime.now(timezone.utc).date()
    future = [d for d in FOMC_DATES if d >= today]
    if not future:
        return None
    return (min(future) - today).days


# ───────────────────────── Rule engine ─────────────────────────

@dataclass
class Rule:
    name: str
    label: str              # human-readable description shown to user
    p_5d: float
    p_10d: float
    p_30d: float
    historical: str         # calibration anchor


def _build_rules(state: dict) -> list[Rule]:
    """Build ordered rule list based on current state. First-match wins."""
    srp_active        = state.get("srp_active", False)
    sofr_iorb_bp      = state.get("sofr_iorb_bp")
    reserves_bn       = state.get("reserves_bn")
    net_liq_bn        = state.get("net_liquidity_bn")
    auction_tail_bp   = state.get("auction_tail_bp")
    market_stress_z   = state.get("market_stress_z")
    days_to_fomc      = state.get("days_to_fomc")

    def lt(a, b):
        return a is not None and a < b
    def gt(a, b):
        return a is not None and a > b

    rules: list[Rule] = []

    # 1. SRP active — Fed IS already intervening (SRP itself is the Fed's action)
    if srp_active:
        rules.append(Rule(
            "SRP_ACTIVE", "SRP 已激活（Fed 正在救）",
            p_5d=0.95, p_10d=0.95, p_30d=0.98,
            historical="2019-09-17 首次救市即如此",
        ))

    # 2. Acute compound scarcity — SOFR-IORB elevated + reserves approaching crisis
    if gt(sofr_iorb_bp, 20) and lt(reserves_bn, 3000):
        rules.append(Rule(
            "SEVERE_COMPOUND", "SOFR-IORB >20bp 且准备金 <$3T",
            p_5d=0.85, p_10d=0.90, p_30d=0.95,
            historical="2019-10 SRP 宣布前 1 周的状态",
        ))

    # 3. Isolated severe funding stress
    if gt(sofr_iorb_bp, 10):
        rules.append(Rule(
            "FUNDING_STRESS", "SOFR-IORB >10bp（银行间紧张）",
            p_5d=0.60, p_10d=0.75, p_30d=0.85,
            historical="2019-09, 2024 quarter-ends",
        ))

    # 4. Net Liquidity crossed CRITICAL
    if lt(net_liq_bn, 2000):
        rules.append(Rule(
            "NETLIQ_CRITICAL", "综合水位 <$2T（结构性危机）",
            p_5d=0.70, p_10d=0.80, p_30d=0.92,
            historical="2020-03 COVID 级别",
        ))

    # 5. Leading-indicator resonance (auction tail + reserves)
    if gt(auction_tail_bp, 6) and lt(reserves_bn, 3000):
        rules.append(Rule(
            "TAIL_PLUS_SCARCE", "30Y tail >6bp 且准备金 <$3T",
            p_5d=0.35, p_10d=0.55, p_30d=0.70,
            historical="2024-11 选举后，+Fed 保持观察",
        ))

    # 6. Market stress close to FOMC — verbal intervention possible
    if gt(market_stress_z, 4) and lt(days_to_fomc, 14):
        rules.append(Rule(
            "PRE_FOMC_MARKET_STRESS", "市场压力 z>4 且 FOMC 会议 <14 天",
            p_5d=0.25, p_10d=0.50, p_30d=0.75,
            historical="Powell 惯常在会前通过公开讲话温和干预",
        ))

    # 7. Reserves below CRITICAL standalone
    if lt(reserves_bn, 2800):
        rules.append(Rule(
            "RESERVES_CRITICAL", "准备金 <$2.8T（接近 2019 级）",
            p_5d=0.45, p_10d=0.60, p_30d=0.80,
            historical="2019-09 前夕水位",
        ))

    # 8. Baseline — no specific trigger
    if not rules:
        rules.append(Rule(
            "BASELINE", "无明确触发条件",
            p_5d=0.05, p_10d=0.10, p_30d=0.20,
            historical="post-2021 SRF era 基准率",
        ))

    return rules


def compute_intervention_probability(state: dict) -> dict:
    """Given current-state dict, return probabilities + triggered rules.

    state keys (all optional, None means 'unknown'):
        srp_active          bool
        sofr_iorb_bp        float
        reserves_bn         float
        net_liquidity_bn    float
        auction_tail_bp     float
        market_stress_z     float
        days_to_fomc        int (can be computed via days_until_next_fomc)

    Returns:
        {
            "p_5d": float,
            "p_10d": float,
            "p_30d": float,
            "top_rule": str,           # rule name of the first matched rule
            "top_rule_label": str,     # human-readable
            "top_rule_historical": str,
            "all_matched_rules": list[str],
        }
    """
    rules = _build_rules(state)

    # First-match wins (most-severe-first ordering inside _build_rules)
    top = rules[0]

    return {
        "p_5d":  top.p_5d,
        "p_10d": top.p_10d,
        "p_30d": top.p_30d,
        "top_rule":              top.name,
        "top_rule_label":        top.label,
        "top_rule_historical":   top.historical,
        "all_matched_rules":     [r.name for r in rules],
    }


# ───────────────────────── Tracker class ─────────────────────────

class FedReactionTracker:
    """Event-driven: recomputes after upstream data changes, writes
    data/raw/fed_reaction.parquet. Dashboard reads from parquet."""

    NAME = "fed_reaction"

    def __init__(self, store: ParquetStore) -> None:
        self.store = store

    def _gather_state(self) -> dict:
        """Read latest snapshots from store, assemble current state dict."""
        state: dict[str, Any] = {}

        srp = self.store.last_snapshot("srp")
        state["srp_active"] = (
            srp is not None and float(srp.get("total_accepted_bn", 0) or 0) > 0
        )

        sofr = self.store.last_snapshot("sofr_iorb")
        if sofr is not None:
            state["sofr_iorb_bp"] = float(sofr.get("spread_bp") or 0)

        res = self.store.last_snapshot("reserves")
        if res is not None:
            state["reserves_bn"] = float(res.get("reserves_bn") or 0)

        nl = self.store.last_snapshot("net_liquidity")
        if nl is not None:
            state["net_liquidity_bn"] = float(nl.get("net_liquidity_bn") or 0)

        tail = self.store.last_snapshot("auction_tail")
        if tail is not None:
            t = tail.get("tail_bp")
            if t is not None and not pd.isna(t):
                state["auction_tail_bp"] = float(t)

        ms = self.store.last_snapshot("market_stress")
        if ms is not None:
            state["market_stress_z"] = float(ms.get("composite_stress_z") or 0)

        state["days_to_fomc"] = days_until_next_fomc()
        return state

    async def recompute(self, _payload: dict | None = None) -> None:
        state = self._gather_state()
        result = compute_intervention_probability(state)

        payload = {
            "as_of_utc": datetime.now(timezone.utc).isoformat(),
            **state,
            **result,
            # all_matched_rules is a list — serialize to string for parquet
            "all_matched_rules_str": "|".join(result["all_matched_rules"]),
        }
        # Parquet can't store list columns in a friendly way here, drop it
        payload.pop("all_matched_rules", None)

        h = hashlib.md5(
            f"{payload['top_rule']}:{payload['p_5d']}:{state.get('reserves_bn')}".encode()
        ).hexdigest()

        last_h = self.store.last_hash(self.NAME)
        if last_h == h:
            return

        self.store.write_snapshot(self.NAME, payload, h)
        logger.info(
            f"[fed_reaction] P(5d)={result['p_5d']:.2f} "
            f"P(10d)={result['p_10d']:.2f} "
            f"rule={result['top_rule']} ({result['top_rule_label']})"
        )
