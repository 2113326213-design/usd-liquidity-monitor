# Adaptive vs Static Threshold Comparison

_Generated: 2026-04-17T10:36:18.777426+00:00_

**TRAIN**: 2021-04-21 → 2024-04-17 (1093 days)
**TEST**:  2024-04-18 → 2026-04-16 (729 days)

Adaptive thresholds fitted on TRAIN only — tested out-of-sample on TEST.

## Comparison: SPY 10-day forward hit rate per alert

| Alert | Static thresh | Static N | Static P(down) | Adaptive N | Adaptive P(down) | Δ P(down) | Verdict |
|---|---|---|---|---|---|---|---|
| reserves_MEDIUM | 3200 | 5 | 0.0% | 11 | 9.1% | +9.1 pp | 🟢 adaptive +9.1 pp better |
| reserves_HIGH | 3000 | 4 | 25.0% | 5 | 0.0% | -25.0 pp | 🔴 adaptive -25.0 pp worse |
| reserves_CRITICAL | 2800 | 0 | — | 4 | 25.0% | — | 🟢 adaptive catches events static missed |
| rrp_MEDIUM | 200 | 18 | 27.8% | 1 | 0.0% | -27.8 pp | 🔴 adaptive -27.8 pp worse |
| rrp_HIGH | 100 | 14 | 28.6% | 1 | 0.0% | -28.6 pp | 🔴 adaptive -28.6 pp worse |
| rrp_CRITICAL | 50 | 7 | 42.9% | 10 | 20.0% | -22.9 pp | 🔴 adaptive -22.9 pp worse |
| net_liq_MEDIUM | 2400 | 2 | 50.0% | 1 | 0.0% | -50.0 pp | 🔴 adaptive -50.0 pp worse |
| net_liq_HIGH | 2200 | 4 | 50.0% | 2 | 0.0% | -50.0 pp | 🔴 adaptive -50.0 pp worse |
| net_liq_CRITICAL | 2000 | 4 | 75.0% | 5 | 20.0% | -55.0 pp | 🔴 adaptive -55.0 pp worse |

## Aggregate verdict across structural alerts

- Static avg 10d P(down) across fired alerts:   37.4%
- Adaptive avg 10d P(down) across fired alerts: 8.2%
- **Δ = -29.2 pp — adaptive is worse.** Keep static.

## Thresholds used

### Adaptive (fitted on TRAIN only)

**reserves**
| Regime | MEDIUM | HIGH | CRITICAL |
|---|---|---|---|
| abundant | 3874 | 3303 | 3249 |
| ample | 3284 | 3125 | 3088 |
| scarce | 3147 | 3043 | 2998 |
| crisis | 3017 | 3016 | 3000 |

**rrp**
| Regime | MEDIUM | HIGH | CRITICAL |
|---|---|---|---|
| abundant | 1039 | 747 | 369 |
| ample | 773 | 483 | 440 |
| scarce | 1437 | 694 | 453 |
| crisis | 1669 | 1301 | 1283 |

**net_liq**
| Regime | MEDIUM | HIGH | CRITICAL |
|---|---|---|---|
| abundant | 4675 | 3924 | 3456 |
| ample | 3576 | 3283 | 3233 |
| scarce | 4003 | 3315 | 3192 |
| crisis | 4170 | 3752 | 3727 |

### Static (from .env)
- reserves: MEDIUM 3200.0, HIGH 3000.0, CRITICAL 2800.0
- rrp: MEDIUM 200.0, HIGH 100.0, CRITICAL 50.0
- net_liq: MEDIUM 2400.0, HIGH 2200.0, CRITICAL 2000.0