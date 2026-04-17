# Walk-forward Validation Report

_Generated: 2026-04-17T10:50:10.990648+00:00_
_SPY window: 2018-04-17 → 2026-04-16_

## ⚠️ Read this first

**Look-ahead bias**: the thresholds below were hand-calibrated
*after* seeing the 2019 / 2020 / 2023 crises. So these numbers
tell you whether the thresholds *pick out* historical events,
**not** whether they would predict new ones. True out-of-sample
validation requires re-fitting thresholds on the early half and
testing on the late half — defer until more data accumulates.

**Small N warning**: CRITICAL-tier alerts typically fire 0-2 times
in 5 years. Statistical power at that tier is weak; treat as
anecdote, not evidence.

**SPY only**: forward returns use SPY. Tech-heavy / small-cap
portfolios typically see 1.5-2.5× the drawdown.

---

## Baseline: unconditional SPY forward return over the same window

| Horizon | N | Mean % | Median % | 10th pct % | P(down) |
|---|---|---|---|---|---|
| 5d | 2,006 | +0.27 | +0.48 | -2.52 | 39.4% |
| 10d | 2,001 | +0.53 | +0.91 | -3.50 | 35.6% |
| 20d | 1,991 | +1.04 | +1.79 | -4.87 | 32.2% |

*Any alert's conditional hit_rate_down must significantly exceed the baseline P(down) to carry information.*

## Per-alert results

### reserves_MEDIUM
- **Threshold**: `reserves < $3200bn`
- **Historical events**: **12**
- Event dates: `2022-06-22, 2022-09-21, 2023-04-26, 2023-06-28, 2023-07-26, 2023-08-23, 2023-09-27, 2024-07-03, 2024-09-25, 2025-01-01, 2025-02-05, 2025-09-03`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 12 | +0.24 | +0.30 | -1.13 | 50.0% | +10.6 pp |
| 10d | 12 | +1.39 | +1.67 | -0.05 | 16.7% | -18.9 pp |
| 20d | 12 | +0.24 | -0.07 | -2.73 | 50.0% | +17.8 pp |

### reserves_HIGH
- **Threshold**: `reserves < $3000bn`
- **Historical events**: **7**
- Event dates: `2022-09-28, 2023-01-04, 2023-03-01, 2025-10-01, 2025-10-22, 2026-01-21, 2026-03-25`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 7 | +1.54 | +1.46 | +0.32 | 14.3% | -25.1 pp |
| 10d | 7 | +0.10 | +0.12 | -2.34 | 42.9% | +7.3 pp |
| 20d | 6 | +2.55 | +2.25 | -0.45 | 33.3% | +1.1 pp |

### reserves_CRITICAL
- **Threshold**: `reserves < $2800bn`
- **Historical events**: **0**
- ⚪ _No triggers in sample window. Cannot validate or invalidate._

### rrp_MEDIUM
- **Threshold**: `rrp < $200bn`
- **Historical events**: **19**
- Event dates: `2025-03-24, 2025-04-03, 2025-05-01, 2025-06-02, 2025-06-12, 2025-06-20, 2025-07-10, 2025-07-15, 2025-07-22, 2025-08-01`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 19 | +0.63 | +1.07 | -2.32 | 26.3% | -13.1 pp |
| 10d | 19 | +0.97 | +1.85 | -2.10 | 26.3% | -9.3 pp |
| 20d | 19 | +2.08 | +3.30 | -1.17 | 15.8% | -16.4 pp |

### rrp_HIGH
- **Threshold**: `rrp < $100bn`
- **Historical events**: **16**
- Event dates: `2025-01-21, 2025-01-27, 2025-02-03, 2025-03-17, 2025-04-11, 2025-04-15, 2025-04-25, 2025-08-01, 2025-08-05, 2026-01-02`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 16 | +0.77 | +0.88 | -0.62 | 31.2% | -8.2 pp |
| 10d | 16 | +1.40 | +1.51 | -0.41 | 25.0% | -10.6 pp |
| 20d | 16 | +2.24 | +2.53 | -2.18 | 25.0% | -7.2 pp |

### rrp_CRITICAL
- **Threshold**: `rrp < $50bn`
- **Historical events**: **7**
- Event dates: `2024-05-08, 2024-10-16, 2025-08-14, 2025-09-02, 2025-09-30, 2025-11-03, 2026-01-02`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 7 | +0.51 | +0.44 | -1.03 | 42.9% | +3.5 pp |
| 10d | 7 | +0.54 | +0.62 | -1.39 | 42.9% | +7.3 pp |
| 20d | 7 | +2.37 | +2.56 | +0.97 | 14.3% | -17.9 pp |

### netliq_MEDIUM
- **Threshold**: `netliq < $2400bn`
- **Historical events**: **3**
- Event dates: `2024-10-16, 2025-09-15, 2026-04-13`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 2 | +0.08 | +0.08 | -0.58 | 50.0% | +10.6 pp |
| 10d | 2 | +0.01 | +0.01 | -0.31 | 50.0% | +14.4 pp |
| 20d | 2 | +1.44 | +1.44 | +0.55 | 0.0% | -32.2 pp |

### netliq_HIGH
- **Threshold**: `netliq < $2200bn`
- **Historical events**: **4**
- Event dates: `2025-09-30, 2025-10-14, 2026-01-16, 2026-03-16`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 4 | -0.02 | +0.30 | -1.38 | 25.0% | -14.4 pp |
| 10d | 4 | -0.46 | -0.03 | -4.06 | 50.0% | +14.4 pp |
| 20d | 4 | +2.20 | +3.14 | +0.05 | 25.0% | -7.2 pp |

### netliq_CRITICAL
- **Threshold**: `netliq < $2000bn`
- **Historical events**: **4**
- Event dates: `2025-10-23, 2026-01-27, 2026-02-03, 2026-02-10`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 4 | -0.03 | -0.23 | -0.85 | 50.0% | +10.6 pp |
| 10d | 4 | -0.26 | -0.34 | -0.48 | 75.0% | +39.4 pp |
| 20d | 4 | -1.53 | -1.46 | -2.69 | 100.0% | +67.8 pp |

### tga_daily_swing
- **Threshold**: `|Δ1d TGA| > $50bn`
- **Historical events**: **147**
- Event dates: `2026-01-30, 2026-02-25, 2026-02-27, 2026-03-02, 2026-03-11, 2026-03-16, 2026-03-18, 2026-04-01, 2026-04-13, 2026-04-15`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 145 | -0.24 | +0.17 | -2.92 | 46.9% | +7.5 pp |
| 10d | 145 | +0.05 | +0.35 | -4.11 | 44.8% | +9.2 pp |
| 20d | 144 | +0.57 | +1.57 | -5.87 | 37.5% | +5.3 pp |

### market_stress_MEDIUM
- **Threshold**: `daily max composite_z > 2.0`
- **Historical events**: **90**
- Event dates: `2025-12-31, 2026-01-12, 2026-01-19, 2026-01-29, 2026-02-26, 2026-03-02, 2026-03-05, 2026-03-23, 2026-04-02, 2026-04-13`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 89 | +0.26 | +0.48 | -2.26 | 40.4% | +1.0 pp |
| 10d | 88 | +0.89 | +1.32 | -2.56 | 26.1% | -9.5 pp |
| 20d | 87 | +1.37 | +2.29 | -3.59 | 24.1% | -8.1 pp |

### market_stress_HIGH
- **Threshold**: `daily max composite_z > 3.0`
- **Historical events**: **58**
- Event dates: `2025-12-17, 2025-12-29, 2026-01-12, 2026-01-14, 2026-01-19, 2026-01-29, 2026-03-02, 2026-03-06, 2026-03-23, 2026-04-13`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 57 | +0.27 | +0.64 | -2.31 | 42.1% | +2.7 pp |
| 10d | 57 | +0.98 | +1.68 | -2.69 | 22.8% | -12.8 pp |
| 20d | 56 | +1.48 | +2.24 | -2.18 | 26.8% | -5.4 pp |

### market_stress_CRITICAL
- **Threshold**: `daily max composite_z > 4.0`
- **Historical events**: **38**
- Event dates: `2025-10-10, 2025-10-14, 2025-11-04, 2025-11-20, 2025-12-12, 2025-12-29, 2026-01-12, 2026-01-19, 2026-03-02, 2026-03-09`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 38 | +0.42 | +0.72 | -2.26 | 36.8% | -2.6 pp |
| 10d | 38 | +1.36 | +1.81 | -2.33 | 18.4% | -17.2 pp |
| 20d | 38 | +1.51 | +1.94 | -2.50 | 26.3% | -5.9 pp |

### auction_tail_MEDIUM
- **Threshold**: `tail_bp > 2.0`
- **Historical events**: **35**
- Event dates: `2021-11-10, 2022-02-10, 2022-08-11, 2022-10-13, 2023-04-13, 2023-06-13, 2023-10-12, 2024-08-08, 2025-04-10, 2025-11-13`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 35 | -1.57 | -2.67 | -2.67 | 82.9% | +43.5 pp |
| 10d | 35 | -0.72 | -1.93 | -1.93 | 74.3% | +38.7 pp |
| 20d | 35 | +0.60 | +0.34 | -0.62 | 17.1% | -15.1 pp |

### auction_tail_HIGH
- **Threshold**: `tail_bp > 4.0`
- **Historical events**: **21**
- Event dates: `2019-07-11, 2021-03-11, 2021-08-12, 2021-11-10, 2022-02-10, 2022-08-11, 2023-10-12, 2024-02-08, 2024-08-08, 2025-04-10`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 21 | -1.31 | -2.67 | -2.67 | 76.2% | +36.8 pp |
| 10d | 21 | -0.78 | -1.93 | -1.93 | 71.4% | +35.8 pp |
| 20d | 21 | +0.45 | +0.34 | -1.90 | 14.3% | -17.9 pp |

### auction_tail_CRITICAL
- **Threshold**: `tail_bp > 6.0`
- **Historical events**: **15**
- Event dates: `2015-07-09, 2016-09-13, 2019-07-11, 2019-10-10, 2021-11-10, 2022-02-10, 2022-08-11, 2023-10-12, 2024-11-06, 2025-04-10`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 15 | -1.13 | -2.67 | -2.67 | 66.7% | +27.3 pp |
| 10d | 15 | -0.82 | -1.93 | -2.32 | 73.3% | +37.7 pp |
| 20d | 15 | +0.45 | +0.34 | -2.67 | 20.0% | -12.2 pp |

### sofr_iorb_MEDIUM
- **Threshold**: `spread_bp > 2.0`
- **Historical events**: **21**
- Event dates: `2025-12-05, 2025-12-15, 2025-12-22, 2025-12-26, 2026-01-30, 2026-02-17, 2026-02-27, 2026-03-16, 2026-03-31, 2026-04-15`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 20 | +0.44 | +0.42 | -1.60 | 35.0% | -4.4 pp |
| 10d | 20 | +0.33 | +0.64 | -3.33 | 40.0% | +4.4 pp |
| 20d | 19 | +0.92 | +1.23 | -1.69 | 21.1% | -11.1 pp |

### sofr_iorb_HIGH
- **Threshold**: `spread_bp > 5.0`
- **Historical events**: **15**
- Event dates: `2025-10-15, 2025-10-21, 2025-11-12, 2025-11-17, 2025-11-24, 2025-12-15, 2025-12-26, 2026-02-17, 2026-03-02, 2026-04-15`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 14 | +0.08 | +0.45 | -1.45 | 35.7% | -3.7 pp |
| 10d | 14 | +0.42 | +0.65 | -1.95 | 35.7% | +0.1 pp |
| 20d | 14 | +0.51 | +1.04 | -1.74 | 21.4% | -10.8 pp |

### sofr_iorb_CRITICAL
- **Threshold**: `spread_bp > 10.0`
- **Historical events**: **8**
- Event dates: `2024-10-01, 2024-12-26, 2025-09-15, 2025-10-15, 2025-10-27, 2025-11-25, 2025-12-26, 2025-12-31`

| Horizon | N | Mean % | Median % | 10th pct % | P(down) | vs baseline |
|---|---|---|---|---|---|---|
| 5d | 8 | +0.29 | +0.60 | -0.73 | 37.5% | -1.9 pp |
| 10d | 8 | +0.74 | +1.11 | -1.38 | 25.0% | -10.6 pp |
| 20d | 8 | +1.00 | +1.11 | -0.50 | 12.5% | -19.7 pp |

## 🎯 Key question: does the playbook have data support?

- **MEDIUM** (playbook: 减仓 15%) — total events across structural alerts: 55. Avg 10d P(SPY down): 33.2% (baseline: 35.6%). Verdict: ❌ no edge
- **HIGH** (playbook: 减仓 30%) — total events across structural alerts: 42. Avg 10d P(SPY down): 38.4% (baseline: 35.6%). Verdict: 🟡 marginal
- **CRITICAL** (playbook: 减仓 60%) — total events across structural alerts: 19. Avg 10d P(SPY down): 47.6% (baseline: 35.6%). Verdict: ✅ signal present

---

## How to interpret

- **P(down)** significantly above baseline = the threshold has signal
- **Mean / median forward return** more negative than baseline = actionable
- **10th percentile** tells you the bad-case drawdown after the alert fires — this is what hedging is priced against
- If a tier has 0 events: expected for CRITICAL in a 5y sample that doesn't contain a real crisis. Not a bug.
- If a tier has many events with **no lift over baseline**: threshold is noise. Tighten or remove.