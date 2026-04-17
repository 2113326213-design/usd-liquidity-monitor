# USD Liquidity Monitor — Working Plan

_Living doc. Last updated: 2026-04-17. Gitignored._

---

## 🟢 System Snapshot (what exists right now)

### Collectors (all wired into `main.py` + `scheduler.py`)
- [x] `tga` — Treasury GA, daily, Fiscal Data API
- [x] `rrp` — ON Reverse Repo, daily, NY Fed
- [x] `srp` — Standing Repo, twice-daily, NY Fed
- [x] `reserves` — WRESBAL, weekly, FRED
- [x] `market_stress` — ETF + VIX 1h z-score, 15-min polling, yfinance
- [x] `auction_tail` — 30Y Treasury tail vs DGS30, daily, TreasuryDirect + FRED

### Derived
- [x] `net_liquidity` = Reserves + RRP − TGA, with 7-day EWMA slope

### Alert channels
- [x] Bark → iPhone lockscreen (wired, tested)
- [ ] Telegram (skipped — Bark-only by choice)
- [x] `MultiAlerter` fan-out (works with any subset)
- [x] `alerts/playbook.py` — turns severity into action suggestions
  (equity reduction % + SPY/QQQ put hedge params)

### Thresholds (in `config.py` + `.env.example`)
- [x] Reserves tiers: 3.2T / 3.0T / 2.8T
- [x] RRP tiers: 200 / 100 / 50 bn
- [x] Net Liquidity tiers: 2.4T / 2.2T / 2.0T (post-QT regime)
- [x] Market stress z: 2 / 3 / 4 σ
- [x] Auction tail bp: 2 / 4 / 6 bp
- [x] TGA daily swing: > 50 bn
- [x] SRP: any non-zero

### Dashboard (`streamlit`, 127.0.0.1:8501)
- [x] 5 KPI cards (TGA, RRP, Reserves, SRP, Net Liquidity)
- [x] Net Liquidity time series + 7-day EWMA
- [x] Components stacked chart (Reserves + RRP − TGA)
- [x] **Layer-2 market stress panel (composite z + threshold bands)**
- [ ] Auction tail panel _(not yet — low priority, fires rarely)_
- [ ] Alert history view _(none — out of scope)_
- [ ] Regime indicator _(blocked on regime_detection revival)_

### Historical data
- [x] Reserves: 5y weekly, 2021-04 → present
- [x] RRP: 5y daily, 2021-04 → present
- [x] TGA: 5y daily, 2021-04 → present
- [x] Net Liquidity: 5y daily forward-filled
- [x] Auction tail: 15y, 2011-07 → present (178 auctions)
- [~] Market stress: 2y hourly (yfinance limit)
- [~] SRP: ~1y (NY Fed API cap)

### Infrastructure
- [x] Tests: 18/18 passing (smoke + playbook unit)
- [x] tmux session `liquidity` (monitor + dashboard)
- [x] `~/start_liquidity.sh` one-liner restart
- [x] `deploy/com.gujiaxin.usd-liquidity-monitor.plist` (install with launchctl)
- [x] `.gitignore` covers secrets, venv, data, archive
- [x] GitHub: `2113326213-design/usd-liquidity-monitor` (5 commits)

---

## 🟡 Pending Decisions

- [ ] **launch.json**: start servers via `preview_start` or stay with tmux?
  - Current: tmux is running both. Picking `preview_start` requires killing tmux first.
- [ ] **launchd autostart**: install `com.gujiaxin.usd-liquidity-monitor.plist` now or
      stay with manual tmux restart after Mac reboot?
  - Install command: `cp deploy/*.plist ~/Library/LaunchAgents/ && launchctl load ~/Library/LaunchAgents/com.gujiaxin.usd-liquidity-monitor.plist`
- [ ] **Telegram token**: decided to skip (Bark-only). Reconsider later?
- [ ] **Polygon key**: skipped ($29/mo). Reconsider when more convinced of value.
- [ ] **Hedge ticker default**: SPY. Switch to QQQ if portfolio is tech-heavy.

---

## 🔴 Outstanding Work (ordered by leverage)

### High-value, single-session each (1-2h)
- [ ] **Walk-forward validation framework** ← **NEXT SESSION** (decided 2026-04-17)
  - For each threshold, compute historical false-positive + true-positive rates
  - Requires 5y data (✓ have it)
  - Output: per-threshold calibration report
  - Blocked on: nothing
- [ ] **Fed reaction function (rule-based, NOT ML)**
  - Decision tree encoding FOMC reaction patterns
  - Inputs: Reserves/GDP, SRF usage, MOVE, credit spreads, FOMC proximity
  - Output: probability of intervention within N days
  - Use as narrative overlay, not trained model (30 events too few for ML)
  - Blocked on: nothing

### Medium-value
- [ ] **Revive `regime_detection.py` from `_archive/`**
  - Currently sitting in `_archive/` — was wired to removed `service.py`
  - Rewrite to use `main.py` pattern (Collector → store)
  - 4-state Gaussian-kernel regime probabilities (Abundant/Ample/Scarce/Crisis)
  - Use as context layer for all other alerts
  - Blocked on: nothing, but moderate refactor
- [ ] **Auction tail panel on dashboard**
  - Show last 20 auctions with tail_bp bar chart
  - Color bars by severity threshold
  - ~15 min work
- [ ] **Regime-conditional feature weights**
  - Different stress signals matter in different regimes
  - Implementation: simple dict lookup on current regime → weight vector
  - Blocked on: regime_detection revival

### Low-value / deferred
- [ ] **Swap spread ingestion**
  - Requires Bloomberg / ICE subscription (≥ $2k/yr)
  - Or manual daily entry via CSV
  - Cost / benefit ratio poor for retail
- [ ] **USD XCY basis ingestion**
  - Same story, requires paid source
- [ ] **Backtest engine revival** (from `_archive/`)
  - SPY hit-rate by stress bucket
  - Meaningful only after validation framework exists

---

## ❓ Open Questions (think about later)

1. Are current thresholds right for post-QT regime, or drifting into
   permanent-MEDIUM state? Check after 2-4 weeks of live runs.
2. Does the `market_stress` basket need TIPS / EEM / DXY? Current 6 tickers
   may under-represent credit / FX stress.
3. Should MEDIUM alerts throttle (once per day max)? HIGH / CRITICAL should
   always fire, but MEDIUM could fatigue the user.
4. Add a "heartbeat" check that Bark is actually receiving pushes? e.g. once
   a week send a "system healthy" ping.

---

## 🧭 Operational Quick Ref

```bash
# Start / restart everything
~/start_liquidity.sh

# Watch live logs
tmux attach -t liquidity     # Ctrl+B then D to detach

# Run tests
cd ~/Desktop/usd_liquidity_monitor
python3 -m pytest tests/ -v

# Re-backfill history (kills monitor, then restart via start_liquidity.sh)
tmux kill-window -t liquidity:monitor
cd ~/Desktop
python3 -m usd_liquidity_monitor.scripts.backfill

# Install launchd autostart
cp deploy/com.gujiaxin.usd-liquidity-monitor.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.gujiaxin.usd-liquidity-monitor.plist

# Uninstall launchd autostart
launchctl unload ~/Library/LaunchAgents/com.gujiaxin.usd-liquidity-monitor.plist
rm ~/Library/LaunchAgents/com.gujiaxin.usd-liquidity-monitor.plist
```
