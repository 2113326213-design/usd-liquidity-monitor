# Promo copy

Ready-to-paste launch blurbs for different channels (Twitter, Reddit,
小红书, Hacker News). Edit to taste — this file is for your convenience
when announcing the project, not documentation.

---

## Twitter / X (≤ 280 chars)

**English:**
```
💧 usd-liquidity-monitor — open-sourced

crash early-warning for retail. tracks TGA + RRP + Bank Reserves
+ 30Y auction tails + 15-min ETF/VIX pulse.

alerts tell you what to DO (reduce equity X%, buy put Y%),
not just "number went down."

free. iPhone push via Bark.

github.com/2113326213-design/usd-liquidity-monitor
```

**中文：**
```
💧 开源了一个美元流动性监控：

追踪 TGA / RRP / 银行准备金 + 30Y 国债拍卖 tail + 15 分钟 ETF/VIX 脉搏。

告警直接告诉你该怎么做（减仓 X%、买 put Y%），而不是只给你看个数字。

全免费。推送到 iPhone（Bark）。

github.com/2113326213-design/usd-liquidity-monitor
```

---

## Reddit (r/algotrading, r/investing, r/MonetaryBase, r/wallstreetbets)

**Title:**
```
I open-sourced a USD liquidity early-warning system with actionable alerts
```

**Body:**
```
Built this over the last few weeks because every "macro dashboard"
out there makes you do the inference yourself, then panic when
something breaks at 3am.

What it does:
- Tracks the three Fed liability pools (TGA / ON RRP / Bank Reserves)
- Plus a 15-min fast-layer pulse (SPY / ^VIX / TLT / IEF / LQD / HYG)
- Plus 30Y Treasury auction tails (a genuine 2–4-week leading indicator
  for dealer stress)
- Composes Net Liquidity = Reserves + RRP − TGA
- Fires tiered alerts with a concrete action: "reduce equity 30%,
  buy SPY 30DTE ATM put, 20% notional, review in 3 days"
- Pushes to iPhone (Bark) or Telegram, free

Three layers, resonance logic:
- If structural plumbing is already stressed AND market pulse spikes,
  severity auto-escalates one level
- If only one layer moves, requires a higher bar before firing

Data is 100% free:
- Treasury Fiscal Data (TGA)
- NY Fed Markets API (RRP / SRP)
- FRED (WRESBAL, DGS30)
- TreasuryDirect (auction results)
- yfinance (15-min delayed market data)

5 years of backfilled history for calibration.
28 unit + smoke tests (caught NY Fed's SRP endpoint drifting mid-build).

Known caveat: action templates are judgment-calibrated based on 2019 repo,
2023 SVB, and 2024 Q4 auction stress events — not backtested yet.
Walk-forward validation is the next milestone. Use as starting points,
not trading signals.

MIT licensed.

github.com/2113326213-design/usd-liquidity-monitor
```

---

## 小红书 / 微信朋友圈

```
💧 开源了自己写的一个美元流动性预警系统

最近越想越不踏实——美联储准备金水位在慢慢下降，
RRP 缓冲垫也快空了，30Y 国债拍卖 tail 时不时冒大 spike。
2019 年 9 月的 repo crisis、2023 年 SVB 那种事件的前兆，
其实都写在公开数据里，只是需要有人帮你盯着。

所以写了这个：

🔹 追踪 Fed 三个水池：TGA + ON RRP + 银行准备金
🔹 加上 ETF/VIX 快层脉搏（15 分钟频率）
🔹 加上 30Y 拍卖 tail（领先 2-4 周的 dealer 压力信号）
🔹 阈值告警 + 慢快层共振自动升级

最关键的是：**告警不是告诉你"数字动了"，而是直接给你动作**——
"减仓 30%，买 SPY 30 天到期 ATM put 对冲 20% 敞口，3 天后复盘"

全免费。推送到 iPhone 锁屏（Bark app）。

自己在跑。不是投资建议，阈值是经验校准的、还没经过回测验证。
但至少比每天自己刷 FRED 看图准头多了。

GitHub 搜：usd-liquidity-monitor
```

---

## Hacker News (if submitting)

**Title:**
```
Show HN: USD Liquidity Monitor — actionable crash early-warning for retail
```

**First comment (自己发，解释动机):**
```
Author here. The thesis behind this:

Every macro-dashboard tool I've seen shows you the data and then
leaves the inference to you. That's fine if you're an institutional
allocator with a team. If you're a retail investor who happens to
care about USD liquidity (maybe you hold levered positions, maybe
you just watched 2019 and 2023 closely), you want the system to
translate "Reserves crossed $3T" into "here's the action template."

So that's what this does. Three layers of monitoring — structural
plumbing (daily/weekly), market reaction (15-min), and 30Y auction
tails as a leading indicator — feed into a single severity ladder.
Each level of severity maps to a concrete playbook (equity reduction
%, put hedge sizing).

Happy to answer questions about the design choices (why Net Liquidity
= Reserves + RRP − TGA and not some other composite, why SPY default
hedge and the under-hedging caveat for tech-heavy portfolios, why 6bp
auction tail = CRITICAL, etc.).

Known limitation: action thresholds are calibrated against historical
crisis events (2019 repo, 2023 SVB, 2024 Q4 auction stress) but not
backtested against actual P&L. Walk-forward validation is the next
milestone.

github.com/2113326213-design/usd-liquidity-monitor
```

---

## Submission checklist before posting

- [ ] All three open PRs merged into main (fix / chore / docs)
- [ ] Screenshot of the dashboard saved somewhere and linked in README
- [ ] LICENSE visible in repo root (already ✓)
- [ ] README renders cleanly on GitHub (open it, scan for broken ASCII / emoji)
- [ ] `./install.sh` tested on a fresh clone on another machine if possible
- [ ] Repo description + topics set on GitHub (Settings → About) — see below
- [ ] (Optional) Add a 30-second demo gif to README
- [ ] (Optional) Pin the repo to your GitHub profile

---

## GitHub "About" — pick one

Ranked from dry → visceral. Pick whichever matches the vibe:

**Option 1 — Plain** (safe for professional networks):
> Crash early-warning for USD liquidity — tracks TGA, RRP, Reserves, 30Y auction tails and pushes actionable alerts to your phone.

**Option 2 — Sharper** (good for Reddit / HN):
> The smoke detector your portfolio doesn't have. Watches Fed plumbing data and tells you what to DO when stress is building — not just what broke.

**Option 3 — Visceral** (viral-friendly, 小红书 / Twitter):
> Institutions watch Fed plumbing data every day. Now you can too — with push alerts that tell you what to DO, not just what's broken.

**Option 4 — Metaphor-forward** (my recommendation):
> An early-warning system for dollar liquidity crises. The next 2019/2023 event is already visible in Fed data — you just need someone watching for you.

### Suggested topics (GitHub tags, max 20)
```
macro-economics    liquidity         federal-reserve    treasury
repo               alerts            early-warning      streamlit
python             retail-investing  risk-management    portfolio-monitoring
bark-notifications financial-data    usd
```
