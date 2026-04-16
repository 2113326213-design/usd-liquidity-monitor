# Fed Liquidity Monitor

实时监控美联储流动性三大池子（**TGA / Reserves / ON RRP**）的最小可运行系统。

核心设计：**三层分频架构**，不同频率的数据在合适的层级消化。不浪费 API 调用，也不丢掉任何压力信号。

## 架构

```
┌─ Layer 3: Minute-level Proxy (Polygon.io websocket)     ──┐
│   BIL / SGOV / SHV / SHY / IEI / IEF / TLT / TLH         │
│   → 60 分钟滚动 z-score 异动检测                           │
│   → 用市场定价"提前嗅出"流动性压力                         │
└─────────────────────────────────────────────────────────┘
           ↑ 触发加权 composite stress score
┌─ Layer 2: Event-driven Scheduler (APScheduler)            ┐
│   TGA    : Fiscal Data API,  ~16:00-17:00 ET, mon-fri    │
│   ON RRP : NY Fed Markets,   ~13:15-13:45 ET, mon-fri    │
│   SRP    : NY Fed Markets,   AM + PM windows             │
│   Reserves: FRED WRESBAL,    ~16:30 ET Thursday          │
└─────────────────────────────────────────────────────────┘
           ↓ 新数据触发衍生状态重算
┌─ Layer 1: Derived State                                  ─┐
│   net_liquidity = Reserves + RRP - TGA                    │
│   7-day EWMA slope 反转检测                                │
└─────────────────────────────────────────────────────────┘
           ↓
       Telegram 告警 + Streamlit 仪表盘
```

## 文件结构

```
usd_liquidity_monitor/
├── config.py                   # 全局配置 (pydantic-settings)
├── main.py                     # 入口：装配 + 事件订阅 + 优雅关闭
├── scheduler.py                # APScheduler cron 配置
├── collectors/
│   ├── base.py                 # 抽象 Collector (hash-dedup polling)
│   ├── tga.py                  # Fiscal Data TGA
│   ├── rrp.py                  # NY Fed ON RRP
│   ├── srp.py                  # NY Fed Standing Repo
│   └── reserves.py             # FRED WRESBAL
├── proxy/
│   ├── polygon_stream.py       # Polygon websocket client
│   └── proxy_state.py          # 滚动 z-score 异动检测
├── state/
│   └── net_liquidity.py        # Reserves + RRP − TGA + 斜率反转
├── alerts/
│   └── telegram.py             # Telegram 告警出口
├── storage/
│   └── parquet_store.py        # Parquet append-only + event pub/sub
├── dashboard/
│   └── app.py                  # Streamlit 仪表盘
├── deploy/
│   ├── usd-liquidity-monitor.service       # systemd (后台 monitor)
│   └── usd-liquidity-dashboard.service     # systemd (Streamlit)
├── requirements.txt
├── .env.example
└── README.md
```

## 快速启动

```bash
# 1. 克隆 / 解压项目
cd usd_liquidity_monitor

# 2. 创建虚拟环境
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. 配置 API keys
cp .env.example .env
# 编辑 .env 填入 POLYGON_API_KEY / FRED_API_KEY / TELEGRAM_*

# 4. 启动后台监控（第一个终端）
python -m usd_liquidity_monitor.main

# 5. 启动仪表盘（另一个终端）
streamlit run usd_liquidity_monitor/dashboard/app.py
# 浏览器打开 http://localhost:8501
```

## API Keys 获取

| 服务 | 用途 | 免费额度 | 链接 |
|---|---|---|---|
| **Polygon.io** | ETF 分钟级 tick | Starter Tier $29/月起（含 realtime） | https://polygon.io/pricing |
| **FRED** | 准备金周频数据 | 完全免费 | https://fred.stlouisfed.org/docs/api/api_key.html |
| **NY Fed Markets** | RRP/SRP 操作数据 | 无需 key，完全开放 | https://markets.newyorkfed.org/static/docs/markets-api.html |
| **Fiscal Data** | TGA 余额 | 无需 key | https://fiscaldata.treasury.gov/api-documentation/ |
| **Telegram Bot** | 告警出口 | 免费 | @BotFather |

**关于 Polygon tier 选择**：Starter Tier（$29/月）提供 ETF 实时分钟 bars，够本项目用。要上 SOFR/ZQ 期货 tick 数据才需要升级到 Advanced 或切换到 Databento。

## 部署到 VPS（生产环境）

```bash
# 拷贝到 /opt/
sudo mkdir -p /opt/usd-liquidity-monitor
sudo chown gavin:gavin /opt/usd-liquidity-monitor
scp -r ./* gavin@server:/opt/usd-liquidity-monitor/

# 安装依赖
cd /opt/usd-liquidity-monitor
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 配置环境变量（不要 commit .env）
cp .env.example .env
nano .env

# 安装 systemd 服务
sudo cp deploy/usd-liquidity-monitor.service /etc/systemd/system/
sudo cp deploy/usd-liquidity-dashboard.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now usd-liquidity-monitor
sudo systemctl enable --now usd-liquidity-dashboard

# 查看日志
journalctl -u usd-liquidity-monitor -f
```

建议配合 Caddy / Nginx 反代 + Cloudflare Access 做仪表盘的访问控制（仪表盘默认只 bind 127.0.0.1）。

## 告警等级

| Level | Emoji | 触发条件 |
|---|---|---|
| **CRITICAL** | 🚨🔴 | SRP 任何非零 acceptance |
| **HIGH** | 🟠 | \|ΔTGA 1d\| > 50bn；ΔRRP 1d < −50bn；Net Liq 7d-EWMA 斜率由正转负 < −20 bn/day |
| **MEDIUM** | 🟡 | Layer-3 代理 z-score > 3σ；\|ΔReserves w/w\| > 100bn |
| **INFO** | 🔵 | 日常心跳 / 系统状态 |

## 已知陷阱（实战踩过的坑）

1. **Fiscal Data TGA `close_today_bal` 永远是字符串 `"null"`**（2022-04-18 起）。真实值在 `account_type == "Treasury General Account (TGA) Closing Balance"` 行的 `open_today_bal` 字段。单位是百万美元。`collectors/tga.py` 已处理。

2. **NY Fed RRP API 在非工作日返回空列表**。`collectors/rrp.py` 会 gracefully 返回 `None`，不写空快照。

3. **WRESBAL 偶尔有 `"."` 表示缺失值**。`collectors/reserves.py` 会过滤掉。

4. **Polygon 需要 realtime 订阅**。Starter Tier 及以上才有实时分钟 bar。如果你只有免费 tier，程序不会 crash 但 Layer-3 不会有数据。可以用 `use_delayed=True` 切到 15 分钟延迟通道（精度够用于监控）。

5. **SRP 从 2025-12-11 起改为 full allotment 无限额度**，所以非零使用量从 "operational test" 转为 "真实压力信号"。阈值设为 `> 0` 即 CRITICAL。

## 扩展方向

- **Layer-3 futures tick**：加一个 `proxy/databento_stream.py`，订阅 `GLBX.MDP3` 数据集的 SR3 / ZQ 连续合约
- **Regime Detection**：用 `scikit-learn` 或 `hmmlearn` 把 net liquidity + proxy stress 喂进 HMM，输出 Abundant / Ample / Scarce / Crisis 制度概率
- **SOFR-IORB spread**：FRED 上的 `SOFR` + `IORB` 两个系列，加一个 collector 算两者差
- **Primary Dealer 持仓**：NY Fed 提供周频的 `https://markets.newyorkfed.org/api/pd/get/all/timeseries.json`
- **Historical Backtest**：`stress_score > X` 之后 5/10/20 日 SPY 条件胜率

## 维护

```bash
# 查看数据目录
ls -lh data/raw/

# 清理 30 天前的 proxy 分钟数据（可选）
find data/raw/proxy -name "*.parquet" -mtime +30 -delete

# 手动触发一次所有 collector 的轮询
python -c "
import asyncio
from usd_liquidity_monitor.main import main
# 或者单独触发某个 collector
"
```

---

**系统目标**：不是每分钟告诉你池子里有多少水（数据源不支持），而是在每分钟的代理信号里捕捉到"即将出现压力"的第一个线索，然后在原生数据的下一次发布窗口**确认或否定**这个线索。

**两层共振时的告警是最高价值的信号。**
