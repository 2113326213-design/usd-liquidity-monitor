# Data completeness audit

_Generated: 2026-04-17T10:56:24.504441+00:00_

## Per-collector gap analysis

A row in the table is one parquet file. `max_gap_days` > `expected_max_gap_days` is the warning signal.

| Collector | Rows | Range | Median gap | Max gap | Expected | Flag |
|---|---|---|---|---|---|---|
| tga | 1,252 | 2021-04-19 → 2026-04-15 | 1d | 4d | ≤ 5d | 🟢 |
| rrp | 1,247 | 2021-04-19 → 2026-04-16 | 1d | 4d | ≤ 5d | 🟢 |
| srp | 501 | 2025-02-20 → 2026-04-16 | 1d | 4d | ≤ 5d | 🟢 |
| reserves | 261 | 2021-04-21 → 2026-04-15 | 7d | 7d | ≤ 14d | 🟢 |
| sofr_iorb | 1,247 | 2021-04-19 → 2026-04-17 | 1d | 4d | ≤ 5d | 🟢 |
| auction_tail | 178 | 2011-07-14 → 2026-04-09 | 29d | 36d | ≤ 90d | 🟢 |
| net_liquidity | 1,822 | 2021-04-21 → 2026-04-16 | 1d | 1d | ≤ 3d | 🟢 |

## Top 10 largest gaps per collector


## Cross-source checks (diff deployed endpoint vs. alternative)

### TGA: `operating_cash_balance` vs `dts_table_1`
- Status: error
- Reason: Client error '404 Not Found' for url 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/dts_table_1?filter=record_date%3Agte%3A2026-02-16%2Crecord_date%3Alte%3A2026-04-17&fields=record_date%2Caccount_type%2Cclose_today_bal&page%5Bsize%5D=10000&format=json'
For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/404

### RRP: deployed parquet vs `all/results/last/500.json` (regression check)
- Deployed rows: 1247
- Rich endpoint rows: 500
- Agreement: **39.5%**
- ⚠️ Operation IDs ONLY in rich endpoint (deployed missing): `RP 012425 27, RP 022725 99, RP 050824 99, RP 101624 1, RP 110525 26`

---

## How to interpret

- 🟢 max gap within expected cadence → no evidence of missing data
- 🟡 max gap 1-2× expected → suspicious, investigate (might be holiday, might be real gap)
- 🔴 max gap >2× expected → likely real coverage gap, re-run backfill
- For cross-source checks: agreement < 100% → some dates/IDs missing from one endpoint. Which endpoint is 'right' depends on the data source. For NY Fed RRP, the rich `all/results/last/N` endpoint is the superset.