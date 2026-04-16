-- Run once against TimescaleDB **after** the API has created `liquidity_snapshots` (SQLAlchemy metadata).
-- psql "$DATABASE_URL" -f scripts/timescale_hypertable.sql

CREATE EXTENSION IF NOT EXISTS timescaledb;

SELECT public.create_hypertable(
    'liquidity_snapshots',
    'ts_utc',
    if_not_exists => TRUE
);

-- Example continuous aggregate (hourly mean net + RRP); refresh policy is site-specific.
-- CREATE MATERIALIZED VIEW liquidity_hourly
-- WITH (timescaledb.continuous) AS
-- SELECT time_bucket('1 hour', ts_utc) AS bucket,
--        avg(net_liquidity_bn) AS net_mean_bn,
--        avg(rrp_bn) AS rrp_mean_bn
-- FROM liquidity_snapshots
-- GROUP BY 1;
-- SELECT add_continuous_aggregate_policy('liquidity_hourly', ...);
