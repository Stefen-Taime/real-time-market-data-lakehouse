CREATE TABLE IF NOT EXISTS gold.latest_price (
  symbol STRING,
  latest_price DOUBLE,
  trade_time TIMESTAMP,
  source STRING
);

CREATE TABLE IF NOT EXISTS gold.volume_1m (
  symbol STRING,
  window_start TIMESTAMP,
  volume_1m DOUBLE,
  notional_1m DOUBLE,
  trade_count INT
);

CREATE TABLE IF NOT EXISTS gold.volatility_5m (
  symbol STRING,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  interval STRING,
  observation_count INT,
  volatility_5m DOUBLE
);

CREATE TABLE IF NOT EXISTS gold.top_movers (
  symbol STRING,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  start_price DOUBLE,
  end_price DOUBLE,
  move_pct DOUBLE
);
