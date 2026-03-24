CREATE TABLE IF NOT EXISTS bronze.trades_live_raw (
  source STRING,
  event_type STRING,
  symbol STRING,
  trade_id BIGINT,
  price STRING,
  quantity STRING,
  trade_time TIMESTAMP,
  event_time TIMESTAMP,
  buyer_order_id BIGINT,
  seller_order_id BIGINT,
  is_market_maker BOOLEAN,
  ingested_at TIMESTAMP,
  raw_payload STRING
);

CREATE TABLE IF NOT EXISTS bronze.klines_history_raw (
  source STRING,
  event_type STRING,
  symbol STRING,
  interval STRING,
  open_time TIMESTAMP,
  close_time TIMESTAMP,
  open STRING,
  high STRING,
  low STRING,
  close STRING,
  volume STRING,
  quote_volume STRING,
  trade_count INT,
  is_closed BOOLEAN,
  event_time TIMESTAMP,
  ingested_at TIMESTAMP,
  raw_payload STRING
);

CREATE TABLE IF NOT EXISTS bronze.klines_live_raw (
  source STRING,
  event_type STRING,
  symbol STRING,
  interval STRING,
  open_time TIMESTAMP,
  close_time TIMESTAMP,
  open STRING,
  high STRING,
  low STRING,
  close STRING,
  volume STRING,
  quote_volume STRING,
  trade_count INT,
  is_closed BOOLEAN,
  event_time TIMESTAMP,
  ingested_at TIMESTAMP,
  raw_payload STRING
);
