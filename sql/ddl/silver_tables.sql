CREATE TABLE IF NOT EXISTS silver.trades (
  symbol STRING,
  trade_id BIGINT,
  price DOUBLE,
  quantity DOUBLE,
  trade_time TIMESTAMP,
  event_time TIMESTAMP,
  ingested_at TIMESTAMP,
  buyer_order_id BIGINT,
  seller_order_id BIGINT,
  is_market_maker BOOLEAN,
  source STRING,
  notional DOUBLE
);

CREATE TABLE IF NOT EXISTS silver.klines_1m (
  symbol STRING,
  interval STRING,
  open_time TIMESTAMP,
  close_time TIMESTAMP,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume DOUBLE,
  quote_volume DOUBLE,
  trade_count INT,
  is_closed BOOLEAN,
  event_time TIMESTAMP,
  ingested_at TIMESTAMP,
  source STRING
);
