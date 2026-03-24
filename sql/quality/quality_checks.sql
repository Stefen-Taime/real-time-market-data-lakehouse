-- Duplicate trades that should have been removed in Silver.
SELECT
  symbol,
  trade_id,
  COUNT(*) AS duplicate_count
FROM silver.trades
GROUP BY symbol, trade_id
HAVING COUNT(*) > 1;

-- Null-critical fields in Silver trades.
SELECT
  SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) AS null_symbol_count,
  SUM(CASE WHEN trade_id IS NULL THEN 1 ELSE 0 END) AS null_trade_id_count,
  SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS null_price_count,
  SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS null_quantity_count,
  SUM(CASE WHEN trade_time IS NULL THEN 1 ELSE 0 END) AS null_trade_time_count
FROM silver.trades;

-- Kline sanity checks for Silver.
SELECT
  symbol,
  interval,
  open_time,
  close_time,
  open,
  high,
  low,
  close,
  volume
FROM silver.klines_1m
WHERE open < 0
   OR high < 0
   OR low < 0
   OR close < 0
   OR volume < 0;

-- Latest Gold freshness snapshot.
SELECT
  MAX(trade_time) AS latest_trade_time,
  CURRENT_TIMESTAMP() AS checked_at
FROM gold.latest_price;
