SELECT 'gold_latest_price' AS dataset_name,
       COUNT(*) AS row_count,
       MAX(trade_time) AS latest_timestamp
FROM gold.latest_price
UNION ALL
SELECT 'gold_volume_1m' AS dataset_name,
       COUNT(*) AS row_count,
       MAX(window_start) AS latest_timestamp
FROM gold.volume_1m
UNION ALL
SELECT 'gold_volatility_5m' AS dataset_name,
       COUNT(*) AS row_count,
       MAX(window_end) AS latest_timestamp
FROM gold.volatility_5m
UNION ALL
SELECT 'gold_top_movers' AS dataset_name,
       COUNT(*) AS row_count,
       MAX(window_end) AS latest_timestamp
FROM gold.top_movers;
