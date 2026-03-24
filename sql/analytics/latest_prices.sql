SELECT symbol, latest_price, trade_time, source
FROM gold.latest_price
ORDER BY trade_time DESC;
