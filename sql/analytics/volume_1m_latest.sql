SELECT symbol, window_start, volume_1m, notional_1m, trade_count
FROM gold.volume_1m
ORDER BY window_start DESC, symbol ASC;
