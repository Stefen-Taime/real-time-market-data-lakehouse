SELECT symbol, window_start, window_end, volatility_5m
FROM gold.volatility_5m
ORDER BY window_start DESC;
