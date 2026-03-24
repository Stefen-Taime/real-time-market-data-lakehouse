SELECT symbol, window_start, window_end, start_price, end_price, move_pct
FROM gold.top_movers
ORDER BY ABS(move_pct) DESC;
