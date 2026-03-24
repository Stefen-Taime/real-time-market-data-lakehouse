"""Build the five-minute volatility Gold dataset."""

from __future__ import annotations

from collections import defaultdict
from statistics import pstdev
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def compute_volatility_5m(rows: list[dict[str, Any]], window_size: int = 5) -> list[dict[str, Any]]:
    """Compute rolling five-minute volatility from one-minute klines."""
    rows_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        rows_by_symbol[row["symbol"]].append(row)

    volatility_rows: list[dict[str, Any]] = []
    for symbol, symbol_rows in sorted(rows_by_symbol.items()):
        ordered_rows = sorted(symbol_rows, key=lambda row: row["open_time"])
        for index in range(window_size - 1, len(ordered_rows)):
            window = ordered_rows[index - window_size + 1 : index + 1]
            close_prices = [row["close"] for row in window]

            returns = []
            for previous_close, current_close in zip(close_prices, close_prices[1:]):
                if previous_close == 0:
                    continue
                returns.append((current_close / previous_close) - 1)

            if not returns:
                continue

            volatility_rows.append(
                {
                    "symbol": symbol,
                    "window_start": window[0]["open_time"],
                    "window_end": window[-1]["close_time"],
                    "interval": window[-1]["interval"],
                    "observation_count": len(window),
                    "volatility_5m": round(pstdev(returns), 8),
                }
            )

    return volatility_rows


def build_volatility_5m_dataframe(klines_dataframe: DataFrame, window_size: int = 5) -> DataFrame:
    """Build rolling volatility metrics from a Spark klines DataFrame."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ordered_window = Window.partitionBy("symbol").orderBy("open_time")
    rolling_window = ordered_window.rowsBetween(-(window_size - 1), 0)

    dataframe_with_returns = (
        klines_dataframe.withColumn("previous_close", F.lag("close").over(ordered_window))
        .withColumn(
            "return_pct",
            F.when(F.col("previous_close").isNull() | (F.col("previous_close") == 0), None).otherwise(
                (F.col("close") / F.col("previous_close")) - F.lit(1.0)
            ),
        )
        .withColumn("window_start", F.first("open_time", ignorenulls=True).over(rolling_window))
        .withColumn("window_end", F.last("close_time", ignorenulls=True).over(rolling_window))
        .withColumn("interval_value", F.last("interval", ignorenulls=True).over(rolling_window))
        .withColumn("observation_count", F.count(F.lit(1)).over(rolling_window))
        .withColumn("return_count", F.count("return_pct").over(rolling_window))
        .withColumn("volatility_5m", F.round(F.stddev_pop("return_pct").over(rolling_window), 8))
    )

    return (
        dataframe_with_returns.where((F.col("observation_count") == window_size) & (F.col("return_count") > 0))
        .select(
            "symbol",
            "window_start",
            "window_end",
            F.col("interval_value").alias("interval"),
            "observation_count",
            "volatility_5m",
        )
        .orderBy("symbol", "window_start")
    )
