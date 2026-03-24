"""Build the latest price Gold dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def select_latest_prices(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Select the latest trade price per symbol."""
    latest_by_symbol: dict[str, dict[str, Any]] = {}

    for row in rows:
        symbol = row["symbol"]
        current = latest_by_symbol.get(symbol)
        if current is None or row["trade_time"] > current["trade_time"]:
            latest_by_symbol[symbol] = row

    return [
        {
            "symbol": symbol,
            "latest_price": record["price"],
            "trade_time": record["trade_time"],
            "source": record["source"],
        }
        for symbol, record in sorted(latest_by_symbol.items())
    ]


def build_latest_price_dataframe(trades_dataframe: DataFrame) -> DataFrame:
    """Build the latest price Gold dataset from a Spark trades DataFrame."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    latest_trade_window = Window.partitionBy("symbol").orderBy(
        F.col("trade_time").desc(),
        F.col("event_time").desc(),
        F.col("ingested_at").desc(),
    )

    return (
        trades_dataframe.withColumn("row_number", F.row_number().over(latest_trade_window))
        .where(F.col("row_number") == 1)
        .select("symbol", F.col("price").alias("latest_price"), "trade_time", "source")
    )
