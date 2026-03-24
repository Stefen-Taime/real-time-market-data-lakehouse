"""Build the top movers Gold dataset."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def build_top_movers(rows: list[dict[str, Any]], top_n: int = 10) -> list[dict[str, Any]]:
    """Rank symbols by absolute price move across the available kline window."""
    rows_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        rows_by_symbol[row["symbol"]].append(row)

    movers: list[dict[str, Any]] = []
    for symbol, symbol_rows in rows_by_symbol.items():
        ordered_rows = sorted(symbol_rows, key=lambda row: row["open_time"])
        first_row = ordered_rows[0]
        last_row = ordered_rows[-1]
        start_price = first_row["close"]
        end_price = last_row["close"]
        move_pct = ((end_price - start_price) / start_price) * 100 if start_price else 0.0

        movers.append(
            {
                "symbol": symbol,
                "window_start": first_row["open_time"],
                "window_end": last_row["close_time"],
                "start_price": start_price,
                "end_price": end_price,
                "move_pct": round(move_pct, 8),
            }
        )

    return sorted(movers, key=lambda row: (-abs(row["move_pct"]), row["symbol"]))[:top_n]


def build_top_movers_dataframe(klines_dataframe: DataFrame, top_n: int = 10) -> DataFrame:
    """Build top movers from a Spark klines DataFrame."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    full_symbol_window = Window.partitionBy("symbol").orderBy("open_time").rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing,
    )
    latest_row_window = Window.partitionBy("symbol").orderBy(F.col("open_time").desc(), F.col("close_time").desc())

    movers_dataframe = (
        klines_dataframe.withColumn("window_start", F.first("open_time", ignorenulls=True).over(full_symbol_window))
        .withColumn("window_end", F.last("close_time", ignorenulls=True).over(full_symbol_window))
        .withColumn("start_price", F.first("close", ignorenulls=True).over(full_symbol_window))
        .withColumn("end_price", F.last("close", ignorenulls=True).over(full_symbol_window))
        .withColumn(
            "move_pct",
            F.round(
                F.when(F.col("start_price") == 0, F.lit(0.0)).otherwise(
                    ((F.col("end_price") - F.col("start_price")) / F.col("start_price")) * F.lit(100.0)
                ),
                8,
            ),
        )
        .withColumn("row_number", F.row_number().over(latest_row_window))
        .where(F.col("row_number") == 1)
        .select("symbol", "window_start", "window_end", "start_price", "end_price", "move_pct")
        .orderBy(F.abs(F.col("move_pct")).desc(), F.col("symbol"))
    )

    return movers_dataframe.limit(top_n)
