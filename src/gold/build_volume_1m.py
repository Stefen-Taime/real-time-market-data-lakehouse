"""Build the one-minute volume Gold dataset."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _minute_floor(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def aggregate_volume_1m(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate trade volume and notional per symbol and minute."""
    aggregated: dict[tuple[str, datetime], dict[str, Any]] = {}

    for row in rows:
        window_start = _minute_floor(row["trade_time"])
        key = (row["symbol"], window_start)
        bucket = aggregated.setdefault(
            key,
            {
                "symbol": row["symbol"],
                "window_start": window_start,
                "volume_1m": 0.0,
                "notional_1m": 0.0,
                "trade_count": 0,
            },
        )
        bucket["volume_1m"] += row["quantity"]
        bucket["notional_1m"] += row["notional"]
        bucket["trade_count"] += 1

    return [
        {
            **record,
            "volume_1m": round(record["volume_1m"], 8),
            "notional_1m": round(record["notional_1m"], 8),
        }
        for _, record in sorted(aggregated.items(), key=lambda item: (item[0][0], item[0][1]))
    ]


def build_volume_1m_dataframe(trades_dataframe: DataFrame) -> DataFrame:
    """Build one-minute volume and notional aggregates from a Spark trades DataFrame."""
    from pyspark.sql import functions as F

    return (
        trades_dataframe.withColumn("window_start", F.date_trunc("minute", F.col("trade_time")))
        .groupBy("symbol", "window_start")
        .agg(
            F.round(F.sum("quantity"), 8).alias("volume_1m"),
            F.round(F.sum("notional"), 8).alias("notional_1m"),
            F.count(F.lit(1)).cast("int").alias("trade_count"),
        )
        .orderBy("symbol", "window_start")
    )
