"""Transform raw kline events into normalized Silver records."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _parse_utc_timestamp(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def transform_kline_record(record: dict[str, Any]) -> dict[str, Any]:
    """Cast and normalize a Bronze kline record into a Silver record."""
    required_fields = ("symbol", "interval", "open_time", "close_time", "open", "high", "low", "close", "volume")
    missing_fields = [field for field in required_fields if field not in record]
    if missing_fields:
        raise ValueError(f"Missing Bronze kline fields: {', '.join(missing_fields)}")

    return {
        "symbol": str(record["symbol"]),
        "interval": str(record["interval"]),
        "open_time": _parse_utc_timestamp(record["open_time"]),
        "close_time": _parse_utc_timestamp(record["close_time"]),
        "open": float(record["open"]),
        "high": float(record["high"]),
        "low": float(record["low"]),
        "close": float(record["close"]),
        "volume": float(record["volume"]),
        "quote_volume": float(record["quote_volume"]) if record.get("quote_volume") is not None else None,
        "trade_count": int(record["trade_count"]) if record.get("trade_count") is not None else 0,
        "is_closed": bool(record.get("is_closed", True)),
        "event_time": _parse_utc_timestamp(record.get("event_time")) or _parse_utc_timestamp(record["close_time"]),
        "ingested_at": _parse_utc_timestamp(record.get("ingested_at")) or _parse_utc_timestamp(record["close_time"]),
        "source": record.get("source", "binance"),
        "raw_payload": record.get("raw_payload"),
    }


def deduplicate_kline_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate klines by symbol, interval and open time."""
    deduplicated: dict[tuple[str, str, datetime], dict[str, Any]] = {}

    for record in records:
        key = (record["symbol"], record["interval"], record["open_time"])
        current = deduplicated.get(key)
        if current is None or (record["event_time"], record["ingested_at"]) > (current["event_time"], current["ingested_at"]):
            deduplicated[key] = record

    return sorted(deduplicated.values(), key=lambda row: (row["symbol"], row["interval"], row["open_time"]))


def transform_kline_batch(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize and deduplicate a batch of Bronze kline records."""
    transformed_records = [transform_kline_record(record) for record in records]
    return deduplicate_kline_records(transformed_records)


def build_silver_klines_dataframe(bronze_klines_dataframe: DataFrame) -> DataFrame:
    """Build a Silver klines DataFrame from Bronze kline records."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    normalized_klines = (
        bronze_klines_dataframe.select(
            F.col("symbol").cast("string").alias("symbol"),
            F.col("interval").cast("string").alias("interval"),
            F.col("open_time").alias("open_time"),
            F.col("close_time").alias("close_time"),
            F.col("open").cast("double").alias("open"),
            F.col("high").cast("double").alias("high"),
            F.col("low").cast("double").alias("low"),
            F.col("close").cast("double").alias("close"),
            F.col("volume").cast("double").alias("volume"),
            F.col("quote_volume").cast("double").alias("quote_volume"),
            F.col("trade_count").cast("int").alias("trade_count"),
            F.col("is_closed").cast("boolean").alias("is_closed"),
            F.col("event_time").alias("event_time"),
            F.col("ingested_at").alias("ingested_at"),
            F.col("source").cast("string").alias("source"),
            F.col("raw_payload").cast("string").alias("raw_payload"),
        )
        .where(
            F.col("symbol").isNotNull()
            & F.col("interval").isNotNull()
            & F.col("open_time").isNotNull()
            & F.col("close_time").isNotNull()
            & F.col("open").isNotNull()
            & F.col("high").isNotNull()
            & F.col("low").isNotNull()
            & F.col("close").isNotNull()
            & F.col("volume").isNotNull()
            & F.col("is_closed").eqNullSafe(F.lit(True))
        )
    )

    deduplication_window = Window.partitionBy("symbol", "interval", "open_time").orderBy(
        F.col("event_time").desc(),
        F.col("ingested_at").desc(),
    )

    return (
        normalized_klines.withColumn("row_number", F.row_number().over(deduplication_window))
        .where(F.col("row_number") == 1)
        .drop("row_number")
        .orderBy("symbol", "interval", "open_time")
    )
