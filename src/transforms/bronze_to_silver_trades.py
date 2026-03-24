"""Transform raw trade events into normalized Silver records."""

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


def transform_trade_record(record: dict[str, Any]) -> dict[str, Any]:
    """Cast and normalize a Bronze trade record into a Silver record."""
    required_fields = ("symbol", "trade_id", "price", "quantity", "trade_time")
    missing_fields = [field for field in required_fields if field not in record]
    if missing_fields:
        raise ValueError(f"Missing Bronze trade fields: {', '.join(missing_fields)}")

    price = float(record["price"])
    quantity = float(record["quantity"])
    trade_time = _parse_utc_timestamp(record["trade_time"])
    event_time = _parse_utc_timestamp(record.get("event_time")) or trade_time
    ingested_at = _parse_utc_timestamp(record.get("ingested_at")) or event_time

    return {
        "symbol": str(record["symbol"]),
        "trade_id": int(record["trade_id"]),
        "price": price,
        "quantity": quantity,
        "trade_time": trade_time,
        "event_time": event_time,
        "ingested_at": ingested_at,
        "buyer_order_id": int(record["buyer_order_id"]) if record.get("buyer_order_id") is not None else None,
        "seller_order_id": int(record["seller_order_id"]) if record.get("seller_order_id") is not None else None,
        "is_market_maker": bool(record.get("is_market_maker", False)),
        "source": record.get("source", "binance"),
        "notional": round(price * quantity, 8),
        "raw_payload": record.get("raw_payload"),
    }


def deduplicate_trade_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate trades by symbol and trade id, keeping the latest event."""
    deduplicated: dict[tuple[str, int], dict[str, Any]] = {}

    for record in records:
        key = (record["symbol"], record["trade_id"])
        current = deduplicated.get(key)
        if current is None or (record["event_time"], record["ingested_at"]) > (current["event_time"], current["ingested_at"]):
            deduplicated[key] = record

    return sorted(deduplicated.values(), key=lambda row: (row["symbol"], row["trade_time"], row["trade_id"]))


def transform_trade_batch(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize and deduplicate a batch of Bronze trade records."""
    transformed_records = [transform_trade_record(record) for record in records]
    return deduplicate_trade_records(transformed_records)


def build_silver_trades_dataframe(bronze_trades_dataframe: DataFrame) -> DataFrame:
    """Build a Silver trades DataFrame from Bronze trade records."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    normalized_trades = (
        bronze_trades_dataframe.select(
            F.col("symbol").cast("string").alias("symbol"),
            F.col("trade_id").cast("long").alias("trade_id"),
            F.col("price").cast("double").alias("price"),
            F.col("quantity").cast("double").alias("quantity"),
            F.col("trade_time").alias("trade_time"),
            F.col("event_time").alias("event_time"),
            F.col("ingested_at").alias("ingested_at"),
            F.col("buyer_order_id").cast("long").alias("buyer_order_id"),
            F.col("seller_order_id").cast("long").alias("seller_order_id"),
            F.col("is_market_maker").cast("boolean").alias("is_market_maker"),
            F.col("source").cast("string").alias("source"),
            F.round(F.col("price").cast("double") * F.col("quantity").cast("double"), 8).alias("notional"),
            F.col("raw_payload").cast("string").alias("raw_payload"),
        )
        .where(
            F.col("symbol").isNotNull()
            & F.col("trade_id").isNotNull()
            & F.col("price").isNotNull()
            & F.col("quantity").isNotNull()
            & F.col("trade_time").isNotNull()
        )
    )

    deduplication_window = Window.partitionBy("symbol", "trade_id").orderBy(
        F.col("event_time").desc(),
        F.col("ingested_at").desc(),
    )

    return (
        normalized_trades.withColumn("row_number", F.row_number().over(deduplication_window))
        .where(F.col("row_number") == 1)
        .drop("row_number")
        .orderBy("symbol", "trade_time", "trade_id")
    )
