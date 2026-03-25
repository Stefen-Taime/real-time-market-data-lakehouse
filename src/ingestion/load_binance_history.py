"""Historical backfill helpers for Binance market data."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib import parse, request

from src.ingestion.parse_raw_payloads import serialize_payload, timestamp_ms_to_iso8601, utc_now_iso
from src.transforms.bronze_to_silver_klines import transform_kline_batch
from src.utils.lakehouse_io import build_storage_path, build_table_name, ensure_schema_exists, write_dataframe

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def parse_historical_kline_row(
    symbol: str,
    interval: str,
    row: list[Any],
    source: str = "binance_history",
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Normalize a historical Binance kline row into a Bronze-ready record."""
    if len(row) < 11:
        raise ValueError("Historical kline rows must contain at least 11 columns.")

    record_ingested_at = ingested_at or utc_now_iso()

    return {
        "source": source,
        "event_type": "kline_history",
        "symbol": symbol,
        "interval": interval,
        "open_time": timestamp_ms_to_iso8601(row[0]),
        "close_time": timestamp_ms_to_iso8601(row[6]),
        "open": str(row[1]),
        "high": str(row[2]),
        "low": str(row[3]),
        "close": str(row[4]),
        "volume": str(row[5]),
        "quote_volume": str(row[7]),
        "trade_count": int(row[8]),
        "is_closed": True,
        "event_time": timestamp_ms_to_iso8601(row[6]),
        "ingested_at": record_ingested_at,
        "raw_payload": serialize_payload(row),
    }


def parse_history_group(group: dict[str, Any], ingested_at: str | None = None) -> list[dict[str, Any]]:
    """Parse a history group keyed by symbol and interval."""
    if "symbol" not in group or "klines" not in group:
        raise ValueError("History payloads must include 'symbol' and 'klines'.")

    symbol = group["symbol"]
    interval = group.get("interval", "1m")
    source = group.get("source", "binance_history")

    return [
        parse_historical_kline_row(symbol=symbol, interval=interval, row=row, source=source, ingested_at=ingested_at)
        for row in group["klines"]
    ]


def load_history_file(path: str | Path, ingested_at: str | None = None) -> list[dict[str, Any]]:
    """Load and normalize a history fixture file."""
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    groups = payload if isinstance(payload, list) else [payload]
    rows: list[dict[str, Any]] = []
    for group in groups:
        rows.extend(parse_history_group(group, ingested_at=ingested_at))

    return rows


def fetch_recent_klines(
    *,
    symbol: str,
    interval: str,
    limit: int = 30,
    base_url: str = "https://api.binance.com",
    timeout_seconds: float = 15.0,
    ingested_at: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch recent klines from the Binance REST API and normalize them into Bronze records."""
    query_string = parse.urlencode(
        {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": int(limit),
        }
    )
    endpoint = f"{base_url.rstrip('/')}/api/v3/klines?{query_string}"
    http_request = request.Request(
        endpoint,
        headers={"User-Agent": "real-time-market-data-lakehouse/1.0"},
        method="GET",
    )
    with request.urlopen(http_request, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))

    return [
        parse_historical_kline_row(
            symbol=symbol.upper(),
            interval=interval,
            row=row,
            source="binance_rest",
            ingested_at=ingested_at,
        )
        for row in payload
    ]


def fetch_recent_history_records(
    *,
    symbols: list[str],
    interval: str,
    limit: int = 30,
    base_url: str = "https://api.binance.com",
    timeout_seconds: float = 15.0,
    ingested_at: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch recent klines for a symbol list and normalize them into Bronze records."""
    records: list[dict[str, Any]] = []
    for symbol in symbols:
        records.extend(
            fetch_recent_klines(
                symbol=symbol,
                interval=interval,
                limit=limit,
                base_url=base_url,
                timeout_seconds=timeout_seconds,
                ingested_at=ingested_at,
            )
        )
    return records


def _parse_iso8601_to_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def create_bronze_history_dataframe(spark: SparkSession, records: list[dict[str, Any]]) -> DataFrame:
    """Create a Spark DataFrame for Bronze historical klines."""
    from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType, TimestampType

    prepared_records = []
    for record in records:
        prepared_record = record.copy()
        for timestamp_field in ("open_time", "close_time", "event_time", "ingested_at"):
            prepared_record[timestamp_field] = _parse_iso8601_to_datetime(prepared_record.get(timestamp_field))
        prepared_records.append(prepared_record)

    schema = StructType(
        [
            StructField("source", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("symbol", StringType(), False),
            StructField("interval", StringType(), False),
            StructField("open_time", TimestampType(), False),
            StructField("close_time", TimestampType(), False),
            StructField("open", StringType(), False),
            StructField("high", StringType(), False),
            StructField("low", StringType(), False),
            StructField("close", StringType(), False),
            StructField("volume", StringType(), False),
            StructField("quote_volume", StringType(), True),
            StructField("trade_count", IntegerType(), False),
            StructField("is_closed", BooleanType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("ingested_at", TimestampType(), False),
            StructField("raw_payload", StringType(), False),
        ]
    )

    return spark.createDataFrame(prepared_records, schema=schema)


def create_silver_history_dataframe(spark: SparkSession, records: list[dict[str, Any]]) -> DataFrame:
    """Create a Spark DataFrame for Silver one-minute klines."""
    from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

    schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("interval", StringType(), False),
            StructField("open_time", TimestampType(), False),
            StructField("close_time", TimestampType(), False),
            StructField("open", DoubleType(), False),
            StructField("high", DoubleType(), False),
            StructField("low", DoubleType(), False),
            StructField("close", DoubleType(), False),
            StructField("volume", DoubleType(), False),
            StructField("quote_volume", DoubleType(), True),
            StructField("trade_count", IntegerType(), False),
            StructField("is_closed", BooleanType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("ingested_at", TimestampType(), False),
            StructField("source", StringType(), False),
            StructField("raw_payload", StringType(), True),
        ]
    )

    return spark.createDataFrame(records, schema=schema)
def run_history_backfill(
    spark: SparkSession,
    *,
    input_path: str | Path,
    catalog: str | None,
    bronze_schema: str,
    silver_schema: str,
    bronze_table: str,
    silver_table: str,
    base_output_path: str | None,
    table_format: str = "delta",
    write_mode: str = "overwrite",
    register_tables: bool = False,
    write_silver: bool = True,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Load historical klines, transform them, and write Bronze/Silver datasets."""
    bronze_records = load_history_file(input_path, ingested_at=ingested_at)
    bronze_dataframe = create_bronze_history_dataframe(spark, bronze_records)
    bronze_table_name = build_table_name(catalog, bronze_schema, bronze_table) if register_tables else None
    bronze_output_path = build_storage_path(base_output_path, "bronze", bronze_table)

    if register_tables:
        ensure_schema_exists(spark, bronze_schema, catalog=catalog)

    write_dataframe(
        bronze_dataframe,
        table_format=table_format,
        mode=write_mode,
        table_name=bronze_table_name,
        output_path=bronze_output_path,
        register_table=register_tables,
    )

    silver_records: list[dict[str, Any]] = []
    silver_table_name: str | None = None
    silver_output_path: str | None = None
    if write_silver:
        silver_records = transform_kline_batch(bronze_records)
        silver_dataframe = create_silver_history_dataframe(spark, silver_records)
        silver_table_name = build_table_name(catalog, silver_schema, silver_table) if register_tables else None
        silver_output_path = build_storage_path(base_output_path, "silver", silver_table)

        if register_tables:
            ensure_schema_exists(spark, silver_schema, catalog=catalog)

        write_dataframe(
            silver_dataframe,
            table_format=table_format,
            mode=write_mode,
            table_name=silver_table_name,
            output_path=silver_output_path,
            register_table=register_tables,
        )

    return {
        "input_path": str(input_path),
        "bronze_record_count": len(bronze_records),
        "silver_record_count": len(silver_records),
        "bronze_table": bronze_table_name,
        "silver_table": silver_table_name,
        "bronze_output_path": bronze_output_path,
        "silver_output_path": silver_output_path,
        "table_format": table_format,
        "write_mode": write_mode,
        "register_tables": register_tables,
        "write_silver": write_silver,
    }


def main(argv: list[str] | None = None) -> None:
    """CLI entrypoint for local backfill validation."""
    parser = argparse.ArgumentParser(description="Normalize Binance historical klines into Bronze records.")
    parser.add_argument("--input-path", required=True, help="Path to a JSON fixture containing historical klines.")
    args = parser.parse_args(argv)

    records = load_history_file(args.input_path)
    print(json.dumps({"record_count": len(records), "symbols": sorted({record["symbol"] for record in records})}, indent=2))
