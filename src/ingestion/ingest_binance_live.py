"""Streaming ingestion helpers for Binance live market data."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib import parse, request

from src.ingestion.load_binance_history import create_bronze_history_dataframe, create_silver_history_dataframe
from src.ingestion.parse_raw_payloads import parse_market_payload, serialize_payload, timestamp_ms_to_iso8601, utc_now_iso
from src.transforms.bronze_to_silver_klines import transform_kline_batch
from src.transforms.bronze_to_silver_trades import transform_trade_batch
from src.utils.lakehouse_io import build_storage_path, build_table_name, ensure_schema_exists, write_dataframe

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def normalize_stream_type(stream_type: str, *, kline_interval: str) -> str:
    """Normalize a configured stream type into a Binance stream suffix."""
    normalized = stream_type.strip().lower()
    if not normalized:
        raise ValueError("Stream types cannot be empty.")
    if normalized == "trade":
        return "trade"
    if normalized == "kline":
        return f"kline_{kline_interval.strip().lower()}"
    if normalized.startswith("kline_"):
        return normalized
    raise ValueError(f"Unsupported stream type: {stream_type}")


def build_stream_names(symbols: list[str], stream_types: list[str], *, kline_interval: str = "1m") -> list[str]:
    """Build Binance stream names from symbols and configured stream types."""
    unique_symbols = []
    for symbol in symbols:
        normalized = symbol.strip().lower()
        if normalized and normalized not in unique_symbols:
            unique_symbols.append(normalized)

    unique_stream_types = []
    for stream_type in stream_types:
        normalized_stream_type = normalize_stream_type(stream_type, kline_interval=kline_interval)
        if normalized_stream_type not in unique_stream_types:
            unique_stream_types.append(normalized_stream_type)

    return [f"{symbol}@{stream_type}" for symbol in unique_symbols for stream_type in unique_stream_types]


def build_trade_stream_names(symbols: list[str]) -> list[str]:
    """Build Binance trade stream names from a symbol list."""
    return build_stream_names(symbols, ["trade"])


def build_combined_stream_url(base_url: str, stream_names: list[str]) -> str:
    """Build a Binance combined stream URL."""
    if not stream_names:
        raise ValueError("At least one stream name is required.")
    return f"{base_url.rstrip('/')}/stream?streams={'/'.join(stream_names)}"


def load_live_file(path: str | Path, ingested_at: str | None = None) -> list[dict[str, Any]]:
    """Load and normalize a live fixture file containing Binance events."""
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    events = payload if isinstance(payload, list) else [payload]
    return [parse_market_payload(event, ingested_at=ingested_at) for event in events]


def fetch_recent_trade_records(
    *,
    symbols: list[str],
    limit: int = 200,
    base_url: str = "https://api.binance.com",
    timeout_seconds: float = 15.0,
    ingested_at: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch recent trades from Binance REST and normalize them into Bronze-ready records."""
    record_ingested_at = ingested_at or utc_now_iso()
    records: list[dict[str, Any]] = []

    for symbol in symbols:
        query_string = parse.urlencode({"symbol": symbol.upper(), "limit": int(limit)})
        endpoint = f"{base_url.rstrip('/')}/api/v3/trades?{query_string}"
        http_request = request.Request(
            endpoint,
            headers={"User-Agent": "real-time-market-data-lakehouse/1.0"},
            method="GET",
        )
        with request.urlopen(http_request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))

        for trade in payload:
            records.append(
                {
                    "source": "binance_rest",
                    "event_type": "trade",
                    "symbol": symbol.upper(),
                    "trade_id": int(trade["id"]),
                    "price": str(trade["price"]),
                    "quantity": str(trade["qty"]),
                    "trade_time": timestamp_ms_to_iso8601(trade["time"]),
                    "event_time": timestamp_ms_to_iso8601(trade["time"]),
                    "buyer_order_id": None,
                    "seller_order_id": None,
                    "is_market_maker": bool(trade.get("isBuyerMaker", False)),
                    "ingested_at": record_ingested_at,
                    "raw_payload": serialize_payload(trade),
                }
            )

    return records


def filter_live_records(records: list[dict[str, Any]], event_type: str) -> list[dict[str, Any]]:
    """Filter normalized live records by event type."""
    return [record for record in records if record["event_type"] == event_type]


def collect_messages_from_socket(
    socket_connection: Any,
    *,
    source: str,
    max_messages: int | None,
    duration_seconds: int | None,
    receive_timeout_seconds: float,
) -> list[dict[str, Any]]:
    """Collect and normalize Binance stream messages from an open socket connection."""
    collected_records: list[dict[str, Any]] = []
    started_at = time.monotonic()

    while True:
        if max_messages is not None and len(collected_records) >= max_messages:
            break

        if duration_seconds is not None:
            elapsed = time.monotonic() - started_at
            remaining = duration_seconds - elapsed
            if remaining <= 0:
                break
            timeout = min(receive_timeout_seconds, remaining)
        else:
            timeout = receive_timeout_seconds

        if hasattr(socket_connection, "settimeout"):
            socket_connection.settimeout(timeout)

        try:
            message = socket_connection.recv()
        except Exception as exc:
            if exc.__class__.__name__ == "WebSocketTimeoutException" or isinstance(exc, TimeoutError):
                if duration_seconds is None:
                    break
                continue
            raise

        if not message:
            continue

        if isinstance(message, bytes):
            message = message.decode("utf-8")

        try:
            collected_records.append(parse_market_payload(message, source=source))
        except ValueError:
            continue

    return collected_records


def collect_binance_market_records(
    *,
    symbols: list[str],
    stream_types: list[str],
    kline_interval: str,
    websocket_base_url: str,
    source: str = "binance_ws",
    max_messages: int | None = None,
    duration_seconds: int | None = None,
    receive_timeout_seconds: float = 5.0,
    connect_retries: int = 3,
    retry_backoff_seconds: float = 2.0,
) -> tuple[list[dict[str, Any]], str, int]:
    """Collect live market records from Binance public WebSocket streams."""
    try:
        from websocket import create_connection
    except ImportError as exc:
        raise RuntimeError("websocket-client is required for Binance WebSocket ingestion.") from exc

    stream_names = build_stream_names(symbols, stream_types, kline_interval=kline_interval)
    websocket_url = build_combined_stream_url(websocket_base_url, stream_names)
    max_attempts = max(connect_retries, 1)
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        socket_connection = None
        try:
            socket_connection = create_connection(websocket_url, timeout=receive_timeout_seconds)
            records = collect_messages_from_socket(
                socket_connection,
                source=source,
                max_messages=max_messages,
                duration_seconds=duration_seconds,
                receive_timeout_seconds=receive_timeout_seconds,
            )
            return records, websocket_url, attempt
        except Exception as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            time.sleep(max(retry_backoff_seconds, 0.0) * attempt)
        finally:
            if socket_connection is not None:
                socket_connection.close()

    assert last_error is not None
    raise last_error


def collect_binance_trade_records(
    *,
    symbols: list[str],
    websocket_base_url: str,
    source: str = "binance_ws",
    max_messages: int | None = None,
    duration_seconds: int | None = None,
    receive_timeout_seconds: float = 5.0,
) -> tuple[list[dict[str, Any]], str, int]:
    """Backward-compatible wrapper for trade-only stream collection."""
    return collect_binance_market_records(
        symbols=symbols,
        stream_types=["trade"],
        kline_interval="1m",
        websocket_base_url=websocket_base_url,
        source=source,
        max_messages=max_messages,
        duration_seconds=duration_seconds,
        receive_timeout_seconds=receive_timeout_seconds,
    )


def load_live_records(
    *,
    source_mode: str,
    input_path: str | Path | None,
    symbols: list[str],
    stream_types: list[str],
    kline_interval: str,
    websocket_base_url: str,
    max_messages: int | None,
    duration_seconds: int | None,
    receive_timeout_seconds: float,
    connect_retries: int = 3,
    retry_backoff_seconds: float = 2.0,
    ingested_at: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Load live market records from either a fixture file or Binance WebSocket."""
    if source_mode == "fixture":
        if input_path is None:
            raise ValueError("input_path is required when source_mode='fixture'.")
        records = load_live_file(input_path, ingested_at=ingested_at)
        return records, {"source_mode": source_mode, "input_path": str(input_path)}

    if source_mode == "binance_ws":
        records, websocket_url, attempts = collect_binance_market_records(
            symbols=symbols,
            stream_types=stream_types,
            kline_interval=kline_interval,
            websocket_base_url=websocket_base_url,
            max_messages=max_messages,
            duration_seconds=duration_seconds,
            receive_timeout_seconds=receive_timeout_seconds,
            connect_retries=connect_retries,
            retry_backoff_seconds=retry_backoff_seconds,
        )
        return records, {
            "source_mode": source_mode,
            "symbols": symbols,
            "stream_types": stream_types,
            "kline_interval": kline_interval,
            "websocket_url": websocket_url,
            "max_messages": max_messages,
            "duration_seconds": duration_seconds,
            "connect_retries": connect_retries,
            "retry_backoff_seconds": retry_backoff_seconds,
            "connection_attempts": attempts,
        }

    raise ValueError(f"Unsupported source_mode: {source_mode}")


def _parse_iso8601_to_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def create_bronze_trade_dataframe(spark: SparkSession, records: list[dict[str, Any]]) -> DataFrame:
    """Create a Spark DataFrame for Bronze live trades."""
    from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType, TimestampType

    prepared_records = []
    for record in records:
        prepared_record = record.copy()
        for timestamp_field in ("trade_time", "event_time", "ingested_at"):
            prepared_record[timestamp_field] = _parse_iso8601_to_datetime(prepared_record.get(timestamp_field))
        prepared_records.append(prepared_record)

    schema = StructType(
        [
            StructField("source", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("symbol", StringType(), False),
            StructField("trade_id", LongType(), False),
            StructField("price", StringType(), False),
            StructField("quantity", StringType(), False),
            StructField("trade_time", TimestampType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("buyer_order_id", LongType(), True),
            StructField("seller_order_id", LongType(), True),
            StructField("is_market_maker", BooleanType(), False),
            StructField("ingested_at", TimestampType(), False),
            StructField("raw_payload", StringType(), False),
        ]
    )

    return spark.createDataFrame(prepared_records, schema=schema)


def create_silver_trade_dataframe(spark: SparkSession, records: list[dict[str, Any]]) -> DataFrame:
    """Create a Spark DataFrame for Silver normalized trades."""
    from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

    schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("trade_id", LongType(), False),
            StructField("price", DoubleType(), False),
            StructField("quantity", DoubleType(), False),
            StructField("trade_time", TimestampType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("ingested_at", TimestampType(), False),
            StructField("buyer_order_id", LongType(), True),
            StructField("seller_order_id", LongType(), True),
            StructField("is_market_maker", BooleanType(), False),
            StructField("source", StringType(), False),
            StructField("notional", DoubleType(), False),
            StructField("raw_payload", StringType(), True),
        ]
    )

    return spark.createDataFrame(records, schema=schema)


def summarize_event_counts(records: list[dict[str, Any]]) -> dict[str, int]:
    """Count events by type for reporting."""
    counts: dict[str, int] = {}
    for record in records:
        counts[record["event_type"]] = counts.get(record["event_type"], 0) + 1
    return counts


def run_live_market_ingestion(
    spark: SparkSession,
    *,
    source_mode: str,
    input_path: str | Path | None,
    symbols: list[str],
    stream_types: list[str],
    kline_interval: str,
    websocket_base_url: str,
    max_messages: int | None,
    duration_seconds: int | None,
    receive_timeout_seconds: float,
    catalog: str | None,
    bronze_schema: str,
    silver_schema: str,
    bronze_trades_table: str,
    bronze_klines_table: str,
    silver_trades_table: str,
    silver_klines_table: str,
    base_output_path: str | None,
    connect_retries: int = 3,
    retry_backoff_seconds: float = 2.0,
    table_format: str = "delta",
    write_mode: str = "append",
    register_tables: bool = False,
    write_silver: bool = True,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Load live events, split trades and klines, and write Bronze/Silver market datasets."""
    all_records, source_details = load_live_records(
        source_mode=source_mode,
        input_path=input_path,
        symbols=symbols,
        stream_types=stream_types,
        kline_interval=kline_interval,
        websocket_base_url=websocket_base_url,
        max_messages=max_messages,
        duration_seconds=duration_seconds,
        receive_timeout_seconds=receive_timeout_seconds,
        connect_retries=connect_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        ingested_at=ingested_at,
    )
    event_counts = summarize_event_counts(all_records)
    bronze_trade_records = filter_live_records(all_records, "trade")
    bronze_kline_records = filter_live_records(all_records, "kline")

    bronze_trades_dataframe = create_bronze_trade_dataframe(spark, bronze_trade_records)
    bronze_klines_dataframe = create_bronze_history_dataframe(spark, bronze_kline_records)
    bronze_trades_table_name = build_table_name(catalog, bronze_schema, bronze_trades_table) if register_tables else None
    bronze_klines_table_name = build_table_name(catalog, bronze_schema, bronze_klines_table) if register_tables else None
    bronze_trades_output_path = build_storage_path(base_output_path, "bronze", bronze_trades_table)
    bronze_klines_output_path = build_storage_path(base_output_path, "bronze", bronze_klines_table)

    if register_tables:
        ensure_schema_exists(spark, bronze_schema, catalog=catalog)

    write_dataframe(
        bronze_trades_dataframe,
        table_format=table_format,
        mode=write_mode,
        table_name=bronze_trades_table_name,
        output_path=bronze_trades_output_path,
        register_table=register_tables,
    )
    write_dataframe(
        bronze_klines_dataframe,
        table_format=table_format,
        mode=write_mode,
        table_name=bronze_klines_table_name,
        output_path=bronze_klines_output_path,
        register_table=register_tables,
    )

    silver_trade_records: list[dict[str, Any]] = []
    silver_kline_records: list[dict[str, Any]] = []
    silver_trades_table_name: str | None = None
    silver_klines_table_name: str | None = None
    silver_trades_output_path: str | None = None
    silver_klines_output_path: str | None = None
    if write_silver:
        silver_trade_records = transform_trade_batch(bronze_trade_records)
        silver_kline_records = transform_kline_batch(bronze_kline_records)
        silver_trades_dataframe = create_silver_trade_dataframe(spark, silver_trade_records)
        silver_klines_dataframe = create_silver_history_dataframe(spark, silver_kline_records)
        silver_trades_table_name = (
            build_table_name(catalog, silver_schema, silver_trades_table) if register_tables else None
        )
        silver_klines_table_name = (
            build_table_name(catalog, silver_schema, silver_klines_table) if register_tables else None
        )
        silver_trades_output_path = build_storage_path(base_output_path, "silver", silver_trades_table)
        silver_klines_output_path = build_storage_path(base_output_path, "silver", silver_klines_table)

        if register_tables:
            ensure_schema_exists(spark, silver_schema, catalog=catalog)

        write_dataframe(
            silver_trades_dataframe,
            table_format=table_format,
            mode=write_mode,
            table_name=silver_trades_table_name,
            output_path=silver_trades_output_path,
            register_table=register_tables,
        )
        write_dataframe(
            silver_klines_dataframe,
            table_format=table_format,
            mode=write_mode,
            table_name=silver_klines_table_name,
            output_path=silver_klines_output_path,
            register_table=register_tables,
        )

    return {
        **source_details,
        "event_counts": event_counts,
        "bronze_trade_record_count": len(bronze_trade_records),
        "bronze_kline_record_count": len(bronze_kline_records),
        "silver_trade_record_count": len(silver_trade_records),
        "silver_kline_record_count": len(silver_kline_records),
        "bronze_trades_table": bronze_trades_table_name,
        "bronze_klines_table": bronze_klines_table_name,
        "silver_trades_table": silver_trades_table_name,
        "silver_klines_table": silver_klines_table_name,
        "bronze_trades_output_path": bronze_trades_output_path,
        "bronze_klines_output_path": bronze_klines_output_path,
        "silver_trades_output_path": silver_trades_output_path,
        "silver_klines_output_path": silver_klines_output_path,
        "table_format": table_format,
        "write_mode": write_mode,
        "register_tables": register_tables,
        "write_silver": write_silver,
    }


def run_live_trade_ingestion(
    spark: SparkSession,
    *,
    source_mode: str,
    input_path: str | Path | None,
    symbols: list[str],
    websocket_base_url: str,
    max_messages: int | None,
    duration_seconds: int | None,
    receive_timeout_seconds: float,
    catalog: str | None,
    bronze_schema: str,
    silver_schema: str,
    bronze_table: str,
    silver_table: str,
    base_output_path: str | None,
    connect_retries: int = 3,
    retry_backoff_seconds: float = 2.0,
    table_format: str = "delta",
    write_mode: str = "append",
    register_tables: bool = False,
    write_silver: bool = True,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Backward-compatible wrapper for trade-only live ingestion."""
    return run_live_market_ingestion(
        spark,
        source_mode=source_mode,
        input_path=input_path,
        symbols=symbols,
        stream_types=["trade"],
        kline_interval="1m",
        websocket_base_url=websocket_base_url,
        max_messages=max_messages,
        duration_seconds=duration_seconds,
        receive_timeout_seconds=receive_timeout_seconds,
        connect_retries=connect_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        catalog=catalog,
        bronze_schema=bronze_schema,
        silver_schema=silver_schema,
        bronze_trades_table=bronze_table,
        bronze_klines_table="klines_live_raw",
        silver_trades_table=silver_table,
        silver_klines_table="klines_1m",
        base_output_path=base_output_path,
        table_format=table_format,
        write_mode=write_mode,
        register_tables=register_tables,
        write_silver=write_silver,
        ingested_at=ingested_at,
    )


def main(argv: list[str] | None = None) -> None:
    """CLI entrypoint for local live ingestion validation."""
    parser = argparse.ArgumentParser(description="Normalize Binance live events into Bronze records.")
    parser.add_argument("--source-mode", default="fixture", choices=["fixture", "binance_ws"])
    parser.add_argument("--input-path", help="Path to a JSON fixture containing live events.")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT", help="Comma-separated Binance symbols.")
    parser.add_argument("--stream-types", default="trade,kline", help="Comma-separated Binance stream types.")
    parser.add_argument("--kline-interval", default="1m", help="Kline interval used when stream type is 'kline'.")
    parser.add_argument("--websocket-base-url", default="wss://data-stream.binance.vision")
    parser.add_argument("--max-messages", type=int, default=20)
    parser.add_argument("--duration-seconds", type=int, default=None)
    parser.add_argument("--receive-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--connect-retries", type=int, default=3)
    parser.add_argument("--retry-backoff-seconds", type=float, default=2.0)
    args = parser.parse_args(argv)

    symbols = [symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()]
    stream_types = [stream_type.strip() for stream_type in args.stream_types.split(",") if stream_type.strip()]
    records, source_details = load_live_records(
        source_mode=args.source_mode,
        input_path=args.input_path,
        symbols=symbols,
        stream_types=stream_types,
        kline_interval=args.kline_interval,
        websocket_base_url=args.websocket_base_url,
        max_messages=args.max_messages,
        duration_seconds=args.duration_seconds,
        receive_timeout_seconds=args.receive_timeout_seconds,
        connect_retries=args.connect_retries,
        retry_backoff_seconds=args.retry_backoff_seconds,
    )
    counts = summarize_event_counts(records)

    print(json.dumps({**source_details, "record_count": len(records), "event_counts": counts}, indent=2))
