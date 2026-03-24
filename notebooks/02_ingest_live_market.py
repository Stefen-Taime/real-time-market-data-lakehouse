# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Ingest Live Market

from pathlib import Path
import sys


def _resolve_project_root() -> Path:
    cwd = Path.cwd()
    if (cwd / "config").exists():
        return cwd
    if (cwd.parent / "config").exists():
        return cwd.parent
    return cwd


project_root = _resolve_project_root()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.ingestion.ingest_binance_live import run_live_market_ingestion
from src.utils.config_loader import load_project_config
from src.utils.notebook_runtime import build_runtime_context, emit_notebook_output
from src.utils.spark_session import get_spark_session


def _get_dbutils():
    try:
        return dbutils  # type: ignore[name-defined]
    except NameError:
        return None


def _to_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _to_optional_int(value: str) -> int | None:
    stripped = value.strip()
    return int(stripped) if stripped else None


def _to_float(value: str) -> float:
    return float(value.strip())


def _resolve_input_path(value: str, project_root: Path) -> str:
    if value.startswith(("dbfs:/", "/Volumes/", "/Workspace/")):
        return value
    candidate = Path(value)
    if candidate.is_absolute():
        return str(candidate)
    return str((project_root / candidate).resolve())


dbutils_handle = _get_dbutils()

default_env = "dev"
if dbutils_handle is not None:
    dbutils_handle.widgets.text("env", default_env)
    env = dbutils_handle.widgets.get("env")
else:
    env = default_env

config = load_project_config(project_root / "config", env)

default_input_path = config.get("paths", {}).get("live_input_path", "tests/fixtures/sample_trades.json")
default_delta_base_path = config.get("paths", {}).get("delta_base_path")
default_register_tables = str(config.get("databricks", {}).get("register_tables", False)).lower()
default_table_format = config.get("databricks", {}).get("table_format", "delta")
default_write_mode = config.get("databricks", {}).get("write_modes", {}).get("live", "append")
default_write_silver = "false"
default_source_mode = config.get("market_source", {}).get("live", {}).get("source_mode", "binance_ws")
default_websocket_base_url = config.get("market_source", {}).get("live", {}).get(
    "websocket_base_url", "wss://data-stream.binance.vision"
)
default_stream_types = ",".join(config.get("market_source", {}).get("live", {}).get("stream_types", ["trade", "kline"]))
default_kline_interval = config.get("market_source", {}).get("live", {}).get("kline_interval", "1m")
default_max_messages = str(config.get("market_source", {}).get("live", {}).get("max_messages", 200))
default_duration_seconds = str(config.get("market_source", {}).get("live", {}).get("duration_seconds", 60))
default_receive_timeout_seconds = str(
    config.get("market_source", {}).get("live", {}).get("receive_timeout_seconds", 5)
)
default_symbols = ",".join(config.get("symbols", []))

catalog = config["catalog"]
bronze_schema = config["schemas"]["bronze"]
silver_schema = config["schemas"]["silver"]
bronze_trades_table = config["tables"]["bronze"]["trades_live_raw"]
bronze_klines_table = config["tables"]["bronze"]["klines_live_raw"]
silver_trades_table = config["tables"]["silver"]["trades"]
silver_klines_table = config["tables"]["silver"]["klines_1m"]

if dbutils_handle is not None:
    dbutils_handle.widgets.text("source_mode", default_source_mode)
    dbutils_handle.widgets.text("input_path", default_input_path)
    dbutils_handle.widgets.text("symbols", default_symbols)
    dbutils_handle.widgets.text("stream_types", default_stream_types)
    dbutils_handle.widgets.text("kline_interval", default_kline_interval)
    dbutils_handle.widgets.text("websocket_base_url", default_websocket_base_url)
    dbutils_handle.widgets.text("max_messages", default_max_messages)
    dbutils_handle.widgets.text("duration_seconds", default_duration_seconds)
    dbutils_handle.widgets.text("receive_timeout_seconds", default_receive_timeout_seconds)
    dbutils_handle.widgets.text("delta_base_path", default_delta_base_path or "")
    dbutils_handle.widgets.text("register_tables", default_register_tables)
    dbutils_handle.widgets.text("table_format", default_table_format)
    dbutils_handle.widgets.text("write_mode", default_write_mode)
    dbutils_handle.widgets.text("write_silver", default_write_silver)

    source_mode = dbutils_handle.widgets.get("source_mode")
    input_path = _resolve_input_path(dbutils_handle.widgets.get("input_path"), project_root)
    symbols = [symbol.strip().upper() for symbol in dbutils_handle.widgets.get("symbols").split(",") if symbol.strip()]
    stream_types = [stream_type.strip() for stream_type in dbutils_handle.widgets.get("stream_types").split(",") if stream_type.strip()]
    kline_interval = dbutils_handle.widgets.get("kline_interval")
    websocket_base_url = dbutils_handle.widgets.get("websocket_base_url")
    max_messages = _to_optional_int(dbutils_handle.widgets.get("max_messages"))
    duration_seconds = _to_optional_int(dbutils_handle.widgets.get("duration_seconds"))
    receive_timeout_seconds = _to_float(dbutils_handle.widgets.get("receive_timeout_seconds"))
    delta_base_path = dbutils_handle.widgets.get("delta_base_path") or None
    register_tables = _to_bool(dbutils_handle.widgets.get("register_tables"))
    table_format = dbutils_handle.widgets.get("table_format")
    write_mode = dbutils_handle.widgets.get("write_mode")
    write_silver = _to_bool(dbutils_handle.widgets.get("write_silver"))
else:
    source_mode = default_source_mode
    input_path = _resolve_input_path(default_input_path, project_root)
    symbols = [symbol.strip().upper() for symbol in default_symbols.split(",") if symbol.strip()]
    stream_types = [stream_type.strip() for stream_type in default_stream_types.split(",") if stream_type.strip()]
    kline_interval = default_kline_interval
    websocket_base_url = default_websocket_base_url
    max_messages = _to_optional_int(default_max_messages)
    duration_seconds = _to_optional_int(default_duration_seconds)
    receive_timeout_seconds = _to_float(default_receive_timeout_seconds)
    delta_base_path = default_delta_base_path
    register_tables = _to_bool(default_register_tables)
    table_format = default_table_format
    write_mode = default_write_mode
    write_silver = _to_bool(default_write_silver)

spark_session = globals().get("spark") or get_spark_session(app_name="market-data-live-ingestion")
summary = run_live_market_ingestion(
    spark_session,
    source_mode=source_mode,
    input_path=input_path,
    symbols=symbols,
    stream_types=stream_types,
    kline_interval=kline_interval,
    websocket_base_url=websocket_base_url,
    max_messages=max_messages,
    duration_seconds=duration_seconds,
    receive_timeout_seconds=receive_timeout_seconds,
    catalog=catalog,
    bronze_schema=bronze_schema,
    silver_schema=silver_schema,
    bronze_trades_table=bronze_trades_table,
    bronze_klines_table=bronze_klines_table,
    silver_trades_table=silver_trades_table,
    silver_klines_table=silver_klines_table,
    base_output_path=delta_base_path,
    table_format=table_format,
    write_mode=write_mode,
    register_tables=register_tables,
    write_silver=write_silver,
)
summary["environment"] = env
summary["runtime_context"] = build_runtime_context(spark_session, requested_catalog=catalog)

emit_notebook_output(summary, dbutils_handle)
