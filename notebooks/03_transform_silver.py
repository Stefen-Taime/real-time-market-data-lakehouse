# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Transform Silver

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

from src.transforms.transform_silver_layer import run_silver_transform
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


def _to_int(value: str) -> int:
    return int(value.strip())


dbutils_handle = _get_dbutils()

default_env = "dev"
if dbutils_handle is not None:
    dbutils_handle.widgets.text("env", default_env)
    env = dbutils_handle.widgets.get("env")
else:
    env = default_env

config = load_project_config(project_root / "config", env)

default_delta_base_path = config.get("paths", {}).get("delta_base_path")
default_register_tables = str(config.get("databricks", {}).get("register_tables", False)).lower()
default_table_format = config.get("databricks", {}).get("table_format", "delta")
default_write_mode = config.get("databricks", {}).get("write_modes", {}).get("silver", "overwrite")
default_watermark_overlap_seconds = str(config.get("processing", {}).get("incremental", {}).get("overlap_seconds", 0))
watermark_column = config.get("processing", {}).get("incremental", {}).get("watermark_column", "ingested_at")

catalog = config["catalog"]
bronze_schema = config["schemas"]["bronze"]
silver_schema = config["schemas"]["silver"]
audit_schema = config["schemas"]["audit"]

if dbutils_handle is not None:
    dbutils_handle.widgets.text("delta_base_path", default_delta_base_path or "")
    dbutils_handle.widgets.text("register_tables", default_register_tables)
    dbutils_handle.widgets.text("table_format", default_table_format)
    dbutils_handle.widgets.text("write_mode", default_write_mode)
    dbutils_handle.widgets.text("watermark_overlap_seconds", default_watermark_overlap_seconds)
    dbutils_handle.widgets.text("transform_trades", "true")
    dbutils_handle.widgets.text("transform_klines", "true")

    delta_base_path = dbutils_handle.widgets.get("delta_base_path") or None
    register_tables = _to_bool(dbutils_handle.widgets.get("register_tables"))
    table_format = dbutils_handle.widgets.get("table_format")
    write_mode = dbutils_handle.widgets.get("write_mode")
    watermark_overlap_seconds = _to_int(dbutils_handle.widgets.get("watermark_overlap_seconds"))
    transform_trades = _to_bool(dbutils_handle.widgets.get("transform_trades"))
    transform_klines = _to_bool(dbutils_handle.widgets.get("transform_klines"))
else:
    delta_base_path = default_delta_base_path
    register_tables = _to_bool(default_register_tables)
    table_format = default_table_format
    write_mode = default_write_mode
    watermark_overlap_seconds = _to_int(default_watermark_overlap_seconds)
    transform_trades = True
    transform_klines = True

spark_session = globals().get("spark") or get_spark_session(app_name="market-data-transform-silver")
summary = run_silver_transform(
    spark_session,
    catalog=catalog,
    bronze_schema=bronze_schema,
    silver_schema=silver_schema,
    audit_schema=audit_schema,
    processing_state_table=config["tables"]["audit"]["processing_state"],
    bronze_trades_table=config["tables"]["bronze"]["trades_live_raw"],
    bronze_klines_history_table=config["tables"]["bronze"]["klines_history_raw"],
    bronze_klines_live_table=config["tables"]["bronze"]["klines_live_raw"],
    silver_trades_table=config["tables"]["silver"]["trades"],
    silver_klines_table=config["tables"]["silver"]["klines_1m"],
    base_output_path=delta_base_path,
    table_format=table_format,
    write_mode=write_mode,
    register_tables=register_tables,
    transform_trades=transform_trades,
    transform_klines=transform_klines,
    watermark_column=watermark_column,
    watermark_overlap_seconds=watermark_overlap_seconds,
    pipeline_name="transform_silver",
)
summary["environment"] = env
summary["runtime_context"] = build_runtime_context(spark_session, requested_catalog=catalog)

emit_notebook_output(summary, dbutils_handle)
