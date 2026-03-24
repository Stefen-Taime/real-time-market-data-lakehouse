# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Build Gold

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

from src.gold.build_gold_metrics import run_gold_build
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
default_write_mode = config.get("databricks", {}).get("write_modes", {}).get("gold", "overwrite")
default_volatility_window_size = str(config.get("processing", {}).get("volatility_window_size", 5))
default_top_movers_limit = str(config.get("processing", {}).get("top_movers_limit", 10))
default_watermark_overlap_seconds = str(config.get("processing", {}).get("incremental", {}).get("overlap_seconds", 0))
watermark_column = config.get("processing", {}).get("incremental", {}).get("watermark_column", "ingested_at")

catalog = config["catalog"]
silver_schema = config["schemas"]["silver"]
gold_schema = config["schemas"]["gold"]
audit_schema = config["schemas"]["audit"]

if dbutils_handle is not None:
    dbutils_handle.widgets.text("delta_base_path", default_delta_base_path or "")
    dbutils_handle.widgets.text("register_tables", default_register_tables)
    dbutils_handle.widgets.text("table_format", default_table_format)
    dbutils_handle.widgets.text("write_mode", default_write_mode)
    dbutils_handle.widgets.text("watermark_overlap_seconds", default_watermark_overlap_seconds)
    dbutils_handle.widgets.text("volatility_window_size", default_volatility_window_size)
    dbutils_handle.widgets.text("top_movers_limit", default_top_movers_limit)

    delta_base_path = dbutils_handle.widgets.get("delta_base_path") or None
    register_tables = _to_bool(dbutils_handle.widgets.get("register_tables"))
    table_format = dbutils_handle.widgets.get("table_format")
    write_mode = dbutils_handle.widgets.get("write_mode")
    watermark_overlap_seconds = _to_int(dbutils_handle.widgets.get("watermark_overlap_seconds"))
    volatility_window_size = _to_int(dbutils_handle.widgets.get("volatility_window_size"))
    top_movers_limit = _to_int(dbutils_handle.widgets.get("top_movers_limit"))
else:
    delta_base_path = default_delta_base_path
    register_tables = _to_bool(default_register_tables)
    table_format = default_table_format
    write_mode = default_write_mode
    watermark_overlap_seconds = _to_int(default_watermark_overlap_seconds)
    volatility_window_size = _to_int(default_volatility_window_size)
    top_movers_limit = _to_int(default_top_movers_limit)

spark_session = globals().get("spark") or get_spark_session(app_name="market-data-build-gold")
summary = run_gold_build(
    spark_session,
    catalog=catalog,
    silver_schema=silver_schema,
    gold_schema=gold_schema,
    audit_schema=audit_schema,
    processing_state_table=config["tables"]["audit"]["processing_state"],
    silver_trades_table=config["tables"]["silver"]["trades"],
    silver_klines_table=config["tables"]["silver"]["klines_1m"],
    gold_latest_price_table=config["tables"]["gold"]["latest_price"],
    gold_volume_1m_table=config["tables"]["gold"]["volume_1m"],
    gold_volatility_5m_table=config["tables"]["gold"]["volatility_5m"],
    gold_top_movers_table=config["tables"]["gold"]["top_movers"],
    base_output_path=delta_base_path,
    table_format=table_format,
    write_mode=write_mode,
    register_tables=register_tables,
    volatility_window_size=volatility_window_size,
    top_movers_limit=top_movers_limit,
    watermark_column=watermark_column,
    watermark_overlap_seconds=watermark_overlap_seconds,
    pipeline_name="build_gold_metrics",
)
summary["environment"] = env
summary["runtime_context"] = build_runtime_context(spark_session, requested_catalog=catalog)

emit_notebook_output(summary, dbutils_handle)
