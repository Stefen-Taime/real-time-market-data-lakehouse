# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Backfill History

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

from src.ingestion.load_binance_history import run_history_backfill
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

default_input_path = config.get("paths", {}).get("history_input_path", "tests/fixtures/sample_klines.json")
default_delta_base_path = config.get("paths", {}).get("delta_base_path")
default_register_tables = str(config.get("databricks", {}).get("register_tables", False)).lower()
default_table_format = config.get("databricks", {}).get("table_format", "delta")
default_write_mode = config.get("databricks", {}).get("write_modes", {}).get("history", "overwrite")
default_write_silver = "false"

catalog = config["catalog"]
bronze_schema = config["schemas"]["bronze"]
silver_schema = config["schemas"]["silver"]
bronze_table = config["tables"]["bronze"]["klines_history_raw"]
silver_table = config["tables"]["silver"]["klines_1m"]

if dbutils_handle is not None:
    dbutils_handle.widgets.text("input_path", default_input_path)
    dbutils_handle.widgets.text("delta_base_path", default_delta_base_path or "")
    dbutils_handle.widgets.text("register_tables", default_register_tables)
    dbutils_handle.widgets.text("table_format", default_table_format)
    dbutils_handle.widgets.text("write_mode", default_write_mode)
    dbutils_handle.widgets.text("write_silver", default_write_silver)

    input_path = _resolve_input_path(dbutils_handle.widgets.get("input_path"), project_root)
    delta_base_path = dbutils_handle.widgets.get("delta_base_path") or None
    register_tables = _to_bool(dbutils_handle.widgets.get("register_tables"))
    table_format = dbutils_handle.widgets.get("table_format")
    write_mode = dbutils_handle.widgets.get("write_mode")
    write_silver = _to_bool(dbutils_handle.widgets.get("write_silver"))
else:
    input_path = _resolve_input_path(default_input_path, project_root)
    delta_base_path = default_delta_base_path
    register_tables = _to_bool(default_register_tables)
    table_format = default_table_format
    write_mode = default_write_mode
    write_silver = _to_bool(default_write_silver)

spark_session = globals().get("spark") or get_spark_session(app_name="market-data-backfill-history")
summary = run_history_backfill(
    spark_session,
    input_path=input_path,
    catalog=catalog,
    bronze_schema=bronze_schema,
    silver_schema=silver_schema,
    bronze_table=bronze_table,
    silver_table=silver_table,
    base_output_path=delta_base_path,
    table_format=table_format,
    write_mode=write_mode,
    register_tables=register_tables,
    write_silver=write_silver,
)
summary["environment"] = env
summary["runtime_context"] = build_runtime_context(spark_session, requested_catalog=catalog)

emit_notebook_output(summary, dbutils_handle)
