# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Publish App Serving Layer

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

from src.serving.publish_app_serving_layer import run_publish_app_serving_layer
from src.utils.config_loader import load_project_config
from src.utils.notebook_runtime import build_runtime_context, emit_notebook_output
from src.utils.spark_session import get_spark_session


def _get_dbutils():
    try:
        return dbutils  # type: ignore[name-defined]
    except NameError:
        return None


def _resolve_optional_input_path(value: str | None, project_root: Path) -> str | None:
    if not value:
        return None
    if value.startswith(("dbfs:/", "/Volumes/", "/Workspace/")):
        return value
    candidate = Path(value)
    if candidate.is_absolute():
        return str(candidate)
    return str((project_root / candidate).resolve())


dbutils_handle = _get_dbutils()

default_env = "dev_app"
if dbutils_handle is not None:
    dbutils_handle.widgets.text("env", default_env)
    env = dbutils_handle.widgets.get("env")
else:
    env = default_env

config = load_project_config(project_root / "config", env)

default_source_base_path = config.get("paths", {}).get("source_delta_base_path")
default_target_base_path = config.get("paths", {}).get("delta_base_path")
default_table_format = config.get("databricks", {}).get("table_format", "delta")
default_write_mode = config.get("databricks", {}).get("write_modes", {}).get("serving", "overwrite")
default_recent_volume_hours = str(config.get("app", {}).get("serving_volume_history_hours", 24))
default_insert_batch_size = str(config.get("app", {}).get("sql_insert_batch_size", 200))
default_warehouse_id = config.get("app", {}).get("warehouse_id", "")

if dbutils_handle is not None:
    dbutils_handle.widgets.text("source_delta_base_path", default_source_base_path or "")
    dbutils_handle.widgets.text("delta_base_path", default_target_base_path or "")
    dbutils_handle.widgets.text("table_format", default_table_format)
    dbutils_handle.widgets.text("write_mode", default_write_mode)
    dbutils_handle.widgets.text("warehouse_id", default_warehouse_id)
    dbutils_handle.widgets.text("recent_volume_hours", default_recent_volume_hours)
    dbutils_handle.widgets.text("insert_batch_size", default_insert_batch_size)

    source_base_path = dbutils_handle.widgets.get("source_delta_base_path")
    target_base_path = dbutils_handle.widgets.get("delta_base_path")
    table_format = dbutils_handle.widgets.get("table_format")
    write_mode = dbutils_handle.widgets.get("write_mode")
    warehouse_id = dbutils_handle.widgets.get("warehouse_id")
    recent_volume_hours = int(dbutils_handle.widgets.get("recent_volume_hours"))
    insert_batch_size = int(dbutils_handle.widgets.get("insert_batch_size"))
else:
    source_base_path = default_source_base_path
    target_base_path = default_target_base_path
    table_format = default_table_format
    write_mode = default_write_mode
    warehouse_id = default_warehouse_id
    recent_volume_hours = int(default_recent_volume_hours)
    insert_batch_size = int(default_insert_batch_size)

direct_publish_config = config.get("direct_publish")
if direct_publish_config:
    direct_publish_config = direct_publish_config.copy()
    direct_publish_config["history_input_path"] = _resolve_optional_input_path(
        direct_publish_config.get("history_input_path"),
        project_root,
    )
    direct_publish_config["live_input_path"] = _resolve_optional_input_path(
        direct_publish_config.get("live_input_path"),
        project_root,
    )

spark_session = globals().get("spark") or get_spark_session(app_name="market-data-publish-app-serving")
summary = run_publish_app_serving_layer(
    spark_session,
    source_base_path=source_base_path,
    target_base_path=target_base_path,
    dbutils_handle=dbutils_handle,
    warehouse_id=warehouse_id,
    volume_catalog=config["volume"]["catalog"],
    volume_schema=config["volume"]["schema"],
    volume_name=config["volume"]["name"],
    serving_table_catalog=config["serving_tables"]["catalog"],
    serving_table_schema=config["serving_tables"]["schema"],
    serving_table_names=config["serving_tables"]["names"],
    table_format=table_format,
    write_mode=write_mode,
    recent_volume_hours=recent_volume_hours,
    insert_batch_size=insert_batch_size,
    source_catalog=config.get("catalog"),
    source_schemas=config.get("schemas"),
    source_tables=config.get("tables"),
    source_register_tables=config.get("databricks", {}).get("register_tables", False),
    direct_publish_config=direct_publish_config,
    environment=env,
)
summary["environment"] = env
summary["runtime_context"] = build_runtime_context(spark_session, requested_catalog=config["catalog"])

emit_notebook_output(summary, dbutils_handle)
