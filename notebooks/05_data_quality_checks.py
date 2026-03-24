# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Data Quality Checks

import json
from pathlib import Path
import sys
from datetime import datetime, timezone


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

from src.quality.run_quality_checks import (
    DataQualityError,
    format_quality_violations,
    run_quality_checks,
    write_quality_audit_results,
)
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
default_fail_on_error = str(config.get("quality", {}).get("fail_on_error", True)).lower()
default_audit_write_mode = config.get("databricks", {}).get("write_modes", {}).get("audit", "append")

if dbutils_handle is not None:
    dbutils_handle.widgets.text("delta_base_path", default_delta_base_path or "")
    dbutils_handle.widgets.text("register_tables", default_register_tables)
    dbutils_handle.widgets.text("table_format", default_table_format)
    dbutils_handle.widgets.text("fail_on_error", default_fail_on_error)
    dbutils_handle.widgets.text("audit_write_mode", default_audit_write_mode)
    dbutils_handle.widgets.text("audit_run_id", "")
    dbutils_handle.widgets.text("job_name", "")
    dbutils_handle.widgets.text("task_name", "")

    delta_base_path = dbutils_handle.widgets.get("delta_base_path") or None
    register_tables = _to_bool(dbutils_handle.widgets.get("register_tables"))
    table_format = dbutils_handle.widgets.get("table_format")
    fail_on_error = _to_bool(dbutils_handle.widgets.get("fail_on_error"))
    audit_write_mode = dbutils_handle.widgets.get("audit_write_mode")
    audit_run_id = dbutils_handle.widgets.get("audit_run_id").strip()
    job_name = dbutils_handle.widgets.get("job_name").strip() or None
    task_name = dbutils_handle.widgets.get("task_name").strip() or None
else:
    delta_base_path = default_delta_base_path
    register_tables = _to_bool(default_register_tables)
    table_format = default_table_format
    fail_on_error = _to_bool(default_fail_on_error)
    audit_write_mode = default_audit_write_mode
    audit_run_id = ""
    job_name = None
    task_name = None

if not audit_run_id:
    audit_run_id = f"quality_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

spark_session = globals().get("spark") or get_spark_session(app_name="market-data-quality-checks")
summary = run_quality_checks(
    spark_session,
    catalog=config["catalog"],
    schemas=config["schemas"],
    tables=config["tables"],
    dataset_rules=config.get("quality", {}).get("datasets", []),
    base_output_path=delta_base_path,
    table_format=table_format,
    register_tables=register_tables,
    fail_on_error=False,
)
summary["audit"] = write_quality_audit_results(
    spark_session,
    summary=summary,
    catalog=config["catalog"],
    audit_schema=config["schemas"]["audit"],
    audit_table=config["tables"]["audit"]["quality_check_runs"],
    base_output_path=delta_base_path,
    environment=env,
    audit_run_id=audit_run_id,
    table_format=table_format,
    write_mode=audit_write_mode,
    register_tables=register_tables,
    job_name=job_name,
    task_name=task_name,
)
summary["audit"]["job_name"] = job_name
summary["audit"]["task_name"] = task_name
summary["audit"]["environment"] = env
summary["environment"] = env
summary["runtime_context"] = build_runtime_context(spark_session, requested_catalog=config["catalog"])

if summary["violations"] and fail_on_error:
    print(json.dumps(summary, indent=2, default=str))
    raise DataQualityError(format_quality_violations(summary["violations"]))

emit_notebook_output(summary, dbutils_handle)
