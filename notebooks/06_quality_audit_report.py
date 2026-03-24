# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Quality Audit Report

import json
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

from pyspark.sql import functions as F

from src.utils.config_loader import load_project_config
from src.utils.lakehouse_io import build_storage_path, build_table_name, read_dataframe
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


def _display_dataframe(dataframe, title: str, limit: int = 20) -> None:
    print(f"\n=== {title} ===")
    display_handle = globals().get("display")
    if callable(display_handle):
        display_handle(dataframe.limit(limit))
    else:
        dataframe.limit(limit).show(truncate=False)


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

if dbutils_handle is not None:
    dbutils_handle.widgets.text("delta_base_path", default_delta_base_path or "")
    dbutils_handle.widgets.text("register_tables", default_register_tables)
    dbutils_handle.widgets.text("table_format", default_table_format)
    dbutils_handle.widgets.text("limit", "20")

    delta_base_path = dbutils_handle.widgets.get("delta_base_path") or None
    register_tables = _to_bool(dbutils_handle.widgets.get("register_tables"))
    table_format = dbutils_handle.widgets.get("table_format")
    limit = _to_int(dbutils_handle.widgets.get("limit"))
else:
    delta_base_path = default_delta_base_path
    register_tables = _to_bool(default_register_tables)
    table_format = default_table_format
    limit = 20

catalog = config["catalog"]
audit_schema = config["schemas"]["audit"]
audit_table = config["tables"]["audit"]["quality_check_runs"]

table_name = build_table_name(catalog, audit_schema, audit_table) if register_tables else None
input_path = build_storage_path(delta_base_path, "audit", audit_table)

spark_session = globals().get("spark") or get_spark_session(app_name="market-data-quality-audit-report")
audit_dataframe = read_dataframe(
    spark_session,
    table_format=table_format,
    table_name=table_name,
    input_path=input_path,
    register_table=register_tables,
)

run_summary_dataframe = (
    audit_dataframe.groupBy("audit_run_id", "checked_at", "environment", "job_name", "task_name")
    .agg(
        F.count(F.lit(1)).cast("int").alias("dataset_count"),
        F.sum(F.when(F.col("passed"), F.lit(0)).otherwise(F.lit(1))).cast("int").alias("failed_dataset_count"),
        F.sum(F.col("violation_count")).cast("int").alias("violation_count"),
    )
    .orderBy(F.col("checked_at").desc(), F.col("audit_run_id").desc())
)

latest_run_row = run_summary_dataframe.first()
latest_run_id = latest_run_row["audit_run_id"] if latest_run_row else None

latest_run_failures_dataframe = (
    audit_dataframe.where((F.col("audit_run_id") == F.lit(latest_run_id)) & (~F.col("passed")))
    .select(
        "checked_at",
        "dataset_name",
        "layer",
        "table_name",
        "row_count",
        "violation_count",
        "violations_json",
    )
    .orderBy("dataset_name")
    if latest_run_id is not None
    else audit_dataframe.limit(0)
)

dataset_health_dataframe = (
    audit_dataframe.groupBy("dataset_name", "layer")
    .agg(
        F.max("checked_at").alias("latest_checked_at"),
        F.count(F.lit(1)).cast("int").alias("run_count"),
        F.sum(F.when(F.col("passed"), F.lit(1)).otherwise(F.lit(0))).cast("int").alias("successful_runs"),
        F.sum(F.when(F.col("passed"), F.lit(0)).otherwise(F.lit(1))).cast("int").alias("failed_runs"),
        F.max("violation_count").cast("int").alias("max_violation_count"),
    )
    .withColumn("pass_rate", F.round(F.col("successful_runs") / F.col("run_count"), 4))
    .orderBy(F.col("failed_runs").desc(), F.col("dataset_name"))
)

recent_failures_dataframe = (
    audit_dataframe.where(~F.col("passed"))
    .select(
        "checked_at",
        "audit_run_id",
        "job_name",
        "task_name",
        "dataset_name",
        "violation_count",
        "violations_json",
    )
    .orderBy(F.col("checked_at").desc(), F.col("dataset_name"))
)

summary = {
    "table": table_name,
    "path": input_path,
    "register_tables": register_tables,
    "table_format": table_format,
    "latest_run_id": latest_run_id,
    "row_count": audit_dataframe.count(),
}

print(json.dumps(summary, indent=2, default=str))
_display_dataframe(run_summary_dataframe, "Recent Quality Runs", limit=limit)
_display_dataframe(latest_run_failures_dataframe, "Latest Run Failures", limit=limit)
_display_dataframe(dataset_health_dataframe, "Dataset Health Overview", limit=limit)
_display_dataframe(recent_failures_dataframe, "Recent Failures History", limit=limit)
