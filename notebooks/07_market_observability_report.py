# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Market Observability Report

from datetime import datetime, timezone
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
from src.utils.lakehouse_io import (
    build_storage_path,
    build_table_name,
    delta_target_exists,
    read_dataframe,
)
from src.utils.notebook_runtime import build_runtime_context, emit_notebook_output
from src.utils.processing_state import normalize_timestamp
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


def _load_optional_dataset(
    spark_session,
    *,
    catalog: str,
    schema: str,
    table: str,
    layer: str,
    delta_base_path: str | None,
    table_format: str,
    register_tables: bool,
):
    table_name = build_table_name(catalog, schema, table) if register_tables else None
    input_path = build_storage_path(delta_base_path, layer, table)
    exists = delta_target_exists(
        spark_session,
        table_name=table_name,
        output_path=input_path,
        register_table=register_tables,
    )

    reference = {
        "layer": layer,
        "schema": schema,
        "table": table,
        "table_name": table_name,
        "input_path": input_path,
        "exists": exists,
    }
    if not exists:
        return None, reference

    return (
        read_dataframe(
            spark_session,
            table_format=table_format,
            table_name=table_name,
            input_path=input_path,
            register_table=register_tables,
        ),
        reference,
    )


def _build_latest_quality_status_dataframe(quality_dataframe):
    if quality_dataframe is None:
        return None, None, None

    run_summary_dataframe = (
        quality_dataframe.groupBy("audit_run_id", "checked_at", "environment", "job_name", "task_name")
        .agg(
            F.count(F.lit(1)).cast("int").alias("dataset_count"),
            F.sum(F.when(F.col("passed"), F.lit(0)).otherwise(F.lit(1))).cast("int").alias("failed_dataset_count"),
            F.sum(F.col("violation_count")).cast("int").alias("violation_count"),
        )
        .orderBy(F.col("checked_at").desc(), F.col("audit_run_id").desc())
    )
    latest_run_row = run_summary_dataframe.first()
    latest_run_id = latest_run_row["audit_run_id"] if latest_run_row else None

    latest_dataset_status_dataframe = (
        quality_dataframe.where(F.col("audit_run_id") == F.lit(latest_run_id))
        .select(
            "checked_at",
            "dataset_name",
            "layer",
            "table_name",
            "passed",
            "row_count",
            "violation_count",
            "job_name",
            "task_name",
        )
        .orderBy(F.col("passed").asc(), F.col("dataset_name"))
        if latest_run_id is not None
        else None
    )
    return run_summary_dataframe, latest_dataset_status_dataframe, latest_run_id


def _build_processing_state_status_dataframe(processing_state_dataframe):
    if processing_state_dataframe is None:
        return None

    return (
        processing_state_dataframe.withColumn(
            "staleness_minutes",
            F.round((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("updated_at"))) / F.lit(60.0), 2),
        )
        .select(
            "pipeline_name",
            "dataset_name",
            "source_layer",
            "target_layer",
            "watermark_column",
            "last_processed_at",
            "updated_at",
            "staleness_minutes",
            "metadata_json",
        )
        .orderBy(F.col("updated_at").desc(), F.col("pipeline_name"), F.col("dataset_name"))
    )


def _build_freshness_overview_rows(dataset_specs):
    now = datetime.now(timezone.utc)
    rows = []

    for dataset_spec in dataset_specs:
        dataframe = dataset_spec["dataframe"]
        timestamp_column = dataset_spec["timestamp_column"]
        if dataframe is None or timestamp_column not in dataframe.columns:
            continue

        aggregated = dataframe.agg(
            F.count(F.lit(1)).cast("int").alias("row_count"),
            F.max(F.col(timestamp_column)).alias("latest_timestamp"),
            (
                F.countDistinct("symbol").cast("int").alias("distinct_symbol_count")
                if "symbol" in dataframe.columns
                else F.lit(None).cast("int").alias("distinct_symbol_count")
            ),
        ).first()

        latest_timestamp = normalize_timestamp(aggregated["latest_timestamp"])
        age_minutes = None
        if latest_timestamp is not None:
            age_minutes = round((now - latest_timestamp).total_seconds() / 60.0, 2)

        rows.append(
            {
                "dataset_name": dataset_spec["dataset_name"],
                "layer": dataset_spec["layer"],
                "row_count": aggregated["row_count"],
                "distinct_symbol_count": aggregated["distinct_symbol_count"],
                "latest_timestamp": latest_timestamp,
                "timestamp_column": timestamp_column,
                "age_minutes": age_minutes,
            }
        )

    return rows


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
schemas = config["schemas"]
tables = config["tables"]

spark_session = globals().get("spark") or get_spark_session(app_name="market-data-observability-report")

latest_price_dataframe, latest_price_reference = _load_optional_dataset(
    spark_session,
    catalog=catalog,
    schema=schemas["gold"],
    table=tables["gold"]["latest_price"],
    layer="gold",
    delta_base_path=delta_base_path,
    table_format=table_format,
    register_tables=register_tables,
)
volume_dataframe, volume_reference = _load_optional_dataset(
    spark_session,
    catalog=catalog,
    schema=schemas["gold"],
    table=tables["gold"]["volume_1m"],
    layer="gold",
    delta_base_path=delta_base_path,
    table_format=table_format,
    register_tables=register_tables,
)
volatility_dataframe, volatility_reference = _load_optional_dataset(
    spark_session,
    catalog=catalog,
    schema=schemas["gold"],
    table=tables["gold"]["volatility_5m"],
    layer="gold",
    delta_base_path=delta_base_path,
    table_format=table_format,
    register_tables=register_tables,
)
top_movers_dataframe, top_movers_reference = _load_optional_dataset(
    spark_session,
    catalog=catalog,
    schema=schemas["gold"],
    table=tables["gold"]["top_movers"],
    layer="gold",
    delta_base_path=delta_base_path,
    table_format=table_format,
    register_tables=register_tables,
)
quality_dataframe, quality_reference = _load_optional_dataset(
    spark_session,
    catalog=catalog,
    schema=schemas["audit"],
    table=tables["audit"]["quality_check_runs"],
    layer="audit",
    delta_base_path=delta_base_path,
    table_format=table_format,
    register_tables=register_tables,
)
processing_state_dataframe, processing_state_reference = _load_optional_dataset(
    spark_session,
    catalog=catalog,
    schema=schemas["audit"],
    table=tables["audit"]["processing_state"],
    layer="audit",
    delta_base_path=delta_base_path,
    table_format=table_format,
    register_tables=register_tables,
)

quality_run_summary_dataframe, latest_quality_status_dataframe, latest_quality_run_id = (
    _build_latest_quality_status_dataframe(quality_dataframe)
)
processing_state_status_dataframe = _build_processing_state_status_dataframe(processing_state_dataframe)

freshness_rows = _build_freshness_overview_rows(
    [
        {
            "dataset_name": "gold_latest_price",
            "layer": "gold",
            "dataframe": latest_price_dataframe,
            "timestamp_column": "trade_time",
        },
        {
            "dataset_name": "gold_volume_1m",
            "layer": "gold",
            "dataframe": volume_dataframe,
            "timestamp_column": "window_start",
        },
        {
            "dataset_name": "gold_volatility_5m",
            "layer": "gold",
            "dataframe": volatility_dataframe,
            "timestamp_column": "window_end",
        },
        {
            "dataset_name": "gold_top_movers",
            "layer": "gold",
            "dataframe": top_movers_dataframe,
            "timestamp_column": "window_end",
        },
        {
            "dataset_name": "audit_quality_check_runs",
            "layer": "audit",
            "dataframe": quality_dataframe,
            "timestamp_column": "checked_at",
        },
        {
            "dataset_name": "audit_processing_state",
            "layer": "audit",
            "dataframe": processing_state_dataframe,
            "timestamp_column": "updated_at",
        },
    ]
)
freshness_overview_dataframe = spark_session.createDataFrame(freshness_rows) if freshness_rows else None

latest_prices_display_dataframe = (
    latest_price_dataframe.orderBy(F.col("trade_time").desc(), F.col("symbol")) if latest_price_dataframe is not None else None
)
volume_display_dataframe = (
    volume_dataframe.orderBy(F.col("window_start").desc(), F.col("symbol")) if volume_dataframe is not None else None
)
volatility_display_dataframe = (
    volatility_dataframe.orderBy(F.col("window_end").desc(), F.col("symbol")) if volatility_dataframe is not None else None
)
top_movers_display_dataframe = (
    top_movers_dataframe.orderBy(F.abs(F.col("move_pct")).desc(), F.col("symbol"))
    if top_movers_dataframe is not None
    else None
)

dataset_references = {
    "gold_latest_price": latest_price_reference,
    "gold_volume_1m": volume_reference,
    "gold_volatility_5m": volatility_reference,
    "gold_top_movers": top_movers_reference,
    "audit_quality_check_runs": quality_reference,
    "audit_processing_state": processing_state_reference,
}
available_dataset_count = sum(1 for reference in dataset_references.values() if reference["exists"])

summary = {
    "environment": env,
    "register_tables": register_tables,
    "table_format": table_format,
    "available_dataset_count": available_dataset_count,
    "latest_quality_run_id": latest_quality_run_id,
    "datasets": dataset_references,
    "runtime_context": build_runtime_context(spark_session, requested_catalog=catalog),
}

print(summary)
if freshness_overview_dataframe is not None:
    _display_dataframe(freshness_overview_dataframe.orderBy(F.col("age_minutes").asc_nulls_last()), "Freshness Overview", limit=limit)
else:
    print("\n=== Freshness Overview ===")
    print("No Gold or audit datasets available for the selected environment.")

if latest_prices_display_dataframe is not None:
    _display_dataframe(latest_prices_display_dataframe, "Latest Prices", limit=limit)

if volume_display_dataframe is not None:
    _display_dataframe(volume_display_dataframe, "Recent Volume 1m", limit=limit)

if volatility_display_dataframe is not None:
    _display_dataframe(volatility_display_dataframe, "Recent Volatility 5m", limit=limit)

if top_movers_display_dataframe is not None:
    _display_dataframe(top_movers_display_dataframe, "Top Movers", limit=limit)

if quality_run_summary_dataframe is not None:
    _display_dataframe(quality_run_summary_dataframe, "Recent Quality Runs", limit=limit)

if latest_quality_status_dataframe is not None:
    _display_dataframe(latest_quality_status_dataframe, "Latest Quality Dataset Status", limit=limit)

if processing_state_status_dataframe is not None:
    _display_dataframe(processing_state_status_dataframe, "Processing State Watermarks", limit=limit)

emit_notebook_output(summary, dbutils_handle)
