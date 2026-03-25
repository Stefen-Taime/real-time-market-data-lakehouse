# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Market Observability Dashboard
# MAGIC
# MAGIC Notebook de presentation pour Databricks Notebook Dashboards.
# MAGIC
# MAGIC Usage recommande:
# MAGIC 1. ouvrir ce notebook dans le workspace Databricks
# MAGIC 2. executer `Run all`
# MAGIC 3. sur chaque sortie utile, choisir `Add to notebook dashboard`
# MAGIC 4. organiser les widgets du dashboard selon ton besoin

# COMMAND ----------

from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any


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

from pyspark.sql import Window, functions as F

from src.apps.market_observability_queries import (
    build_freshness_query,
    build_latest_prices_query,
    build_latest_volatility_query,
    build_processing_state_query,
    build_quality_latest_status_query,
    build_recent_volume_query,
    build_top_movers_query,
)
from src.utils.config_loader import load_project_config
from src.utils.databricks_warehouse_sql import fetch_query_rows_with_context
from src.utils.lakehouse_io import (
    build_storage_path,
    build_table_name,
    delta_target_exists,
    read_dataframe,
)
from src.utils.notebook_runtime import build_runtime_context, emit_notebook_output
from src.utils.processing_state import normalize_timestamp
from src.utils.spark_session import get_spark_session


WAREHOUSE_DEFAULT_COLUMNS = {
    "gold_latest_price": ["symbol", "latest_price", "trade_time", "source"],
    "gold_volume_1m": ["symbol", "window_start", "volume_1m", "notional_1m", "trade_count"],
    "gold_volatility_5m": ["symbol", "window_start", "window_end", "volatility_5m", "observation_count"],
    "gold_top_movers": ["symbol", "window_start", "window_end", "start_price", "end_price", "move_pct"],
    "audit_quality_check_runs": [
        "checked_at",
        "dataset_name",
        "layer",
        "table_name",
        "passed",
        "row_count",
        "violation_count",
        "job_name",
        "task_name",
        "audit_run_id",
    ],
    "audit_processing_state": [
        "pipeline_name",
        "dataset_name",
        "source_layer",
        "target_layer",
        "watermark_column",
        "last_processed_at",
        "updated_at",
        "metadata_json",
    ],
    "freshness": ["dataset_name", "row_count", "latest_timestamp"],
}

WAREHOUSE_CASTS = {
    "gold_latest_price": {"latest_price": "double", "trade_time": "timestamp"},
    "gold_volume_1m": {"window_start": "timestamp", "volume_1m": "double", "notional_1m": "double", "trade_count": "int"},
    "gold_volatility_5m": {
        "window_start": "timestamp",
        "window_end": "timestamp",
        "volatility_5m": "double",
        "observation_count": "int",
    },
    "gold_top_movers": {
        "window_start": "timestamp",
        "window_end": "timestamp",
        "start_price": "double",
        "end_price": "double",
        "move_pct": "double",
    },
    "audit_quality_check_runs": {
        "checked_at": "timestamp",
        "passed": "boolean",
        "row_count": "int",
        "violation_count": "int",
    },
    "audit_processing_state": {
        "last_processed_at": "timestamp",
        "updated_at": "timestamp",
    },
    "freshness": {"row_count": "int", "latest_timestamp": "timestamp"},
}


def _get_dbutils():
    try:
        return dbutils  # type: ignore[name-defined]
    except NameError:
        return None


def _to_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _to_int(value: str) -> int:
    return int(value.strip())


def _normalize_symbols(value: str) -> list[str]:
    return [symbol.strip().upper() for symbol in value.split(",") if symbol.strip()]


def _display_html(html: str) -> None:
    display_html_handle = globals().get("displayHTML")
    if callable(display_html_handle):
        display_html_handle(html)
    else:
        print(html)


def _display_dataframe(dataframe, *, limit: int | None = None) -> None:
    if dataframe is None:
        print("No data available for this section.")
        return

    display_handle = globals().get("display")
    target_dataframe = dataframe.limit(limit) if limit is not None else dataframe
    if callable(display_handle):
        display_handle(target_dataframe)
    else:
        target_dataframe.show(truncate=False)


def _resolve_notebook_path(dbutils_handle) -> str | None:
    if dbutils_handle is None:
        return None

    try:  # pragma: no cover - depends on Databricks runtime objects
        context = dbutils_handle.notebook.entry_point.getDbutils().notebook().getContext()
        notebook_path = context.notebookPath()
        getter = getattr(notebook_path, "get", None)
        if callable(getter):
            value = getter()
            return value or None
    except Exception:
        return None

    return None


def _infer_bundle_target_from_notebook_path(notebook_path: str | None) -> str | None:
    if not notebook_path:
        return None

    marker = "/.bundle/real-time-market-data-lakehouse/"
    if marker not in notebook_path:
        return None

    suffix = notebook_path.split(marker, 1)[1]
    target = suffix.split("/", 1)[0].strip()
    return target or None


def _infer_default_env(dbutils_handle) -> str:
    notebook_path = _resolve_notebook_path(dbutils_handle)
    bundle_target = _infer_bundle_target_from_notebook_path(notebook_path)
    if bundle_target == "azure_trial":
        return "azure_trial_governed"
    return "prod"


def _render_metric_cards(metrics: dict[str, str | int | float | None]) -> None:
    def _format_value(value):
        if value is None:
            return "n/a"
        return value

    cards = [
        ("Environment", metrics.get("environment")),
        ("Datasets Available", metrics.get("available_dataset_count")),
        ("Latest Price Symbols", metrics.get("latest_price_symbol_count")),
        ("Failed Quality Datasets", metrics.get("failed_quality_dataset_count")),
        ("Stale Watermarks", metrics.get("stale_watermark_count")),
        ("Worst Freshness (min)", metrics.get("worst_dataset_age_minutes")),
    ]
    cards_html = "".join(
        f"""
        <div style="flex: 1 1 180px; background: linear-gradient(180deg, #111827, #0f172a); border: 1px solid #334155; border-radius: 14px; padding: 16px 18px;">
          <div style="font-size: 12px; letter-spacing: 0.08em; text-transform: uppercase; color: #94a3b8; margin-bottom: 6px;">{label}</div>
          <div style="font-size: 28px; font-weight: 700; color: #f8fafc;">{_format_value(value)}</div>
        </div>
        """
        for label, value in cards
    )
    _display_html(
        f"""
        <div style="padding: 6px 0 14px 0;">
          <div style="font-size: 13px; color: #94a3b8; margin-bottom: 10px;">
            Symbols filter: {metrics.get('selected_symbols_label', 'ALL')}
          </div>
          <div style="display: flex; gap: 12px; flex-wrap: wrap;">
            {cards_html}
          </div>
        </div>
        """
    )


def _render_section_title(title: str, subtitle: str) -> None:
    _display_html(
        f"""
        <div style="padding: 8px 0 2px 0;">
          <div style="font-size: 24px; font-weight: 700; color: #111827;">{title}</div>
          <div style="font-size: 13px; color: #475569; margin-top: 4px;">{subtitle}</div>
        </div>
        """
    )


def _filter_symbols(dataframe, symbols: list[str]):
    if dataframe is None or not symbols or "symbol" not in dataframe.columns:
        return dataframe
    return dataframe.where(F.col("symbol").isin(symbols))


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


def _build_serving_query_kwargs(config: dict) -> dict[str, Any] | None:
    app_config = config.get("app", {})
    serving_tables_config = config.get("serving_tables", {})
    serving_table_names = serving_tables_config.get("names", {})
    warehouse_id = app_config.get("warehouse_id")
    query_mode = str(app_config.get("query_mode", "")).strip().lower()

    if not warehouse_id or query_mode != "table" or not serving_table_names:
        return None

    return {
        "warehouse_id": warehouse_id,
        "catalog": serving_tables_config.get("catalog", app_config.get("serving_catalog", config.get("catalog"))),
        "schema": serving_tables_config.get("schema", app_config.get("serving_schema", config["schemas"]["gold"])),
        "serving_tables": serving_table_names,
    }


def _warehouse_rows_to_dataframe(spark_session, *, dataset_name: str, columns: list[str], rows: list[list[object]]):
    from pyspark.sql.types import StringType, StructField, StructType

    effective_columns = columns or WAREHOUSE_DEFAULT_COLUMNS[dataset_name]
    if rows:
        dataframe = spark_session.createDataFrame(rows, effective_columns)
    else:
        schema = StructType([StructField(column, StringType(), True) for column in effective_columns])
        dataframe = spark_session.createDataFrame([], schema=schema)

    for column_name, data_type in WAREHOUSE_CASTS.get(dataset_name, {}).items():
        if column_name in dataframe.columns:
            dataframe = dataframe.withColumn(column_name, F.col(column_name).cast(data_type))

    return dataframe


def _load_dataset_from_warehouse(
    spark_session,
    *,
    dataset_name: str,
    statement: str,
    warehouse_id: str,
    dbutils_handle,
    serving_catalog: str,
    serving_schema: str,
    serving_table_name: str | None,
):
    columns, rows = fetch_query_rows_with_context(
        dbutils_handle,
        warehouse_id=warehouse_id,
        statement=statement,
    )
    dataframe = _warehouse_rows_to_dataframe(
        spark_session,
        dataset_name=dataset_name,
        columns=columns,
        rows=rows,
    )
    reference = {
        "layer": "warehouse",
        "schema": serving_schema,
        "table": serving_table_name or dataset_name,
        "table_name": ".".join(part for part in (serving_catalog, serving_schema, serving_table_name) if part),
        "input_path": None,
        "exists": True,
        "query_mode": "warehouse_table",
    }
    return dataframe, reference


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


def _build_latest_volatility_per_symbol_dataframe(volatility_dataframe, *, symbols: list[str], top_n: int):
    if volatility_dataframe is None:
        return None

    filtered = _filter_symbols(volatility_dataframe, symbols)
    ranking = Window.partitionBy("symbol").orderBy(F.col("window_end").desc(), F.col("window_start").desc())
    return (
        filtered.withColumn("row_number", F.row_number().over(ranking))
        .where(F.col("row_number") == 1)
        .drop("row_number")
        .orderBy(F.col("volatility_5m").desc_nulls_last(), F.col("symbol"))
        .limit(top_n)
    )


def _build_volume_trend_dataframe(volume_dataframe, *, symbols: list[str], lookback_hours: int):
    if volume_dataframe is None:
        return None

    filtered = _filter_symbols(volume_dataframe, symbols)
    return filtered.where(
        F.col("window_start") >= F.current_timestamp() - F.expr(f"INTERVAL {int(lookback_hours)} HOURS")
    ).orderBy(F.col("window_start").desc(), F.col("symbol"))


def _build_dashboard_metrics(
    *,
    env: str,
    selected_symbols: list[str],
    dataset_references: dict[str, dict],
    latest_prices_display_dataframe,
    latest_quality_status_dataframe,
    processing_state_status_dataframe,
    freshness_overview_dataframe,
) -> dict[str, str | int | float | None]:
    latest_price_symbol_count = latest_prices_display_dataframe.select("symbol").distinct().count() if latest_prices_display_dataframe is not None else 0
    failed_quality_dataset_count = (
        latest_quality_status_dataframe.where(~F.col("passed")).count()
        if latest_quality_status_dataframe is not None
        else 0
    )
    stale_watermark_count = (
        processing_state_status_dataframe.where(F.col("staleness_minutes") > F.lit(20.0)).count()
        if processing_state_status_dataframe is not None
        else 0
    )
    worst_dataset_age_minutes = None
    if freshness_overview_dataframe is not None:
        row = freshness_overview_dataframe.orderBy(F.col("age_minutes").desc_nulls_last()).first()
        if row is not None:
            worst_dataset_age_minutes = row["age_minutes"]

    return {
        "environment": env,
        "selected_symbols_label": ", ".join(selected_symbols) if selected_symbols else "ALL",
        "available_dataset_count": sum(1 for reference in dataset_references.values() if reference["exists"]),
        "latest_price_symbol_count": latest_price_symbol_count,
        "failed_quality_dataset_count": failed_quality_dataset_count,
        "stale_watermark_count": stale_watermark_count,
        "worst_dataset_age_minutes": worst_dataset_age_minutes,
    }


dbutils_handle = _get_dbutils()

default_env = _infer_default_env(dbutils_handle)
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
    dbutils_handle.widgets.dropdown("include_runtime_context", "false", ["false", "true"])
    dbutils_handle.widgets.text("limit", "20")
    dbutils_handle.widgets.text("lookback_hours", "6")
    dbutils_handle.widgets.text("top_n", "10")
    dbutils_handle.widgets.text("symbols", "")

    delta_base_path = dbutils_handle.widgets.get("delta_base_path") or None
    register_tables = _to_bool(dbutils_handle.widgets.get("register_tables"))
    table_format = dbutils_handle.widgets.get("table_format")
    include_runtime_context = _to_bool(dbutils_handle.widgets.get("include_runtime_context"))
    limit = _to_int(dbutils_handle.widgets.get("limit"))
    lookback_hours = _to_int(dbutils_handle.widgets.get("lookback_hours"))
    top_n = _to_int(dbutils_handle.widgets.get("top_n"))
    selected_symbols = _normalize_symbols(dbutils_handle.widgets.get("symbols"))
else:
    delta_base_path = default_delta_base_path
    register_tables = _to_bool(default_register_tables)
    table_format = default_table_format
    include_runtime_context = False
    limit = 20
    lookback_hours = 6
    top_n = 10
    selected_symbols = []

catalog = config["catalog"]
schemas = config["schemas"]
tables = config["tables"]
serving_query_kwargs = _build_serving_query_kwargs(config)

spark_session = globals().get("spark") or get_spark_session(app_name="market-data-observability-dashboard")

if serving_query_kwargs and dbutils_handle is not None:
    latest_price_dataframe, latest_price_reference = _load_dataset_from_warehouse(
        spark_session,
        dataset_name="gold_latest_price",
        statement=build_latest_prices_query(
            catalog=serving_query_kwargs["catalog"],
            schema=serving_query_kwargs["schema"],
            serving_table=serving_query_kwargs["serving_tables"]["gold_latest_price"],
        ),
        warehouse_id=serving_query_kwargs["warehouse_id"],
        dbutils_handle=dbutils_handle,
        serving_catalog=serving_query_kwargs["catalog"],
        serving_schema=serving_query_kwargs["schema"],
        serving_table_name=serving_query_kwargs["serving_tables"]["gold_latest_price"],
    )
    volume_dataframe, volume_reference = _load_dataset_from_warehouse(
        spark_session,
        dataset_name="gold_volume_1m",
        statement=build_recent_volume_query(
            catalog=serving_query_kwargs["catalog"],
            schema=serving_query_kwargs["schema"],
            serving_table=serving_query_kwargs["serving_tables"]["gold_volume_1m"],
            lookback_hours=lookback_hours,
        ),
        warehouse_id=serving_query_kwargs["warehouse_id"],
        dbutils_handle=dbutils_handle,
        serving_catalog=serving_query_kwargs["catalog"],
        serving_schema=serving_query_kwargs["schema"],
        serving_table_name=serving_query_kwargs["serving_tables"]["gold_volume_1m"],
    )
    volatility_dataframe, volatility_reference = _load_dataset_from_warehouse(
        spark_session,
        dataset_name="gold_volatility_5m",
        statement=build_latest_volatility_query(
            catalog=serving_query_kwargs["catalog"],
            schema=serving_query_kwargs["schema"],
            serving_table=serving_query_kwargs["serving_tables"]["gold_volatility_5m"],
        ),
        warehouse_id=serving_query_kwargs["warehouse_id"],
        dbutils_handle=dbutils_handle,
        serving_catalog=serving_query_kwargs["catalog"],
        serving_schema=serving_query_kwargs["schema"],
        serving_table_name=serving_query_kwargs["serving_tables"]["gold_volatility_5m"],
    )
    top_movers_dataframe, top_movers_reference = _load_dataset_from_warehouse(
        spark_session,
        dataset_name="gold_top_movers",
        statement=build_top_movers_query(
            catalog=serving_query_kwargs["catalog"],
            schema=serving_query_kwargs["schema"],
            serving_table=serving_query_kwargs["serving_tables"]["gold_top_movers"],
            limit=top_n,
        ),
        warehouse_id=serving_query_kwargs["warehouse_id"],
        dbutils_handle=dbutils_handle,
        serving_catalog=serving_query_kwargs["catalog"],
        serving_schema=serving_query_kwargs["schema"],
        serving_table_name=serving_query_kwargs["serving_tables"]["gold_top_movers"],
    )
    quality_dataframe, quality_reference = _load_dataset_from_warehouse(
        spark_session,
        dataset_name="audit_quality_check_runs",
        statement=build_quality_latest_status_query(
            catalog=serving_query_kwargs["catalog"],
            schema=serving_query_kwargs["schema"],
            serving_table=serving_query_kwargs["serving_tables"]["audit_quality_check_runs"],
        ),
        warehouse_id=serving_query_kwargs["warehouse_id"],
        dbutils_handle=dbutils_handle,
        serving_catalog=serving_query_kwargs["catalog"],
        serving_schema=serving_query_kwargs["schema"],
        serving_table_name=serving_query_kwargs["serving_tables"]["audit_quality_check_runs"],
    )
    processing_state_dataframe, processing_state_reference = _load_dataset_from_warehouse(
        spark_session,
        dataset_name="audit_processing_state",
        statement=build_processing_state_query(
            catalog=serving_query_kwargs["catalog"],
            schema=serving_query_kwargs["schema"],
            serving_table=serving_query_kwargs["serving_tables"]["audit_processing_state"],
        ),
        warehouse_id=serving_query_kwargs["warehouse_id"],
        dbutils_handle=dbutils_handle,
        serving_catalog=serving_query_kwargs["catalog"],
        serving_schema=serving_query_kwargs["schema"],
        serving_table_name=serving_query_kwargs["serving_tables"]["audit_processing_state"],
    )
    freshness_overview_dataframe, _freshness_reference = _load_dataset_from_warehouse(
        spark_session,
        dataset_name="freshness",
        statement=build_freshness_query(
            catalog=serving_query_kwargs["catalog"],
            schema=serving_query_kwargs["schema"],
            serving_tables=serving_query_kwargs["serving_tables"],
        ),
        warehouse_id=serving_query_kwargs["warehouse_id"],
        dbutils_handle=dbutils_handle,
        serving_catalog=serving_query_kwargs["catalog"],
        serving_schema=serving_query_kwargs["schema"],
        serving_table_name=None,
    )
    freshness_overview_dataframe = (
        freshness_overview_dataframe.withColumn("distinct_symbol_count", F.lit(None).cast("int"))
        .withColumn("timestamp_column", F.lit(None).cast("string"))
        .withColumn(
            "age_minutes",
            F.round(
                (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("latest_timestamp"))) / F.lit(60.0),
                2,
            ),
        )
        if freshness_overview_dataframe is not None
        else None
    )
    latest_quality_status_dataframe = quality_dataframe
    latest_quality_run_id = (
        quality_dataframe.select("audit_run_id").first()["audit_run_id"]
        if quality_dataframe is not None and quality_dataframe.count() > 0
        else None
    )
    quality_run_summary_dataframe = (
        quality_dataframe.groupBy("audit_run_id", "checked_at", "job_name", "task_name")
        .agg(
            F.count(F.lit(1)).cast("int").alias("dataset_count"),
            F.sum(F.when(F.col("passed"), F.lit(0)).otherwise(F.lit(1))).cast("int").alias("failed_dataset_count"),
            F.sum(F.col("violation_count")).cast("int").alias("violation_count"),
        )
        .orderBy(F.col("checked_at").desc(), F.col("audit_run_id").desc())
        if quality_dataframe is not None
        else None
    )
    processing_state_status_dataframe = _build_processing_state_status_dataframe(processing_state_dataframe)
else:
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

    quality_run_summary_dataframe, latest_quality_status_dataframe, latest_quality_run_id = _build_latest_quality_status_dataframe(
        quality_dataframe
    )
    processing_state_status_dataframe = _build_processing_state_status_dataframe(processing_state_dataframe)

    freshness_rows = _build_freshness_overview_rows(
        [
            {"dataset_name": "gold_latest_price", "layer": "gold", "dataframe": latest_price_dataframe, "timestamp_column": "trade_time"},
            {"dataset_name": "gold_volume_1m", "layer": "gold", "dataframe": volume_dataframe, "timestamp_column": "window_start"},
            {"dataset_name": "gold_volatility_5m", "layer": "gold", "dataframe": volatility_dataframe, "timestamp_column": "window_end"},
            {"dataset_name": "gold_top_movers", "layer": "gold", "dataframe": top_movers_dataframe, "timestamp_column": "window_end"},
            {"dataset_name": "audit_quality_check_runs", "layer": "audit", "dataframe": quality_dataframe, "timestamp_column": "checked_at"},
            {"dataset_name": "audit_processing_state", "layer": "audit", "dataframe": processing_state_dataframe, "timestamp_column": "updated_at"},
        ]
    )
    freshness_overview_dataframe = spark_session.createDataFrame(freshness_rows) if freshness_rows else None

latest_prices_display_dataframe = (
    _filter_symbols(latest_price_dataframe, selected_symbols).orderBy(F.col("trade_time").desc(), F.col("symbol"))
    if latest_price_dataframe is not None
    else None
)
volume_trend_dataframe = _build_volume_trend_dataframe(
    volume_dataframe,
    symbols=selected_symbols,
    lookback_hours=lookback_hours,
)
latest_volatility_per_symbol_dataframe = _build_latest_volatility_per_symbol_dataframe(
    volatility_dataframe,
    symbols=selected_symbols,
    top_n=top_n,
)
top_movers_display_dataframe = (
    _filter_symbols(top_movers_dataframe, selected_symbols).orderBy(F.abs(F.col("move_pct")).desc(), F.col("symbol")).limit(top_n)
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
dashboard_metrics = _build_dashboard_metrics(
    env=env,
    selected_symbols=selected_symbols,
    dataset_references=dataset_references,
    latest_prices_display_dataframe=latest_prices_display_dataframe,
    latest_quality_status_dataframe=latest_quality_status_dataframe,
    processing_state_status_dataframe=processing_state_status_dataframe,
    freshness_overview_dataframe=freshness_overview_dataframe,
)
summary = {
    "environment": env,
    "register_tables": register_tables,
    "table_format": table_format,
    "lookback_hours": lookback_hours,
    "top_n": top_n,
    "selected_symbols": selected_symbols,
    "latest_quality_run_id": latest_quality_run_id,
    "datasets": dataset_references,
    "metrics": dashboard_metrics,
}

if include_runtime_context:
    summary["runtime_context"] = build_runtime_context(spark_session, requested_catalog=catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Headline KPIs
# MAGIC Suggested widget type: value cards pinned directly from this cell output.

# COMMAND ----------

_render_metric_cards(dashboard_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset Freshness
# MAGIC Suggested viz: bar chart using `dataset_name` and `age_minutes`.

# COMMAND ----------

_render_section_title(
    "Dataset Freshness",
    "Overview of Gold and audit dataset recency for the selected environment.",
)
if freshness_overview_dataframe is not None:
    _display_dataframe(freshness_overview_dataframe.orderBy(F.col("age_minutes").desc_nulls_last()), limit=limit)
else:
    print("No dataset freshness information available.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latest Prices
# MAGIC Suggested viz: bar chart using `symbol` and `latest_price`.

# COMMAND ----------

_render_section_title(
    "Latest Prices",
    "Most recent Gold snapshot for each symbol after the optional filter.",
)
_display_dataframe(latest_prices_display_dataframe, limit=limit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume Trend 1m
# MAGIC Suggested viz: line chart using `window_start` and `volume_1m`, split by `symbol`.

# COMMAND ----------

_render_section_title(
    "Volume Trend 1m",
    f"Recent volume rows limited to the last {lookback_hours} hour(s).",
)
_display_dataframe(volume_trend_dataframe, limit=limit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latest Volatility 5m
# MAGIC Suggested viz: bar chart using `symbol` and `volatility_5m`.

# COMMAND ----------

_render_section_title(
    "Latest Volatility 5m",
    f"Latest volatility snapshot per symbol, limited to top {top_n}.",
)
_display_dataframe(latest_volatility_per_symbol_dataframe, limit=top_n)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Movers
# MAGIC Suggested viz: horizontal bar chart using `symbol` and `move_pct`.

# COMMAND ----------

_render_section_title(
    "Top Movers",
    f"Current top movers sorted by absolute percentage move, limited to top {top_n}.",
)
_display_dataframe(top_movers_display_dataframe, limit=top_n)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Runs
# MAGIC Suggested viz: stacked bar or table for recent run outcomes.

# COMMAND ----------

_render_section_title(
    "Quality Runs",
    "Recent audit execution summary and latest dataset-level status.",
)
_display_dataframe(quality_run_summary_dataframe, limit=limit)

# COMMAND ----------

_display_dataframe(latest_quality_status_dataframe, limit=limit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Watermarks
# MAGIC Suggested viz: table or bar chart using `dataset_name` and `staleness_minutes`.

# COMMAND ----------

_render_section_title(
    "Processing Watermarks",
    "Incremental state and watermark staleness for Silver and Gold pipelines.",
)
_display_dataframe(processing_state_status_dataframe, limit=limit)

# COMMAND ----------

emit_notebook_output(summary, dbutils_handle)
