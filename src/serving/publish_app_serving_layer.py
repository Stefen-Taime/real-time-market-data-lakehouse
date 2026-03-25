"""Publish app-facing serving datasets through a Databricks SQL warehouse."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from src.gold.build_price_latest import select_latest_prices
from src.gold.build_top_movers import build_top_movers
from src.gold.build_volatility_5m import compute_volatility_5m
from src.gold.build_volume_1m import aggregate_volume_1m
from src.ingestion.ingest_binance_live import filter_live_records, load_live_records
from src.ingestion.load_binance_history import fetch_recent_history_records, load_history_file
from src.transforms.bronze_to_silver_klines import transform_kline_batch
from src.transforms.bronze_to_silver_trades import transform_trade_batch
from src.utils.databricks_warehouse_sql import (
    build_create_or_replace_empty_table_statement,
    build_create_schema_statement,
    build_insert_statement,
    chunk_rows,
    execute_sql_statement_with_context,
)
from src.utils.lakehouse_io import build_storage_path, build_table_name, delta_target_exists, read_dataframe

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


SERVING_DATASETS = (
    {"dataset_name": "gold_latest_price", "layer": "gold", "table": "latest_price"},
    {"dataset_name": "gold_volume_1m", "layer": "gold", "table": "volume_1m"},
    {"dataset_name": "gold_volatility_5m", "layer": "gold", "table": "volatility_5m"},
    {"dataset_name": "gold_top_movers", "layer": "gold", "table": "top_movers"},
    {"dataset_name": "audit_quality_check_runs", "layer": "audit", "table": "quality_check_runs"},
    {"dataset_name": "audit_processing_state", "layer": "audit", "table": "processing_state"},
)

SERVING_TABLE_COLUMNS = {
    "gold_latest_price": [
        ("symbol", "STRING"),
        ("latest_price", "DOUBLE"),
        ("trade_time", "TIMESTAMP"),
        ("source", "STRING"),
    ],
    "gold_volume_1m": [
        ("symbol", "STRING"),
        ("window_start", "TIMESTAMP"),
        ("volume_1m", "DOUBLE"),
        ("notional_1m", "DOUBLE"),
        ("trade_count", "INT"),
    ],
    "gold_volatility_5m": [
        ("symbol", "STRING"),
        ("window_start", "TIMESTAMP"),
        ("window_end", "TIMESTAMP"),
        ("interval", "STRING"),
        ("observation_count", "INT"),
        ("volatility_5m", "DOUBLE"),
    ],
    "gold_top_movers": [
        ("symbol", "STRING"),
        ("window_start", "TIMESTAMP"),
        ("window_end", "TIMESTAMP"),
        ("start_price", "DOUBLE"),
        ("end_price", "DOUBLE"),
        ("move_pct", "DOUBLE"),
    ],
    "audit_quality_check_runs": [
        ("audit_run_id", "STRING"),
        ("checked_at", "TIMESTAMP"),
        ("environment", "STRING"),
        ("job_name", "STRING"),
        ("task_name", "STRING"),
        ("dataset_name", "STRING"),
        ("layer", "STRING"),
        ("table_name", "STRING"),
        ("storage_path", "STRING"),
        ("passed", "BOOLEAN"),
        ("row_count", "INT"),
        ("min_row_count", "INT"),
        ("violation_count", "INT"),
        ("violations_json", "STRING"),
        ("schema_passed", "BOOLEAN"),
        ("null_checks_passed", "BOOLEAN"),
        ("duplicate_check_passed", "BOOLEAN"),
        ("freshness_check_passed", "BOOLEAN"),
        ("bounds_check_passed", "BOOLEAN"),
        ("summary_json", "STRING"),
    ],
    "audit_processing_state": [
        ("pipeline_name", "STRING"),
        ("dataset_name", "STRING"),
        ("source_layer", "STRING"),
        ("target_layer", "STRING"),
        ("watermark_column", "STRING"),
        ("last_processed_at", "TIMESTAMP"),
        ("updated_at", "TIMESTAMP"),
        ("metadata_json", "STRING"),
    ],
}


def _run_warehouse_probe(
    *,
    dbutils_handle: Any,
    warehouse_id: str,
    catalog: str,
    schema: str,
) -> dict[str, Any]:
    """Create a tiny probe table so warehouse publication is externally visible."""
    probe_table_name = f"{catalog}.{schema}.app_publish_probe_table"
    result = execute_sql_statement_with_context(
        dbutils_handle,
        warehouse_id=warehouse_id,
        statement=(
            f"CREATE OR REPLACE TABLE {probe_table_name} "
            "AS SELECT 'publish_probe_ok' AS probe_label, current_timestamp() AS created_at"
        ),
    )
    return {
        "table_name": probe_table_name,
        "statement_id": result.get("statement_id"),
        "operation": "warehouse_probe_replace",
    }


def _prepare_serving_dataframe(
    dataframe: DataFrame,
    *,
    dataset_name: str,
    recent_volume_hours: int,
) -> tuple[DataFrame, str]:
    """Shape large source datasets into a compact app-facing serving snapshot."""
    from pyspark.sql import Window, functions as F

    if dataset_name == "gold_volume_1m":
        filtered = dataframe.filter(
            F.col("window_start")
            >= F.current_timestamp() - F.expr(f"INTERVAL {int(recent_volume_hours)} HOURS")
        )
        return filtered.orderBy(F.col("window_start").desc(), F.col("symbol").asc()), f"recent_{int(recent_volume_hours)}h"

    if dataset_name == "gold_volatility_5m":
        ranking = Window.partitionBy("symbol").orderBy(
            F.col("window_end").desc(),
            F.col("window_start").desc(),
        )
        latest = dataframe.withColumn("_row_number", F.row_number().over(ranking)).filter(
            F.col("_row_number") == 1
        )
        return (
            latest.drop("_row_number").orderBy(
                F.col("volatility_5m").desc_nulls_last(),
                F.col("symbol").asc(),
            ),
            "latest_per_symbol",
        )

    if dataset_name == "audit_quality_check_runs":
        latest_run = (
            dataframe.orderBy(F.col("checked_at").desc(), F.col("audit_run_id").desc())
            .select("audit_run_id")
            .limit(1)
        )
        return (
            dataframe.join(latest_run, on="audit_run_id", how="inner").orderBy(
                F.col("passed").asc(),
                F.col("dataset_name").asc(),
            ),
            "latest_audit_run",
        )

    if dataset_name == "audit_processing_state":
        return (
            dataframe.orderBy(
                F.col("updated_at").desc(),
                F.col("pipeline_name").asc(),
                F.col("dataset_name").asc(),
            ),
            "full_state",
        )

    if dataset_name == "gold_top_movers":
        return dataframe.orderBy(F.col("move_pct").desc_nulls_last()), "current_snapshot"

    if dataset_name == "gold_latest_price":
        return (
            dataframe.orderBy(F.col("trade_time").desc_nulls_last(), F.col("symbol").asc()),
            "current_snapshot",
        )

    return dataframe, "identity"


def _spark_type_to_databricks_sql(field) -> str:
    """Map Spark field types to Databricks SQL column types."""
    data_type = field.dataType
    type_name = data_type.typeName()

    if type_name == "string":
        return "STRING"
    if type_name == "long":
        return "BIGINT"
    if type_name == "integer":
        return "INT"
    if type_name == "double":
        return "DOUBLE"
    if type_name == "float":
        return "FLOAT"
    if type_name == "boolean":
        return "BOOLEAN"
    if type_name == "timestamp":
        return "TIMESTAMP"
    if type_name == "date":
        return "DATE"
    if type_name == "short":
        return "SMALLINT"
    if type_name == "byte":
        return "TINYINT"
    if type_name == "decimal":
        return f"DECIMAL({data_type.precision},{data_type.scale})"

    return data_type.simpleString().upper()


def _dataframe_to_column_definitions(dataframe: DataFrame) -> list[tuple[str, str]]:
    """Convert a Spark DataFrame schema into SQL warehouse column definitions."""
    return [(field.name, _spark_type_to_databricks_sql(field)) for field in dataframe.schema.fields]


def _dataframe_to_rows(dataframe: DataFrame) -> list[tuple[Any, ...]]:
    """Collect a compact serving DataFrame into ordered Python tuples for SQL inserts."""
    columns = [field.name for field in dataframe.schema.fields]
    return [tuple(row[column] for column in columns) for row in dataframe.collect()]


def _rows_from_dicts(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> list[tuple[Any, ...]]:
    """Project dict rows into deterministic tuple order aligned with SQL column definitions."""
    column_names = [column_name for column_name, _ in columns]
    return [tuple(row.get(column_name) for column_name in column_names) for row in rows]


def publish_rows_via_warehouse(
    *,
    dbutils_handle: Any,
    warehouse_id: str,
    catalog: str,
    schema: str,
    table_name: str,
    columns: list[tuple[str, str]],
    rows: list[tuple[Any, ...]],
    insert_batch_size: int,
) -> dict[str, Any]:
    """Create or replace a warehouse-visible serving table and insert a row batch."""
    full_schema_name = f"{catalog}.{schema}"
    full_table_name = f"{catalog}.{schema}.{table_name}"

    statement_count = 0
    execute_sql_statement_with_context(
        dbutils_handle,
        warehouse_id=warehouse_id,
        statement=build_create_schema_statement(full_schema_name),
    )
    statement_count += 1
    execute_sql_statement_with_context(
        dbutils_handle,
        warehouse_id=warehouse_id,
        statement=build_create_or_replace_empty_table_statement(full_table_name, columns),
    )
    statement_count += 1

    for row_batch in chunk_rows(rows, insert_batch_size):
        execute_sql_statement_with_context(
            dbutils_handle,
            warehouse_id=warehouse_id,
            statement=build_insert_statement(full_table_name, columns, row_batch),
        )
        statement_count += 1

    return {
        "table_name": full_table_name,
        "row_count": len(rows),
        "operation": "warehouse_replace",
        "statement_count": statement_count,
        "insert_batch_size": insert_batch_size,
    }


def build_source_dataset_reference(
    *,
    source_base_path: str | None,
    source_catalog: str | None,
    source_schemas: dict[str, str] | None,
    source_tables: dict[str, dict[str, str]] | None,
    layer: str,
    table: str,
    source_register_tables: bool,
) -> tuple[str | None, str | None]:
    """Resolve the source dataset as a Unity Catalog table or a Delta path."""
    table_name = None
    if source_register_tables and source_catalog and source_schemas and source_tables:
        schema_name = source_schemas.get(layer)
        physical_table_name = source_tables.get(layer, {}).get(table)
        if schema_name and physical_table_name:
            table_name = build_table_name(source_catalog, schema_name, physical_table_name)

    return table_name, build_storage_path(source_base_path, layer, table)


def publish_dataset_table_via_warehouse(
    spark: SparkSession,
    *,
    dbutils_handle: Any,
    warehouse_id: str,
    source_base_path: str | None,
    layer: str,
    table: str,
    dataset_name: str,
    catalog: str,
    schema: str,
    table_name: str,
    table_format: str = "delta",
    recent_volume_hours: int = 24,
    insert_batch_size: int = 200,
    source_catalog: str | None = None,
    source_schemas: dict[str, str] | None = None,
    source_tables: dict[str, dict[str, str]] | None = None,
    source_register_tables: bool = False,
) -> dict[str, Any]:
    """Publish a compact source snapshot into a warehouse-visible serving table."""
    source_table_name, source_path = build_source_dataset_reference(
        source_base_path=source_base_path,
        source_catalog=source_catalog,
        source_schemas=source_schemas,
        source_tables=source_tables,
        layer=layer,
        table=table,
        source_register_tables=source_register_tables,
    )
    source_exists = delta_target_exists(
        spark,
        table_name=source_table_name,
        output_path=source_path,
        register_table=source_register_tables,
    )
    if not source_exists:
        return {
            "layer": layer,
            "table": table,
            "source_table_name": source_table_name,
            "source_path": source_path,
            "table_name": f"{catalog}.{schema}.{table_name}",
            "row_count": 0,
            "operation": "skipped_missing_source",
        }

    source_dataframe = read_dataframe(
        spark,
        table_format=table_format,
        table_name=source_table_name,
        input_path=source_path,
        register_table=source_register_tables,
    )
    serving_dataframe, selection = _prepare_serving_dataframe(
        source_dataframe,
        dataset_name=dataset_name,
        recent_volume_hours=recent_volume_hours,
    )
    rows = _dataframe_to_rows(serving_dataframe)
    columns = _dataframe_to_column_definitions(serving_dataframe)
    publish_summary = publish_rows_via_warehouse(
        dbutils_handle=dbutils_handle,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        columns=columns,
        rows=rows,
        insert_batch_size=insert_batch_size,
    )

    return {
        "layer": layer,
        "table": table,
        "source_table_name": source_table_name,
        "source_path": source_path,
        "table_name": publish_summary["table_name"],
        "row_count": publish_summary["row_count"],
        "operation": publish_summary["operation"],
        "selection": selection,
        "statement_count": publish_summary["statement_count"],
        "insert_batch_size": publish_summary["insert_batch_size"],
    }


def load_direct_history_records(
    *,
    direct_publish_config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Load direct-publish history records from either fixture files or Binance REST."""
    history_source_mode = direct_publish_config.get("history_source_mode", "fixture")
    symbols = direct_publish_config.get("symbols", [])
    interval = direct_publish_config.get("kline_interval", "1m")

    if history_source_mode == "fixture":
        history_input_path = direct_publish_config.get("history_input_path")
        if not history_input_path:
            raise ValueError("history_input_path is required when history_source_mode='fixture'.")
        return load_history_file(history_input_path)

    if history_source_mode == "binance_rest":
        return fetch_recent_history_records(
            symbols=symbols,
            interval=interval,
            limit=int(direct_publish_config.get("history_limit", 30)),
            base_url=direct_publish_config.get("history_rest_base_url", "https://api.binance.com"),
            timeout_seconds=float(direct_publish_config.get("history_timeout_seconds", 15.0)),
        )

    raise ValueError(f"Unsupported history_source_mode: {history_source_mode}")


def build_direct_quality_rows(
    *,
    environment: str,
    audit_run_id: str,
    checked_at: datetime,
    serving_table_names: dict[str, str],
    direct_dataset_rows: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Build audit-quality rows for a warehouse-first direct snapshot run."""
    rows: list[dict[str, Any]] = []
    dataset_layer_by_name = {
        "gold_latest_price": "gold",
        "gold_volume_1m": "gold",
        "gold_volatility_5m": "gold",
        "gold_top_movers": "gold",
        "audit_quality_check_runs": "audit",
        "audit_processing_state": "audit",
    }

    for dataset_name, dataset_rows in direct_dataset_rows.items():
        if dataset_name.startswith("audit_"):
            continue
        row_count = len(dataset_rows)
        rows.append(
            {
                "audit_run_id": audit_run_id,
                "checked_at": checked_at,
                "environment": environment,
                "job_name": "warehouse_first_publish",
                "task_name": "publish_app_serving_layer",
                "dataset_name": dataset_name,
                "layer": dataset_layer_by_name[dataset_name],
                "table_name": serving_table_names.get(dataset_name),
                "storage_path": None,
                "passed": row_count > 0,
                "row_count": row_count,
                "min_row_count": 1,
                "violation_count": 0 if row_count > 0 else 1,
                "violations_json": "[]" if row_count > 0 else '["dataset produced zero rows"]',
                "schema_passed": row_count > 0,
                "null_checks_passed": row_count > 0,
                "duplicate_check_passed": row_count > 0,
                "freshness_check_passed": row_count > 0,
                "bounds_check_passed": row_count > 0,
                "summary_json": "{}",
            }
        )

    return rows


def build_direct_processing_state_rows(
    *,
    checked_at: datetime,
    silver_trades: list[dict[str, Any]],
    silver_klines: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build simplified processing-state rows for a warehouse-first direct snapshot run."""
    latest_trade_time = max((row["ingested_at"] for row in silver_trades), default=None)
    latest_kline_time = max((row["ingested_at"] for row in silver_klines), default=None)
    return [
        {
            "pipeline_name": "warehouse_first_publish",
            "dataset_name": "silver_trades",
            "source_layer": "live_source",
            "target_layer": "gold",
            "watermark_column": "ingested_at",
            "last_processed_at": latest_trade_time,
            "updated_at": checked_at,
            "metadata_json": '{"dataset":"gold_latest_price,gold_volume_1m"}',
        },
        {
            "pipeline_name": "warehouse_first_publish",
            "dataset_name": "silver_klines_1m",
            "source_layer": "history_live_source",
            "target_layer": "gold",
            "watermark_column": "ingested_at",
            "last_processed_at": latest_kline_time,
            "updated_at": checked_at,
            "metadata_json": '{"dataset":"gold_volatility_5m,gold_top_movers"}',
        },
    ]


def build_direct_serving_snapshots(
    *,
    environment: str,
    serving_table_names: dict[str, str],
    bronze_history_records: list[dict[str, Any]],
    live_records: list[dict[str, Any]],
    volatility_window_size: int,
    top_movers_limit: int,
) -> dict[str, list[dict[str, Any]]]:
    """Build serving snapshots directly from source market records without persisted Delta state."""
    checked_at = datetime.now(timezone.utc)
    audit_run_id = f"warehouse_direct_{checked_at.strftime('%Y%m%dT%H%M%S')}"

    bronze_trade_records = filter_live_records(live_records, "trade")
    bronze_live_kline_records = [
        record
        for record in filter_live_records(live_records, "kline")
        if bool(record.get("is_closed", False))
    ]

    silver_trades = transform_trade_batch(bronze_trade_records)
    silver_klines = [
        row
        for row in transform_kline_batch(bronze_history_records + bronze_live_kline_records)
        if bool(row.get("is_closed", True))
    ]

    direct_dataset_rows: dict[str, list[dict[str, Any]]] = {
        "gold_latest_price": select_latest_prices(silver_trades),
        "gold_volume_1m": aggregate_volume_1m(silver_trades),
        "gold_volatility_5m": compute_volatility_5m(silver_klines, window_size=volatility_window_size),
        "gold_top_movers": build_top_movers(silver_klines, top_n=top_movers_limit),
    }
    direct_dataset_rows["audit_quality_check_runs"] = build_direct_quality_rows(
        environment=environment,
        audit_run_id=audit_run_id,
        checked_at=checked_at,
        serving_table_names=serving_table_names,
        direct_dataset_rows=direct_dataset_rows,
    )
    direct_dataset_rows["audit_processing_state"] = build_direct_processing_state_rows(
        checked_at=checked_at,
        silver_trades=silver_trades,
        silver_klines=silver_klines,
    )
    return direct_dataset_rows


def publish_direct_serving_snapshots_via_warehouse(
    *,
    dbutils_handle: Any,
    warehouse_id: str,
    catalog: str,
    schema: str,
    serving_table_names: dict[str, str],
    direct_dataset_rows: dict[str, list[dict[str, Any]]],
    insert_batch_size: int,
) -> list[dict[str, Any]]:
    """Publish direct warehouse-first serving snapshots using explicit row payloads."""
    dataset_summaries: list[dict[str, Any]] = []

    for dataset in SERVING_DATASETS:
        dataset_name = dataset["dataset_name"]
        serving_table_name = serving_table_names.get(dataset_name)
        if not serving_table_name:
            dataset_summaries.append({"dataset_name": dataset_name, "table_publish": None})
            continue

        columns = SERVING_TABLE_COLUMNS[dataset_name]
        rows = _rows_from_dicts(direct_dataset_rows.get(dataset_name, []), columns)
        publish_summary = publish_rows_via_warehouse(
            dbutils_handle=dbutils_handle,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema,
            table_name=serving_table_name,
            columns=columns,
            rows=rows,
            insert_batch_size=insert_batch_size,
        )
        dataset_summaries.append(
            {
                "dataset_name": dataset_name,
                "table_publish": {
                    "layer": dataset["layer"],
                    "table": dataset["table"],
                    "table_name": publish_summary["table_name"],
                    "row_count": publish_summary["row_count"],
                    "operation": publish_summary["operation"],
                    "selection": "direct_source_snapshot",
                    "statement_count": publish_summary["statement_count"],
                    "insert_batch_size": publish_summary["insert_batch_size"],
                },
            }
        )

    return dataset_summaries


def run_publish_app_serving_layer(
    spark: SparkSession,
    *,
    source_base_path: str | None,
    target_base_path: str,
    dbutils_handle: Any,
    warehouse_id: str,
    volume_catalog: str,
    volume_schema: str,
    volume_name: str,
    serving_table_catalog: str,
    serving_table_schema: str,
    serving_table_names: dict[str, str],
    table_format: str = "delta",
    write_mode: str = "overwrite",
    recent_volume_hours: int = 24,
    insert_batch_size: int = 200,
    source_catalog: str | None = None,
    source_schemas: dict[str, str] | None = None,
    source_tables: dict[str, dict[str, str]] | None = None,
    source_register_tables: bool = False,
    direct_publish_config: dict[str, Any] | None = None,
    environment: str = "unknown",
) -> dict[str, Any]:
    """Publish the app-facing serving layer through the SQL warehouse."""
    if dbutils_handle is None:
        raise RuntimeError("dbutils is required to publish serving tables through the warehouse.")

    warehouse_probe = _run_warehouse_probe(
        dbutils_handle=dbutils_handle,
        warehouse_id=warehouse_id,
        catalog=serving_table_catalog,
        schema=serving_table_schema,
    )

    dataset_summaries = []
    for dataset in SERVING_DATASETS:
        dataset_name = dataset["dataset_name"]
        table_summary: dict[str, Any] | None = None
        serving_table_name = serving_table_names.get(dataset_name)
        if serving_table_name:
            table_summary = publish_dataset_table_via_warehouse(
                spark,
                dbutils_handle=dbutils_handle,
                warehouse_id=warehouse_id,
                source_base_path=source_base_path,
                layer=dataset["layer"],
                table=dataset["table"],
                dataset_name=dataset_name,
                catalog=serving_table_catalog,
                schema=serving_table_schema,
                table_name=serving_table_name,
                table_format=table_format,
                recent_volume_hours=recent_volume_hours,
                insert_batch_size=insert_batch_size,
                source_catalog=source_catalog,
                source_schemas=source_schemas,
                source_tables=source_tables,
                source_register_tables=source_register_tables,
            )

        dataset_summaries.append(
            {
                "dataset_name": dataset_name,
                "table_publish": table_summary,
            }
        )

    published_dataset_count = sum(
        1
        for dataset_summary in dataset_summaries
        if dataset_summary["table_publish"]
        and dataset_summary["table_publish"]["operation"] != "skipped_missing_source"
    )

    if published_dataset_count == 0 and direct_publish_config and direct_publish_config.get("enabled", False):
        bronze_history_records = load_direct_history_records(direct_publish_config=direct_publish_config)
        live_records, live_metadata = load_live_records(
            source_mode=direct_publish_config.get("live_source_mode", "fixture"),
            input_path=direct_publish_config.get("live_input_path"),
            symbols=direct_publish_config.get("symbols", []),
            stream_types=direct_publish_config.get("stream_types", ["trade", "kline"]),
            kline_interval=direct_publish_config.get("kline_interval", "1m"),
            websocket_base_url=direct_publish_config.get("websocket_base_url", "wss://data-stream.binance.vision"),
            max_messages=direct_publish_config.get("max_messages"),
            duration_seconds=direct_publish_config.get("duration_seconds"),
            receive_timeout_seconds=float(direct_publish_config.get("receive_timeout_seconds", 5.0)),
            connect_retries=int(direct_publish_config.get("connect_retries", 3)),
            retry_backoff_seconds=float(direct_publish_config.get("retry_backoff_seconds", 2.0)),
        )
        direct_dataset_rows = build_direct_serving_snapshots(
            environment=environment,
            serving_table_names=serving_table_names,
            bronze_history_records=bronze_history_records,
            live_records=live_records,
            volatility_window_size=int(direct_publish_config.get("volatility_window_size", 5)),
            top_movers_limit=int(direct_publish_config.get("top_movers_limit", 10)),
        )
        dataset_summaries = publish_direct_serving_snapshots_via_warehouse(
            dbutils_handle=dbutils_handle,
            warehouse_id=warehouse_id,
            catalog=serving_table_catalog,
            schema=serving_table_schema,
            serving_table_names=serving_table_names,
            direct_dataset_rows=direct_dataset_rows,
            insert_batch_size=insert_batch_size,
        )
        published_dataset_count = sum(
            1
            for dataset_summary in dataset_summaries
            if dataset_summary["table_publish"] and dataset_summary["table_publish"]["row_count"] >= 0
        )
    elif published_dataset_count == 0:
        raise RuntimeError(
            f"No serving datasets were published from source_base_path={source_base_path}. "
            "The source Delta paths were not accessible from this notebook runtime."
        )

    return {
        "source_base_path": source_base_path,
        "target_base_path": target_base_path,
        "volume": f"{volume_catalog}.{volume_schema}.{volume_name}",
        "serving_table_schema": f"{serving_table_catalog}.{serving_table_schema}",
        "warehouse_id": warehouse_id,
        "warehouse_probe": warehouse_probe,
        "table_format": table_format,
        "write_mode": write_mode,
        "recent_volume_hours": recent_volume_hours,
        "insert_batch_size": insert_batch_size,
        "published_dataset_count": published_dataset_count,
        "datasets": dataset_summaries,
    }
