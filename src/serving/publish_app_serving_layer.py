"""Publish app-facing serving datasets through a Databricks SQL warehouse."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.utils.databricks_warehouse_sql import (
    build_create_or_replace_empty_table_statement,
    build_create_schema_statement,
    build_insert_statement,
    chunk_rows,
    execute_sql_statement_with_context,
)
from src.utils.lakehouse_io import build_storage_path, delta_target_exists, read_dataframe

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


def publish_dataset_table_via_warehouse(
    spark: SparkSession,
    *,
    dbutils_handle: Any,
    warehouse_id: str,
    source_base_path: str,
    layer: str,
    table: str,
    dataset_name: str,
    catalog: str,
    schema: str,
    table_name: str,
    table_format: str = "delta",
    recent_volume_hours: int = 24,
    insert_batch_size: int = 200,
) -> dict[str, Any]:
    """Publish a compact source snapshot into a warehouse-visible serving table."""
    source_path = build_storage_path(source_base_path, layer, table)
    source_exists = delta_target_exists(
        spark,
        output_path=source_path,
        register_table=False,
    )
    if not source_exists:
        return {
            "layer": layer,
            "table": table,
            "source_path": source_path,
            "table_name": f"{catalog}.{schema}.{table_name}",
            "row_count": 0,
            "operation": "skipped_missing_source",
        }

    source_dataframe = read_dataframe(
        spark,
        table_format=table_format,
        input_path=source_path,
        register_table=False,
    )
    serving_dataframe, selection = _prepare_serving_dataframe(
        source_dataframe,
        dataset_name=dataset_name,
        recent_volume_hours=recent_volume_hours,
    )
    rows = _dataframe_to_rows(serving_dataframe)
    columns = _dataframe_to_column_definitions(serving_dataframe)
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
        "layer": layer,
        "table": table,
        "source_path": source_path,
        "table_name": full_table_name,
        "row_count": len(rows),
        "operation": "warehouse_replace",
        "selection": selection,
        "statement_count": statement_count,
        "insert_batch_size": insert_batch_size,
    }


def run_publish_app_serving_layer(
    spark: SparkSession,
    *,
    source_base_path: str,
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
    if published_dataset_count == 0:
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
