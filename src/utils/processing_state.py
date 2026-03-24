"""Helpers for watermark-based incremental processing state."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import TYPE_CHECKING, Any

from src.utils.lakehouse_io import (
    build_storage_path,
    build_table_name,
    delta_target_exists,
    ensure_schema_exists,
    merge_dataframe,
    read_dataframe,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def normalize_timestamp(value: datetime | None) -> datetime | None:
    """Normalize a timestamp to UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def build_state_key(pipeline_name: str, dataset_name: str) -> str:
    """Build a deterministic processing-state key."""
    return f"{pipeline_name}:{dataset_name}"


def load_processing_state(
    spark: SparkSession,
    *,
    catalog: str | None,
    audit_schema: str,
    state_table: str,
    base_output_path: str | None,
    table_format: str,
    register_tables: bool,
    pipeline_name: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Load processing state rows keyed by state_key when a state table already exists."""
    table_name = build_table_name(catalog, audit_schema, state_table) if register_tables else None
    input_path = build_storage_path(base_output_path, "audit", state_table)
    target_exists = delta_target_exists(
        spark,
        table_name=table_name,
        output_path=input_path,
        register_table=register_tables,
    )
    if not target_exists:
        return {}

    dataframe = read_dataframe(
        spark,
        table_format=table_format,
        table_name=table_name,
        input_path=input_path,
        register_table=register_tables,
    )
    if pipeline_name is not None:
        from pyspark.sql import functions as F

        dataframe = dataframe.where(F.col("pipeline_name") == F.lit(pipeline_name))

    rows = dataframe.collect()
    return {
        row["state_key"]: {
            "pipeline_name": row["pipeline_name"],
            "dataset_name": row["dataset_name"],
            "source_layer": row["source_layer"],
            "target_layer": row["target_layer"],
            "watermark_column": row["watermark_column"],
            "last_processed_at": normalize_timestamp(row["last_processed_at"]),
            "updated_at": normalize_timestamp(row["updated_at"]),
            "metadata_json": row["metadata_json"],
        }
        for row in rows
    }


def filter_dataframe_by_watermark(
    dataframe: DataFrame,
    *,
    watermark_column: str,
    last_processed_at: datetime | None,
    overlap_seconds: int = 0,
) -> DataFrame:
    """Filter a DataFrame to rows newer than the persisted watermark minus optional overlap."""
    if last_processed_at is None:
        return dataframe

    from pyspark.sql import functions as F

    effective_watermark = last_processed_at - timedelta(seconds=max(overlap_seconds, 0))
    return dataframe.where(F.col(watermark_column) > F.lit(effective_watermark))


def dataframe_has_rows(dataframe: DataFrame) -> bool:
    """Return True when a DataFrame contains at least one row."""
    return bool(dataframe.limit(1).count())


def get_max_timestamp(dataframe: DataFrame, *, column: str) -> datetime | None:
    """Return the maximum timestamp value observed for a column."""
    from pyspark.sql import functions as F

    row = dataframe.select(F.max(F.col(column)).alias("max_timestamp")).first()
    if row is None:
        return None
    return normalize_timestamp(row["max_timestamp"])


def build_processing_state_rows(
    states: list[dict[str, Any]],
    *,
    updated_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Normalize state payloads for persistence."""
    if updated_at is None:
        updated_at = datetime.now(timezone.utc)
    normalized_updated_at = normalize_timestamp(updated_at) or datetime.now(timezone.utc)

    rows: list[dict[str, Any]] = []
    for state in states:
        rows.append(
            {
                "state_key": state["state_key"],
                "pipeline_name": state["pipeline_name"],
                "dataset_name": state["dataset_name"],
                "source_layer": state["source_layer"],
                "target_layer": state["target_layer"],
                "watermark_column": state["watermark_column"],
                "last_processed_at": normalize_timestamp(state["last_processed_at"]),
                "updated_at": normalized_updated_at,
                "metadata_json": json.dumps(state.get("metadata", {}), sort_keys=True, default=str),
            }
        )
    return rows


def write_processing_state(
    spark: SparkSession,
    *,
    catalog: str | None,
    audit_schema: str,
    state_table: str,
    base_output_path: str | None,
    states: list[dict[str, Any]],
    table_format: str = "delta",
    register_tables: bool = False,
) -> dict[str, Any]:
    """Persist incremental watermark state via Delta MERGE."""
    rows = build_processing_state_rows(states)
    table_name = build_table_name(catalog, audit_schema, state_table) if register_tables else None
    output_path = build_storage_path(base_output_path, "audit", state_table)

    if not rows:
        return {
            "table": table_name,
            "output_path": output_path,
            "row_count": 0,
            "operation": "skipped",
        }

    if register_tables:
        ensure_schema_exists(spark, audit_schema, catalog=catalog)

    dataframe = spark.createDataFrame(rows)
    operation = merge_dataframe(
        spark,
        dataframe,
        key_columns=["state_key"],
        table_format=table_format,
        table_name=table_name,
        output_path=output_path,
        register_table=register_tables,
    )
    return {
        "table": table_name,
        "output_path": output_path,
        "row_count": len(rows),
        "operation": operation,
    }
