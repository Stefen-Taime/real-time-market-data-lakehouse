"""Orchestrate Silver layer builds from Bronze Delta datasets."""

from __future__ import annotations

from typing import Any

from src.transforms.bronze_to_silver_klines import build_silver_klines_dataframe
from src.transforms.bronze_to_silver_trades import build_silver_trades_dataframe
from src.utils.lakehouse_io import (
    build_storage_path,
    build_table_name,
    ensure_schema_exists,
    merge_dataframe,
    read_dataframe,
    write_dataframe,
)
from src.utils.processing_state import (
    build_state_key,
    dataframe_has_rows,
    filter_dataframe_by_watermark,
    get_max_timestamp,
    load_processing_state,
    write_processing_state,
)


def _dataset_reference(
    *,
    catalog: str | None,
    schema: str,
    table: str,
    base_output_path: str | None,
    layer: str,
    register_tables: bool,
) -> tuple[str | None, str | None]:
    table_name = build_table_name(catalog, schema, table) if register_tables else None
    storage_path = build_storage_path(base_output_path, layer, table)
    return table_name, storage_path


def _write_silver_dataset(
    spark,
    dataframe,
    *,
    catalog: str | None,
    silver_schema: str,
    table: str,
    base_output_path: str | None,
    table_format: str,
    write_mode: str,
    register_tables: bool,
    merge_key_columns: list[str],
) -> dict[str, Any]:
    table_name, output_path = _dataset_reference(
        catalog=catalog,
        schema=silver_schema,
        table=table,
        base_output_path=base_output_path,
        layer="silver",
        register_tables=register_tables,
    )
    row_count = dataframe.count()
    operation = write_mode
    if write_mode == "merge":
        operation = merge_dataframe(
            spark,
            dataframe,
            key_columns=merge_key_columns,
            table_format=table_format,
            table_name=table_name,
            output_path=output_path,
            register_table=register_tables,
        )
    else:
        write_dataframe(
            dataframe,
            table_format=table_format,
            mode=write_mode,
            table_name=table_name,
            output_path=output_path,
            register_table=register_tables,
        )
    return {
        "table": table_name,
        "output_path": output_path,
        "row_count": row_count,
        "operation": operation,
    }


def _empty_output_summary(
    *,
    catalog: str | None,
    silver_schema: str,
    table: str,
    base_output_path: str | None,
    register_tables: bool,
) -> dict[str, Any]:
    table_name, output_path = _dataset_reference(
        catalog=catalog,
        schema=silver_schema,
        table=table,
        base_output_path=base_output_path,
        layer="silver",
        register_tables=register_tables,
    )
    return {
        "table": table_name,
        "output_path": output_path,
        "row_count": 0,
        "operation": "skipped",
    }


def run_silver_transform(
    spark,
    *,
    catalog: str | None,
    bronze_schema: str,
    silver_schema: str,
    audit_schema: str,
    processing_state_table: str,
    bronze_trades_table: str,
    bronze_klines_history_table: str,
    bronze_klines_live_table: str,
    silver_trades_table: str,
    silver_klines_table: str,
    base_output_path: str | None,
    table_format: str = "delta",
    write_mode: str = "overwrite",
    register_tables: bool = False,
    transform_trades: bool = True,
    transform_klines: bool = True,
    watermark_column: str = "ingested_at",
    watermark_overlap_seconds: int = 0,
    pipeline_name: str = "transform_silver",
) -> dict[str, Any]:
    """Build Silver datasets from Bronze trades and combined historical/live klines."""
    if not transform_trades and not transform_klines:
        raise ValueError("At least one Silver transformation must be enabled.")

    if register_tables:
        ensure_schema_exists(spark, silver_schema, catalog=catalog)

    inputs: dict[str, Any] = {}
    outputs: dict[str, Any] = {}
    state_updates: list[dict[str, Any]] = []
    incremental_enabled = write_mode == "merge"
    processing_state = (
        load_processing_state(
            spark,
            catalog=catalog,
            audit_schema=audit_schema,
            state_table=processing_state_table,
            base_output_path=base_output_path,
            table_format=table_format,
            register_tables=register_tables,
            pipeline_name=pipeline_name,
        )
        if incremental_enabled
        else {}
    )

    if transform_trades:
        trade_state_key = build_state_key(pipeline_name, bronze_trades_table)
        bronze_trades_table_name, bronze_trades_path = _dataset_reference(
            catalog=catalog,
            schema=bronze_schema,
            table=bronze_trades_table,
            base_output_path=base_output_path,
            layer="bronze",
            register_tables=register_tables,
        )
        bronze_trades_dataframe = read_dataframe(
            spark,
            table_format=table_format,
            table_name=bronze_trades_table_name,
            input_path=bronze_trades_path,
            register_table=register_tables,
        )
        last_processed_at = processing_state.get(trade_state_key, {}).get("last_processed_at")
        bronze_trades_incremental_dataframe = (
            filter_dataframe_by_watermark(
                bronze_trades_dataframe,
                watermark_column=watermark_column,
                last_processed_at=last_processed_at,
                overlap_seconds=watermark_overlap_seconds,
            )
            if incremental_enabled
            else bronze_trades_dataframe
        )
        source_max_timestamp = get_max_timestamp(bronze_trades_incremental_dataframe, column=watermark_column)
        inputs["bronze_trades"] = {
            "table": bronze_trades_table_name,
            "path": bronze_trades_path,
            "state_key": trade_state_key,
            "last_processed_at": last_processed_at,
            "watermark_column": watermark_column,
            "watermark_overlap_seconds": watermark_overlap_seconds,
            "source_max_timestamp": source_max_timestamp,
        }

        if source_max_timestamp is not None and dataframe_has_rows(bronze_trades_incremental_dataframe):
            silver_trades_dataframe = build_silver_trades_dataframe(bronze_trades_incremental_dataframe)
            outputs["trades"] = _write_silver_dataset(
                spark,
                silver_trades_dataframe,
                catalog=catalog,
                silver_schema=silver_schema,
                table=silver_trades_table,
                base_output_path=base_output_path,
                table_format=table_format,
                write_mode=write_mode,
                register_tables=register_tables,
                merge_key_columns=["symbol", "trade_id"],
            )
            state_updates.append(
                {
                    "state_key": trade_state_key,
                    "pipeline_name": pipeline_name,
                    "dataset_name": bronze_trades_table,
                    "source_layer": "bronze",
                    "target_layer": "silver",
                    "watermark_column": watermark_column,
                    "last_processed_at": source_max_timestamp,
                    "metadata": {
                        "target_table": silver_trades_table,
                        "write_mode": write_mode,
                    },
                }
            )
        else:
            outputs["trades"] = _empty_output_summary(
                catalog=catalog,
                silver_schema=silver_schema,
                table=silver_trades_table,
                base_output_path=base_output_path,
                register_tables=register_tables,
            )

    if transform_klines:
        history_state_key = build_state_key(pipeline_name, bronze_klines_history_table)
        live_state_key = build_state_key(pipeline_name, bronze_klines_live_table)
        bronze_klines_history_table_name, bronze_klines_history_path = _dataset_reference(
            catalog=catalog,
            schema=bronze_schema,
            table=bronze_klines_history_table,
            base_output_path=base_output_path,
            layer="bronze",
            register_tables=register_tables,
        )
        bronze_klines_live_table_name, bronze_klines_live_path = _dataset_reference(
            catalog=catalog,
            schema=bronze_schema,
            table=bronze_klines_live_table,
            base_output_path=base_output_path,
            layer="bronze",
            register_tables=register_tables,
        )
        bronze_klines_history_dataframe = read_dataframe(
            spark,
            table_format=table_format,
            table_name=bronze_klines_history_table_name,
            input_path=bronze_klines_history_path,
            register_table=register_tables,
        )
        bronze_klines_live_dataframe = read_dataframe(
            spark,
            table_format=table_format,
            table_name=bronze_klines_live_table_name,
            input_path=bronze_klines_live_path,
            register_table=register_tables,
        )
        history_last_processed_at = processing_state.get(history_state_key, {}).get("last_processed_at")
        live_last_processed_at = processing_state.get(live_state_key, {}).get("last_processed_at")
        bronze_klines_history_incremental_dataframe = (
            filter_dataframe_by_watermark(
                bronze_klines_history_dataframe,
                watermark_column=watermark_column,
                last_processed_at=history_last_processed_at,
                overlap_seconds=watermark_overlap_seconds,
            )
            if incremental_enabled
            else bronze_klines_history_dataframe
        )
        bronze_klines_live_incremental_dataframe = (
            filter_dataframe_by_watermark(
                bronze_klines_live_dataframe,
                watermark_column=watermark_column,
                last_processed_at=live_last_processed_at,
                overlap_seconds=watermark_overlap_seconds,
            )
            if incremental_enabled
            else bronze_klines_live_dataframe
        )
        history_max_timestamp = get_max_timestamp(bronze_klines_history_incremental_dataframe, column=watermark_column)
        live_max_timestamp = get_max_timestamp(bronze_klines_live_incremental_dataframe, column=watermark_column)
        inputs["bronze_klines_history"] = {
            "table": bronze_klines_history_table_name,
            "path": bronze_klines_history_path,
            "state_key": history_state_key,
            "last_processed_at": history_last_processed_at,
            "watermark_column": watermark_column,
            "watermark_overlap_seconds": watermark_overlap_seconds,
            "source_max_timestamp": history_max_timestamp,
        }
        inputs["bronze_klines_live"] = {
            "table": bronze_klines_live_table_name,
            "path": bronze_klines_live_path,
            "state_key": live_state_key,
            "last_processed_at": live_last_processed_at,
            "watermark_column": watermark_column,
            "watermark_overlap_seconds": watermark_overlap_seconds,
            "source_max_timestamp": live_max_timestamp,
        }

        bronze_klines_dataframe = bronze_klines_history_incremental_dataframe.unionByName(
            bronze_klines_live_incremental_dataframe,
            allowMissingColumns=True,
        )
        if dataframe_has_rows(bronze_klines_dataframe):
            silver_klines_dataframe = build_silver_klines_dataframe(bronze_klines_dataframe)
            outputs["klines_1m"] = _write_silver_dataset(
                spark,
                silver_klines_dataframe,
                catalog=catalog,
                silver_schema=silver_schema,
                table=silver_klines_table,
                base_output_path=base_output_path,
                table_format=table_format,
                write_mode=write_mode,
                register_tables=register_tables,
                merge_key_columns=["symbol", "interval", "open_time"],
            )
            if history_max_timestamp is not None:
                state_updates.append(
                    {
                        "state_key": history_state_key,
                        "pipeline_name": pipeline_name,
                        "dataset_name": bronze_klines_history_table,
                        "source_layer": "bronze",
                        "target_layer": "silver",
                        "watermark_column": watermark_column,
                        "last_processed_at": history_max_timestamp,
                        "metadata": {
                            "target_table": silver_klines_table,
                            "write_mode": write_mode,
                        },
                    }
                )
            if live_max_timestamp is not None:
                state_updates.append(
                    {
                        "state_key": live_state_key,
                        "pipeline_name": pipeline_name,
                        "dataset_name": bronze_klines_live_table,
                        "source_layer": "bronze",
                        "target_layer": "silver",
                        "watermark_column": watermark_column,
                        "last_processed_at": live_max_timestamp,
                        "metadata": {
                            "target_table": silver_klines_table,
                            "write_mode": write_mode,
                        },
                    }
                )
        else:
            outputs["klines_1m"] = _empty_output_summary(
                catalog=catalog,
                silver_schema=silver_schema,
                table=silver_klines_table,
                base_output_path=base_output_path,
                register_tables=register_tables,
            )

    state_summary = (
        write_processing_state(
            spark,
            catalog=catalog,
            audit_schema=audit_schema,
            state_table=processing_state_table,
            base_output_path=base_output_path,
            states=state_updates,
            table_format=table_format,
            register_tables=register_tables,
        )
        if incremental_enabled
        else {
            "table": None,
            "output_path": None,
            "row_count": 0,
            "operation": "disabled",
        }
    )

    return {
        "inputs": inputs,
        "outputs": outputs,
        "processing_state": state_summary,
        "table_format": table_format,
        "write_mode": write_mode,
        "register_tables": register_tables,
        "transform_trades": transform_trades,
        "transform_klines": transform_klines,
        "incremental_enabled": incremental_enabled,
        "watermark_column": watermark_column,
        "watermark_overlap_seconds": watermark_overlap_seconds,
    }
