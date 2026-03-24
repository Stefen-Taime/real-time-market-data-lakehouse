"""Orchestrate Gold metric builds on top of Silver Delta datasets."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.gold.build_price_latest import build_latest_price_dataframe
from src.gold.build_top_movers import build_top_movers_dataframe
from src.gold.build_volatility_5m import build_volatility_5m_dataframe
from src.gold.build_volume_1m import build_volume_1m_dataframe
from src.utils.lakehouse_io import (
    build_storage_path,
    build_table_name,
    delta_target_exists,
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

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


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


def _write_gold_dataset(
    spark: SparkSession,
    dataframe: DataFrame,
    *,
    catalog: str | None,
    gold_schema: str,
    table: str,
    base_output_path: str | None,
    table_format: str,
    write_mode: str,
    register_tables: bool,
    merge_key_columns: list[str],
    delete_not_matched_by_source: bool = False,
) -> dict[str, Any]:
    table_name, output_path = _dataset_reference(
        catalog=catalog,
        schema=gold_schema,
        table=table,
        base_output_path=base_output_path,
        layer="gold",
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
            delete_not_matched_by_source=delete_not_matched_by_source,
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


def _empty_gold_output_summary(
    *,
    catalog: str | None,
    gold_schema: str,
    table: str,
    base_output_path: str | None,
    register_tables: bool,
) -> dict[str, Any]:
    table_name, output_path = _dataset_reference(
        catalog=catalog,
        schema=gold_schema,
        table=table,
        base_output_path=base_output_path,
        layer="gold",
        register_tables=register_tables,
    )
    return {
        "table": table_name,
        "output_path": output_path,
        "row_count": 0,
        "operation": "skipped",
    }


def _read_existing_gold_dataset(
    spark: SparkSession,
    *,
    catalog: str | None,
    gold_schema: str,
    table: str,
    base_output_path: str | None,
    table_format: str,
    register_tables: bool,
):
    table_name, input_path = _dataset_reference(
        catalog=catalog,
        schema=gold_schema,
        table=table,
        base_output_path=base_output_path,
        layer="gold",
        register_tables=register_tables,
    )
    if not delta_target_exists(
        spark,
        table_name=table_name,
        output_path=input_path,
        register_table=register_tables,
    ):
        return None

    return read_dataframe(
        spark,
        table_format=table_format,
        table_name=table_name,
        input_path=input_path,
        register_table=register_tables,
    )


def run_gold_build(
    spark: SparkSession,
    *,
    catalog: str | None,
    silver_schema: str,
    gold_schema: str,
    audit_schema: str,
    processing_state_table: str,
    silver_trades_table: str,
    silver_klines_table: str,
    gold_latest_price_table: str,
    gold_volume_1m_table: str,
    gold_volatility_5m_table: str,
    gold_top_movers_table: str,
    base_output_path: str | None,
    table_format: str = "delta",
    write_mode: str = "overwrite",
    register_tables: bool = False,
    volatility_window_size: int = 5,
    top_movers_limit: int = 10,
    watermark_column: str = "ingested_at",
    watermark_overlap_seconds: int = 0,
    pipeline_name: str = "build_gold_metrics",
) -> dict[str, Any]:
    """Build Gold datasets from Silver trades and klines."""
    silver_trades_table_name, silver_trades_path = _dataset_reference(
        catalog=catalog,
        schema=silver_schema,
        table=silver_trades_table,
        base_output_path=base_output_path,
        layer="silver",
        register_tables=register_tables,
    )
    silver_klines_table_name, silver_klines_path = _dataset_reference(
        catalog=catalog,
        schema=silver_schema,
        table=silver_klines_table,
        base_output_path=base_output_path,
        layer="silver",
        register_tables=register_tables,
    )

    if register_tables:
        ensure_schema_exists(spark, gold_schema, catalog=catalog)

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

    silver_trades_dataframe = read_dataframe(
        spark,
        table_format=table_format,
        table_name=silver_trades_table_name,
        input_path=silver_trades_path,
        register_table=register_tables,
    )
    silver_klines_dataframe = read_dataframe(
        spark,
        table_format=table_format,
        table_name=silver_klines_table_name,
        input_path=silver_klines_path,
        register_table=register_tables,
    )

    trades_state_key = build_state_key(pipeline_name, silver_trades_table)
    klines_state_key = build_state_key(pipeline_name, silver_klines_table)
    trades_last_processed_at = processing_state.get(trades_state_key, {}).get("last_processed_at")
    klines_last_processed_at = processing_state.get(klines_state_key, {}).get("last_processed_at")

    silver_trades_incremental_dataframe = (
        filter_dataframe_by_watermark(
            silver_trades_dataframe,
            watermark_column=watermark_column,
            last_processed_at=trades_last_processed_at,
            overlap_seconds=watermark_overlap_seconds,
        )
        if incremental_enabled
        else silver_trades_dataframe
    )
    silver_klines_incremental_dataframe = (
        filter_dataframe_by_watermark(
            silver_klines_dataframe,
            watermark_column=watermark_column,
            last_processed_at=klines_last_processed_at,
            overlap_seconds=watermark_overlap_seconds,
        )
        if incremental_enabled
        else silver_klines_dataframe
    )
    trades_source_max_timestamp = get_max_timestamp(silver_trades_incremental_dataframe, column=watermark_column)
    klines_source_max_timestamp = get_max_timestamp(silver_klines_incremental_dataframe, column=watermark_column)
    trades_changed = trades_source_max_timestamp is not None and dataframe_has_rows(silver_trades_incremental_dataframe)
    klines_changed = klines_source_max_timestamp is not None and dataframe_has_rows(silver_klines_incremental_dataframe)

    outputs: dict[str, Any] = {}
    state_updates: list[dict[str, Any]] = []

    if trades_changed:
        from pyspark.sql import functions as F

        affected_symbols_dataframe = silver_trades_incremental_dataframe.select("symbol").distinct()
        incremental_latest_candidates = silver_trades_incremental_dataframe.select(
            "symbol",
            "price",
            "trade_time",
            "event_time",
            "ingested_at",
            "source",
        )
        existing_latest_dataframe = _read_existing_gold_dataset(
            spark,
            catalog=catalog,
            gold_schema=gold_schema,
            table=gold_latest_price_table,
            base_output_path=base_output_path,
            table_format=table_format,
            register_tables=register_tables,
        )
        if existing_latest_dataframe is not None:
            existing_latest_candidates = (
                existing_latest_dataframe.join(affected_symbols_dataframe, "symbol", "inner")
                .select(
                    "symbol",
                    F.col("latest_price").alias("price"),
                    "trade_time",
                    F.col("trade_time").alias("event_time"),
                    F.col("trade_time").alias("ingested_at"),
                    "source",
                )
            )
            latest_price_source_dataframe = incremental_latest_candidates.unionByName(
                existing_latest_candidates,
                allowMissingColumns=True,
            )
        else:
            latest_price_source_dataframe = incremental_latest_candidates

        latest_price_dataframe = build_latest_price_dataframe(latest_price_source_dataframe)
        outputs["latest_price"] = _write_gold_dataset(
            spark,
            latest_price_dataframe,
            catalog=catalog,
            gold_schema=gold_schema,
            table=gold_latest_price_table,
            base_output_path=base_output_path,
            table_format=table_format,
            write_mode=write_mode,
            register_tables=register_tables,
            merge_key_columns=["symbol"],
        )

        affected_windows_dataframe = silver_trades_incremental_dataframe.withColumn(
            "window_start",
            F.date_trunc("minute", F.col("trade_time")),
        ).select("symbol", "window_start").distinct()
        relevant_trades_dataframe = (
            silver_trades_dataframe.withColumn("window_start", F.date_trunc("minute", F.col("trade_time")))
            .join(affected_windows_dataframe, ["symbol", "window_start"], "inner")
            .drop("window_start")
        )
        volume_1m_dataframe = build_volume_1m_dataframe(relevant_trades_dataframe)
        outputs["volume_1m"] = _write_gold_dataset(
            spark,
            volume_1m_dataframe,
            catalog=catalog,
            gold_schema=gold_schema,
            table=gold_volume_1m_table,
            base_output_path=base_output_path,
            table_format=table_format,
            write_mode=write_mode,
            register_tables=register_tables,
            merge_key_columns=["symbol", "window_start"],
        )

        state_updates.append(
            {
                "state_key": trades_state_key,
                "pipeline_name": pipeline_name,
                "dataset_name": silver_trades_table,
                "source_layer": "silver",
                "target_layer": "gold",
                "watermark_column": watermark_column,
                "last_processed_at": trades_source_max_timestamp,
                "metadata": {
                    "targets": [gold_latest_price_table, gold_volume_1m_table],
                    "write_mode": write_mode,
                },
            }
        )
    else:
        outputs["latest_price"] = _empty_gold_output_summary(
            catalog=catalog,
            gold_schema=gold_schema,
            table=gold_latest_price_table,
            base_output_path=base_output_path,
            register_tables=register_tables,
        )
        outputs["volume_1m"] = _empty_gold_output_summary(
            catalog=catalog,
            gold_schema=gold_schema,
            table=gold_volume_1m_table,
            base_output_path=base_output_path,
            register_tables=register_tables,
        )

    if klines_changed:
        affected_symbols_dataframe = silver_klines_incremental_dataframe.select("symbol").distinct()
        relevant_klines_dataframe = silver_klines_dataframe.join(affected_symbols_dataframe, "symbol", "inner")
        volatility_5m_dataframe = build_volatility_5m_dataframe(
            relevant_klines_dataframe,
            window_size=volatility_window_size,
        )
        top_movers_dataframe = build_top_movers_dataframe(
            silver_klines_dataframe,
            top_n=top_movers_limit,
        )
        outputs["volatility_5m"] = _write_gold_dataset(
            spark,
            volatility_5m_dataframe,
            catalog=catalog,
            gold_schema=gold_schema,
            table=gold_volatility_5m_table,
            base_output_path=base_output_path,
            table_format=table_format,
            write_mode=write_mode,
            register_tables=register_tables,
            merge_key_columns=["symbol", "window_start"],
        )
        outputs["top_movers"] = _write_gold_dataset(
            spark,
            top_movers_dataframe,
            catalog=catalog,
            gold_schema=gold_schema,
            table=gold_top_movers_table,
            base_output_path=base_output_path,
            table_format=table_format,
            write_mode=write_mode,
            register_tables=register_tables,
            merge_key_columns=["symbol"],
            delete_not_matched_by_source=(write_mode == "merge"),
        )
        state_updates.append(
            {
                "state_key": klines_state_key,
                "pipeline_name": pipeline_name,
                "dataset_name": silver_klines_table,
                "source_layer": "silver",
                "target_layer": "gold",
                "watermark_column": watermark_column,
                "last_processed_at": klines_source_max_timestamp,
                "metadata": {
                    "targets": [gold_volatility_5m_table, gold_top_movers_table],
                    "write_mode": write_mode,
                },
            }
        )
    else:
        outputs["volatility_5m"] = _empty_gold_output_summary(
            catalog=catalog,
            gold_schema=gold_schema,
            table=gold_volatility_5m_table,
            base_output_path=base_output_path,
            register_tables=register_tables,
        )
        outputs["top_movers"] = _empty_gold_output_summary(
            catalog=catalog,
            gold_schema=gold_schema,
            table=gold_top_movers_table,
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
        "inputs": {
            "silver_trades": {
                "table": silver_trades_table_name,
                "path": silver_trades_path,
                "state_key": trades_state_key,
                "last_processed_at": trades_last_processed_at,
                "source_max_timestamp": trades_source_max_timestamp,
            },
            "silver_klines": {
                "table": silver_klines_table_name,
                "path": silver_klines_path,
                "state_key": klines_state_key,
                "last_processed_at": klines_last_processed_at,
                "source_max_timestamp": klines_source_max_timestamp,
            },
        },
        "outputs": outputs,
        "processing_state": state_summary,
        "table_format": table_format,
        "write_mode": write_mode,
        "register_tables": register_tables,
        "volatility_window_size": volatility_window_size,
        "top_movers_limit": top_movers_limit,
        "incremental_enabled": incremental_enabled,
        "watermark_column": watermark_column,
        "watermark_overlap_seconds": watermark_overlap_seconds,
    }
