"""Shared helpers for reading and writing lakehouse datasets with Spark."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def build_table_name(catalog: str | None, schema: str, table: str) -> str:
    """Build a fully qualified Spark table name."""
    return ".".join(part for part in (catalog, schema, table) if part)


def build_storage_path(base_path: str | None, layer: str, table: str) -> str | None:
    """Build a deterministic storage path for a dataset layer."""
    if not base_path:
        return None
    return f"{base_path.rstrip('/')}/{layer}/{table}"


def ensure_schema_exists(spark: SparkSession, schema: str, catalog: str | None = None) -> None:
    """Create a schema when table registration is enabled."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {build_table_name(catalog, schema, '')}".rstrip("."))


def build_merge_condition(
    key_columns: list[str],
    *,
    source_alias: str = "source",
    target_alias: str = "target",
) -> str:
    """Build a null-safe Delta merge condition from a set of key columns."""
    if not key_columns:
        raise ValueError("key_columns must contain at least one column.")

    return " AND ".join(
        f"{target_alias}.`{column}` <=> {source_alias}.`{column}`"
        for column in key_columns
    )


def delta_target_exists(
    spark: SparkSession,
    *,
    table_name: str | None = None,
    output_path: str | None = None,
    register_table: bool = False,
) -> bool:
    """Return True when a Delta target already exists as a table or at a storage path."""
    if register_table:
        if not table_name:
            raise ValueError("table_name is required when register_table=True.")
        return bool(spark.catalog.tableExists(table_name))

    if not output_path:
        raise ValueError("output_path is required when register_table=False.")

    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        path = spark._jvm.org.apache.hadoop.fs.Path(output_path)
        filesystem = path.getFileSystem(hadoop_conf)
        return bool(filesystem.exists(path))
    except Exception:
        # Serverless Spark Connect runtimes do not expose JVM-backed filesystem handles.
        try:
            spark.read.format("delta").load(output_path).limit(1).collect()
            return True
        except Exception:
            return False


def write_dataframe(
    dataframe: DataFrame,
    *,
    table_format: str,
    mode: str,
    table_name: str | None = None,
    output_path: str | None = None,
    register_table: bool = False,
) -> None:
    """Write a Spark DataFrame either to a path or as a registered table."""
    writer = dataframe.write.format(table_format).mode(mode)

    if register_table:
        if not table_name:
            raise ValueError("table_name is required when register_table=True.")
        if output_path:
            writer = writer.option("path", output_path)
        writer.saveAsTable(table_name)
        return

    if not output_path:
        raise ValueError("output_path is required when register_table=False.")

    writer.save(output_path)


def merge_dataframe(
    spark: SparkSession,
    dataframe: DataFrame,
    *,
    key_columns: list[str],
    table_format: str,
    table_name: str | None = None,
    output_path: str | None = None,
    register_table: bool = False,
    delete_not_matched_by_source: bool = False,
) -> str:
    """Upsert a Spark DataFrame into an existing Delta target, creating it on first write."""
    if table_format != "delta":
        raise ValueError("merge_dataframe currently supports only Delta format targets.")

    target_exists = delta_target_exists(
        spark,
        table_name=table_name,
        output_path=output_path,
        register_table=register_table,
    )
    if not target_exists:
        write_dataframe(
            dataframe,
            table_format=table_format,
            mode="overwrite",
            table_name=table_name,
            output_path=output_path,
            register_table=register_table,
        )
        return "created"

    from delta.tables import DeltaTable

    merge_condition = build_merge_condition(key_columns)
    if register_table:
        if not table_name:
            raise ValueError("table_name is required when register_table=True.")
        delta_table = DeltaTable.forName(spark, table_name)
    else:
        if not output_path:
            raise ValueError("output_path is required when register_table=False.")
        delta_table = DeltaTable.forPath(spark, output_path)

    merge_builder = (
        delta_table.alias("target")
        .merge(dataframe.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
    )

    if delete_not_matched_by_source:
        merge_builder = merge_builder.whenNotMatchedBySourceDelete()

    merge_builder.execute()
    return "merged"


def read_dataframe(
    spark: SparkSession,
    *,
    table_format: str,
    table_name: str | None = None,
    input_path: str | None = None,
    register_table: bool = False,
) -> DataFrame:
    """Read a Spark DataFrame either from a registered table or a storage path."""
    if register_table:
        if not table_name:
            raise ValueError("table_name is required when register_table=True.")
        return spark.table(table_name)

    if not input_path:
        raise ValueError("input_path is required when register_table=False.")

    return spark.read.format(table_format).load(input_path)
