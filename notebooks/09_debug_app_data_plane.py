# Databricks notebook source
# MAGIC %md
# MAGIC # 09 - Debug App Data Plane

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

from src.utils.databricks_warehouse_sql import execute_sql_statement_with_context
from src.utils.config_loader import load_project_config
from src.utils.notebook_runtime import build_runtime_context, emit_notebook_output
from src.utils.spark_session import get_spark_session


def _get_dbutils():
    try:
        return dbutils  # type: ignore[name-defined]
    except NameError:
        return None


def _write_text_file(spark_session, path: str, content: str) -> None:
    jvm = spark_session._jvm
    j_path = jvm.org.apache.hadoop.fs.Path(path)
    filesystem = j_path.getFileSystem(spark_session._jsc.hadoopConfiguration())
    parent = j_path.getParent()
    if parent is not None:
        filesystem.mkdirs(parent)

    output_stream = filesystem.create(j_path, True)
    try:
        output_stream.write(bytearray(content.encode("utf-8")))
    finally:
        output_stream.close()


dbutils_handle = _get_dbutils()
if dbutils_handle is None:
    raise RuntimeError("dbutils is unavailable in this notebook runtime; warehouse publication cannot be tested.")

default_env = "dev_app"
if dbutils_handle is not None:
    dbutils_handle.widgets.text("env", default_env)
    env = dbutils_handle.widgets.get("env")
else:
    env = default_env

config = load_project_config(project_root / "config", env)
spark_session = globals().get("spark") or get_spark_session(app_name="market-data-debug-app-data-plane")

catalog = config["serving_tables"]["catalog"]
schema = config["serving_tables"]["schema"]
volume_catalog = config["volume"]["catalog"]
volume_schema = config["volume"]["schema"]
volume_name = config["volume"]["name"]
volume_path = config["app"]["volume_path"]
volume_dbfs_path = config["paths"]["delta_base_path"]

spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark_session.sql(f"CREATE VOLUME IF NOT EXISTS {volume_catalog}.{volume_schema}.{volume_name}")

probe_table_name = f"{catalog}.{schema}.app_probe_table"
probe_table_dataframe = spark_session.range(1).select(
    F.lit("probe-ok").alias("probe_label"),
    F.current_timestamp().alias("created_at"),
)
probe_table_dataframe.write.format("delta").mode("overwrite").saveAsTable(probe_table_name)

probe_table_sql_name = f"{catalog}.{schema}.app_probe_table_sql"
probe_table_dataframe.createOrReplaceTempView("probe_source")
spark_session.sql(f"CREATE OR REPLACE TABLE {probe_table_sql_name} AS SELECT * FROM probe_source")

probe_text_dbfs_path = f"{volume_dbfs_path}/probe/debug_probe.txt"
_write_text_file(spark_session, probe_text_dbfs_path, "probe-ok\n")

probe_delta_dbfs_path = f"{volume_dbfs_path}/probe/probe_delta"
probe_delta_dataframe = spark_session.range(1).select(
    F.lit("probe-delta-ok").alias("probe_label"),
    F.current_timestamp().alias("created_at"),
)
probe_delta_dataframe.write.format("delta").mode("overwrite").save(probe_delta_dbfs_path)

notebook_api_probe_result = None
notebook_api_probe_select_result = None
if dbutils_handle is not None:
    notebook_api_probe_result = execute_sql_statement_with_context(
        dbutils_handle,
        warehouse_id=config["app"]["warehouse_id"],
        statement=(
            "CREATE OR REPLACE TABLE workspace.default.notebook_api_probe_table "
            "AS SELECT 'notebook_api_ok' AS probe_label, current_timestamp() AS created_at"
        ),
    )
    notebook_api_probe_select_result = execute_sql_statement_with_context(
        dbutils_handle,
        warehouse_id=config["app"]["warehouse_id"],
        statement=(
            "SELECT probe_label, created_at "
            "FROM workspace.default.notebook_api_probe_table "
            "ORDER BY created_at DESC LIMIT 1"
        ),
    )

summary = {
    "environment": env,
    "probe_table": probe_table_name,
    "probe_table_sql": probe_table_sql_name,
    "probe_text_dbfs_path": probe_text_dbfs_path,
    "probe_text_volume_path": f"{volume_path}/dev/probe/debug_probe.txt",
    "probe_delta_dbfs_path": probe_delta_dbfs_path,
    "probe_delta_volume_path": f"{volume_path}/dev/probe/probe_delta",
    "notebook_api_probe_result": notebook_api_probe_result,
    "notebook_api_probe_select_result": notebook_api_probe_select_result,
    "runtime_context": build_runtime_context(spark_session, requested_catalog=catalog),
}

emit_notebook_output(summary, dbutils_handle)
