# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - Unity Catalog Write Probe

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

from src.utils.config_loader import load_project_config
from src.utils.notebook_runtime import emit_notebook_output
from src.utils.spark_session import get_spark_session


def _get_dbutils():
    try:
        return dbutils  # type: ignore[name-defined]
    except NameError:
        return None


dbutils_handle = _get_dbutils()

default_env = "azure_trial"
if dbutils_handle is not None:
    dbutils_handle.widgets.text("env", default_env)
    env = dbutils_handle.widgets.get("env")
else:
    env = default_env

config = load_project_config(project_root / "config", env)
spark_session = globals().get("spark") or get_spark_session(app_name="unity-catalog-write-probe")

probe_table_name = f"{config['catalog']}.{config['schemas']['gold']}.codex_uc_probe"
spark_session.sql(
    f"""
    CREATE OR REPLACE TABLE {probe_table_name}
    AS SELECT
      '{env}' AS environment,
      current_timestamp() AS created_at
    """
)
probe_row_count = spark_session.table(probe_table_name).count()

emit_notebook_output(
    {
        "environment": env,
        "catalog": config["catalog"],
        "gold_schema": config["schemas"]["gold"],
        "register_tables": config["databricks"]["register_tables"],
        "probe_table_name": probe_table_name,
        "probe_row_count": probe_row_count,
    },
    dbutils_handle,
)
