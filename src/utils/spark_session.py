"""Spark session factory utilities."""

from __future__ import annotations

import os
from importlib.util import find_spec


def get_spark_session(
    app_name: str = "real-time-market-data-lakehouse",
    master: str | None = None,
    enable_delta: bool = True,
):
    """Create or return a Spark session suitable for Databricks or local development."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)

    is_databricks_runtime = bool(os.getenv("DATABRICKS_RUNTIME_VERSION"))

    if master is not None:
        builder = builder.master(master)
    elif not is_databricks_runtime:
        builder = builder.master("local[*]")

    # Databricks configures Spark + Delta natively and may attach Unity Catalog.
    # Overriding spark_catalog in that environment can break warehouse-visible table creation.
    if enable_delta and not is_databricks_runtime and find_spec("delta") is not None:
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )

    return builder.getOrCreate()
