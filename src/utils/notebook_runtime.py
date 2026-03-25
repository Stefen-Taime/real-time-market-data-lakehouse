"""Helpers for emitting structured Databricks notebook runtime metadata."""

from __future__ import annotations

import json
from typing import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _collect_first_column(rows: list[Any]) -> list[Any]:
    """Collect the first field value from Spark Row objects."""
    values: list[Any] = []
    for row in rows:
        row_dict = row.asDict(recursive=True)
        if not row_dict:
            values.append(None)
            continue
        values.append(next(iter(row_dict.values())))
    return values


def resolve_notebook_path(dbutils_handle: Any | None) -> str | None:
    """Best-effort Databricks notebook path lookup."""
    if dbutils_handle is None:
        return None

    try:  # pragma: no cover - depends on Databricks runtime objects
        context = dbutils_handle.notebook.entry_point.getDbutils().notebook().getContext()
        notebook_path = context.notebookPath()
        getter: Callable[[], str] | None = getattr(notebook_path, "get", None)
        if callable(getter):
            value = getter()
            return value or None
    except Exception:
        return None

    return None


def infer_bundle_target_from_notebook_path(notebook_path: str | None, *, bundle_name: str) -> str | None:
    """Infer the bundle target from a deployed notebook workspace path."""
    if not notebook_path:
        return None

    marker = f"/.bundle/{bundle_name}/"
    if marker not in notebook_path:
        return None

    suffix = notebook_path.split(marker, 1)[1]
    target = suffix.split("/", 1)[0].strip()
    return target or None


def build_runtime_context(spark: SparkSession, *, requested_catalog: str | None = None) -> dict[str, Any]:
    """Capture lightweight Spark runtime context for notebook observability."""
    context: dict[str, Any] = {}

    try:
        current = spark.sql(
            "SELECT current_catalog() AS current_catalog, current_database() AS current_database"
        ).collect()[0]
        context["current_catalog"] = current["current_catalog"]
        context["current_database"] = current["current_database"]
    except Exception as exc:  # pragma: no cover - depends on Databricks runtime
        context["current_context_error"] = str(exc)

    try:
        context["available_catalogs"] = _collect_first_column(spark.sql("SHOW CATALOGS").collect())
    except Exception as exc:  # pragma: no cover - depends on Databricks runtime
        context["available_catalogs_error"] = str(exc)

    if requested_catalog:
        context["requested_catalog"] = requested_catalog
        try:
            context["requested_catalog_schemas"] = _collect_first_column(
                spark.sql(f"SHOW SCHEMAS IN {requested_catalog}").collect()
            )
        except Exception as exc:  # pragma: no cover - depends on Databricks runtime
            context["requested_catalog_schemas_error"] = str(exc)

    return context


def emit_notebook_output(summary: dict[str, Any], dbutils_handle: Any | None = None) -> None:
    """Print a JSON summary and, when possible, return it as notebook output."""
    pretty_payload = json.dumps(summary, indent=2, default=str)
    print(pretty_payload)

    if dbutils_handle is None:
        return

    try:
        dbutils_handle.notebook.exit(json.dumps(summary, default=str))
    except Exception:  # pragma: no cover - depends on Databricks runtime
        return
