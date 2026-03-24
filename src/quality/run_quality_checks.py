"""Run configurable data quality checks across Bronze, Silver and Gold datasets."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import TYPE_CHECKING, Any

from src.quality.freshness_checks import age_in_seconds
from src.quality.schema_checks import required_columns_present
from src.utils.lakehouse_io import (
    build_storage_path,
    build_table_name,
    ensure_schema_exists,
    read_dataframe,
    write_dataframe,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class DataQualityError(RuntimeError):
    """Raised when one or more configured data quality checks fail."""


def finalize_dataset_quality_summary(dataset_summary: dict[str, Any]) -> dict[str, Any]:
    """Evaluate computed metrics and attach a pass/fail verdict with violations."""
    dataset_name = dataset_summary["name"]
    violations: list[str] = []

    if dataset_summary.get("error"):
        violations.append(f"[{dataset_name}] unable to read dataset: {dataset_summary['error']}")

    row_count = int(dataset_summary.get("row_count", 0))
    min_row_count = int(dataset_summary.get("min_row_count", 0))
    if row_count < min_row_count:
        violations.append(f"[{dataset_name}] row_count {row_count} is below minimum {min_row_count}")

    schema_summary = dataset_summary.get("schema", {})
    missing_columns = schema_summary.get("missing_columns", [])
    if missing_columns:
        violations.append(f"[{dataset_name}] missing required columns: {', '.join(sorted(missing_columns))}")

    for column, metric in dataset_summary.get("null_checks", {}).get("metrics", {}).items():
        if metric.get("passed", True):
            continue
        if metric.get("error"):
            violations.append(f"[{dataset_name}] null check for {column} failed: {metric['error']}")
            continue
        violations.append(
            f"[{dataset_name}] null ratio for {column} "
            f"{float(metric['null_ratio']):.6f} exceeds max {float(metric['max_null_ratio']):.6f}"
        )

    duplicate_check = dataset_summary.get("duplicate_check")
    if duplicate_check and not duplicate_check.get("passed", True):
        if duplicate_check.get("error"):
            violations.append(f"[{dataset_name}] duplicate check failed: {duplicate_check['error']}")
        else:
            key_columns = ", ".join(duplicate_check["key_columns"])
            violations.append(
                f"[{dataset_name}] duplicate_count {int(duplicate_check['duplicate_count'])} "
                f"exceeds max {int(duplicate_check['max_duplicate_count'])} for keys [{key_columns}]"
            )

    freshness_check = dataset_summary.get("freshness_check")
    if freshness_check and not freshness_check.get("passed", True):
        if freshness_check.get("error"):
            violations.append(f"[{dataset_name}] freshness check failed: {freshness_check['error']}")
        else:
            violations.append(
                f"[{dataset_name}] freshness age {float(freshness_check['age_seconds']):.2f}s "
                f"exceeds max {int(freshness_check['max_age_seconds'])}s for column {freshness_check['column']}"
            )

    for column, metric in dataset_summary.get("bounds_check", {}).get("metrics", {}).items():
        if metric.get("passed", True):
            continue
        if metric.get("error"):
            violations.append(f"[{dataset_name}] bounds check for {column} failed: {metric['error']}")
            continue
        minimum = metric.get("min")
        maximum = metric.get("max")
        observed_min = metric.get("observed_min")
        observed_max = metric.get("observed_max")
        if minimum is not None and observed_min is not None and float(observed_min) < float(minimum):
            violations.append(
                f"[{dataset_name}] observed minimum for {column} "
                f"{float(observed_min):.8f} is below allowed {float(minimum):.8f}"
            )
        if maximum is not None and observed_max is not None and float(observed_max) > float(maximum):
            violations.append(
                f"[{dataset_name}] observed maximum for {column} "
                f"{float(observed_max):.8f} exceeds allowed {float(maximum):.8f}"
            )
        if observed_min is None and observed_max is None:
            violations.append(f"[{dataset_name}] bounds check for {column} has no comparable values")

    dataset_summary["violations"] = violations
    dataset_summary["passed"] = not violations
    return dataset_summary


def collect_quality_violations(dataset_summaries: list[dict[str, Any]]) -> list[str]:
    """Flatten violations across dataset summaries."""
    violations: list[str] = []
    for dataset_summary in dataset_summaries:
        violations.extend(dataset_summary.get("violations", []))
    return violations


def format_quality_violations(violations: list[str]) -> str:
    """Render violations into a single multi-line message."""
    if not violations:
        return "No data quality violations detected."
    return "\n".join(violations)


def build_quality_audit_rows(
    summary: dict[str, Any],
    *,
    environment: str,
    audit_run_id: str,
    checked_at: datetime | None = None,
    job_name: str | None = None,
    task_name: str | None = None,
) -> list[dict[str, Any]]:
    """Flatten a quality summary into audit rows suitable for Delta persistence."""
    if checked_at is None:
        checked_at = datetime.now(timezone.utc)
    checked_at = _normalize_timestamp(checked_at) or datetime.now(timezone.utc)

    rows: list[dict[str, Any]] = []
    for dataset in summary["datasets"]:
        rows.append(
            {
                "audit_run_id": audit_run_id,
                "checked_at": checked_at,
                "environment": environment,
                "job_name": job_name,
                "task_name": task_name,
                "dataset_name": dataset["name"],
                "layer": dataset["layer"],
                "table_name": dataset["table"],
                "storage_path": dataset.get("path"),
                "passed": bool(dataset["passed"]),
                "row_count": int(dataset["row_count"]),
                "min_row_count": int(dataset["min_row_count"]),
                "violation_count": len(dataset.get("violations", [])),
                "violations_json": json.dumps(dataset.get("violations", []), sort_keys=True),
                "schema_passed": bool(dataset.get("schema", {}).get("passed", True)),
                "null_checks_passed": bool(dataset.get("null_checks", {}).get("passed", True)),
                "duplicate_check_passed": bool(
                    dataset.get("duplicate_check", {}).get("passed", True)
                    if dataset.get("duplicate_check") is not None
                    else True
                ),
                "freshness_check_passed": bool(
                    dataset.get("freshness_check", {}).get("passed", True)
                    if dataset.get("freshness_check") is not None
                    else True
                ),
                "bounds_check_passed": bool(dataset.get("bounds_check", {}).get("passed", True)),
                "summary_json": json.dumps(
                    {
                        "schema": dataset.get("schema"),
                        "null_checks": dataset.get("null_checks"),
                        "duplicate_check": dataset.get("duplicate_check"),
                        "freshness_check": dataset.get("freshness_check"),
                        "bounds_check": dataset.get("bounds_check"),
                    },
                    sort_keys=True,
                    default=str,
                ),
            }
        )

    return rows


def write_quality_audit_results(
    spark: SparkSession,
    *,
    summary: dict[str, Any],
    catalog: str | None,
    audit_schema: str,
    audit_table: str,
    base_output_path: str | None,
    environment: str,
    audit_run_id: str,
    table_format: str = "delta",
    write_mode: str = "append",
    register_tables: bool = False,
    checked_at: datetime | None = None,
    job_name: str | None = None,
    task_name: str | None = None,
) -> dict[str, Any]:
    """Persist quality check outcomes as an audit Delta dataset."""
    rows = build_quality_audit_rows(
        summary,
        environment=environment,
        audit_run_id=audit_run_id,
        checked_at=checked_at,
        job_name=job_name,
        task_name=task_name,
    )
    if not rows:
        return {
            "audit_run_id": audit_run_id,
            "table": None,
            "output_path": None,
            "row_count": 0,
            "write_mode": write_mode,
            "table_format": table_format,
            "register_tables": register_tables,
        }

    audit_dataframe = spark.createDataFrame(rows)
    table_name = build_table_name(catalog, audit_schema, audit_table) if register_tables else None
    output_path = build_storage_path(base_output_path, "audit", audit_table)

    if register_tables:
        ensure_schema_exists(spark, audit_schema, catalog=catalog)

    write_dataframe(
        audit_dataframe,
        table_format=table_format,
        mode=write_mode,
        table_name=table_name,
        output_path=output_path,
        register_table=register_tables,
    )
    return {
        "audit_run_id": audit_run_id,
        "table": table_name,
        "output_path": output_path,
        "row_count": len(rows),
        "write_mode": write_mode,
        "table_format": table_format,
        "register_tables": register_tables,
    }


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


def _compute_null_check_metrics(
    dataframe: DataFrame,
    *,
    available_columns: set[str],
    row_count: int,
    null_thresholds: dict[str, float],
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    metrics: dict[str, Any] = {}
    selected_columns = [column for column in null_thresholds if column in available_columns]
    aggregate_row = None

    if selected_columns and row_count > 0:
        expressions = [
            F.avg(F.when(F.col(column).isNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias(column)
            for column in selected_columns
        ]
        aggregate_row = dataframe.select(*expressions).first()

    for column, max_null_ratio in null_thresholds.items():
        if column not in available_columns:
            metrics[column] = {
                "null_ratio": None,
                "max_null_ratio": float(max_null_ratio),
                "passed": False,
                "error": "column is missing",
            }
            continue

        observed_ratio = 0.0
        if aggregate_row is not None and aggregate_row[column] is not None:
            observed_ratio = float(aggregate_row[column])

        metrics[column] = {
            "null_ratio": observed_ratio,
            "max_null_ratio": float(max_null_ratio),
            "passed": observed_ratio <= float(max_null_ratio),
        }

    return {
        "metrics": metrics,
        "passed": all(metric.get("passed", False) for metric in metrics.values()) if metrics else True,
    }


def _compute_duplicate_check(
    dataframe: DataFrame,
    *,
    available_columns: set[str],
    key_columns: list[str],
    max_duplicate_count: int,
) -> dict[str, Any] | None:
    from pyspark.sql import functions as F

    if not key_columns:
        return None

    missing_key_columns = [column for column in key_columns if column not in available_columns]
    if missing_key_columns:
        return {
            "key_columns": key_columns,
            "duplicate_count": None,
            "max_duplicate_count": int(max_duplicate_count),
            "passed": False,
            "error": f"missing key columns: {', '.join(missing_key_columns)}",
        }

    duplicate_row = (
        dataframe.groupBy(*key_columns)
        .count()
        .where(F.col("count") > 1)
        .select(F.coalesce(F.sum(F.col("count") - F.lit(1)), F.lit(0)).alias("duplicate_count"))
        .first()
    )
    duplicate_count = int(duplicate_row["duplicate_count"]) if duplicate_row else 0

    return {
        "key_columns": key_columns,
        "duplicate_count": duplicate_count,
        "max_duplicate_count": int(max_duplicate_count),
        "passed": duplicate_count <= int(max_duplicate_count),
    }


def _normalize_timestamp(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _compute_freshness_check(
    dataframe: DataFrame,
    *,
    available_columns: set[str],
    freshness_rule: dict[str, Any] | None,
) -> dict[str, Any] | None:
    from pyspark.sql import functions as F

    if not freshness_rule:
        return None

    column = freshness_rule["column"]
    max_age_seconds = int(freshness_rule["max_age_seconds"])

    if column not in available_columns:
        return {
            "column": column,
            "age_seconds": None,
            "max_age_seconds": max_age_seconds,
            "passed": False,
            "error": "column is missing",
        }

    freshness_row = dataframe.select(F.max(F.col(column)).alias("max_timestamp")).first()
    max_timestamp = _normalize_timestamp(freshness_row["max_timestamp"]) if freshness_row else None
    if max_timestamp is None:
        return {
            "column": column,
            "age_seconds": None,
            "max_age_seconds": max_age_seconds,
            "passed": False,
            "error": "no timestamp values available",
        }

    observed_age_seconds = float(age_in_seconds(max_timestamp, datetime.now(timezone.utc)))
    return {
        "column": column,
        "age_seconds": observed_age_seconds,
        "max_age_seconds": max_age_seconds,
        "passed": observed_age_seconds <= max_age_seconds,
    }


def _compute_bounds_check(
    dataframe: DataFrame,
    *,
    available_columns: set[str],
    column_bounds: dict[str, dict[str, float]],
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    metrics: dict[str, Any] = {}
    selected_columns = [column for column in column_bounds if column in available_columns]
    aggregate_row = None

    if selected_columns:
        expressions = []
        for column in selected_columns:
            expressions.append(F.min(F.col(column)).alias(f"{column}__observed_min"))
            expressions.append(F.max(F.col(column)).alias(f"{column}__observed_max"))
        aggregate_row = dataframe.select(*expressions).first()

    for column, bounds in column_bounds.items():
        minimum = bounds.get("min")
        maximum = bounds.get("max")

        if column not in available_columns:
            metrics[column] = {
                "min": minimum,
                "max": maximum,
                "observed_min": None,
                "observed_max": None,
                "passed": False,
                "error": "column is missing",
            }
            continue

        observed_min = aggregate_row[f"{column}__observed_min"] if aggregate_row is not None else None
        observed_max = aggregate_row[f"{column}__observed_max"] if aggregate_row is not None else None

        passed = True
        if minimum is not None and observed_min is not None:
            passed = passed and float(observed_min) >= float(minimum)
        if maximum is not None and observed_max is not None:
            passed = passed and float(observed_max) <= float(maximum)
        if observed_min is None and observed_max is None:
            passed = False

        metrics[column] = {
            "min": minimum,
            "max": maximum,
            "observed_min": observed_min,
            "observed_max": observed_max,
            "passed": passed,
        }

    return {
        "metrics": metrics,
        "passed": all(metric.get("passed", False) for metric in metrics.values()) if metrics else True,
    }


def _build_dataset_summary(
    spark: SparkSession,
    *,
    catalog: str | None,
    schemas: dict[str, str],
    tables: dict[str, dict[str, str]],
    dataset_rule: dict[str, Any],
    base_output_path: str | None,
    table_format: str,
    register_tables: bool,
) -> dict[str, Any]:
    layer = dataset_rule["layer"]
    table_key = dataset_rule["table_key"]
    schema = schemas[layer]
    table = tables[layer][table_key]
    table_name, input_path = _dataset_reference(
        catalog=catalog,
        schema=schema,
        table=table,
        base_output_path=base_output_path,
        layer=layer,
        register_tables=register_tables,
    )

    dataset_summary: dict[str, Any] = {
        "name": dataset_rule["name"],
        "layer": layer,
        "table": table_name or table,
        "path": input_path,
        "row_count": 0,
        "min_row_count": int(dataset_rule.get("min_row_count", 0)),
        "schema": {
            "required_columns": dataset_rule.get("required_columns", []),
            "missing_columns": [],
            "passed": True,
        },
        "null_checks": {"metrics": {}, "passed": True},
        "duplicate_check": None,
        "freshness_check": None,
        "bounds_check": {"metrics": {}, "passed": True},
    }

    try:
        dataframe = read_dataframe(
            spark,
            table_format=table_format,
            table_name=table_name,
            input_path=input_path,
            register_table=register_tables,
        )
        row_count = int(dataframe.count())
        available_columns = set(dataframe.columns)
        required_columns = dataset_rule.get("required_columns", [])
        missing_columns = sorted(set(required_columns) - available_columns)

        dataset_summary["row_count"] = row_count
        dataset_summary["schema"] = {
            "required_columns": required_columns,
            "missing_columns": missing_columns,
            "passed": required_columns_present(available_columns, required_columns),
        }
        dataset_summary["null_checks"] = _compute_null_check_metrics(
            dataframe,
            available_columns=available_columns,
            row_count=row_count,
            null_thresholds=dataset_rule.get("null_thresholds", {}),
        )
        dataset_summary["duplicate_check"] = _compute_duplicate_check(
            dataframe,
            available_columns=available_columns,
            key_columns=dataset_rule.get("duplicate_key_columns", []),
            max_duplicate_count=int(dataset_rule.get("max_duplicate_count", 0)),
        )
        dataset_summary["freshness_check"] = _compute_freshness_check(
            dataframe,
            available_columns=available_columns,
            freshness_rule=dataset_rule.get("freshness"),
        )
        dataset_summary["bounds_check"] = _compute_bounds_check(
            dataframe,
            available_columns=available_columns,
            column_bounds=dataset_rule.get("column_bounds", {}),
        )
    except Exception as exc:  # pragma: no cover - exercised via runtime integration.
        dataset_summary["error"] = str(exc)

    return finalize_dataset_quality_summary(dataset_summary)


def run_quality_checks(
    spark: SparkSession,
    *,
    catalog: str | None,
    schemas: dict[str, str],
    tables: dict[str, dict[str, str]],
    dataset_rules: list[dict[str, Any]],
    base_output_path: str | None,
    table_format: str = "delta",
    register_tables: bool = False,
    fail_on_error: bool = True,
) -> dict[str, Any]:
    """Run configured quality checks and optionally fail on any violation."""
    dataset_summaries = [
        _build_dataset_summary(
            spark,
            catalog=catalog,
            schemas=schemas,
            tables=tables,
            dataset_rule=dataset_rule,
            base_output_path=base_output_path,
            table_format=table_format,
            register_tables=register_tables,
        )
        for dataset_rule in dataset_rules
    ]
    violations = collect_quality_violations(dataset_summaries)
    summary = {
        "datasets": dataset_summaries,
        "passed": not violations,
        "violations": violations,
        "table_format": table_format,
        "register_tables": register_tables,
        "base_output_path": base_output_path,
    }

    if violations and fail_on_error:
        raise DataQualityError(format_quality_violations(violations))

    return summary
