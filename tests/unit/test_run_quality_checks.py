import pytest

from src.quality.run_quality_checks import (
    DataQualityError,
    build_quality_audit_rows,
    collect_quality_violations,
    finalize_dataset_quality_summary,
    format_quality_violations,
)


def test_finalize_dataset_quality_summary_detects_multiple_failures() -> None:
    dataset_summary = {
        "name": "silver_trades",
        "row_count": 0,
        "min_row_count": 1,
        "schema": {"missing_columns": ["trade_time"]},
        "null_checks": {
            "metrics": {
                "price": {
                    "null_ratio": 0.1,
                    "max_null_ratio": 0.0,
                    "passed": False,
                }
            }
        },
        "duplicate_check": {
            "key_columns": ["symbol", "trade_id"],
            "duplicate_count": 2,
            "max_duplicate_count": 0,
            "passed": False,
        },
        "freshness_check": {
            "column": "trade_time",
            "age_seconds": 1200.0,
            "max_age_seconds": 900,
            "passed": False,
        },
        "bounds_check": {
            "metrics": {
                "price": {
                    "min": 0.0,
                    "max": None,
                    "observed_min": -1.0,
                    "observed_max": 10.0,
                    "passed": False,
                }
            }
        },
    }

    finalized_summary = finalize_dataset_quality_summary(dataset_summary)

    assert finalized_summary["passed"] is False
    assert finalized_summary["violations"] == [
        "[silver_trades] row_count 0 is below minimum 1",
        "[silver_trades] missing required columns: trade_time",
        "[silver_trades] null ratio for price 0.100000 exceeds max 0.000000",
        "[silver_trades] duplicate_count 2 exceeds max 0 for keys [symbol, trade_id]",
        "[silver_trades] freshness age 1200.00s exceeds max 900s for column trade_time",
        "[silver_trades] observed minimum for price -1.00000000 is below allowed 0.00000000",
    ]


def test_collect_quality_violations_flattens_dataset_failures() -> None:
    dataset_summaries = [
        {"violations": ["[silver_trades] duplicate_count 1 exceeds max 0 for keys [symbol, trade_id]"]},
        {"violations": ["[gold_latest_price] row_count 0 is below minimum 1"]},
    ]

    assert collect_quality_violations(dataset_summaries) == [
        "[silver_trades] duplicate_count 1 exceeds max 0 for keys [symbol, trade_id]",
        "[gold_latest_price] row_count 0 is below minimum 1",
    ]


def test_format_quality_violations_returns_multiline_message() -> None:
    message = format_quality_violations(
        [
            "[silver_trades] duplicate_count 1 exceeds max 0 for keys [symbol, trade_id]",
            "[gold_latest_price] row_count 0 is below minimum 1",
        ]
    )

    assert message.splitlines() == [
        "[silver_trades] duplicate_count 1 exceeds max 0 for keys [symbol, trade_id]",
        "[gold_latest_price] row_count 0 is below minimum 1",
    ]


def test_format_quality_violations_handles_empty_state() -> None:
    assert format_quality_violations([]) == "No data quality violations detected."


def test_data_quality_error_can_be_raised_with_formatted_message() -> None:
    with pytest.raises(DataQualityError, match="silver_trades"):
        raise DataQualityError(format_quality_violations(["[silver_trades] row_count 0 is below minimum 1"]))


def test_build_quality_audit_rows_flattens_dataset_summary() -> None:
    summary = {
        "datasets": [
            {
                "name": "silver_trades",
                "layer": "silver",
                "table": "trades",
                "path": "dbfs:/tmp/project/dev/silver/trades",
                "passed": False,
                "row_count": 10,
                "min_row_count": 1,
                "violations": ["[silver_trades] duplicate_count 1 exceeds max 0 for keys [symbol, trade_id]"],
                "schema": {"passed": True},
                "null_checks": {"passed": True},
                "duplicate_check": {"passed": False},
                "freshness_check": {"passed": True},
                "bounds_check": {"passed": True},
            }
        ]
    }

    rows = build_quality_audit_rows(
        summary,
        environment="dev",
        audit_run_id="job_123",
        job_name="market_data_medallion_pipeline_dev",
        task_name="data_quality_checks",
    )

    assert len(rows) == 1
    assert rows[0]["audit_run_id"] == "job_123"
    assert rows[0]["environment"] == "dev"
    assert rows[0]["job_name"] == "market_data_medallion_pipeline_dev"
    assert rows[0]["task_name"] == "data_quality_checks"
    assert rows[0]["dataset_name"] == "silver_trades"
    assert rows[0]["layer"] == "silver"
    assert rows[0]["table_name"] == "trades"
    assert rows[0]["storage_path"] == "dbfs:/tmp/project/dev/silver/trades"
    assert rows[0]["passed"] is False
    assert rows[0]["violation_count"] == 1
    assert "duplicate_count" in rows[0]["violations_json"]
    assert rows[0]["duplicate_check_passed"] is False
