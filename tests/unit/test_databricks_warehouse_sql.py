from datetime import date, datetime, timezone
from decimal import Decimal

from src.utils.databricks_warehouse_sql import (
    build_create_or_replace_empty_table_statement,
    build_insert_statement,
    chunk_rows,
    quote_qualified_name,
    sql_literal,
)


def test_quote_qualified_name_quotes_each_identifier() -> None:
    assert quote_qualified_name("workspace.default.app_gold_latest_price") == "`workspace`.`default`.`app_gold_latest_price`"


def test_sql_literal_supports_common_types() -> None:
    timestamp = datetime(2026, 3, 24, 17, 0, 0, tzinfo=timezone.utc)

    assert sql_literal(None) == "NULL"
    assert sql_literal(True) == "TRUE"
    assert sql_literal(42) == "42"
    assert sql_literal(Decimal("12.3400")) == "12.3400"
    assert sql_literal(date(2026, 3, 24)) == "DATE '2026-03-24'"
    assert sql_literal(timestamp) == "TIMESTAMP '2026-03-24 17:00:00.000000'"
    assert sql_literal("O'Hare") == "'O''Hare'"


def test_build_create_or_replace_empty_table_statement_renders_schema_projection() -> None:
    statement = build_create_or_replace_empty_table_statement(
        "workspace.default.app_gold_latest_price",
        [("symbol", "STRING"), ("latest_price", "DOUBLE")],
    )

    assert "CREATE OR REPLACE TABLE `workspace`.`default`.`app_gold_latest_price`" in statement
    assert "CAST(NULL AS STRING) AS `symbol`" in statement
    assert "CAST(NULL AS DOUBLE) AS `latest_price`" in statement
    assert statement.endswith("WHERE 1 = 0")


def test_build_insert_statement_renders_multi_row_values() -> None:
    statement = build_insert_statement(
        "workspace.default.app_gold_latest_price",
        [("symbol", "STRING"), ("latest_price", "DOUBLE"), ("passed", "BOOLEAN")],
        [("BTCUSDT", 84321.12, True), ("ETHUSDT", 2100.5, False)],
    )

    assert "INSERT INTO `workspace`.`default`.`app_gold_latest_price`" in statement
    assert "('BTCUSDT', 84321.12, TRUE)" in statement
    assert "('ETHUSDT', 2100.5, FALSE)" in statement


def test_chunk_rows_splits_batches_deterministically() -> None:
    chunks = list(chunk_rows([(1,), (2,), (3,)], batch_size=2))

    assert chunks == [[(1,), (2,)], [(3,)]]
