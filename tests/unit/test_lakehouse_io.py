import pytest

from src.utils.lakehouse_io import build_merge_condition, build_storage_path, build_table_name, read_dataframe


def test_build_table_name_with_catalog() -> None:
    assert build_table_name("market_data", "bronze", "trades_live_raw") == "market_data.bronze.trades_live_raw"


def test_build_table_name_without_catalog() -> None:
    assert build_table_name(None, "silver", "trades") == "silver.trades"


def test_build_storage_path() -> None:
    assert build_storage_path("dbfs:/tmp/project/dev", "silver", "trades") == "dbfs:/tmp/project/dev/silver/trades"


def test_build_merge_condition_uses_null_safe_equality() -> None:
    assert build_merge_condition(["symbol", "trade_id"]) == (
        "target.`symbol` <=> source.`symbol` AND target.`trade_id` <=> source.`trade_id`"
    )


def test_build_merge_condition_requires_at_least_one_key() -> None:
    with pytest.raises(ValueError, match="key_columns must contain at least one column"):
        build_merge_condition([])


def test_read_dataframe_requires_table_name_in_table_mode() -> None:
    with pytest.raises(ValueError, match="table_name is required"):
        read_dataframe(None, table_format="delta", register_table=True)  # type: ignore[arg-type]


def test_read_dataframe_requires_input_path_in_path_mode() -> None:
    with pytest.raises(ValueError, match="input_path is required"):
        read_dataframe(None, table_format="delta", register_table=False)  # type: ignore[arg-type]
