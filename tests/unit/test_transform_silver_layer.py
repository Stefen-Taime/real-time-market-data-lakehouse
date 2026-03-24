import pytest

from src.transforms.transform_silver_layer import run_silver_transform


def test_run_silver_transform_requires_at_least_one_layer() -> None:
    with pytest.raises(ValueError, match="At least one Silver transformation must be enabled"):
        run_silver_transform(  # type: ignore[arg-type]
            None,
            catalog="market_data",
            bronze_schema="bronze",
            silver_schema="silver",
            audit_schema="audit",
            processing_state_table="processing_state",
            bronze_trades_table="trades_live_raw",
            bronze_klines_history_table="klines_history_raw",
            bronze_klines_live_table="klines_live_raw",
            silver_trades_table="trades",
            silver_klines_table="klines_1m",
            base_output_path="dbfs:/tmp/project/dev",
            transform_trades=False,
            transform_klines=False,
        )
