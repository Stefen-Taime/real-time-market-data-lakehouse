from datetime import datetime, timezone

from src.utils.processing_state import build_processing_state_rows, build_state_key, normalize_timestamp


def test_build_state_key_is_deterministic() -> None:
    assert build_state_key("transform_silver", "trades_live_raw") == "transform_silver:trades_live_raw"


def test_normalize_timestamp_converts_naive_to_utc() -> None:
    value = normalize_timestamp(datetime(2026, 3, 24, 12, 0, 0))

    assert value is not None
    assert value.tzinfo == timezone.utc
    assert value.isoformat() == "2026-03-24T12:00:00+00:00"


def test_build_processing_state_rows_serializes_metadata() -> None:
    rows = build_processing_state_rows(
        [
            {
                "state_key": "transform_silver:trades_live_raw",
                "pipeline_name": "transform_silver",
                "dataset_name": "trades_live_raw",
                "source_layer": "bronze",
                "target_layer": "silver",
                "watermark_column": "ingested_at",
                "last_processed_at": datetime(2026, 3, 24, 12, 0, 0, tzinfo=timezone.utc),
                "metadata": {"target_table": "trades", "write_mode": "merge"},
            }
        ],
        updated_at=datetime(2026, 3, 24, 12, 5, 0, tzinfo=timezone.utc),
    )

    assert len(rows) == 1
    assert rows[0]["state_key"] == "transform_silver:trades_live_raw"
    assert rows[0]["updated_at"].isoformat() == "2026-03-24T12:05:00+00:00"
    assert rows[0]["metadata_json"] == '{"target_table": "trades", "write_mode": "merge"}'
