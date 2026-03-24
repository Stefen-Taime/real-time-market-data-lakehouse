from pathlib import Path

from src.ingestion.load_binance_history import load_history_file
from src.transforms.bronze_to_silver_klines import transform_kline_batch


FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "sample_klines.json"


def test_end_to_end_backfill_builds_silver_klines() -> None:
    bronze_records = load_history_file(FIXTURE_PATH, ingested_at="2026-03-24T14:30:00Z")
    silver_records = transform_kline_batch(bronze_records)

    assert len(bronze_records) == 11
    assert len(silver_records) == 11
    assert {record["symbol"] for record in silver_records} == {"BTCUSDT", "ETHUSDT"}
    assert all(record["is_closed"] for record in silver_records)
