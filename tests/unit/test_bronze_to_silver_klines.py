from src.ingestion.load_binance_history import parse_historical_kline_row
from src.transforms.bronze_to_silver_klines import transform_kline_batch, transform_kline_record


def test_transform_kline_record_casts_fields() -> None:
    bronze_record = parse_historical_kline_row(
        symbol="ETHUSDT",
        interval="1m",
        row=[
            1711288800000,
            "3505.00",
            "3510.00",
            "3490.00",
            "3500.00",
            "340.0",
            1711288859999,
            "1190000.00",
            900,
            "170.0",
            "595000.00",
            "0",
        ],
        ingested_at="2026-03-24T14:30:00Z",
    )

    result = transform_kline_record(bronze_record)

    assert result["symbol"] == "ETHUSDT"
    assert result["close"] == 3500.0
    assert result["volume"] == 340.0
    assert result["open_time"].isoformat().replace("+00:00", "Z") == "2024-03-24T14:00:00Z"


def test_transform_kline_batch_deduplicates_on_symbol_interval_and_open_time() -> None:
    bronze_record = parse_historical_kline_row(
        symbol="ETHUSDT",
        interval="1m",
        row=[
            1711288800000,
            "3505.00",
            "3510.00",
            "3490.00",
            "3500.00",
            "340.0",
            1711288859999,
            "1190000.00",
            900,
            "170.0",
            "595000.00",
            "0",
        ],
        ingested_at="2026-03-24T14:30:00Z",
    )

    result = transform_kline_batch([bronze_record, bronze_record.copy()])

    assert len(result) == 1
    assert result[0]["interval"] == "1m"
