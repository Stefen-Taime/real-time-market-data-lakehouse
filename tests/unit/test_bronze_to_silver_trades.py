from src.ingestion.parse_raw_payloads import parse_trade_payload
from src.transforms.bronze_to_silver_trades import transform_trade_batch, transform_trade_record


def test_transform_trade_record_casts_fields() -> None:
    bronze_record = parse_trade_payload(
        {
            "e": "trade",
            "E": 1711288800000,
            "s": "BTCUSDT",
            "t": 1001,
            "p": "65000.00",
            "q": "0.10",
            "b": 5001,
            "a": 7001,
            "T": 1711288800000,
            "m": True,
        },
        ingested_at="2026-03-24T14:30:00Z",
    )

    result = transform_trade_record(bronze_record)

    assert result["trade_id"] == 1001
    assert result["price"] == 65000.0
    assert result["quantity"] == 0.1
    assert result["notional"] == 6500.0
    assert result["trade_time"].isoformat().replace("+00:00", "Z") == "2024-03-24T14:00:00Z"


def test_transform_trade_batch_deduplicates_trade_id() -> None:
    bronze_records = [
        parse_trade_payload(
            {
                "e": "trade",
                "E": 1711288800000,
                "s": "BTCUSDT",
                "t": 1001,
                "p": "65000.00",
                "q": "0.10",
                "T": 1711288800000,
            },
            ingested_at="2026-03-24T14:30:00Z",
        ),
        parse_trade_payload(
            {
                "e": "trade",
                "E": 1711288800000,
                "s": "BTCUSDT",
                "t": 1001,
                "p": "65000.00",
                "q": "0.10",
                "T": 1711288800000,
            },
            ingested_at="2026-03-24T14:30:01Z",
        ),
    ]

    result = transform_trade_batch(bronze_records)

    assert len(result) == 1
    assert result[0]["trade_id"] == 1001
