import json

from src.ingestion.parse_raw_payloads import (
    parse_batch,
    parse_json_payload,
    parse_kline_payload,
    parse_trade_payload,
    unwrap_binance_stream_payload,
)


def test_parse_json_payload() -> None:
    payload = '{"symbol": "BTCUSDT", "price": "65000.00"}'

    result = parse_json_payload(payload)

    assert result["symbol"] == "BTCUSDT"
    assert result["price"] == "65000.00"


def test_parse_batch() -> None:
    payloads = ['{"symbol": "BTCUSDT"}', '{"symbol": "ETHUSDT"}']

    result = parse_batch(payloads)

    assert len(result) == 2


def test_parse_trade_payload_normalizes_binance_trade_event() -> None:
    payload = json.dumps(
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
        }
    )

    result = parse_trade_payload(payload, ingested_at="2026-03-24T14:30:00Z")

    assert result["symbol"] == "BTCUSDT"
    assert result["trade_id"] == 1001
    assert result["trade_time"] == "2024-03-24T14:00:00Z"
    assert result["event_time"] == "2024-03-24T14:00:00Z"
    assert result["is_market_maker"] is True
    assert result["ingested_at"] == "2026-03-24T14:30:00Z"


def test_parse_kline_payload_normalizes_binance_live_kline_event() -> None:
    payload = {
        "e": "kline",
        "E": 1711288860000,
        "s": "BTCUSDT",
        "k": {
            "t": 1711288800000,
            "T": 1711288859999,
            "s": "BTCUSDT",
            "i": "1m",
            "o": "64950.00",
            "c": "65000.00",
            "h": "65020.00",
            "l": "64920.00",
            "v": "125.50",
            "n": 1200,
            "x": True,
            "q": "8150000.00",
        },
    }

    result = parse_kline_payload(payload, ingested_at="2026-03-24T14:30:00Z")

    assert result["symbol"] == "BTCUSDT"
    assert result["interval"] == "1m"
    assert result["open_time"] == "2024-03-24T14:00:00Z"
    assert result["close_time"] == "2024-03-24T14:00:59Z"
    assert result["is_closed"] is True


def test_unwrap_binance_stream_payload_extracts_combined_stream_data() -> None:
    payload = {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "E": 1711288800000,
            "s": "BTCUSDT",
            "t": 1001,
            "p": "65000.00",
            "q": "0.10",
            "T": 1711288800000,
        },
    }

    result = unwrap_binance_stream_payload(payload)

    assert result["e"] == "trade"
    assert result["s"] == "BTCUSDT"
