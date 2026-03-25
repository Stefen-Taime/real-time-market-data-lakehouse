import json
from pathlib import Path
import sys
import types

from src.ingestion.ingest_binance_live import (
    build_combined_stream_url,
    build_stream_names,
    build_trade_stream_names,
    filter_live_records,
    load_live_file,
    load_live_records,
    summarize_event_counts,
)
from src.transforms.bronze_to_silver_klines import transform_kline_batch
from src.transforms.bronze_to_silver_trades import transform_trade_batch


FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "sample_trades.json"
MIXED_FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "sample_live_market.json"


def test_live_ingestion_flow_deduplicates_trade_events() -> None:
    bronze_records = load_live_file(FIXTURE_PATH, ingested_at="2026-03-24T14:31:00Z")
    trade_records = filter_live_records(bronze_records, "trade")
    silver_records = transform_trade_batch(trade_records)

    assert len(bronze_records) == 6
    assert len(silver_records) == 5
    assert [record["trade_id"] for record in silver_records if record["symbol"] == "BTCUSDT"] == [1001, 1002, 1003]


def test_live_ingestion_flow_counts_trade_events() -> None:
    bronze_records = load_live_file(FIXTURE_PATH, ingested_at="2026-03-24T14:31:00Z")

    assert summarize_event_counts(bronze_records) == {"trade": 6}


def test_live_ingestion_flow_loads_fixture_mode() -> None:
    bronze_records, source_details = load_live_records(
        source_mode="fixture",
        input_path=FIXTURE_PATH,
        symbols=["BTCUSDT", "ETHUSDT"],
        stream_types=["trade"],
        kline_interval="1m",
        websocket_base_url="wss://data-stream.binance.vision",
        max_messages=10,
        duration_seconds=5,
        receive_timeout_seconds=1.0,
        ingested_at="2026-03-24T14:31:00Z",
    )

    assert len(bronze_records) == 6
    assert source_details["source_mode"] == "fixture"


def test_live_ingestion_flow_builds_combined_stream_url() -> None:
    assert build_trade_stream_names(["BTCUSDT", "ETHUSDT"]) == ["btcusdt@trade", "ethusdt@trade"]
    assert build_stream_names(["BTCUSDT"], ["trade", "kline"], kline_interval="1m") == [
        "btcusdt@trade",
        "btcusdt@kline_1m",
    ]
    assert build_combined_stream_url(
        "wss://data-stream.binance.vision", ["btcusdt@trade", "ethusdt@trade"]
    ) == "wss://data-stream.binance.vision/stream?streams=btcusdt@trade/ethusdt@trade"


def test_live_ingestion_flow_supports_mixed_trade_and_kline_events() -> None:
    bronze_records = load_live_file(MIXED_FIXTURE_PATH, ingested_at="2026-03-24T14:31:00Z")
    trade_records = filter_live_records(bronze_records, "trade")
    kline_records = filter_live_records(bronze_records, "kline")
    silver_trade_records = transform_trade_batch(trade_records)
    silver_kline_records = transform_kline_batch(kline_records)

    assert summarize_event_counts(bronze_records) == {"trade": 2, "kline": 3}
    assert len(silver_trade_records) == 2
    assert len(silver_kline_records) == 2
    assert all(record["is_closed"] for record in silver_kline_records)
    assert [record["symbol"] for record in silver_kline_records] == ["BTCUSDT", "ETHUSDT"]


def test_live_ingestion_flow_retries_websocket_connections(monkeypatch) -> None:
    attempts = {"count": 0}

    class DummySocket:
        def settimeout(self, _timeout: float) -> None:
            return None

        def recv(self) -> str:
            return json.dumps(
                {
                    "stream": "btcusdt@trade",
                    "data": {
                        "e": "trade",
                        "E": 1711281060000,
                        "s": "BTCUSDT",
                        "t": 2001,
                        "p": "64125.10",
                        "q": "0.005",
                        "b": 3001,
                        "a": 3002,
                        "T": 1711281060000,
                        "m": False,
                        "M": True,
                    },
                }
            )

        def close(self) -> None:
            return None

    def create_connection(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise TimeoutError("transient websocket timeout")
        return DummySocket()

    monkeypatch.setitem(sys.modules, "websocket", types.SimpleNamespace(create_connection=create_connection))

    bronze_records, source_details = load_live_records(
        source_mode="binance_ws",
        input_path=None,
        symbols=["BTCUSDT"],
        stream_types=["trade"],
        kline_interval="1m",
        websocket_base_url="wss://data-stream.binance.vision",
        max_messages=1,
        duration_seconds=None,
        receive_timeout_seconds=0.1,
        connect_retries=2,
        retry_backoff_seconds=0.0,
    )

    assert len(bronze_records) == 1
    assert source_details["connection_attempts"] == 2
    assert source_details["connect_retries"] == 2
