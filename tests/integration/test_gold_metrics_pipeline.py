from pathlib import Path

import pytest

from src.gold.build_price_latest import select_latest_prices
from src.gold.build_top_movers import build_top_movers
from src.gold.build_volatility_5m import compute_volatility_5m
from src.gold.build_volume_1m import aggregate_volume_1m
from src.ingestion.ingest_binance_live import filter_live_records, load_live_file
from src.ingestion.load_binance_history import load_history_file
from src.transforms.bronze_to_silver_klines import transform_kline_batch
from src.transforms.bronze_to_silver_trades import transform_trade_batch


TRADES_FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "sample_trades.json"
KLINES_FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "sample_klines.json"


def test_gold_metrics_pipeline_builds_expected_outputs() -> None:
    silver_trades = transform_trade_batch(
        filter_live_records(load_live_file(TRADES_FIXTURE_PATH, ingested_at="2026-03-24T14:31:00Z"), "trade")
    )
    silver_klines = transform_kline_batch(load_history_file(KLINES_FIXTURE_PATH, ingested_at="2026-03-24T14:32:00Z"))

    latest_prices = {row["symbol"]: row for row in select_latest_prices(silver_trades)}
    volumes = {(row["symbol"], row["window_start"].isoformat().replace("+00:00", "Z")): row for row in aggregate_volume_1m(silver_trades)}
    volatility = compute_volatility_5m(silver_klines)
    top_movers = build_top_movers(silver_klines, top_n=2)

    assert latest_prices["BTCUSDT"]["latest_price"] == 65050.0
    assert latest_prices["ETHUSDT"]["latest_price"] == 3535.0

    assert volumes[("BTCUSDT", "2024-03-24T14:00:00Z")]["volume_1m"] == pytest.approx(0.3)
    assert volumes[("ETHUSDT", "2024-03-24T14:01:00Z")]["notional_1m"] == pytest.approx(5280.0)

    assert len(volatility) == 3
    assert {row["symbol"] for row in volatility} == {"BTCUSDT", "ETHUSDT"}
    assert all(row["volatility_5m"] > 0 for row in volatility)

    assert [row["symbol"] for row in top_movers] == ["ETHUSDT", "BTCUSDT"]
    assert top_movers[0]["move_pct"] == pytest.approx(1.28571429)
    assert top_movers[1]["move_pct"] == pytest.approx(0.61538462)
