from pathlib import Path

from src.ingestion.ingest_binance_live import load_live_file
from src.ingestion.load_binance_history import load_history_file
from src.serving.publish_app_serving_layer import build_direct_serving_snapshots


def test_build_direct_serving_snapshots_from_fixture_sources() -> None:
    fixture_root = Path("tests/fixtures")
    bronze_history_records = load_history_file(fixture_root / "sample_klines.json")
    live_records = load_live_file(fixture_root / "sample_live_market.json")

    snapshots = build_direct_serving_snapshots(
        environment="test",
        serving_table_names={
            "gold_latest_price": "app_gold_latest_price",
            "gold_volume_1m": "app_gold_volume_1m",
            "gold_volatility_5m": "app_gold_volatility_5m",
            "gold_top_movers": "app_gold_top_movers",
            "audit_quality_check_runs": "app_audit_quality_check_runs",
            "audit_processing_state": "app_audit_processing_state",
        },
        bronze_history_records=bronze_history_records,
        live_records=live_records,
        volatility_window_size=5,
        top_movers_limit=10,
    )

    assert snapshots["gold_latest_price"]
    assert snapshots["gold_volume_1m"]
    assert snapshots["gold_volatility_5m"]
    assert snapshots["gold_top_movers"]
    assert snapshots["audit_quality_check_runs"]
    assert snapshots["audit_processing_state"]
    assert {row["dataset_name"] for row in snapshots["audit_quality_check_runs"]} == {
        "gold_latest_price",
        "gold_volume_1m",
        "gold_volatility_5m",
        "gold_top_movers",
    }
