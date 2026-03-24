from src.apps.market_observability_queries import (
    build_delta_relation,
    build_freshness_query,
    build_latest_prices_query,
    build_recent_volume_query,
    build_symbol_filter_clause,
    build_table_relation,
)


def test_build_delta_relation_uses_delta_path_notation() -> None:
    relation = build_delta_relation("/Volumes/workspace/default/market_data_app/dev", "gold", "latest_price")

    assert relation == "delta.`/Volumes/workspace/default/market_data_app/dev/gold/latest_price`"


def test_build_symbol_filter_clause_normalizes_symbols() -> None:
    clause = build_symbol_filter_clause(["btcusdt", " ETHUSDT "])

    assert clause == "WHERE symbol IN ('BTCUSDT', 'ETHUSDT')"


def test_build_latest_prices_query_includes_symbol_filter() -> None:
    query = build_latest_prices_query(base_path="/Volumes/workspace/default/market_data_app/dev", symbols=["BTCUSDT"])

    assert "FROM delta.`/Volumes/workspace/default/market_data_app/dev/gold/latest_price`" in query
    assert "WHERE symbol IN ('BTCUSDT')" in query


def test_build_recent_volume_query_applies_lookback_window() -> None:
    query = build_recent_volume_query(
        base_path="/Volumes/workspace/default/market_data_app/dev",
        symbols=["BTCUSDT", "ETHUSDT"],
        lookback_hours=12,
    )

    assert "INTERVAL 12 HOURS" in query
    assert "symbol IN ('BTCUSDT', 'ETHUSDT')" in query


def test_build_freshness_query_references_all_serving_datasets() -> None:
    query = build_freshness_query(base_path="/Volumes/workspace/default/market_data_app/dev")

    assert "gold/latest_price" in query
    assert "gold/volume_1m" in query
    assert "gold/volatility_5m" in query
    assert "gold/top_movers" in query
    assert "audit/quality_check_runs" in query
    assert "audit/processing_state" in query


def test_build_table_relation_uses_three_part_name() -> None:
    relation = build_table_relation("workspace", "default", "app_gold_latest_price")

    assert relation == "workspace.default.app_gold_latest_price"


def test_build_latest_prices_query_supports_serving_tables() -> None:
    query = build_latest_prices_query(
        catalog="workspace",
        schema="default",
        serving_table="app_gold_latest_price",
        symbols=["SOLUSDT"],
    )

    assert "FROM workspace.default.app_gold_latest_price" in query
    assert "WHERE symbol IN ('SOLUSDT')" in query
