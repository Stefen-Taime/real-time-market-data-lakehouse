from src.serving.publish_app_serving_layer import build_source_dataset_reference


def test_build_source_dataset_reference_prefers_unity_catalog_table_when_enabled() -> None:
    table_name, source_path = build_source_dataset_reference(
        source_base_path=None,
        source_catalog="dbx_trial_marketdata",
        source_schemas={"gold": "default"},
        source_tables={"gold": {"latest_price": "azure_trial_gold_latest_price"}},
        layer="gold",
        table="latest_price",
        source_register_tables=True,
    )

    assert table_name == "dbx_trial_marketdata.default.azure_trial_gold_latest_price"
    assert source_path is None


def test_build_source_dataset_reference_returns_delta_path_when_table_registration_is_disabled() -> None:
    table_name, source_path = build_source_dataset_reference(
        source_base_path="dbfs:/tmp/real-time-market-data-lakehouse/dev",
        source_catalog=None,
        source_schemas=None,
        source_tables=None,
        layer="gold",
        table="latest_price",
        source_register_tables=False,
    )

    assert table_name is None
    assert source_path == "dbfs:/tmp/real-time-market-data-lakehouse/dev/gold/latest_price"
