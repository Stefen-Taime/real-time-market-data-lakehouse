from src.utils.config_loader import load_project_config


def test_load_project_config_merges_common_env_and_symbols() -> None:
    config = load_project_config("config", "dev")

    assert config["project_name"] == "real-time-market-data-lakehouse"
    assert config["environment"] == "dev"
    assert config["schemas"]["gold"] == "gold"
    assert "BTCUSDT" in config["symbols"]


def test_load_project_config_applies_dev_governed_overrides() -> None:
    config = load_project_config("config", "dev_governed")

    assert config["catalog"] == "workspace"
    assert config["databricks"]["register_tables"] is True
    assert config["schemas"]["gold"] == "default"
    assert config["tables"]["gold"]["latest_price"] == "gold_latest_price"


def test_load_project_config_applies_dev_app_overrides() -> None:
    config = load_project_config("config", "dev_app")

    assert config["volume"]["name"] == "market_data_app"
    assert config["paths"]["source_delta_base_path"] == "dbfs:/tmp/real-time-market-data-lakehouse/dev"
    assert config["paths"]["delta_base_path"] == "dbfs:/Volumes/workspace/default/market_data_app/dev"
    assert config["app"]["warehouse_id"] == "38144212fd15dd19"


def test_load_project_config_applies_staging_overrides() -> None:
    config = load_project_config("config", "staging")

    assert config["environment"] == "staging"
    assert config["workspace"]["target"] == "staging"
    assert config["paths"]["delta_base_path"] == "dbfs:/tmp/real-time-market-data-lakehouse/staging"
    assert config["databricks"]["register_tables"] is False


def test_load_project_config_applies_prod_governed_overrides() -> None:
    config = load_project_config("config", "prod_governed")

    assert config["environment"] == "prod_governed"
    assert config["catalog"] == "workspace"
    assert config["databricks"]["register_tables"] is True
    assert config["tables"]["gold"]["latest_price"] == "prod_gold_latest_price"


def test_load_project_config_applies_prod_app_overrides() -> None:
    config = load_project_config("config", "prod_app")

    assert config["environment"] == "prod_app"
    assert config["paths"]["source_delta_base_path"] == "dbfs:/tmp/real-time-market-data-lakehouse/prod"
    assert config["app"]["serving_base_path"] == "/Volumes/workspace/default/market_data_app_prod/prod"


def test_load_project_config_applies_azure_trial_overrides() -> None:
    config = load_project_config("config", "azure_trial")

    assert config["environment"] == "azure_trial"
    assert config["catalog"] == "dbx_trial_marketdata"
    assert config["databricks"]["register_tables"] is False
    assert (
        config["paths"]["delta_base_path"]
        == "/Volumes/dbx_trial_marketdata/default/market_data_app_azure_trial/azure_trial/lakehouse"
    )
    assert config["tables"]["gold"]["latest_price"] == "azure_trial_gold_latest_price"


def test_load_project_config_applies_azure_trial_app_overrides() -> None:
    config = load_project_config("config", "azure_trial_app")

    assert config["environment"] == "azure_trial_app"
    assert config["volume"]["catalog"] == "dbx_trial_marketdata"
    assert (
        config["paths"]["source_delta_base_path"]
        == "/Volumes/dbx_trial_marketdata/default/market_data_app_azure_trial/azure_trial/lakehouse"
    )
    assert config["app"]["warehouse_id"] == "14c0f2ddc3545df2"
    assert config["app"]["serving_base_path"] == "/Volumes/dbx_trial_marketdata/default/market_data_app_azure_trial/azure_trial"
    assert config["direct_publish"]["enabled"] is True
    assert config["direct_publish"]["history_source_mode"] == "binance_rest"


def test_load_project_config_applies_azure_trial_governed_dashboard_overrides() -> None:
    config = load_project_config("config", "azure_trial_governed")

    assert config["environment"] == "azure_trial_governed"
    assert config["app"]["warehouse_id"] == "14c0f2ddc3545df2"
    assert config["app"]["query_mode"] == "table"
    assert config["serving_tables"]["names"]["gold_latest_price"] == "azure_trial_app_gold_latest_price"
