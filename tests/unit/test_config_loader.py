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
