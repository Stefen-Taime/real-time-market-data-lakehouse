#!/usr/bin/env python3
"""Publish a compact market-observability serving snapshot directly to a Databricks SQL warehouse."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any


def _resolve_project_root() -> Path:
    cwd = Path(__file__).resolve().parent
    for candidate in (cwd, *cwd.parents):
        if (candidate / "config").exists() and (candidate / "src").exists():
            return candidate
    return cwd


project_root = _resolve_project_root()

import sys

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.ingestion.ingest_binance_live import fetch_recent_trade_records
from src.ingestion.load_binance_history import fetch_recent_history_records
from src.serving.publish_app_serving_layer import SERVING_TABLE_COLUMNS, build_direct_serving_snapshots
from src.utils.config_loader import load_project_config
from src.utils.databricks_warehouse_sql import (
    build_create_or_replace_empty_table_statement,
    build_create_schema_statement,
    build_insert_statement,
    chunk_rows,
    execute_sql_statement,
)


def _load_auth(profile: str) -> tuple[str, str]:
    host_payload = json.loads(
        subprocess.check_output(["databricks", "auth", "env", "--profile", profile, "-o", "json"])
    )
    token_payload = json.loads(
        subprocess.check_output(["databricks", "auth", "token", "--profile", profile, "-o", "json"])
    )
    return host_payload["env"]["DATABRICKS_HOST"], token_payload["access_token"]


def _rows_from_dicts(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> list[tuple[Any, ...]]:
    column_names = [column_name for column_name, _ in columns]
    return [tuple(row.get(column_name) for column_name in column_names) for row in rows]


def publish_snapshot(*, env: str, profile: str) -> dict[str, Any]:
    config = load_project_config(project_root / "config", env)
    direct_publish = config.get("direct_publish", {})
    serving_table_names = config["serving_tables"]["names"]
    serving_catalog = config["serving_tables"]["catalog"]
    serving_schema = config["serving_tables"]["schema"]
    warehouse_id = config["app"]["warehouse_id"]

    host, token = _load_auth(profile)

    symbols = direct_publish.get("symbols", config.get("symbols", []))
    kline_interval = direct_publish.get("kline_interval", config.get("market_source", {}).get("history_interval", "1m"))

    bronze_history_records = fetch_recent_history_records(
        symbols=symbols,
        interval=kline_interval,
        limit=int(direct_publish.get("history_limit", 30)),
        base_url=direct_publish.get("history_rest_base_url", "https://api.binance.com"),
        timeout_seconds=float(direct_publish.get("history_timeout_seconds", 15.0)),
    )
    live_records = fetch_recent_trade_records(
        symbols=symbols,
        limit=int(direct_publish.get("trade_rest_limit", 200)),
        base_url=direct_publish.get("trade_rest_base_url", "https://api.binance.com"),
        timeout_seconds=float(direct_publish.get("trade_timeout_seconds", 15.0)),
    )

    direct_dataset_rows = build_direct_serving_snapshots(
        environment=env,
        serving_table_names=serving_table_names,
        bronze_history_records=bronze_history_records,
        live_records=live_records,
        volatility_window_size=int(direct_publish.get("volatility_window_size", 5)),
        top_movers_limit=int(direct_publish.get("top_movers_limit", 10)),
    )

    execute_sql_statement(
        host=host,
        token=token,
        warehouse_id=warehouse_id,
        statement=build_create_schema_statement(f"{serving_catalog}.{serving_schema}"),
    )

    dataset_summaries: list[dict[str, Any]] = []
    for dataset_name, serving_table_name in serving_table_names.items():
        columns = SERVING_TABLE_COLUMNS[dataset_name]
        rows = _rows_from_dicts(direct_dataset_rows.get(dataset_name, []), columns)
        full_table_name = f"{serving_catalog}.{serving_schema}.{serving_table_name}"

        execute_sql_statement(
            host=host,
            token=token,
            warehouse_id=warehouse_id,
            statement=build_create_or_replace_empty_table_statement(full_table_name, columns),
        )
        for row_batch in chunk_rows(rows, int(config["app"].get("sql_insert_batch_size", 200))):
            execute_sql_statement(
                host=host,
                token=token,
                warehouse_id=warehouse_id,
                statement=build_insert_statement(full_table_name, columns, row_batch),
            )

        dataset_summaries.append(
            {
                "dataset_name": dataset_name,
                "table_name": full_table_name,
                "row_count": len(rows),
            }
        )

    return {
        "environment": env,
        "profile": profile,
        "warehouse_id": warehouse_id,
        "published_dataset_count": len(dataset_summaries),
        "datasets": dataset_summaries,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish a local warehouse-first observability snapshot.")
    parser.add_argument("--env", default="azure_trial_app")
    parser.add_argument("--profile", default="azure-trial")
    args = parser.parse_args()

    summary = publish_snapshot(env=args.env, profile=args.profile)
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
