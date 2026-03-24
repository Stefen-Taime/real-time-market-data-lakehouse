"""SQL query builders for the Streamlit market observability app."""

from __future__ import annotations

from src.utils.lakehouse_io import build_storage_path


def _quote_sql_string(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _quote_delta_path(path: str) -> str:
    escaped = path.replace("`", "``")
    return f"delta.`{escaped}`"


def build_delta_relation(base_path: str, layer: str, table: str) -> str:
    path = build_storage_path(base_path, layer, table)
    if not path:
        raise ValueError("base_path is required to build a Delta relation.")
    return _quote_delta_path(path)


def build_table_relation(catalog: str, schema: str, table: str) -> str:
    return ".".join(part for part in (catalog, schema, table) if part)


def build_relation(
    *,
    base_path: str | None = None,
    layer: str | None = None,
    physical_table: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    serving_table: str | None = None,
) -> str:
    if base_path and layer and physical_table:
        return build_delta_relation(base_path, layer, physical_table)
    if catalog and schema and serving_table:
        return build_table_relation(catalog, schema, serving_table)
    raise ValueError("Either a Delta base_path or a serving table reference is required.")


def build_symbol_filter_clause(symbols: list[str] | None, *, column: str = "symbol") -> str:
    normalized_symbols = [symbol.strip().upper() for symbol in (symbols or []) if symbol.strip()]
    if not normalized_symbols:
        return ""
    symbol_literals = ", ".join(_quote_sql_string(symbol) for symbol in normalized_symbols)
    return f"WHERE {column} IN ({symbol_literals})"


def build_latest_prices_query(
    *,
    base_path: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    serving_table: str | None = None,
    symbols: list[str] | None = None,
) -> str:
    relation = build_relation(
        base_path=base_path,
        layer="gold",
        physical_table="latest_price",
        catalog=catalog,
        schema=schema,
        serving_table=serving_table,
    )
    filter_clause = build_symbol_filter_clause(symbols)
    return f"""
SELECT symbol, latest_price, trade_time, source
FROM {relation}
{filter_clause}
ORDER BY trade_time DESC, symbol ASC
""".strip()


def build_recent_volume_query(
    *,
    base_path: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    serving_table: str | None = None,
    symbols: list[str] | None = None,
    lookback_hours: int = 6,
) -> str:
    relation = build_relation(
        base_path=base_path,
        layer="gold",
        physical_table="volume_1m",
        catalog=catalog,
        schema=schema,
        serving_table=serving_table,
    )
    symbol_filter = build_symbol_filter_clause(symbols)
    clauses = [f"window_start >= current_timestamp() - INTERVAL {int(lookback_hours)} HOURS"]
    if symbol_filter:
        clauses.append(symbol_filter.removeprefix("WHERE ").strip())
    where_clause = f"WHERE {' AND '.join(clauses)}"
    return f"""
SELECT symbol, window_start, volume_1m, notional_1m, trade_count
FROM {relation}
{where_clause}
ORDER BY window_start DESC, symbol ASC
""".strip()


def build_latest_volatility_query(
    *,
    base_path: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    serving_table: str | None = None,
    symbols: list[str] | None = None,
) -> str:
    relation = build_relation(
        base_path=base_path,
        layer="gold",
        physical_table="volatility_5m",
        catalog=catalog,
        schema=schema,
        serving_table=serving_table,
    )
    filter_clause = build_symbol_filter_clause(symbols)
    return f"""
WITH ranked AS (
  SELECT symbol,
         window_start,
         window_end,
         volatility_5m,
         observation_count,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY window_end DESC, window_start DESC) AS row_number
  FROM {relation}
  {filter_clause}
)
SELECT symbol, window_start, window_end, volatility_5m, observation_count
FROM ranked
WHERE row_number = 1
ORDER BY volatility_5m DESC, symbol ASC
""".strip()


def build_top_movers_query(
    *,
    base_path: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    serving_table: str | None = None,
    symbols: list[str] | None = None,
    limit: int = 10,
) -> str:
    relation = build_relation(
        base_path=base_path,
        layer="gold",
        physical_table="top_movers",
        catalog=catalog,
        schema=schema,
        serving_table=serving_table,
    )
    filter_clause = build_symbol_filter_clause(symbols)
    return f"""
SELECT symbol, window_start, window_end, start_price, end_price, move_pct
FROM {relation}
{filter_clause}
ORDER BY ABS(move_pct) DESC, symbol ASC
LIMIT {int(limit)}
""".strip()


def build_quality_latest_status_query(
    *,
    base_path: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    serving_table: str | None = None,
) -> str:
    relation = build_relation(
        base_path=base_path,
        layer="audit",
        physical_table="quality_check_runs",
        catalog=catalog,
        schema=schema,
        serving_table=serving_table,
    )
    return f"""
WITH latest_run AS (
  SELECT audit_run_id
  FROM {relation}
  ORDER BY checked_at DESC, audit_run_id DESC
  LIMIT 1
)
SELECT checked_at, dataset_name, layer, table_name, passed, row_count, violation_count, job_name, task_name
FROM {relation}
WHERE audit_run_id IN (SELECT audit_run_id FROM latest_run)
ORDER BY passed ASC, dataset_name ASC
""".strip()


def build_processing_state_query(
    *,
    base_path: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    serving_table: str | None = None,
) -> str:
    relation = build_relation(
        base_path=base_path,
        layer="audit",
        physical_table="processing_state",
        catalog=catalog,
        schema=schema,
        serving_table=serving_table,
    )
    return f"""
SELECT pipeline_name,
       dataset_name,
       source_layer,
       target_layer,
       watermark_column,
       last_processed_at,
       updated_at,
       metadata_json
FROM {relation}
ORDER BY updated_at DESC, pipeline_name ASC, dataset_name ASC
""".strip()


def build_freshness_query(
    *,
    base_path: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    serving_tables: dict[str, str] | None = None,
) -> str:
    serving_tables = serving_tables or {}
    latest_price = build_relation(
        base_path=base_path,
        layer="gold",
        physical_table="latest_price",
        catalog=catalog,
        schema=schema,
        serving_table=serving_tables.get("gold_latest_price"),
    )
    volume_1m = build_relation(
        base_path=base_path,
        layer="gold",
        physical_table="volume_1m",
        catalog=catalog,
        schema=schema,
        serving_table=serving_tables.get("gold_volume_1m"),
    )
    volatility_5m = build_relation(
        base_path=base_path,
        layer="gold",
        physical_table="volatility_5m",
        catalog=catalog,
        schema=schema,
        serving_table=serving_tables.get("gold_volatility_5m"),
    )
    top_movers = build_relation(
        base_path=base_path,
        layer="gold",
        physical_table="top_movers",
        catalog=catalog,
        schema=schema,
        serving_table=serving_tables.get("gold_top_movers"),
    )
    quality_runs = build_relation(
        base_path=base_path,
        layer="audit",
        physical_table="quality_check_runs",
        catalog=catalog,
        schema=schema,
        serving_table=serving_tables.get("audit_quality_check_runs"),
    )
    processing_state = build_relation(
        base_path=base_path,
        layer="audit",
        physical_table="processing_state",
        catalog=catalog,
        schema=schema,
        serving_table=serving_tables.get("audit_processing_state"),
    )
    return f"""
SELECT 'gold_latest_price' AS dataset_name, COUNT(*) AS row_count, MAX(trade_time) AS latest_timestamp
FROM {latest_price}
UNION ALL
SELECT 'gold_volume_1m' AS dataset_name, COUNT(*) AS row_count, MAX(window_start) AS latest_timestamp
FROM {volume_1m}
UNION ALL
SELECT 'gold_volatility_5m' AS dataset_name, COUNT(*) AS row_count, MAX(window_end) AS latest_timestamp
FROM {volatility_5m}
UNION ALL
SELECT 'gold_top_movers' AS dataset_name, COUNT(*) AS row_count, MAX(window_end) AS latest_timestamp
FROM {top_movers}
UNION ALL
SELECT 'audit_quality_check_runs' AS dataset_name, COUNT(*) AS row_count, MAX(checked_at) AS latest_timestamp
FROM {quality_runs}
UNION ALL
SELECT 'audit_processing_state' AS dataset_name, COUNT(*) AS row_count, MAX(updated_at) AS latest_timestamp
FROM {processing_state}
""".strip()
