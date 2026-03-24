from __future__ import annotations

import os
from pathlib import Path
import sys
import time

import pandas as pd
import plotly.express as px
import streamlit as st
from databricks.sdk import WorkspaceClient


def _resolve_project_root() -> Path:
    cwd = Path(__file__).resolve().parent
    for candidate in (cwd, *cwd.parents):
        if (candidate / "config").exists() and (candidate / "src").exists():
            return candidate
    return cwd


project_root = _resolve_project_root()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.apps.market_observability_queries import (
    build_freshness_query,
    build_latest_prices_query,
    build_latest_volatility_query,
    build_processing_state_query,
    build_quality_latest_status_query,
    build_recent_volume_query,
    build_top_movers_query,
)
from src.utils.config_loader import load_project_config


st.set_page_config(
    page_title="Market Observability",
    layout="wide",
)


st.markdown(
    """
<style>
  .stApp {
    background:
      radial-gradient(circle at top right, rgba(245, 158, 11, 0.14), transparent 28%),
      radial-gradient(circle at top left, rgba(59, 130, 246, 0.12), transparent 24%),
      linear-gradient(180deg, #07111f 0%, #0f172a 60%, #111827 100%);
    color: #e5e7eb;
  }
  [data-testid="stMetricValue"] {
    font-size: 1.8rem;
  }
  .block-container {
    padding-top: 2rem;
    padding-bottom: 2rem;
  }
  .dashboard-kicker {
    text-transform: uppercase;
    letter-spacing: 0.14em;
    font-size: 0.72rem;
    color: #f59e0b;
    margin-bottom: 0.5rem;
  }
  .dashboard-title {
    font-size: 2.35rem;
    font-weight: 700;
    margin-bottom: 0.35rem;
  }
  .dashboard-subtitle {
    color: #94a3b8;
    margin-bottom: 1.35rem;
  }
  .section-label {
    font-size: 0.85rem;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: #cbd5e1;
    margin-bottom: 0.5rem;
  }
</style>
""",
    unsafe_allow_html=True,
)


env = os.getenv("APP_ENV", "dev_app")
config = load_project_config(project_root / "config", env)

def _normalize_volume_path(value: str) -> str:
    normalized = value.strip().rstrip("/")
    if normalized.startswith("/Volumes/"):
        return normalized
    if normalized.count(".") == 2:
        return f"/Volumes/{normalized.replace('.', '/')}"
    return normalized


default_volume_path = config.get("app", {}).get("volume_path", "")
volume_root_path = _normalize_volume_path(os.getenv("MARKET_DATA_VOLUME_PATH", default_volume_path))
query_mode = os.getenv("MARKET_DATA_QUERY_MODE", config.get("app", {}).get("query_mode", "table")).strip().lower()
serving_base_path = os.getenv(
    "MARKET_DATA_SERVING_BASE_PATH",
    config.get("app", {}).get("serving_base_path", ""),
)
warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", config.get("app", {}).get("warehouse_id", "")).strip()
serving_catalog = os.getenv("MARKET_DATA_SERVING_CATALOG", config.get("app", {}).get("serving_catalog", "workspace"))
serving_schema = os.getenv("MARKET_DATA_SERVING_SCHEMA", config.get("app", {}).get("serving_schema", "default"))
serving_tables = config.get("serving_tables", {}).get("names", {})
default_limit = int(config.get("app", {}).get("default_limit", 25))
default_volume_hours = int(config.get("app", {}).get("default_volume_hours", 6))
default_top_movers_limit = int(config.get("app", {}).get("default_top_movers_limit", 10))


@st.cache_resource(show_spinner=False)
def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


def execute_query(statement: str) -> pd.DataFrame:
    if not warehouse_id:
        raise RuntimeError("DATABRICKS_WAREHOUSE_ID is not configured.")

    client = get_workspace_client()
    payload = {
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": "20s",
        "format": "JSON_ARRAY",
    }
    response = client.api_client.do(
        "POST",
        "/api/2.0/sql/statements/",
        body=payload,
        headers={"Content-Type": "application/json"},
    )

    statement_id = response.get("statement_id")
    status = response.get("status", {})
    state = status.get("state")
    deadline = time.time() + 60

    while state in {"PENDING", "RUNNING"} and statement_id and time.time() < deadline:
        time.sleep(2)
        response = client.api_client.do("GET", f"/api/2.0/sql/statements/{statement_id}")
        status = response.get("status", {})
        state = status.get("state")

    if state != "SUCCEEDED":
        error = status.get("error", {})
        message = error.get("message") or status.get("state") or "Unknown Databricks SQL error."
        raise RuntimeError(message)

    manifest = response.get("manifest", {})
    columns = [column["name"] for column in manifest.get("schema", {}).get("columns", [])]
    rows = response.get("result", {}).get("data_array", []) or []
    return pd.DataFrame(rows, columns=columns)


def build_query_kwargs(dataset_name: str) -> dict[str, str]:
    if query_mode == "table":
        return {
            "catalog": serving_catalog,
            "schema": serving_schema,
            "serving_table": serving_tables[dataset_name],
        }

    return {"base_path": serving_base_path}


@st.cache_data(ttl=30, show_spinner=False)
def load_dashboard_data(
    symbols: tuple[str, ...],
    lookback_hours: int,
    top_movers_limit: int,
) -> dict[str, pd.DataFrame]:
    selected_symbols = list(symbols)
    return {
        "latest_prices": execute_query(
            build_latest_prices_query(
                **build_query_kwargs("gold_latest_price"),
                symbols=selected_symbols,
            )
        ),
        "recent_volume": execute_query(
            build_recent_volume_query(
                **build_query_kwargs("gold_volume_1m"),
                symbols=selected_symbols,
                lookback_hours=lookback_hours,
            )
        ),
        "latest_volatility": execute_query(
            build_latest_volatility_query(
                **build_query_kwargs("gold_volatility_5m"),
                symbols=selected_symbols,
            )
        ),
        "top_movers": execute_query(
            build_top_movers_query(
                **build_query_kwargs("gold_top_movers"),
                symbols=selected_symbols,
                limit=top_movers_limit,
            )
        ),
        "quality_latest_status": execute_query(
            build_quality_latest_status_query(**build_query_kwargs("audit_quality_check_runs"))
        ),
        "processing_state": execute_query(
            build_processing_state_query(**build_query_kwargs("audit_processing_state"))
        ),
        "freshness": execute_query(
            build_freshness_query(
                base_path=serving_base_path if query_mode != "table" else None,
                catalog=serving_catalog if query_mode == "table" else None,
                schema=serving_schema if query_mode == "table" else None,
                serving_tables=serving_tables if query_mode == "table" else None,
            )
        ),
    }


def _coerce_timestamps(dataframe: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    converted = dataframe.copy()
    for column in columns:
        if column in converted.columns:
            converted[column] = pd.to_datetime(converted[column], utc=True, errors="coerce")
    return converted


def _render_metric_row(data: dict[str, pd.DataFrame]) -> None:
    freshness = _coerce_timestamps(data["freshness"], ["latest_timestamp"])
    latest_prices = _coerce_timestamps(data["latest_prices"], ["trade_time"])
    quality = data["quality_latest_status"].copy()
    processing_state = _coerce_timestamps(data["processing_state"], ["last_processed_at", "updated_at"])

    latest_trade_time = latest_prices["trade_time"].max() if not latest_prices.empty else None
    failed_dataset_count = 0
    if not quality.empty and "passed" in quality.columns:
        failed_dataset_count = int((quality["passed"].astype(str).str.lower() != "true").sum())

    stale_watermark_count = 0
    if not processing_state.empty and "updated_at" in processing_state.columns:
        watermark_age_minutes = (
            (pd.Timestamp.now(tz="UTC") - processing_state["updated_at"])
            .dt.total_seconds()
            .div(60.0)
        )
        stale_watermark_count = int((watermark_age_minutes > 20).sum())

    latest_price_symbols = int(len(latest_prices))
    freshest_dataset = (
        freshness.sort_values("latest_timestamp", ascending=False).iloc[0]["dataset_name"]
        if not freshness.empty
        else "n/a"
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Latest Price Symbols", latest_price_symbols)
    col2.metric("Failed Quality Datasets", failed_dataset_count)
    col3.metric("Stale Watermarks", stale_watermark_count)
    col4.metric(
        "Freshest Dataset",
        freshest_dataset,
        delta=str(latest_trade_time) if latest_trade_time is not None else "no trade data",
    )


def _render_charts(data: dict[str, pd.DataFrame]) -> None:
    latest_prices = _coerce_timestamps(data["latest_prices"], ["trade_time"])
    recent_volume = _coerce_timestamps(data["recent_volume"], ["window_start"])
    latest_volatility = _coerce_timestamps(data["latest_volatility"], ["window_start", "window_end"])
    top_movers = _coerce_timestamps(data["top_movers"], ["window_start", "window_end"])
    freshness = _coerce_timestamps(data["freshness"], ["latest_timestamp"])
    quality = data["quality_latest_status"].copy()
    processing_state = _coerce_timestamps(data["processing_state"], ["last_processed_at", "updated_at"])

    left, right = st.columns((1.25, 1))

    with left:
        st.markdown('<div class="section-label">Gold Signals</div>', unsafe_allow_html=True)
        if not latest_prices.empty:
            latest_price_fig = px.bar(
                latest_prices.sort_values("latest_price", ascending=False),
                x="symbol",
                y="latest_price",
                color="source",
                title="Latest Price by Symbol",
                color_discrete_sequence=["#f59e0b", "#38bdf8", "#34d399"],
            )
            latest_price_fig.update_layout(height=360, margin=dict(l=10, r=10, t=50, b=10))
            st.plotly_chart(latest_price_fig, use_container_width=True)

        if not recent_volume.empty:
            volume_fig = px.line(
                recent_volume.sort_values("window_start"),
                x="window_start",
                y="volume_1m",
                color="symbol",
                title="Recent Volume 1m",
                color_discrete_sequence=px.colors.qualitative.Bold,
            )
            volume_fig.update_layout(height=360, margin=dict(l=10, r=10, t=50, b=10))
            st.plotly_chart(volume_fig, use_container_width=True)

    with right:
        st.markdown('<div class="section-label">Operational Health</div>', unsafe_allow_html=True)
        if not latest_volatility.empty:
            volatility_fig = px.bar(
                latest_volatility.sort_values("volatility_5m", ascending=False),
                x="symbol",
                y="volatility_5m",
                title="Latest Volatility 5m",
                color="volatility_5m",
                color_continuous_scale="Solar",
            )
            volatility_fig.update_layout(height=255, margin=dict(l=10, r=10, t=50, b=10), coloraxis_showscale=False)
            st.plotly_chart(volatility_fig, use_container_width=True)

        if not top_movers.empty:
            movers_fig = px.bar(
                top_movers.sort_values("move_pct"),
                x="move_pct",
                y="symbol",
                orientation="h",
                title="Top Movers",
                color="move_pct",
                color_continuous_scale=["#1d4ed8", "#94a3b8", "#f59e0b"],
            )
            movers_fig.update_layout(height=255, margin=dict(l=10, r=10, t=50, b=10), coloraxis_showscale=False)
            st.plotly_chart(movers_fig, use_container_width=True)

    lower_left, lower_right = st.columns((1.1, 0.9))

    with lower_left:
        if not freshness.empty:
            freshness["age_minutes"] = (
                pd.Timestamp.now(tz="UTC") - freshness["latest_timestamp"]
            ).dt.total_seconds().div(60.0).round(2)
            freshness_fig = px.bar(
                freshness.sort_values("age_minutes", ascending=False),
                x="dataset_name",
                y="age_minutes",
                title="Dataset Freshness Age (minutes)",
                color="dataset_name",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            freshness_fig.update_layout(height=320, margin=dict(l=10, r=10, t=50, b=10), showlegend=False)
            st.plotly_chart(freshness_fig, use_container_width=True)

    with lower_right:
        if not quality.empty:
            quality["status"] = quality["passed"].astype(str).str.lower().map({"true": "passed", "false": "failed"}).fillna("unknown")
            quality_fig = px.pie(
                quality,
                names="status",
                title="Latest Quality Run Status",
                color="status",
                color_discrete_map={"passed": "#10b981", "failed": "#ef4444", "unknown": "#94a3b8"},
            )
            quality_fig.update_layout(height=320, margin=dict(l=10, r=10, t=50, b=10))
            st.plotly_chart(quality_fig, use_container_width=True)

    st.markdown('<div class="section-label">Detailed Tables</div>', unsafe_allow_html=True)
    table_left, table_right = st.columns(2)
    with table_left:
        st.dataframe(
            quality[["dataset_name", "layer", "passed", "row_count", "violation_count", "job_name", "task_name"]],
            use_container_width=True,
            hide_index=True,
        )
    with table_right:
        if not processing_state.empty:
            processing_state["updated_at"] = processing_state["updated_at"].dt.strftime("%Y-%m-%d %H:%M:%S %Z")
            processing_state["last_processed_at"] = processing_state["last_processed_at"].dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        st.dataframe(
            processing_state[
                [
                    "pipeline_name",
                    "dataset_name",
                    "source_layer",
                    "target_layer",
                    "last_processed_at",
                    "updated_at",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )


st.markdown('<div class="dashboard-kicker">Real-Time Market Data Lakehouse</div>', unsafe_allow_html=True)
st.markdown('<div class="dashboard-title">Market Observability</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="dashboard-subtitle">Cockpit Streamlit + Plotly branche sur Databricks SQL pour suivre les signaux Gold, la qualite et les watermarks de traitement.</div>',
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Configuration")
    st.caption("L app interroge le Serverless Starter Warehouse via Statement Execution.")
    symbol_input = st.text_input("Symbols", value="BTCUSDT,ETHUSDT,SOLUSDT")
    selected_symbols = tuple(symbol.strip().upper() for symbol in symbol_input.split(",") if symbol.strip())
    lookback_hours = st.slider("Volume lookback (hours)", min_value=1, max_value=24, value=default_volume_hours)
    top_movers_limit = st.slider("Top movers limit", min_value=5, max_value=25, value=default_top_movers_limit)
    st.text_input("Warehouse ID", value=warehouse_id, disabled=True)
    st.text_input("Serving base path", value=serving_base_path, disabled=True)
    refresh = st.button("Refresh now", type="primary", use_container_width=True)

if refresh:
    load_dashboard_data.clear()

try:
    dashboard_data = load_dashboard_data(selected_symbols, lookback_hours, top_movers_limit)
    _render_metric_row(dashboard_data)
    _render_charts(dashboard_data)
except Exception as exc:
    st.error(f"Unable to load observability data: {exc}")
    st.code(
        "\n".join(
            [
                f"APP_ENV={env}",
                f"MARKET_DATA_QUERY_MODE={query_mode}",
                f"DATABRICKS_WAREHOUSE_ID={warehouse_id}",
                f"MARKET_DATA_SERVING_BASE_PATH={serving_base_path}",
                f"MARKET_DATA_SERVING_SCHEMA={serving_catalog}.{serving_schema}",
            ]
        )
    )
    st.stop()
