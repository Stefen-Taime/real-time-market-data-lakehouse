"""Helpers for publishing small serving datasets through Databricks SQL warehouses."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import json
import math
import time
from typing import Any, Iterable
from urllib import error, request


def quote_identifier(value: str) -> str:
    """Quote a SQL identifier using Databricks-compatible backticks."""
    return f"`{value.replace('`', '``')}`"


def quote_qualified_name(value: str) -> str:
    """Quote a dotted qualified name component-by-component."""
    return ".".join(quote_identifier(part) for part in value.split(".") if part)


def sql_literal(value: Any) -> str:
    """Render a Python value as a Databricks SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return "NULL"
        return format(value, ".15g")
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        normalized = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        naive_utc = normalized.astimezone(timezone.utc).replace(tzinfo=None)
        return f"TIMESTAMP '{naive_utc.isoformat(sep=' ', timespec='microseconds')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, (dict, list, tuple)):
        return sql_literal(json.dumps(value, sort_keys=True, default=str))
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def build_create_schema_statement(schema_name: str) -> str:
    """Build a CREATE SCHEMA statement for a qualified schema name."""
    return f"CREATE SCHEMA IF NOT EXISTS {quote_qualified_name(schema_name)}"


def build_create_or_replace_empty_table_statement(
    table_name: str,
    columns: list[tuple[str, str]],
) -> str:
    """Create or replace a Delta table with a specific schema and no rows."""
    if not columns:
        raise ValueError("columns must contain at least one column.")

    projection = ", ".join(
        f"CAST(NULL AS {sql_type}) AS {quote_identifier(column_name)}"
        for column_name, sql_type in columns
    )
    return f"CREATE OR REPLACE TABLE {quote_qualified_name(table_name)} AS SELECT {projection} WHERE 1 = 0"


def build_insert_statement(
    table_name: str,
    columns: list[tuple[str, str]],
    rows: list[tuple[Any, ...]],
) -> str:
    """Build a single INSERT statement for a batch of rows."""
    if not rows:
        raise ValueError("rows must contain at least one row.")

    column_sql = ", ".join(quote_identifier(column_name) for column_name, _ in columns)
    values_sql = ", ".join(
        f"({', '.join(sql_literal(value) for value in row)})"
        for row in rows
    )
    return f"INSERT INTO {quote_qualified_name(table_name)} ({column_sql}) VALUES {values_sql}"


def chunk_rows(rows: list[tuple[Any, ...]], batch_size: int) -> Iterable[list[tuple[Any, ...]]]:
    """Yield deterministic row batches for multi-statement inserts."""
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero.")
    for index in range(0, len(rows), batch_size):
        yield rows[index : index + batch_size]


def extract_statement_error_message(result: dict[str, Any]) -> str:
    """Extract the most useful error message from a Statement Execution API payload."""
    status = result.get("status", {})
    error_payload = status.get("error", {})
    return (
        error_payload.get("message")
        or error_payload.get("error_code")
        or status.get("state")
        or "Unknown Databricks SQL error."
    )


def execute_sql_statement_with_context(
    dbutils_handle: Any,
    *,
    warehouse_id: str,
    statement: str,
    wait_timeout: str = "20s",
    poll_interval_seconds: float = 2.0,
    timeout_seconds: float = 60.0,
) -> dict[str, Any]:
    """Execute a SQL statement against a warehouse using the notebook context token."""
    if not warehouse_id:
        raise ValueError("warehouse_id is required.")

    context = dbutils_handle.notebook.entry_point.getDbutils().notebook().getContext()
    host = context.apiUrl().get()
    token = context.apiToken().get()

    payload = json.dumps(
        {
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": wait_timeout,
            "format": "JSON_ARRAY",
        }
    ).encode("utf-8")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    def _request(path: str, *, method: str, data: bytes | None = None) -> dict[str, Any]:
        http_request = request.Request(
            f"{host}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with request.urlopen(http_request) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:  # pragma: no cover - Databricks runtime specific
            payload_text = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Statement Execution API request failed with HTTP {exc.code}: {payload_text}"
            ) from exc

    result = _request("/api/2.0/sql/statements/", method="POST", data=payload)
    statement_id = result.get("statement_id")
    status = result.get("status", {})
    state = status.get("state")
    deadline = time.time() + timeout_seconds

    while state in {"PENDING", "RUNNING"} and statement_id and time.time() < deadline:
        time.sleep(poll_interval_seconds)
        result = _request(f"/api/2.0/sql/statements/{statement_id}", method="GET")
        status = result.get("status", {})
        state = status.get("state")

    if state != "SUCCEEDED":
        raise RuntimeError(
            f"Warehouse statement failed for {statement_id or 'unknown_statement'}: "
            f"{extract_statement_error_message(result)}"
        )

    return result
