"""Utilities for parsing raw Binance market payloads."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def timestamp_ms_to_iso8601(timestamp_ms: int | float | str | None) -> str | None:
    """Convert a millisecond Unix timestamp to UTC ISO-8601."""
    if timestamp_ms is None:
        return None

    return (
        datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def serialize_payload(payload: str | dict[str, Any] | list[Any]) -> str:
    """Serialize a payload into a stable JSON string when needed."""
    if isinstance(payload, str):
        return payload

    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def parse_json_payload(raw_payload: str) -> dict[str, Any]:
    """Parse a raw JSON payload into a Python dictionary."""
    payload = json.loads(raw_payload)
    if not isinstance(payload, dict):
        raise TypeError("Expected a JSON object payload.")
    return payload


def unwrap_binance_stream_payload(raw_payload: str | dict[str, Any]) -> dict[str, Any]:
    """Return the inner event payload from a combined Binance stream message when present."""
    payload = parse_json_payload(raw_payload) if isinstance(raw_payload, str) else raw_payload.copy()
    data = payload.get("data")
    if isinstance(data, dict):
        return data.copy()
    return payload


def parse_batch(raw_payloads: list[str | dict[str, Any]]) -> list[dict[str, Any]]:
    """Parse a batch of raw JSON payloads or Python dictionaries."""
    parsed_payloads: list[dict[str, Any]] = []
    for payload in raw_payloads:
        if isinstance(payload, str):
            parsed_payloads.append(parse_json_payload(payload))
            continue

        parsed_payloads.append(payload.copy())

    return parsed_payloads


def parse_trade_payload(
    raw_payload: str | dict[str, Any],
    source: str = "binance_live",
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Normalize a Binance trade payload into a Bronze-ready record."""
    payload = unwrap_binance_stream_payload(raw_payload)
    required_fields = ("s", "t", "p", "q", "T")
    missing_fields = [field for field in required_fields if field not in payload]
    if missing_fields:
        raise ValueError(f"Missing trade fields: {', '.join(missing_fields)}")

    record_ingested_at = ingested_at or utc_now_iso()

    return {
        "source": source,
        "event_type": payload.get("e", "trade"),
        "symbol": payload["s"],
        "trade_id": int(payload["t"]),
        "price": str(payload["p"]),
        "quantity": str(payload["q"]),
        "trade_time": timestamp_ms_to_iso8601(payload["T"]),
        "event_time": timestamp_ms_to_iso8601(payload.get("E", payload["T"])),
        "buyer_order_id": int(payload["b"]) if payload.get("b") is not None else None,
        "seller_order_id": int(payload["a"]) if payload.get("a") is not None else None,
        "is_market_maker": bool(payload.get("m", False)),
        "ingested_at": record_ingested_at,
        "raw_payload": serialize_payload(raw_payload),
    }


def parse_kline_payload(
    raw_payload: str | dict[str, Any],
    source: str = "binance_live",
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Normalize a Binance live kline payload into a Bronze-ready record."""
    payload = unwrap_binance_stream_payload(raw_payload)
    if "k" not in payload:
        raise ValueError("Expected a live kline payload containing the 'k' field.")

    kline = payload["k"]
    required_fields = ("s", "t", "T", "i", "o", "h", "l", "c", "v")
    missing_fields = [field for field in required_fields if field not in kline]
    if missing_fields:
        raise ValueError(f"Missing kline fields: {', '.join(missing_fields)}")

    record_ingested_at = ingested_at or utc_now_iso()

    return {
        "source": source,
        "event_type": payload.get("e", "kline"),
        "symbol": kline["s"],
        "interval": kline["i"],
        "open_time": timestamp_ms_to_iso8601(kline["t"]),
        "close_time": timestamp_ms_to_iso8601(kline["T"]),
        "open": str(kline["o"]),
        "high": str(kline["h"]),
        "low": str(kline["l"]),
        "close": str(kline["c"]),
        "volume": str(kline["v"]),
        "quote_volume": str(kline["q"]) if kline.get("q") is not None else None,
        "trade_count": int(kline["n"]) if kline.get("n") is not None else 0,
        "is_closed": bool(kline.get("x", False)),
        "event_time": timestamp_ms_to_iso8601(payload.get("E", kline["T"])),
        "ingested_at": record_ingested_at,
        "raw_payload": serialize_payload(raw_payload),
    }


def detect_payload_kind(raw_payload: str | dict[str, Any]) -> str:
    """Infer whether a payload is a trade or a kline event."""
    payload = unwrap_binance_stream_payload(raw_payload)
    if "k" in payload:
        return "kline"
    if {"s", "t", "p", "q", "T"}.issubset(payload):
        return "trade"
    raise ValueError("Unsupported payload shape for market ingestion.")


def parse_market_payload(
    raw_payload: str | dict[str, Any],
    source: str = "binance_live",
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Normalize a market payload by automatically selecting the parser."""
    payload_kind = detect_payload_kind(raw_payload)
    if payload_kind == "trade":
        return parse_trade_payload(raw_payload, source=source, ingested_at=ingested_at)
    return parse_kline_payload(raw_payload, source=source, ingested_at=ingested_at)
