#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:-dev}"
GOVERNED_ENV="${2:-${TARGET}_governed}"
APP_ENV="${3:-${TARGET}_app}"

if ! command -v databricks >/dev/null 2>&1; then
  echo "Databricks CLI is required. Install it before running this script." >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to resolve YAML configuration." >&2
  exit 1
fi

if [[ ! -f "config/${GOVERNED_ENV}.yml" ]]; then
  echo "Unknown governed config: config/${GOVERNED_ENV}.yml" >&2
  exit 1
fi

if [[ ! -f "config/${APP_ENV}.yml" ]]; then
  APP_ENV=""
fi

databricks bundle validate -t "${TARGET}" >/dev/null

eval "$(
  python3 - "${GOVERNED_ENV}" "${APP_ENV}" <<'PY'
from __future__ import annotations

import shlex
import sys
from pathlib import Path

from src.utils.config_loader import load_project_config

governed_env = sys.argv[1]
app_env = sys.argv[2]

governed = load_project_config(Path("config"), governed_env)
schema_names = list(dict.fromkeys(governed.get("schemas", {}).values()))

values = {
    "GOVERNED_REGISTER_TABLES": str(governed.get("databricks", {}).get("register_tables", False)).lower(),
    "GOVERNED_CATALOG": governed.get("catalog", ""),
    "GOVERNED_SCHEMAS": " ".join(schema_names),
}

if app_env:
    app_config = load_project_config(Path("config"), app_env)
    volume = app_config.get("volume", {})
    values["APP_VOLUME_CATALOG"] = volume.get("catalog", "")
    values["APP_VOLUME_SCHEMA"] = volume.get("schema", "")
    values["APP_VOLUME_NAME"] = volume.get("name", "")
else:
    values["APP_VOLUME_CATALOG"] = ""
    values["APP_VOLUME_SCHEMA"] = ""
    values["APP_VOLUME_NAME"] = ""

for key, value in values.items():
    print(f"{key}={shlex.quote(value)}")
PY
)"

ensure_catalog() {
  local catalog="$1"

  if [[ -z "${catalog}" || "${catalog}" == "workspace" ]]; then
    echo "Catalog ${catalog:-<empty>} is assumed to exist on this workspace."
    return
  fi

  if databricks catalogs get "${catalog}" >/dev/null 2>&1; then
    echo "Catalog ${catalog} already exists."
    return
  fi

  echo "Creating catalog ${catalog}."
  databricks catalogs create "${catalog}" \
    --comment "Managed by ${TARGET} bootstrap for ${GOVERNED_ENV}."
}

ensure_schema() {
  local catalog="$1"
  local schema="$2"
  local full_name="${catalog}.${schema}"

  if databricks schemas get "${full_name}" >/dev/null 2>&1; then
    echo "Schema ${full_name} already exists."
    return
  fi

  echo "Creating schema ${full_name}."
  databricks schemas create "${schema}" "${catalog}" \
    --comment "Managed by ${TARGET} bootstrap for ${GOVERNED_ENV}."
}

ensure_volume() {
  local catalog="$1"
  local schema="$2"
  local volume="$3"
  local full_name="${catalog}.${schema}.${volume}"

  if [[ -z "${catalog}" || -z "${schema}" || -z "${volume}" ]]; then
    return
  fi

  if databricks volumes read "${full_name}" >/dev/null 2>&1; then
    echo "Volume ${full_name} already exists."
    return
  fi

  echo "Creating volume ${full_name}."
  databricks volumes create "${catalog}" "${schema}" "${volume}" MANAGED \
    --comment "Managed app volume for ${TARGET}."
}

echo "Bootstrapping target=${TARGET} governed_env=${GOVERNED_ENV} app_env=${APP_ENV:-<none>}"

if [[ "${GOVERNED_REGISTER_TABLES}" != "true" ]]; then
  echo "Configuration ${GOVERNED_ENV} does not enable table registration. Nothing to bootstrap."
  exit 0
fi

ensure_catalog "${GOVERNED_CATALOG}"

for schema in ${GOVERNED_SCHEMAS}; do
  ensure_schema "${GOVERNED_CATALOG}" "${schema}"
done

if [[ -n "${APP_VOLUME_SCHEMA}" ]]; then
  ensure_schema "${APP_VOLUME_CATALOG}" "${APP_VOLUME_SCHEMA}"
fi

ensure_volume "${APP_VOLUME_CATALOG}" "${APP_VOLUME_SCHEMA}" "${APP_VOLUME_NAME}"

echo "Workspace bootstrap completed for ${TARGET}."
