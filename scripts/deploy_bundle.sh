#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:-dev}"

databricks bundle deploy -t "$TARGET"
