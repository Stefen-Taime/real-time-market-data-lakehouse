#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:-dev}"

databricks bundle validate -t "$TARGET"
