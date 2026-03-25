# Real-Time Market Data Lakehouse

A real-time market data platform built on a Bronze / Silver / Gold lakehouse architecture with Databricks, Binance market data ingestion, data quality controls, auditability, bundle-based orchestration, and a Databricks notebook dashboard.

The project was pushed to a production-like level across Databricks Free Edition and Azure Databricks Trial, with real pipeline runs, GitHub Actions CI/CD, and a working serving layer for dashboard consumption.

## Problem

Building a real-time market data platform is more than showing a few prices:

- you need both batch and live ingestion
- you need normalized and historized data
- you need stable analytical tables
- you need quality and freshness controls
- you need a simple consumption layer for dashboards and monitoring

## Solution

This repository implements an end-to-end pipeline:

- historical and live Binance ingestion
- Medallion architecture with Bronze / Silver / Gold layers
- incremental calculations for latest price, 1-minute volume, 5-minute volatility, and top movers
- quality checks, audit trails, and processing-state watermarks
- Databricks Bundles with notebook jobs
- a Databricks notebook dashboard backed by warehouse-serving tables

## Results

The project has been validated with real executions:

- Databricks pipelines executed successfully
- GitHub Actions CI/CD is operational
- `dev`, `staging`, `prod`, and `azure_trial` environments are configured
- the notebook dashboard displays real data on Azure trial

## At A Glance

- historical and live Binance ingestion
- Bronze / Silver / Gold Medallion architecture
- incremental `MERGE`-based transformations with watermarks
- data quality checks, audit, and freshness monitoring
- Databricks Bundle orchestration with notebook jobs
- GitHub Actions CI/CD to Databricks
- notebook dashboard powered by real serving data

## Architecture

The logical flow is:

1. `Bronze`
   - raw historical klines
   - raw live trades
   - raw live klines
2. `Silver`
   - normalized trades
   - normalized 1-minute klines
3. `Gold`
   - latest price
   - 1-minute volume
   - 5-minute volatility
   - top movers
4. `Quality / Audit`
   - schema, null, duplicate, freshness, and bounds checks
   - persisted quality runs
   - processing watermark tracking
5. `Serving / Dashboard`
   - serving tables for dashboard consumption
   - Databricks notebook dashboard for market observability

## Current Execution Model

The project currently operates in two main modes:

- `dev / staging / prod` on the original workspace
- `azure_trial` on Azure Databricks Trial for dashboard validation

On `azure_trial`, the working dashboard path is:

1. Databricks remains the canonical Bronze / Silver / Gold / Quality pipeline
2. dashboard-serving datasets are published reliably into a Databricks SQL warehouse
3. the notebook dashboard reads those serving tables from the warehouse

This path was chosen because, on the tested runtimes, Spark notebook/job writes to tables or volumes were not reliable enough to serve the dashboard directly.

## Canonical Pipeline

The main orchestrated job is [market_data_medallion_pipeline.yml](resources/jobs/market_data_medallion_pipeline.yml). It runs:

1. [01_backfill_history.py](notebooks/01_backfill_history.py)
2. [02_ingest_live_market.py](notebooks/02_ingest_live_market.py)
3. [03_transform_silver.py](notebooks/03_transform_silver.py)
4. [04_build_gold.py](notebooks/04_build_gold.py)
5. [05_data_quality_checks.py](notebooks/05_data_quality_checks.py)
6. [08_publish_app_serving_layer.py](notebooks/08_publish_app_serving_layer.py)

## Azure Trial Dashboard

The currently working dashboard relies on:

- governed config: [azure_trial_governed.yml](config/azure_trial_governed.yml)
- serving/app config: [azure_trial_app.yml](config/azure_trial_app.yml)
- notebook dashboard: [10_market_observability_dashboard.py](notebooks/10_market_observability_dashboard.py)
- reliable serving publisher: [publish_warehouse_snapshot_local.py](scripts/publish_warehouse_snapshot_local.py)

Recommended dashboard refresh command:

```bash
python3 scripts/publish_warehouse_snapshot_local.py --env azure_trial_app --profile azure-trial
```

Then open the deployed notebook:

```text
/Workspace/Users/<user>/.bundle/real-time-market-data-lakehouse/azure_trial/files/notebooks/10_market_observability_dashboard
```

Recommended widget value:

```text
env = azure_trial_governed
```

## Repository Structure

- [src/](src): business logic for ingestion, transforms, gold, quality, serving, and utilities
- [notebooks/](notebooks): Databricks notebook entrypoints for each pipeline step
- [resources/jobs/](resources/jobs): bundle job orchestration
- [config/](config): environment-specific configuration
- [sql/](sql): analytical queries, data quality SQL, and DDL
- [tests/](tests): unit and integration tests
- [scripts/](scripts): workspace bootstrap and operational helpers

## Local Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Run tests:

```bash
pytest -q
```

3. Validate the bundle:

```bash
databricks bundle validate -t dev
```

## GitHub CI/CD

GitHub workflows live in:

- [ci.yml](.github/workflows/ci.yml)
- [cd.yml](.github/workflows/cd.yml)

They cover:

- Python compilation
- tests
- bundle validation
- bundle deployment to Databricks
- Databricks smoke runs

Required GitHub secrets for token-based auth:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

## Workspace Bootstrap

The [setup_workspace.sh](scripts/setup_workspace.sh) script prepares the target workspace idempotently.

Examples:

```bash
./scripts/setup_workspace.sh dev
./scripts/setup_workspace.sh staging
./scripts/setup_workspace.sh prod
```

## Project Status

Implemented today:

- batch and live pipelines
- Bronze / Silver / Gold lakehouse layers
- `MERGE`-based incrementality with watermarks
- quality checks, audit, and processing state
- Databricks Bundle orchestration
- GitHub Actions CI/CD
- Databricks notebook dashboard with real data on Azure trial

Still partial or platform-dependent:

- full enterprise Unity Catalog governance
- true multi-workspace isolation for `dev / staging / prod`
- Streamlit Databricks app as the main frontend
- enterprise-grade security and governance hardening

## Known Constraints

The repository was pushed as far as possible on limited Databricks editions. As a result:

- Free Edition is strong for prototyping, but limited for serving applications
- Azure Trial enabled dashboard validation, but through a warehouse-first serving path
- the project is a strong portfolio, demo, and technical reference, without pretending to be a fully hardened enterprise platform
