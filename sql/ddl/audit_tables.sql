CREATE TABLE IF NOT EXISTS audit.quality_check_runs (
  audit_run_id STRING,
  checked_at TIMESTAMP,
  environment STRING,
  job_name STRING,
  task_name STRING,
  dataset_name STRING,
  layer STRING,
  table_name STRING,
  storage_path STRING,
  passed BOOLEAN,
  row_count BIGINT,
  min_row_count BIGINT,
  violation_count INT,
  violations_json STRING,
  schema_passed BOOLEAN,
  null_checks_passed BOOLEAN,
  duplicate_check_passed BOOLEAN,
  freshness_check_passed BOOLEAN,
  bounds_check_passed BOOLEAN,
  summary_json STRING
);

CREATE TABLE IF NOT EXISTS audit.processing_state (
  state_key STRING,
  pipeline_name STRING,
  dataset_name STRING,
  source_layer STRING,
  target_layer STRING,
  watermark_column STRING,
  last_processed_at TIMESTAMP,
  updated_at TIMESTAMP,
  metadata_json STRING
);
