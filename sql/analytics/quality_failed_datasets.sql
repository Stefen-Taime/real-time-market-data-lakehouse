SELECT
  checked_at,
  audit_run_id,
  environment,
  job_name,
  task_name,
  dataset_name,
  layer,
  table_name,
  row_count,
  violation_count,
  violations_json
FROM audit.quality_check_runs
WHERE passed = FALSE
ORDER BY checked_at DESC, dataset_name;
