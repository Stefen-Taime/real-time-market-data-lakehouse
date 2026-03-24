SELECT
  audit_run_id,
  checked_at,
  environment,
  job_name,
  task_name,
  COUNT(*) AS dataset_count,
  SUM(CASE WHEN passed THEN 0 ELSE 1 END) AS failed_dataset_count,
  SUM(violation_count) AS violation_count
FROM audit.quality_check_runs
GROUP BY audit_run_id, checked_at, environment, job_name, task_name
ORDER BY checked_at DESC, audit_run_id DESC;
