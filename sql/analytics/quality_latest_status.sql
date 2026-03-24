WITH latest_run AS (
  SELECT audit_run_id
  FROM audit.quality_check_runs
  ORDER BY checked_at DESC, audit_run_id DESC
  LIMIT 1
)
SELECT checked_at, dataset_name, layer, table_name, passed, row_count, violation_count
FROM audit.quality_check_runs
WHERE audit_run_id IN (SELECT audit_run_id FROM latest_run)
ORDER BY passed ASC, dataset_name ASC;
