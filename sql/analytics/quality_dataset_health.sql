SELECT
  dataset_name,
  layer,
  MAX(checked_at) AS latest_checked_at,
  COUNT(*) AS run_count,
  SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS successful_runs,
  SUM(CASE WHEN passed THEN 0 ELSE 1 END) AS failed_runs,
  ROUND(AVG(CASE WHEN passed THEN 1.0 ELSE 0.0 END), 4) AS pass_rate,
  MAX(violation_count) AS max_violation_count
FROM audit.quality_check_runs
GROUP BY dataset_name, layer
ORDER BY failed_runs DESC, dataset_name;
