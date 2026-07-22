WITH pipe_names AS (
  SELECT pipeline_id, MAX_BY(name, change_time) AS name
  FROM system.lakeflow.pipelines
  GROUP BY pipeline_id
)
SELECT
  COALESCE(u.usage_metadata.job_name, n.name, '(unnamed)') AS entity,
  CASE WHEN u.usage_metadata.dlt_pipeline_id IS NOT NULL THEN 'pipeline' ELSE 'job' END AS kind,
  SUM(u.usage_quantity) AS quantity,
  u.usage_unit,
  SUM(u.usage_quantity * p.pricing.effective_list.default) AS usd,
  COUNT(DISTINCT u.usage_date) AS active_days
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_end_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
LEFT JOIN pipe_names n
  ON n.pipeline_id = u.usage_metadata.dlt_pipeline_id
WHERE u.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND COALESCE(u.usage_metadata.job_id, u.usage_metadata.dlt_pipeline_id) IS NOT NULL
GROUP BY entity, kind, u.usage_unit
ORDER BY usd DESC
