-- Data Freshness Check
-- Validates that data has been updated within expected time window
-- Useful for monitoring data pipeline health

WITH latest_update AS (
  SELECT 
    table_name,
    MAX(updated_at) as last_update_time,
    CURRENT_TIMESTAMP() as check_time,
    TIMESTAMPDIFF(HOUR, MAX(updated_at), CURRENT_TIMESTAMP()) as hours_since_update
  FROM (
    SELECT 'customers' as table_name, MAX(updated_at) as updated_at FROM silver.customers
    UNION ALL
    SELECT 'sales' as table_name, MAX(updated_at) as updated_at FROM silver.sales_summary
    UNION ALL
    SELECT 'products' as table_name, MAX(updated_at) as updated_at FROM silver.products
  ) combined
  GROUP BY table_name
)
SELECT 
  table_name,
  last_update_time,
  check_time,
  hours_since_update,
  CASE 
    WHEN hours_since_update > CAST('${max_hours}' AS INT) THEN 'STALE'
    WHEN hours_since_update IS NULL THEN 'NO_DATA'
    ELSE 'FRESH'
  END as freshness_status
FROM latest_update
ORDER BY table_name

