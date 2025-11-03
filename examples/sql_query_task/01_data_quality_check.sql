-- Data Quality Check: Null Rate Validation
-- This SQL task checks for excessive null values in customer data
-- Fails if null percentage exceeds threshold

WITH null_analysis AS (
  SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_id,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_email,
    SUM(CASE WHEN registration_date IS NULL THEN 1 ELSE 0 END) as null_registration_date
  FROM bronze.customers
  WHERE date = CURRENT_DATE()
),
null_percentages AS (
  SELECT 
    total_rows,
    ROUND(null_customer_id * 100.0 / NULLIF(total_rows, 0), 2) as customer_id_null_pct,
    ROUND(null_email * 100.0 / NULLIF(total_rows, 0), 2) as email_null_pct,
    ROUND(null_registration_date * 100.0 / NULLIF(total_rows, 0), 2) as registration_date_null_pct
  FROM null_analysis
)
SELECT 
  total_rows,
  customer_id_null_pct,
  email_null_pct,
  registration_date_null_pct,
  CASE 
    WHEN customer_id_null_pct > CAST('${threshold}' AS DOUBLE) THEN 'FAIL'
    WHEN email_null_pct > CAST('${threshold}' AS DOUBLE) THEN 'FAIL'
    WHEN registration_date_null_pct > CAST('${threshold}' AS DOUBLE) THEN 'FAIL'
    ELSE 'PASS'
  END as quality_status
FROM null_percentages

