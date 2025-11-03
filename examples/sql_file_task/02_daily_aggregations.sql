-- Daily Sales Aggregations
-- Aggregates daily sales data by product category
-- Writes to silver layer for downstream analytics

INSERT INTO silver.daily_sales_summary
SELECT 
  date,
  product_category,
  product_subcategory,
  SUM(revenue) as total_revenue,
  SUM(quantity) as total_quantity,
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(DISTINCT order_id) as total_orders,
  AVG(revenue) as avg_order_value,
  CURRENT_TIMESTAMP() as processed_at
FROM bronze.sales
WHERE date = CURRENT_DATE()
  AND status = 'completed'
GROUP BY 
  date,
  product_category,
  product_subcategory

