-- Incremental Load: Sales Data
-- Loads only new/updated records since last run
-- Uses change data capture pattern with timestamp comparison

MERGE INTO silver.sales AS target
USING (
  SELECT 
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    revenue,
    status,
    shipping_address,
    CURRENT_TIMESTAMP() as processed_at
  FROM bronze.sales
  WHERE order_date >= COALESCE(
    (SELECT MAX(order_date) FROM silver.sales),
    DATE_SUB(CURRENT_DATE(), 7)  -- Fallback to last 7 days if no existing data
  )
    AND order_date <= CURRENT_DATE()
) AS source
ON target.order_id = source.order_id
WHEN MATCHED AND target.processed_at < source.processed_at THEN
  UPDATE SET
    customer_id = source.customer_id,
    product_id = source.product_id,
    order_date = source.order_date,
    quantity = source.quantity,
    unit_price = source.unit_price,
    revenue = source.revenue,
    status = source.status,
    shipping_address = source.shipping_address,
    processed_at = source.processed_at
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, product_id, order_date, quantity, unit_price, 
          revenue, status, shipping_address, processed_at)
  VALUES (source.order_id, source.customer_id, source.product_id, source.order_date,
          source.quantity, source.unit_price, source.revenue, source.status,
          source.shipping_address, source.processed_at)

