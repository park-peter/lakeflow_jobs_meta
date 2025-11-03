-- Bronze to Silver Transformation: Customer Data
-- Cleanses and transforms raw customer data from bronze to silver
-- Applies data quality rules and standardizes formats

MERGE INTO silver.customers AS target
USING (
  SELECT 
    customer_id,
    TRIM(UPPER(email)) as email,
    INITCAP(first_name) as first_name,
    INITCAP(last_name) as last_name,
    CAST(registration_date AS DATE) as registration_date,
    CASE 
      WHEN status IN ('active', 'Active', 'ACTIVE') THEN 'ACTIVE'
      WHEN status IN ('inactive', 'Inactive', 'INACTIVE') THEN 'INACTIVE'
      ELSE 'UNKNOWN'
    END as status,
    COALESCE(phone, '') as phone,
    COALESCE(address, '') as address,
    CURRENT_TIMESTAMP() as updated_at
  FROM bronze.customers
  WHERE date = CURRENT_DATE()
    AND customer_id IS NOT NULL
    AND email IS NOT NULL
    AND email LIKE '%@%.%'  -- Basic email validation
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET
    email = source.email,
    first_name = source.first_name,
    last_name = source.last_name,
    status = source.status,
    phone = source.phone,
    address = source.address,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, email, first_name, last_name, registration_date, status, phone, address, updated_at)
  VALUES (source.customer_id, source.email, source.first_name, source.last_name, 
          source.registration_date, source.status, source.phone, source.address, source.updated_at)

