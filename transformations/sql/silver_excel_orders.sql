-- Silver Transformation: Clean, deduplicate and validate Bronze Excel orders
CREATE SCHEMA IF NOT EXISTS iceberg.silver;

CREATE TABLE IF NOT EXISTS iceberg.silver.excel_orders_cleaned (
    order_id VARCHAR,
    customer_name VARCHAR,
    category VARCHAR,
    amount DOUBLE,
    order_date DATE,
    city VARCHAR,
    processed_at TIMESTAMP(6) WITH TIME ZONE
) WITH (
    format = 'PARQUET',
    location = 's3://silver/excel_orders_cleaned/'
);

DELETE FROM iceberg.silver.excel_orders_cleaned;

INSERT INTO iceberg.silver.excel_orders_cleaned
SELECT 
    trim(order_id) AS order_id,
    trim(customer_name) AS customer_name,
    trim(category) AS category,
    amount,
    CAST(order_date AS DATE) AS order_date,
    trim(city) AS city,
    CURRENT_TIMESTAMP AS processed_at
FROM (
    SELECT 
        order_id,
        customer_name,
        category,
        amount,
        order_date,
        city,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingested_at DESC) AS rn
    FROM iceberg.bronze.excel_orders
)
WHERE rn = 1 AND amount > 0;

