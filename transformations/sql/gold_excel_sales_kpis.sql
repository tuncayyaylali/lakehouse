-- Gold Transformation: Aggregate Silver cleaned orders into Business KPIs
CREATE SCHEMA IF NOT EXISTS iceberg.gold;

CREATE TABLE IF NOT EXISTS iceberg.gold.excel_sales_kpis (
    category VARCHAR,
    total_orders BIGINT,
    total_sales DOUBLE,
    avg_order_value DOUBLE,
    top_city VARCHAR,
    calculated_at TIMESTAMP(6) WITH TIME ZONE
) WITH (
    format = 'PARQUET',
    location = 's3://gold/excel_sales_kpis/'
);

DELETE FROM iceberg.gold.excel_sales_kpis;

INSERT INTO iceberg.gold.excel_sales_kpis
SELECT 
    category,
    count(*) AS total_orders,
    round(sum(amount), 2) AS total_sales,
    round(avg(amount), 2) AS avg_order_value,
    max(city) AS top_city,
    CURRENT_TIMESTAMP AS calculated_at
FROM iceberg.silver.excel_orders_cleaned
GROUP BY category;

