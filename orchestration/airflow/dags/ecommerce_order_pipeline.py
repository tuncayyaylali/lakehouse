from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import trino

default_args = {
    'owner': 'lakehouse',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def get_trino_cursor():
    conn = trino.dbapi.connect(
        host='trino.lakehouse.svc.cluster.local',
        port=8080,
        user='admin',
        auth=trino.auth.BasicAuthentication('admin', 'Admin@123'),
        catalog='iceberg',
    )
    return conn.cursor()

def task_ingest_orders_bronze():
    print("1. Ingesting E-Commerce Orders into Bronze Layer...")
    cur = get_trino_cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.bronze")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS iceberg.bronze.orders (
            order_id VARCHAR,
            customer_name VARCHAR,
            customer_email VARCHAR,
            category VARCHAR,
            amount DOUBLE,
            payment_method VARCHAR,
            status VARCHAR,
            order_timestamp TIMESTAMP(6) WITH TIME ZONE,
            ingested_at TIMESTAMP(6) WITH TIME ZONE
        ) WITH (
            format = 'PARQUET',
            location = 's3://bronze/orders/'
        )
    """)
    
    sample_orders = [
        ("ORD-1001", "Ahmet Yilmaz", "ahmet@example.com", "Electronics", 1250.00, "Credit Card", "COMPLETED", "2026-09-08 10:15:00 UTC"),
        ("ORD-1002", "Ayse Demir", "ayse@example.com", "Apparel", 85.50, "Debit Card", "COMPLETED", "2026-09-08 11:20:00 UTC"),
        ("ORD-1003", "Mehmet Kaya", "mehmet@example.com", "Electronics", 450.00, "Credit Card", "SHIPPED", "2026-09-08 13:45:00 UTC"),
        ("ORD-1004", "Fatma Celik", "fatma@example.com", "Home & Kitchen", 320.00, "Wire Transfer", "COMPLETED", "2026-09-09 09:10:00 UTC"),
        ("ORD-1005", "Can Yildiz", "can@example.com", "Books", 42.00, "Credit Card", "COMPLETED", "2026-09-09 12:00:00 UTC"),
        ("ORD-1006", "Zeynep Sahin", "zeynep@example.com", "Apparel", 195.00, "Credit Card", "CANCELLED", "2026-09-09 14:30:00 UTC"),
        ("ORD-1007", "Burak Ozturk", "burak@example.com", "Electronics", 2100.00, "Credit Card", "SHIPPED", "2026-09-09 16:15:00 UTC"),
        ("ORD-1008", "Elif Koc", "elif@example.com", "Home & Kitchen", 115.00, "Debit Card", "RETURNED", "2026-09-10 08:30:00 UTC"),
        ("ORD-1009", "Emre Aydin", "emre@example.com", "Books", 68.00, "Credit Card", "COMPLETED", "2026-09-10 10:45:00 UTC"),
        ("ORD-1010", "Selin Arslan", "selin@example.com", "Electronics", 780.00, "Credit Card", "COMPLETED", "2026-09-10 11:50:00 UTC"),
    ]
    
    values_clause = ", ".join([
        f"('{oid}', '{cname}', '{cemail}', '{cat}', {amt}, '{pm}', '{st}', TIMESTAMP '{ts}', CURRENT_TIMESTAMP)"
        for oid, cname, cemail, cat, amt, pm, st, ts in sample_orders
    ])
    
    cur.execute(f"INSERT INTO iceberg.bronze.orders VALUES {values_clause}")
    cur.execute("SELECT count(*) FROM iceberg.bronze.orders")
    total_bronze = cur.fetchone()[0]
    print(f"Bronze Ingestion completed. Total records in bronze.orders: {total_bronze}")

def task_transform_orders_silver():
    print("2. Transforming and Cleaning Orders into Silver Layer...")
    cur = get_trino_cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.silver")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.orders_cleaned (
            order_id VARCHAR,
            customer_name VARCHAR,
            category VARCHAR,
            amount DOUBLE,
            payment_method VARCHAR,
            status VARCHAR,
            is_delivered BOOLEAN,
            order_timestamp TIMESTAMP(6) WITH TIME ZONE,
            transformed_at TIMESTAMP(6) WITH TIME ZONE
        ) WITH (
            format = 'PARQUET',
            location = 's3://silver/orders_cleaned/'
        )
    """)
    
    cur.execute("DELETE FROM iceberg.silver.orders_cleaned")
    cur.execute("""
        INSERT INTO iceberg.silver.orders_cleaned
        WITH ranked_orders AS (
            SELECT
                order_id,
                UPPER(TRIM(customer_name)) AS customer_name,
                UPPER(TRIM(category)) AS category,
                amount,
                payment_method,
                status,
                (status IN ('COMPLETED', 'SHIPPED')) AS is_delivered,
                order_timestamp,
                CURRENT_TIMESTAMP AS transformed_at,
                ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingested_at DESC) AS rn
            FROM iceberg.bronze.orders
        )
        SELECT
            order_id,
            customer_name,
            category,
            amount,
            payment_method,
            status,
            is_delivered,
            order_timestamp,
            transformed_at
        FROM ranked_orders
        WHERE rn = 1
    """)
    
    cur.execute("SELECT count(*) FROM iceberg.silver.orders_cleaned")
    total_silver = cur.fetchone()[0]
    print(f"Silver Transformation completed. Total cleaned records: {total_silver}")

def task_check_orders_quality():
    print("3. Running Data Quality Validations on Silver...")
    cur = get_trino_cursor()
    
    cur.execute("SELECT count(*) FROM iceberg.silver.orders_cleaned WHERE amount <= 0")
    invalid_amounts = cur.fetchone()[0]
    if invalid_amounts > 0:
        raise ValueError(f"Quality Check Failed: Found {invalid_amounts} orders with amount <= 0!")
    
    cur.execute("""
        SELECT count(*) FROM (
            SELECT order_id FROM iceberg.silver.orders_cleaned GROUP BY order_id HAVING count(*) > 1
        )
    """)
    duplicate_ids = cur.fetchone()[0]
    if duplicate_ids > 0:
        raise ValueError(f"Quality Check Failed: Found {duplicate_ids} duplicate order IDs in Silver!")
        
    print("Data Quality checks PASSED: All order amounts are positive and IDs are unique.")

def task_build_financial_gold():
    print("4. Computing Financial & Sales KPIs in Gold Layer...")
    cur = get_trino_cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.gold")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.sales_financial_kpis (
            category VARCHAR,
            total_orders BIGINT,
            successful_orders BIGINT,
            total_revenue DOUBLE,
            avg_order_value DOUBLE,
            last_calculated_at TIMESTAMP(6) WITH TIME ZONE
        ) WITH (
            format = 'PARQUET',
            location = 's3://gold/sales_financial_kpis/'
        )
    """)
    
    cur.execute("DELETE FROM iceberg.gold.sales_financial_kpis")
    cur.execute("""
        INSERT INTO iceberg.gold.sales_financial_kpis
        SELECT
            category,
            COUNT(*) AS total_orders,
            COUNT(CASE WHEN is_delivered THEN 1 END) AS successful_orders,
            ROUND(SUM(CASE WHEN is_delivered THEN amount ELSE 0.0 END), 2) AS total_revenue,
            ROUND(AVG(CASE WHEN is_delivered THEN amount ELSE NULL END), 2) AS avg_order_value,
            CURRENT_TIMESTAMP AS last_calculated_at
        FROM iceberg.silver.orders_cleaned
        GROUP BY category
    """)
    
    cur.execute("SELECT category, total_orders, successful_orders, total_revenue, avg_order_value FROM iceberg.gold.sales_financial_kpis ORDER BY total_revenue DESC")
    kpis = cur.fetchall()
    print("--- Gold Financial KPIs ---")
    for row in kpis:
        print(f"Category: {row[0]} | Orders: {row[1]} | Delivered: {row[2]} | Revenue: ${row[3]} | AOV: ${row[4]}")

with DAG(
    'ecommerce_order_pipeline',
    default_args=default_args,
    description='End-to-End E-Commerce Orders ELT Pipeline (Bronze -> Silver -> Quality -> Gold)',
    schedule_interval=None,
    catchup=False,
    tags=['lakehouse', 'ecommerce', 'financial', 'iceberg']
) as dag:

    t1 = PythonOperator(
        task_id='ingest_orders_bronze',
        python_callable=task_ingest_orders_bronze
    )

    t2 = PythonOperator(
        task_id='transform_orders_silver',
        python_callable=task_transform_orders_silver
    )

    t3 = PythonOperator(
        task_id='check_orders_quality',
        python_callable=task_check_orders_quality
    )

    t4 = PythonOperator(
        task_id='build_financial_gold',
        python_callable=task_build_financial_gold
    )

    t1 >> t2 >> t3 >> t4

