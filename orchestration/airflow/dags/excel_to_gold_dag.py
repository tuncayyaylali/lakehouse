"""
Airflow DAG: End-to-End Excel Medallion Pipeline (Bronze -> Silver -> Quality -> Gold)
1. Ingests raw sample_orders.xlsx from MinIO S3 into iceberg.bronze.excel_orders
2. Cleans, casts types, and deduplicates into iceberg.silver.excel_orders_cleaned
3. Runs automated Data Quality assertions on Silver
4. Aggregates financial KPIs into iceberg.gold.excel_sales_kpis
"""

from datetime import datetime, timedelta
import io
import boto3
import pandas as pd
import trino
from airflow import DAG
from airflow.operators.python import PythonOperator

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
        catalog='iceberg',
    )
    return conn.cursor()

# ----------------------------------------------------
# 1. BRONZE LAYER: Raw Excel Ingestion
# ----------------------------------------------------
def task_ingest_excel_to_bronze():
    print("1. [BRONZE] Reading sample_orders.xlsx from MinIO S3...")
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio.lakehouse.svc.cluster.local:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        region_name='us-east-1',
    )
    
    obj = s3.get_object(Bucket='bronze', Key='sample_orders.xlsx')
    df = pd.read_excel(io.BytesIO(obj['Body'].read()))
    print(f"Loaded {len(df)} raw rows from Excel.")
    
    cur = get_trino_cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.bronze")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS iceberg.bronze.excel_orders (
            order_id VARCHAR,
            customer_name VARCHAR,
            category VARCHAR,
            amount DOUBLE,
            order_date VARCHAR,
            city VARCHAR,
            ingested_at TIMESTAMP(6) WITH TIME ZONE
        ) WITH (
            format = 'PARQUET',
            location = 's3://bronze/excel_orders/'
        )
    """)
    
    cur.execute("DELETE FROM iceberg.bronze.excel_orders")
    
    for _, row in df.iterrows():
        oid = str(row['order_id'])
        cname = str(row['customer_name']).replace("'", "''")
        cat = str(row['category']).replace("'", "''")
        amt = float(row['amount'])
        odate = str(row['order_date'])
        city = str(row['city']).replace("'", "''")
        
        insert_sql = f"""
            INSERT INTO iceberg.bronze.excel_orders 
            VALUES ('{oid}', '{cname}', '{cat}', {amt}, '{odate}', '{city}', CURRENT_TIMESTAMP)
        """
        cur.execute(insert_sql)
        
    print(f"Bronze ingestion complete: {len(df)} records stored in s3://bronze/excel_orders/")

# ----------------------------------------------------
# 2. SILVER LAYER: Cleaning & Deduplication
# ----------------------------------------------------
def task_transform_to_silver():
    print("2. [SILVER] Transforming Bronze -> Silver (Cleaning & Deduplication)...")
    cur = get_trino_cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.silver")
    cur.execute("""
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
        )
    """)
    
    cur.execute("DELETE FROM iceberg.silver.excel_orders_cleaned")
    
    silver_sql = """
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
        WHERE rn = 1 AND amount > 0
    """
    cur.execute(silver_sql)
    print("Silver transformation complete: cleaned data stored in s3://silver/excel_orders_cleaned/")

# ----------------------------------------------------
# 3. DATA QUALITY CHECKS ON SILVER
# ----------------------------------------------------
def task_quality_checks_silver():
    print("3. [QUALITY] Running data quality assertions on Silver layer...")
    cur = get_trino_cursor()
    
    # Check 1: No negative or zero amounts
    cur.execute("SELECT count(*) FROM iceberg.silver.excel_orders_cleaned WHERE amount <= 0")
    invalid_amounts = cur.fetchone()[0]
    if invalid_amounts > 0:
        raise ValueError(f"Data Quality Violation: Found {invalid_amounts} rows with non-positive amount!")
    
    # Check 2: No duplicate order_ids
    cur.execute("""
        SELECT count(*) FROM (
            SELECT order_id FROM iceberg.silver.excel_orders_cleaned 
            GROUP BY order_id HAVING count(*) > 1
        )
    """)
    duplicate_ids = cur.fetchone()[0]
    if duplicate_ids > 0:
        raise ValueError(f"Data Quality Violation: Found {duplicate_ids} duplicate order_ids!")
        
    print("All Data Quality assertions passed successfully! (0 violations)")

# ----------------------------------------------------
# 4. GOLD LAYER: Business KPI Aggregations
# ----------------------------------------------------
def task_aggregate_to_gold():
    print("4. [GOLD] Aggregating Silver orders into Financial KPIs...")
    cur = get_trino_cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.gold")
    cur.execute("""
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
        )
    """)
    
    cur.execute("DELETE FROM iceberg.gold.excel_sales_kpis")
    
    gold_sql = """
        INSERT INTO iceberg.gold.excel_sales_kpis
        SELECT 
            category,
            count(*) AS total_orders,
            round(sum(amount), 2) AS total_sales,
            round(avg(amount), 2) AS avg_order_value,
            max(city) AS top_city,
            CURRENT_TIMESTAMP AS calculated_at
        FROM iceberg.silver.excel_orders_cleaned
        GROUP BY category
    """
    cur.execute(gold_sql)
    print("Gold KPI aggregation complete: metrics stored in s3://gold/excel_sales_kpis/")

with DAG(
    dag_id='excel_orders_to_gold_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['lakehouse', 'excel', 'iceberg', 'bronze', 'silver', 'gold'],
) as dag:

    ingest_bronze = PythonOperator(
        task_id='1_ingest_excel_to_bronze',
        python_callable=task_ingest_excel_to_bronze,
    )

    transform_silver = PythonOperator(
        task_id='2_transform_bronze_to_silver',
        python_callable=task_transform_to_silver,
    )

    quality_checks = PythonOperator(
        task_id='3_quality_checks_silver',
        python_callable=task_quality_checks_silver,
    )

    aggregate_gold = PythonOperator(
        task_id='4_aggregate_silver_to_gold',
        python_callable=task_aggregate_to_gold,
    )

    ingest_bronze >> transform_silver >> quality_checks >> aggregate_gold
