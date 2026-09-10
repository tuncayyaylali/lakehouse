"""
Airflow DAG: Excel to Gold Layer Pipeline
Reads sample_orders.xlsx from MinIO S3 bronze bucket,
ingests into iceberg.bronze.excel_orders, and
aggregates business metrics into iceberg.gold.excel_sales_kpis.
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
        port=8443,
        http_scheme='https',
        verify=False,
        user='admin',
        auth=trino.auth.BasicAuthentication('admin', 'Admin@2026'),
        catalog='iceberg',
    )
    return conn.cursor()

def task_read_excel_and_load_bronze():
    print("1. Connecting to MinIO S3 to read sample_orders.xlsx...")
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio.lakehouse.svc.cluster.local:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        region_name='us-east-1',
    )
    
    # Read Excel file from MinIO bronze bucket
    obj = s3.get_object(Bucket='bronze', Key='sample_orders.xlsx')
    df = pd.read_excel(io.BytesIO(obj['Body'].read()))
    print(f"Successfully loaded {len(df)} rows from Excel:")
    print(df)
    
    cur = get_trino_cursor()
    
    # Ensure Bronze schema exists
    cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.bronze")
    
    # Ensure Bronze Iceberg table exists
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
    
    # Clear previous run data to ensure idempotency
    cur.execute("DELETE FROM iceberg.bronze.excel_orders")
    
    # Insert rows into Bronze Iceberg table
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
        
    print(f"Loaded {len(df)} records into iceberg.bronze.excel_orders.")

def task_aggregate_to_gold():
    print("2. Aggregating Bronze Excel orders into Gold analytical KPI table...")
    cur = get_trino_cursor()
    
    # Ensure Gold schema exists
    cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.gold")
    
    # Ensure Gold Iceberg table exists
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
    
    # Clear previous run data
    cur.execute("DELETE FROM iceberg.gold.excel_sales_kpis")
    
    # Insert aggregated KPI metrics
    cur.execute("""
        INSERT INTO iceberg.gold.excel_sales_kpis
        SELECT 
            category,
            count(*) AS total_orders,
            round(sum(amount), 2) AS total_sales,
            round(avg(amount), 2) AS avg_order_value,
            max(city) AS top_city,
            CURRENT_TIMESTAMP AS calculated_at
        FROM iceberg.bronze.excel_orders
        GROUP BY category
    """)
    
    print("Gold KPI aggregation complete.")

with DAG(
    dag_id='excel_orders_to_gold_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['lakehouse', 'excel', 'iceberg', 'bronze', 'gold'],
) as dag:

    read_excel_task = PythonOperator(
        task_id='read_excel_and_load_bronze',
        python_callable=task_read_excel_and_load_bronze,
    )

    gold_aggregation_task = PythonOperator(
        task_id='transform_and_aggregate_to_gold',
        python_callable=task_aggregate_to_gold,
    )

    read_excel_task >> gold_aggregation_task

