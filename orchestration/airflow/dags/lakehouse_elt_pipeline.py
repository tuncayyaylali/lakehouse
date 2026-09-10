from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
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
        catalog='iceberg'
    )
    return conn.cursor()

def task_ingest_bronze():
    print("1. Ingesting open data from REST API into Bronze...")
    res = requests.get("https://jsonplaceholder.typicode.com/users", timeout=15)
    res.raise_for_status()
    users = res.json()
    cur = get_trino_cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS iceberg.bronze.raw_users (
            id INT,
            name VARCHAR,
            username VARCHAR,
            email VARCHAR,
            city VARCHAR,
            company_name VARCHAR,
            ingested_at VARCHAR
        ) WITH (format = 'PARQUET')
    """)
    now = datetime.utcnow().isoformat()
    for u in users:
        uid = int(u.get('id', 0))
        name = str(u.get('name', '')).replace("'", "''")
        username = str(u.get('username', '')).replace("'", "''")
        email = str(u.get('email', '')).replace("'", "''")
        city = str(u.get('address', {}).get('city', '')).replace("'", "''")
        company = str(u.get('company', {}).get('name', '')).replace("'", "''")
        cur.execute(f"""
            INSERT INTO iceberg.bronze.raw_users VALUES 
            ({uid}, '{name}', '{username}', '{email}', '{city}', '{company}', '{now}')
        """)
    print(f"Loaded {len(users)} records into bronze.raw_users")

def task_transform_silver():
    print("2. Transforming Bronze -> Silver with Deduplication (stg_users)...")
    cur = get_trino_cursor()
    cur.execute("DROP TABLE IF EXISTS iceberg.silver.stg_users")
    cur.execute("""
        CREATE TABLE iceberg.silver.stg_users
        WITH (format = 'PARQUET') AS
        SELECT user_id, full_name, email_address, city, company_name, transformed_at
        FROM (
            SELECT
                CAST(id AS INTEGER) AS user_id,
                TRIM(name) AS full_name,
                LOWER(TRIM(email)) AS email_address,
                TRIM(city) AS city,
                TRIM(company_name) AS company_name,
                CURRENT_TIMESTAMP AS transformed_at,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingested_at DESC) AS rn
            FROM iceberg.bronze.raw_users
            WHERE id IS NOT NULL
        ) WHERE rn = 1
    """)
    print("Silver stg_users created and deduplicated successfully.")

def task_quality_checks():
    print("3. Running data quality checks...")
    cur = get_trino_cursor()
    cur.execute("""
        SELECT 
            COUNT(*) - COUNT(DISTINCT user_id) AS dup_ids,
            COUNT(*) - COUNT(email_address) AS null_emails
        FROM iceberg.silver.stg_users
    """)
    dup_ids, null_emails = cur.fetchone()
    assert dup_ids == 0 and null_emails == 0, f"Quality check failed: dup={dup_ids}, null={null_emails}"
    print(f"Quality checks passed: {dup_ids} duplicates, {null_emails} null emails.")

def task_transform_gold():
    print("4. Transforming Silver -> Gold (dim_users_summary)...")
    cur = get_trino_cursor()
    cur.execute("DROP TABLE IF EXISTS iceberg.gold.dim_users_summary")
    cur.execute("""
        CREATE TABLE iceberg.gold.dim_users_summary
        WITH (format = 'PARQUET') AS
        SELECT
            city,
            COUNT(user_id) AS total_users,
            COUNT(DISTINCT company_name) AS unique_companies,
            CURRENT_TIMESTAMP AS calculated_at
        FROM iceberg.silver.stg_users
        GROUP BY city
    """)
    print("Gold dim_users_summary created successfully.")

with DAG(
    dag_id='lakehouse_elt_pipeline',
    default_args=default_args,
    description='End-to-End Lakehouse Pipeline: Ingest -> Silver -> Quality -> Gold',
    schedule_interval=None,
    catchup=False,
    tags=['lakehouse', 'elt', 'bronze', 'silver', 'gold']
) as dag:

    t1 = PythonOperator(task_id='ingest_to_bronze', python_callable=task_ingest_bronze)
    t2 = PythonOperator(task_id='transform_to_silver', python_callable=task_transform_silver)
    t3 = PythonOperator(task_id='run_quality_checks', python_callable=task_quality_checks)
    t4 = PythonOperator(task_id='transform_to_gold', python_callable=task_transform_gold)

    t1 >> t2 >> t3 >> t4

