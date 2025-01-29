from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_SCHEMA = 'raw_data'
DATA_MART_SCHEMA = 'data_mart'

def get_postgres_connection():
    postgres_hook = PostgresHook(postgres_conn_id="local_postgres")
    return postgres_hook.get_sqlalchemy_engine()

# Function to check and create raw tables
def table_exists(engine, table_name):
    with engine.connect() as conn:
        result = conn.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{RAW_SCHEMA}' AND table_name = '{table_name}')")
        return result.scalar()

# Function to load CSV files into raw tables
def load_raw_data():
    engine = get_postgres_connection()
    data_mapping = {
        'customers_dataset.csv': 'customers',
        'orders_dataset.csv': 'orders',
        'order_items_dataset.csv': 'order_items',
        'order_payments_dataset.csv': 'order_payments',
        'products_dataset.csv': 'products',
        'sellers_dataset.csv': 'sellers',
        'geolocation_dataset.csv': 'geolocation',
        'product_category_name_translation.csv': 'product_category_translation'
    }
    
    with engine.connect() as conn:
        for file_name, table_name in data_mapping.items():
            file_path = f"data/{file_name}"
            df = pd.read_csv(file_path)
            
            if table_exists(engine, table_name):
                conn.execute(f"TRUNCATE TABLE {RAW_SCHEMA}.{table_name} RESTART IDENTITY CASCADE;")
            
            df.to_sql(table_name, engine, schema=RAW_SCHEMA, if_exists='append', index=False)
            print(f"Truncated and loaded {table_name} successfully into schema {RAW_SCHEMA}")

# Function to truncate and create sales summary and load into data_mart
def create_sales_summary():
    engine = get_postgres_connection()
    query = f"""
    DO $$
    BEGIN
        IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{DATA_MART_SCHEMA}' AND table_name = 'sales_summary') THEN
            TRUNCATE TABLE {DATA_MART_SCHEMA}.sales_summary RESTART IDENTITY CASCADE;
        END IF;
    END $$;
    INSERT INTO {DATA_MART_SCHEMA}.sales_summary (order_date, product_id, total_orders, total_revenue, avg_order_value)
    SELECT o.order_purchase_timestamp::DATE AS order_date,
           oi.product_id,
           COUNT(o.order_id) AS total_orders,
           SUM(oi.price) AS total_revenue,
           AVG(oi.price) AS avg_order_value
    FROM {RAW_SCHEMA}.orders o
    JOIN {RAW_SCHEMA}.order_items oi ON o.order_id = oi.order_id
    GROUP BY order_date, oi.product_id;
    """
    
    with engine.connect() as conn:
        conn.execute(query)
        print("Sales summary table truncated and reloaded successfully")

# Define DAGs
def load_raw_dag():
    with DAG(
        dag_id='load_raw_data',
        schedule_interval='@daily',
        start_date=datetime(2024, 1, 1),
        catchup=False
    ) as dag:
        load_task = PythonOperator(
            task_id='load_csv_to_postgres',
            python_callable=load_raw_data
        )
        trigger_summary = TriggerDagRunOperator(
            task_id='trigger_sales_summary',
            trigger_dag_id='create_sales_summary'
        )
        load_task >> trigger_summary
    return dag

def create_sales_summary_dag():
    with DAG(
        dag_id='create_sales_summary',
        schedule_interval='@daily',
        start_date=datetime(2024, 1, 1),
        catchup=False
    ) as dag:
        transform_task = PythonOperator(
            task_id='create_sales_summary',
            python_callable=create_sales_summary
        )
    return dag

# Assign DAGs
load_raw_data_dag = load_raw_dag()
create_sales_summary_dag = create_sales_summary_dag()
