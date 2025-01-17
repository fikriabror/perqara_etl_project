from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import logging
import os


def extract_data(**kwargs):
    # Setup this connection in the airflow connection
    postgres_conn_id = 'local_postgres'
    
    # List of the tables
    tables = ["Users", "Transactions", "Membership_Purchases", "MDR_Data", "User_Activity"]
    file_paths = {}

    # try to connect to db
    try:
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        for table in tables:
            query = f"SELECT * FROM sprout.{table};"
            df = postgres_hook.get_pandas_df(query)

            file_path = f"/tmp/{table}.csv"
            df.to_csv(file_path, index=False)
            file_paths[table] = file_path
            logging.info(f"Extracted {table} and saved to {file_path}.")
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        raise

    # Push the of file path to xcoms
    kwargs['ti'].xcom_push(key='file_paths', value=file_paths)


def transform_data(**kwargs):
    # Pull data from xcom
    file_paths = kwargs['ti'].xcom_pull(key='file_paths', task_ids='extract_data')

    # Get data from CSV
    try:
        users = pd.read_csv(file_paths['Users'])
        transactions = pd.read_csv(file_paths['Transactions'])
        memberships = pd.read_csv(file_paths['Membership_Purchases'])
        mdr_data = pd.read_csv(file_paths['MDR_Data'])
        user_activity = pd.read_csv(file_paths['User_Activity'])
        
        # fill 0 for the null transactios amount
        mdr_data = mdr_data.tail(2)
        transactions.fillna(0, inplace=True)
        memberships.dropna(subset=['membership_type'], inplace=True)

        # transform columns date
        transactions['timestamp'] = pd.to_datetime(transactions['timestamp'])
        memberships['purchase_date'] = pd.to_datetime(memberships['purchase_date'])
        memberships['expiry_date'] = pd.to_datetime(memberships['expiry_date'])
        user_activity['activity_date'] = pd.to_datetime(user_activity['activity_date'])

        # Join table users and membership
        users_membership_df = pd.merge(users, memberships, on='user_id', how='inner')

        # Join users membership and transactions
        transactions_df = pd.merge(transactions, users_membership_df, on='user_id', how='inner')

        # Add MDR percentage to transactions
        transactions_final = pd.merge(transactions_df, mdr_data, on='membership_type', how='inner')
        
        transactions_with_mdr_df = pd.merge(transactions_df, mdr_data, on='membership_type', how='inner')

        # Status membership when purchase
        transactions_with_mdr_df['membership_purchase_status'] = transactions_with_mdr_df.apply(
            lambda row: 'Active' if row['purchase_date'] <= row['expiry_date'] else 'Expired',
            axis=1
        )
        transactions_with_mdr_df = transactions_with_mdr_df.drop_duplicates()
        
        # User Profiling
        user_profiling = transactions_with_mdr_df.groupby(['user_id', 'name', 'timestamp', 'membership_type', 'membership_purchase_status', 'mdr_percentage']).agg(
            First_Transaction=('timestamp', 'min'),
            Invoice_Total_Count=('transaction_id', 'count'),
            Invoice_Total_Amount=('transaction_amount', 'sum')
        ).reset_index()
        user_profiling = user_profiling.drop_duplicates()
        
        # Transaction breakdown per type related
        transaction_totals_per_type = transactions_with_mdr_df.groupby(
            [transactions_with_mdr_df['timestamp'].dt.date, 'membership_type']
        ).agg(
            invoice_total_count=('transaction_id', 'count'),
            invoice_total_amount=('transaction_amount', 'sum')
        ).reset_index()
        transaction_totals_per_type = transaction_totals_per_type.drop_duplicates()
        
        # Daily Activity (Transaction)
        # Group transactions by date and calculate totals
        daily_activity_report = transactions_with_mdr_df.groupby(transactions_with_mdr_df['timestamp'].dt.date).agg(
            invoice_total_count=('transaction_id', 'count'),
            invoice_total_amount=('transaction_amount', 'sum'),
        ).reset_index()
        daily_activity_report = daily_activity_report.drop_duplicates()
        
        # Apply MDR percentage
        transactions_mdr = transactions_with_mdr_df.dropna(subset=['membership_type'])
        transactions_mdr.loc[
            transactions_mdr['membership_purchase_status'] == 'Active', 'effective_amount'
        ] = transactions_mdr['transaction_amount'] * (
            1 - transactions_mdr['mdr_percentage'] / 100
        )
        transactions_mdr = transactions_mdr.drop_duplicates()
    
        user_profiling_path = "/tmp/user_profiling.csv"
        transaction_totals_per_type_path = "/tmp/transaction_totals_per_type.csv"
        daily_activity_report_path = "/tmp/daily_activity_report.csv"
        transactions_mdr_path = "/tmp/transactions_with_mdr_df.csv"

        user_profiling.to_csv(user_profiling_path, index=False)
        transaction_totals_per_type.to_csv(transaction_totals_per_type_path, index=False)
        daily_activity_report.to_csv(daily_activity_report_path, index=False)
        transactions_mdr.to_csv(transactions_mdr_path, index=False)

        kwargs['ti'].xcom_push(key='user_profiling_path', value=user_profiling_path)
        kwargs['ti'].xcom_push(key='transaction_totals_per_type_path', value=transaction_totals_per_type_path)
        kwargs['ti'].xcom_push(key='daily_activity_report_path', value=daily_activity_report_path)
        kwargs['ti'].xcom_push(key='transactions_with_mdr_path', value=transactions_mdr_path)
        
        logging.info("Transformation completed and reports saved.")
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise

def load_data(**kwargs):
    postgres_conn_id = 'local_postgres'

    # Pull paths from XCom
    user_profiling_path = kwargs['ti'].xcom_pull(key='user_profiling_path', task_ids='transform_data')
    transaction_totals_per_type_path = kwargs['ti'].xcom_pull(key='transaction_totals_per_type_path', task_ids='transform_data')
    daily_activity_report_path = kwargs['ti'].xcom_pull(key='daily_activity_report_path', task_ids='transform_data')
    transactions_with_mdr_path = kwargs['ti'].xcom_pull(key='transactions_with_mdr_path', task_ids='transform_data')

    try:
        # Read and clean data
        def load_and_clean_csv(file_path, custom_dtypes=None, missing_value_defaults=None, date_columns=None):
            # Define column-specific dtypes for parsing
            if custom_dtypes is None:
                custom_dtypes = {}
            if missing_value_defaults is None:
                missing_value_defaults = {}
            if date_columns is None:
                date_columns = []

            # Check for existing columns in the CSV
            with open(file_path, 'r') as f:
                first_line = f.readline().strip()
                available_columns = first_line.split(',')

            # Include only available date columns
            parse_dates = [col for col in date_columns if col in available_columns]

            # Load the CSV file with custom dtypes and date parsing
            df = pd.read_csv(file_path, dtype=custom_dtypes, parse_dates=parse_dates)

            # Replace missing values with defaults for specific columns
            for col, default in missing_value_defaults.items():
                if col in df.columns:
                    if default is not None:
                        df[col] = df[col].fillna(default)  # Fill missing values with the default
                    else:
                        logging.warning(f"No default value provided for column '{col}'. Skipping fillna.")

            # Replace any remaining NaN, NaT, or pd.NA with None
            df = df.where(pd.notnull, None).replace({pd.NA: None})

            logging.info(f"Loaded {file_path} with {df.shape[0]} rows and {df.shape[1]} columns.")
            return df



        # Load and clean each dataset
        user_profiling = load_and_clean_csv(user_profiling_path)
        transaction_totals_per_type = load_and_clean_csv(transaction_totals_per_type_path)
        daily_activity_report = load_and_clean_csv(daily_activity_report_path)
        transactions_with_mdr = load_and_clean_csv(
            transactions_with_mdr_path,
            custom_dtypes={
                "transaction_id": "Int64",
                "user_id": "Int64",
                "transaction_amount": "float",
                "phone": "str",  # Ensure 'phone' is treated as string
                "membership_id": "Int64",
                "mdr_id": "Int64",
                "mdr_percentage": "float",
            },
            missing_value_defaults={"phone": None}  # Replace empty phone values with None
        )

        logging.info(f"Checking for missing values in transactions_with_mdr:")
        logging.info(transactions_with_mdr.isnull().sum())

        # Initialize Postgres Hook
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        # Table definitions
        table_definitions = {
            "sprout.user_profiling": """
                CREATE TABLE IF NOT EXISTS sprout.user_profiling (
                    user_id INT,
                    name TEXT,
                    timestamp TIMESTAMP,
                    membership_type TEXT,
                    membership_purchase_status TEXT,
                    mdr_percentage FLOAT,
                    first_transaction TIMESTAMP,
                    invoice_total_count INT,
                    invoice_total_amount FLOAT
                );
            """,
            "sprout.transaction_totals_per_type": """
                CREATE TABLE IF NOT EXISTS sprout.transaction_totals_per_type (
                    timestamp TIMESTAMP,
                    membership_type TEXT,
                    invoice_total_count INT,
                    invoice_total_amount FLOAT
                );
            """,
            "sprout.daily_activity_report": """
                CREATE TABLE IF NOT EXISTS sprout.daily_activity_report (
                    timestamp TIMESTAMP,
                    invoice_total_count INT,
                    invoice_total_amount FLOAT
                );
            """,
            "sprout.transactions_with_mdr": """
                CREATE TABLE IF NOT EXISTS sprout.transactions_with_mdr (
                    transaction_id INT,
                    user_id INT,
                    transaction_amount FLOAT,
                    timestamp TIMESTAMP,
                    name TEXT,
                    email TEXT,
                    phone TEXT,
                    membership_id FLOAT,
                    membership_type TEXT,
                    purchase_date DATE,
                    expiry_date DATE,
                    mdr_id FLOAT,
                    mdr_percentage FLOAT,
                    membership_purchase_status TEXT,
                    effective_amount FLOAT
                );
            """
        }

        # Create and truncate tables
        for table, create_sql in table_definitions.items():
            postgres_hook.run(create_sql)
            postgres_hook.run(f"TRUNCATE TABLE {table};")
            logging.info(f"Table {table} truncated.")

        # Load data into PostgreSQL
        def insert_data(df, table_name):
            postgres_hook.insert_rows(
                table=table_name,
                rows=df.astype(object).to_records(index=False),
                target_fields=df.columns.tolist()
            )
            logging.info(f"Inserted {df.shape[0]} rows into {table_name}.")

        insert_data(user_profiling, 'sprout.user_profiling')
        insert_data(transaction_totals_per_type, 'sprout.transaction_totals_per_type')
        insert_data(daily_activity_report, 'sprout.daily_activity_report')
        insert_data(transactions_with_mdr, 'sprout.transactions_with_mdr')

        logging.info("Data loaded into PostgreSQL successfully.")

    except Exception as e:
        logging.error(f"Error during data loading: {e}")
        raise


def generate_excel_report(**kwargs):
    postgres_conn_id = 'local_postgres'

    try:
        # Ensure the data folder exists
        report_folder = "./data"
        os.makedirs(report_folder, exist_ok=True)

        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        # Queries to fetch data
        queries = {
            "user_profiling": "SELECT * FROM sprout.user_profiling;",
            "transaction_type": "SELECT * FROM sprout.transaction_totals_per_type;",
            "daily_activity_report": "SELECT * FROM sprout.daily_activity_report;",
            "transactions_with_mdr": "SELECT * FROM sprout.transactions_with_mdr;",
        }

        # Fetch data from PostgreSQL
        dataframes = {}
        for name, query in queries.items():
            logging.info(f"Fetching data for {name}")
            dataframes[name] = postgres_hook.get_pandas_df(query)
            logging.info(f"Fetched {len(dataframes[name])} rows for {name}")

        # Generate Excel report
        report_file_path = os.path.join(report_folder, "report.xlsx")
        with pd.ExcelWriter(report_file_path, engine='xlsxwriter') as writer:
            for sheet_name, df in dataframes.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                logging.info(f"Written sheet {sheet_name} with {len(df)} rows.")

        logging.info(f"Excel report successfully generated at: {report_file_path}")

    except ModuleNotFoundError as e:
        logging.error("The required module for Excel report generation is missing. Install 'xlsxwriter' or 'openpyxl'.")
        raise
    except Exception as e:
        logging.error(f"Error generating Excel report: {e}")
        raise



# Define the DAG
with DAG(
    dag_id='etl_process_with_excel_report',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    generate_report_task = PythonOperator(
        task_id='generate_excel_report',
        python_callable=generate_excel_report
    )

    extract_task >> transform_task >> load_task >> generate_report_task
