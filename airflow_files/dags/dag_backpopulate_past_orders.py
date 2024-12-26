import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from tasks.data_extraction.extract_all_yampi_orders import fetch_orders
from tasks.data_load.db_insert_customer import db_insert_customer
from tasks.data_transformation.prepare_orders_to_db import extract_order_info
from tasks.data_load.db_insert_orders import db_insert_orders

# Load environment variables
path_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path=path_env)

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

yampi_user_token = os.getenv('YAMPI_TOKEN')
yampi_secret_key = os.getenv('YAMPI_SECRET_KEY')
yampi_alias = os.getenv('YAMPI_ALIAS')

headers = {
    "content-type": "application/json",
    "User-Token": yampi_user_token,
    "User-Secret-Key": yampi_secret_key
}

# Define the first date of the data to be fetched
first_date = pd.to_datetime('2021-02-01', format="%Y-%m-%d")
yesterday = datetime.now() - timedelta(1)

week_ranges = []

# Week ranges
current_start_date = first_date
while current_start_date <= yesterday:
    current_end_date = current_start_date + timedelta(days=6)
    if current_end_date > yesterday:
        current_end_date = yesterday
    week_ranges.append((current_start_date, current_end_date))
    current_start_date = current_end_date + timedelta(days=1)

# Function to fetch orders for a given week
def fetch_orders_for_week(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Fetches orders for a given week.
    """
    order_data = fetch_orders(start_date, end_date, headers, yampi_alias)
    return order_data

# Function to insert customer data into the database
def insert_customer_data(**kwargs):
    """
    Inserts customer data into the database.
    """
    ti = kwargs['ti']
    order_data = ti.xcom_pull(task_ids=f'fetch_orders_{kwargs["start_date"]}_{kwargs["end_date"]}')
    df_orders = pd.DataFrame(order_data)
    db_insert_customer(df_orders, db_url)

# Function to extract order info
def extract_order_info_for_db(**kwargs):
    """
    Extracts order info from the DataFrame.
    """
    ti = kwargs['ti']
    order_data = ti.xcom_pull(task_ids=f'fetch_orders_{kwargs["start_date"]}_{kwargs["end_date"]}')
    df_orders = pd.DataFrame(order_data)
    df_extracted = extract_order_info(df_orders, db_url)
    ti.xcom_push(key='extracted_orders', value=df_extracted.to_dict())

# Function to insert orders into the database
def insert_orders_data(**kwargs):
    """
    Inserts order data into the database.
    """
    ti = kwargs['ti']
    extracted_orders = ti.xcom_pull(task_ids=f'extract_order_info_{kwargs["start_date"]}_{kwargs["end_date"]}', key='extracted_orders')
    df_extracted_orders = pd.DataFrame(extracted_orders)
    db_insert_orders(df_extracted_orders, db_url)

with DAG(
    'capture_orders_dag',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=None,
    start_date=datetime.today(),
    catchup=False,
) as dag:

    for start_date, end_date in week_ranges:
        extract_orders_task = PythonOperator(
            task_id=f'fetch_orders_{start_date.strftime("%Y-%m-%d")}_{end_date.strftime("%Y-%m-%d")}',
            python_callable=fetch_orders_for_week,
            op_args=[start_date, end_date],
            provide_context=True,
        )

        insert_customer_task = PythonOperator(
            task_id=f'insert_customers_{start_date.strftime("%Y-%m-%d")}_{end_date.strftime("%Y-%m-%d")}',
            python_callable=insert_customer_data,
            provide_context=True,
            op_kwargs={'start_date': start_date.strftime("%Y-%m-%d"), 'end_date': end_date.strftime("%Y-%m-%d")},
        )

        extract_order_info_task = PythonOperator(
            task_id=f'extract_order_info_{start_date.strftime("%Y-%m-%d")}_{end_date.strftime("%Y-%m-%d")}',
            python_callable=extract_order_info_for_db,
            provide_context=True,
            op_kwargs={'start_date': start_date.strftime("%Y-%m-%d"), 'end_date': end_date.strftime("%Y-%m-%d")},
        )

        insert_orders_task = PythonOperator(
            task_id=f'insert_orders_{start_date.strftime("%Y-%m-%d")}_{end_date.strftime("%Y-%m-%d")}',
            python_callable=insert_orders_data,
            provide_context=True,
            op_kwargs={'start_date': start_date.strftime("%Y-%m-%d"), 'end_date': end_date.strftime("%Y-%m-%d")},
        )

        extract_orders_task >> insert_customer_task >> extract_order_info_task >> insert_orders_task
