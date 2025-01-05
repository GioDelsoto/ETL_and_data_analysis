import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from airflow_files.tasks.data_extraction.extract_yampi_orders import fetch_orders
from tasks.data_transformation.transform_orders_csv import extract_order_info
from tasks.data_load.db_insert_customer import db_insert_customer
from tasks.data_load.db_insert_orders import db_insert_orders
from tasks.delete_temp_data import delete_files_in_folder

from airflow.utils.task_group import TaskGroup

# Load environment variables
path_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path=path_env)

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"
#db_url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

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
def fetch_orders_for_week(start_date: datetime, end_date: datetime, **kwargs) -> None:
    """
    Fetches orders for a given week.
    """
    path_customers_csv, path_orders_csv = fetch_orders(start_date, end_date, headers, yampi_alias)

    # Push the paths to XCom so the next task can access them
    ti = kwargs['ti']
    ti.xcom_push(key='file_paths', value=(path_customers_csv, path_orders_csv))

# Function to insert customer data into the database
def insert_customers_data(**kwargs):
    """
    Inserts customer data into the database.
    """
    ti = kwargs['ti']

    # Retrieve the list of file paths from all fetch_orders tasks in the TaskGroup
    all_file_paths = []

    # Iterate through all task instances in the group to gather their file paths
    for task_instance in kwargs['dag_run'].get_task_instances():
        file_paths = task_instance.xcom_pull(task_ids=task_instance.task_id, key='file_paths')
        if file_paths:
            all_file_paths.append(file_paths)

    # Insert the customer data into the database for each of the files
    dtypes = {
        'name': str,
        'email': str,
        'cpf': str,
        'phone': str
    }

    print(len(all_file_paths))
    for path_customers_csv, _ in all_file_paths:
        # Assuming the CSV file contains orders data, read it into a DataFrame
        df_customers = pd.read_csv(path_customers_csv, dtype=dtypes)

        # Insert the customer data into the database
        db_insert_customer(df_customers, db_url)

# Function to transform orders info
def transform_orders_info(path_orders_csv: str, **kwargs):
    """
    Transforms orders data.
    """
    new_path = extract_order_info(path_orders_csv, db_url)
    return new_path

# Function to insert orders data into the database
def insert_orders_data(**kwargs):
    """
    Inserts orders data into the database.
    """
    ti = kwargs['ti']
    new_path_orders_csv = ti.xcom_pull(task_ids='transform_orders_group', key='new_file_paths')

    # Iterate through all transformed file paths to insert orders data

    dtypes = {
        'order_id': 'int64',
        #'order_date': 'str',
        'customer_id': 'int64',
        'status': 'str',
        'payment_method': 'str',
        'kit_id': 'int64',
        'quantity': 'int64',
        'total_value': 'float64',
        'total_product': 'float64',
        'total_shipment': 'float64',
        'coupom_code': 'str',
        'coupom_value': 'float64',
        'delivery_state': 'str',
        'utm_source': 'str',
        'utm_medium': 'str',
        'utm_campaign': 'str',
        'transaction_installments': 'int64',
        'transaction_value': 'float64'
    }
    for order_path in new_path_orders_csv:
        df_orders = pd.read_csv(order_path, dtype=dtypes)
        db_insert_orders(df_orders, db_url)

def delete_temp_data(**kwargs):

    """
    Deletes temporary data files.
    """

    current_directory = os.path.dirname(os.path.abspath(__file__))
    customers_data_path = os.path.join(current_directory, "../temp_data/customers_data")
    orders_data_path = os.path.join(current_directory, "../temp_data/orders_data")

    delete_files_in_folder(customers_data_path)
    delete_files_in_folder(orders_data_path)

# Create DAG
with DAG(
    'backpopulate_orders_V2',
    default_args={
        'owner': 'airflow',
        'retries': 4,
        'retry_delay': timedelta(seconds=30),
    },
    schedule_interval=None,
    start_date=datetime.today(),
    catchup=False,
) as dag:

    # Create a TaskGroup to group the fetching of orders
    with TaskGroup("fetch_orders_group") as fetch_orders_group:
        for start_date, end_date in week_ranges:
            fetch_orders_task = PythonOperator(
                task_id=f'fetch_orders_task_{start_date.strftime("%Y-%m-%d")}_{end_date.strftime("%Y-%m-%d")}',
                python_callable=fetch_orders_for_week,
                op_args=[start_date, end_date],
                provide_context=True,
            )

    # Task to insert the customer data into the database
    insert_customers_task = PythonOperator(
        task_id='insert_customers',
        python_callable=insert_customers_data,
        provide_context=True,
    )

    # Create a TaskGroup to transform orders in parallel
    with TaskGroup("transform_orders_group") as transform_orders_group:
        ti = kwargs['ti']
        all_file_paths = ti.xcom_pull(task_ids='fetch_orders_group', key='file_paths')
        for _, path_orders_csv in all_file_paths:
            transform_orders_task = PythonOperator(
                task_id=f'transform_orders_{os.path.basename(path_orders_csv)}',
                python_callable=transform_orders_info,
                op_args=[path_orders_csv],
                provide_context=True,
            )

    insert_order_task = PythonOperator(
        task_id='insert_orders',
        python_callable=insert_orders_data,
        provide_context=True,
    )

    delete_temp_data_task = PythonOperator(
        task_id='delete_temp_data',
        python_callable=delete_temp_data,
        provide_context=True,
    )

    # Set the task dependencies
    fetch_orders_group >> insert_customers_task >> transform_orders_group >> insert_order_task >> delete_temp_data_task
