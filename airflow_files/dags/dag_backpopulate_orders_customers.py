import os
import psycopg2
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tasks.data_extraction.extract_yampi_orders import fetch_orders
from tasks.data_transformation.transform_orders_csv import extract_order_info
from tasks.data_load.db_insert_customer import db_insert_customer
from tasks.data_load.db_insert_orders import db_insert_orders
from tasks.delete_temp_data import delete_files_in_folder

from airflow.utils.task_group import TaskGroup
# Load environment variables
path_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path=path_env)

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

#Get stores from the database
def get_stores_from_db(db_url):
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    cursor.execute("SELECT id, store_name, yampi_alias, created_at FROM etl_schema.stores")  # Ajuste para sua tabela
    stores = cursor.fetchall()
    conn.close()
    return stores

#Create week ranges
def create_week_ranges(first_date, last_date):
    week_ranges = []
    current_start_date = first_date
    while current_start_date <= last_date:
        current_end_date = current_start_date + timedelta(days=6)
        if current_end_date > last_date:
            current_end_date = last_date
        week_ranges.append((current_start_date, current_end_date))
        current_start_date = current_end_date + timedelta(days=1)
    return week_ranges
# Fetch stores and generate week ranges for each store
def generate_store_week_ranges(stores):
    """
    Generate a dictionary of store_alias: week_ranges.
    """
    store_week_ranges = {}

    # Define the last date as yesterday
    last_date = datetime.now().date() - timedelta(days=1)

    for _, _, alias, created_at in stores:
        # Convert created_at to a date object if it's not already
        first_date = created_at if isinstance(created_at, datetime) else datetime.strptime(str(created_at), '%Y-%m-%d').date()
        # Generate week ranges starting from the store's creation date
        week_ranges = create_week_ranges(first_date, last_date)
        # Add to dictionary
        store_week_ranges[alias] = week_ranges

    return store_week_ranges

# Example usage
stores = get_stores_from_db(db_url)
store_week_ranges = generate_store_week_ranges(stores)

#Yampi Header
headers = {
    "content-type": "application/json",
    "User-Token": os.getenv('YAMPI_TOKEN'),
    "User-Secret-Key": os.getenv('YAMPI_SECRET_KEY')
}


# Python Callable definitions

# Function to fetch orders for a given week
def fetch_orders_for_week(start_date: datetime, end_date: datetime, yampi_alias: str, store_id: int,  **kwargs) -> None:
    """
    Fetches orders for a given week.
    """
    path_customers_csv, path_orders_csv = fetch_orders(start_date, end_date, headers, yampi_alias)

    # Push the paths to XCom so the next task can access them
    ti = kwargs['ti']
    ti.xcom_push(key='file_paths', value=(path_customers_csv, path_orders_csv, store_id))

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

    for path_customers_csv, _, store_id  in all_file_paths:
        # Assuming the CSV file contains orders data, read it into a DataFrame
        df_customers = pd.read_csv(path_customers_csv, dtype=dtypes)

        # Insert the customer data into the database
        db_insert_customer(df_customers, db_url, store_id)

        # Remove the temporary file
        os.remove(path_customers_csv)
# Function to transform orders info
def transform_orders_info(**kwargs):
    """
    Transforms orders data.
    """
    ti = kwargs['ti']

    # Retrieve the list of file paths from all fetch_orders tasks in the TaskGroup
    all_file_paths = []

    # Iterate through all task instances in the group to gather their file paths
    for task_instance in kwargs['dag_run'].get_task_instances():
        file_paths = task_instance.xcom_pull(task_ids=task_instance.task_id, key='file_paths')
        if file_paths:
            all_file_paths.append(file_paths)

    # Transform the orders data for each of the files
    new_path_orders_csv = []
    for _, path_orders_csv, store_id in all_file_paths:
        new_path = extract_order_info(path_orders_csv, db_url, store_id)
        new_path_orders_csv.append((new_path, store_id))
        # Remove the temporary file
        os.remove(path_orders_csv)
    ti.xcom_push(key='new_file_paths', value=new_path_orders_csv)

# Function to insert orders data into the database
def insert_orders_data(**kwargs):
    """
    Inserts orders data into the database.
    """
    ti = kwargs['ti']
    new_path_orders_csv = ti.xcom_pull(task_ids='transform_orders', key='new_file_paths')

    # Iterate through all transformed file paths to insert orders data

    dtypes = {
        'order_id': 'int64',
        #'order_date': 'str',
        'customer_id': 'int64',
        'status': 'str',
        'payment_method': 'str',
        'kit_sku': 'str',
        'quantity': 'int64',
        'total_value': 'float64',
        'total_product': 'float64',
        'total_shipment': 'float64',
        'coupom_code': 'str',
        'coupom_value': 'float64',
        'delivery_state': 'str',
        'delivery_city': 'str',
        'delivery_street': 'str',
        'delivery_number': 'str',
        'delivery_complement': 'str',
        'delivery_zipcode': 'str',
        'utm_source': 'str',
        'utm_medium': 'str',
        'utm_campaign': 'str',
        'transaction_installments': 'int64',
        'transaction_value': 'float64'
    }
    for order_path, store_id in new_path_orders_csv:
        df_orders = pd.read_csv(order_path, dtype=dtypes)
        db_insert_orders(df_orders, db_url, store_id)
        # Delete the transformed orders CSV after insertion
        os.remove(order_path)

# Create DAG
with DAG(
    'backpopulate_orders_customers',
    default_args={
        'owner': 'airflow',
        'retries': 4,
        'retry_delay': timedelta(seconds=15),
    },
    schedule_interval=None,
    start_date=datetime.today(),
    catchup=False,
) as dag:

    # Create a TaskGroup to group the fetching of orders
    with TaskGroup("fetch_orders_group") as fetch_orders_group:
        for store_id, store_name, alias, created_at in stores:
            week_ranges = store_week_ranges[alias]
            for start_date, end_date in week_ranges:
                fetch_orders_task = PythonOperator(
                    task_id=f'{alias}_fetch_orders_task_{start_date.strftime("%Y-%m-%d")}_{end_date.strftime("%Y-%m-%d")}',
                    python_callable=fetch_orders_for_week,
                    op_args=[start_date, end_date, alias, store_id],
                )



        # Task to insert the customer data into the database
    insert_customers_task = PythonOperator(
        task_id='insert_customers',
        python_callable=insert_customers_data,
    )

    transform_orders_info_task = PythonOperator(
        task_id='transform_orders',
        python_callable=transform_orders_info,
    )

    insert_order_task = PythonOperator(
        task_id='insert_orders',
        python_callable=insert_orders_data,
    )

    # Set the task dependencies
    fetch_orders_group >> insert_customers_task >> transform_orders_info_task >> insert_order_task
