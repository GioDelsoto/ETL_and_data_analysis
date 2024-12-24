import os
import sys

from dotenv import load_dotenv
from pathlib import Path
from tasks.data_retrieve.extract_sku import extract_sku_from_sheets
from tasks.database_operations.db_insert_sku_product import db_insert_sku_products
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
env_path = Path(__file__).resolve().parent.parent / '.env'  # Caminho relativo ao .env
load_dotenv(dotenv_path=env_path)



# Load environment variables
sheet_id = os.getenv('SKU_GOOGLE_SHEET_ID')
store = os.getenv('YAMPI_ALIAS')
#db_url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"
db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id='backpopulate_product_table',
    default_args=default_args,
    description='Pipeline to extract and insert SKU data',
    schedule_interval=None,  # Run only on trigger
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def extract_task_callable(**kwargs):
        ti = kwargs['ti']
        df_sku, sku_product_list = extract_sku_from_sheets(sheet_id, store)
        ti.xcom_push(key='sku_product_list', value=sku_product_list.to_dict())

    def load_task_callable(**kwargs):
        ti = kwargs['ti']
        sku_product_list_dict = ti.xcom_pull(key='sku_product_list', task_ids='extract_sku_data')
        sku_product_list = pd.DataFrame(sku_product_list_dict)
        db_insert_sku_products(sku_product_list, db_url)

    # Tasks
    extract_data = PythonOperator(
        task_id='extract_sku_data',
        python_callable=extract_task_callable,
    )

    load_data = PythonOperator(
        task_id='load_sku_data',
        python_callable=load_task_callable,
    )

    # Set task dependencies
    extract_data >> load_data
