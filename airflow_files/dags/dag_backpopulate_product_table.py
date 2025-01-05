import os
import sys

from dotenv import load_dotenv
from pathlib import Path
from tasks.data_extraction.extract_sku import extract_sku_from_sheets
from tasks.data_load.db_insert_sku_product import db_insert_sku_products
from tasks.data_load.db_insert_sku_kits import db_insert_sku_kits
from tasks.data_load.db_insert_kit_composition import db_insert_kit_composition
from tasks.data_transformation.kits_composition_transformation import kits_composition_transformation
from tasks.data_transformation.transform_skus import retrieve_products_sku

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
        ti.xcom_push(key='df_sku', value=df_sku.to_dict())

    def retrieve_products_skus_callable(**kwargs):
        ti = kwargs['ti']
        df_sku_dict = ti.xcom_pull(key='df_sku', task_ids='extract_sku_data')
        df_sku = pd.DataFrame(df_sku_dict)
        sku_product_list_dict = ti.xcom_pull(key='sku_product_list', task_ids='extract_sku_data')
        sku_product_list = pd.DataFrame(sku_product_list_dict)
        sku_product_list_transformed = retrieve_products_sku(df_sku, sku_product_list)
        ti.xcom_push(key='sku_product_list_transformed', value=sku_product_list_transformed.to_dict())

    def transform_kit_composition_task_callable(**kwargs):
        ti = kwargs['ti']
        df_sku_dict = ti.xcom_pull(key='df_sku', task_ids='extract_sku_data')
        df_sku = pd.DataFrame(df_sku_dict)
        df_kit_composition = kits_composition_transformation(df_sku, db_url)
        ti.xcom_push(key='df_kit_composition', value=df_kit_composition.to_dict())

    def load_products_task_callable(**kwargs):
        ti = kwargs['ti']
        sku_product_list_dict_transformed = ti.xcom_pull(key='sku_product_list_transformed', task_ids='retrieve_products_skus')
        sku_product_list_transformed = pd.DataFrame(sku_product_list_dict_transformed)
        db_insert_sku_products(sku_product_list_transformed, db_url)

    def load_kits_task_callable(**kwargs):
        ti = kwargs['ti']
        df_sku_dict = ti.xcom_pull(key='df_sku', task_ids='extract_sku_data')
        df_sku = pd.DataFrame(df_sku_dict)
        db_insert_sku_kits(df_sku, db_url)

    def load_kit_composition_task_callable(**kwargs):
        ti = kwargs['ti']
        df_kit_composition_dict = ti.xcom_pull(key='df_kit_composition', task_ids='transform_kit_composition')
        df_kit_composition = pd.DataFrame(df_kit_composition_dict)
        db_insert_kit_composition(df_kit_composition, db_url)

    # Tasks
    extract_data = PythonOperator(
        task_id='extract_sku_data',
        python_callable=extract_task_callable,
    )

    transform_sku_product = PythonOperator(
        task_id='retrieve_products_skus',
        python_callable=retrieve_products_skus_callable,
    )

    transform_kit_composition = PythonOperator(
        task_id='transform_kit_composition',
        python_callable=transform_kit_composition_task_callable,
    )

    load_products_data = PythonOperator(
        task_id='load_products_sku_data',
        python_callable=load_products_task_callable,
    )

    load_kits_data = PythonOperator(
        task_id='load_kits_sku_data',
        python_callable=load_kits_task_callable,
    )

    load_kit_composition = PythonOperator(
        task_id='load_kit_composition_data',
        python_callable=load_kit_composition_task_callable,
    )

    # Set task dependencies
    extract_data >> transform_sku_product >> [load_products_data, load_kits_data]
    [load_products_data, load_kits_data] >> transform_kit_composition >> load_kit_composition
