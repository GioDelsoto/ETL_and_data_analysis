import os
import psycopg2
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow.utils.task_group import TaskGroup
from tasks.data_extraction.extract_google import fetch_google_ads_campaigns
from airflow_files.tasks.data_load.db_insert_gg_campaign_data import db_insert_campaign_data
from airflow_files.tasks.data_load.db_register_gg_campaign import db_register_campaign
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log
# Load environment variables
path_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path=path_env)

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"
yesterday_date = datetime.now().date() - timedelta(days=1)

# Functions
def get_stores_from_db(db_url):
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    cursor.execute("SELECT id, store_name, yampi_alias, created_at, google_ads_account FROM etl_schema.stores")
    stores = cursor.fetchall()
    conn.close()
    return stores

stores = get_stores_from_db(db_url)

client_id = os.getenv('GOOGLE_CLIENT_ID')
client_secret = os.getenv('GOOGLE_CLIENT_SECRET')
refresh_token = os.getenv('GOOGLE_REFRESH_TOKEN')
developer_token = os.getenv('GOOGLE_DEVELOPER_TOKEN')

# OAuth 2.0 authentication endpoint URL
auth_url = "https://oauth2.googleapis.com/token"

# Data required for OAuth authentication
auth_data = {
    "client_id": client_id,
    "client_secret": client_secret,
    "refresh_token": refresh_token,
    "grant_type": "refresh_token"
}

# Request access token
auth_response = requests.post(auth_url, data=auth_data)
auth_response_data = auth_response.json()

# Extract the access token from the response
access_token = auth_response_data["access_token"]


# Python Callable functions

def fetch_google_data(store_id, customer_id, access_token, developer_token, start_date: datetime, end_date: datetime, **kwargs):

    all_data_path, unique_campaigns_path = fetch_google_ads_campaigns(store_id, customer_id, start_date, end_date, access_token, developer_token)

    ti = kwargs['ti']
    if all_data_path is not None:
        ti.xcom_push(key='file_paths', value=(all_data_path, unique_campaigns_path, store_id))

def insert_campaigns(**kwargs):
    ti = kwargs['ti']

    all_file_paths = []

    # Iterate through all task instances in the group to gather their file paths
    for task_instance in kwargs['dag_run'].get_task_instances():
        file_paths = task_instance.xcom_pull(task_ids=task_instance.task_id, key='file_paths')
        if file_paths:
            all_file_paths.append(file_paths)


    for _, path_campaigns_csv, store_id in all_file_paths:
        if path_campaigns_csv is None:
            continue
        df_campaigns = pd.read_csv(path_campaigns_csv, dtype={'campaign_name': 'str', 'campaign_id': 'str', 'channel': 'str'})
        db_register_campaign(df_campaigns, db_url, store_id)
        os.remove(path_campaigns_csv)

def insert_google_data(**kwargs):
    ti = kwargs['ti']

    all_file_paths = []

    # Iterate through all task instances in the group to gather their file paths
    for task_instance in kwargs['dag_run'].get_task_instances():
        file_paths = task_instance.xcom_pull(task_ids=task_instance.task_id, key='file_paths')
        if file_paths:
            all_file_paths.append(file_paths)

    for google_data_path_csv,_ , store_id in all_file_paths:
        if google_data_path_csv is None:
            continue
        df_google = pd.read_csv(google_data_path_csv, dtype={'spend': 'float64', 'conversions': 'float'})
        db_insert_campaign_data(df_google, db_url, store_id)
        os.remove(google_data_path_csv)

with DAG(
    'daily_google_data',
    default_args={'owner': 'airflow', 'retries': 4, 'retry_delay': timedelta(seconds=30)},
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    with TaskGroup("fetch_google_group") as fetch_google_group:
        for store_id, store_name, alias, created_at, google_ads_id in stores:

            PythonOperator(
                task_id=f'{alias}_fetch_gg_data_task_{yesterday_date.strftime("%Y-%m-%d")}',
                python_callable=fetch_google_data,
                op_args = [store_id, google_ads_id, access_token, developer_token, yesterday_date, yesterday_date],
                provide_context=True,
            )

    insert_campaigns_task = PythonOperator(task_id='insert_campaigns', python_callable=insert_campaigns)
    insert_gg_data_task = PythonOperator(task_id='insert_gg_data', python_callable=insert_google_data)

    fetch_google_group >> insert_campaigns_task >> insert_gg_data_task
