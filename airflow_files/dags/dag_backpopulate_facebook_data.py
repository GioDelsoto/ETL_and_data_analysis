import os
import psycopg2
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow.utils.task_group import TaskGroup
from tasks.data_extraction.extract_facebook import fetch_facebook_campaigns
from airflow_files.tasks.data_load.db_insert_fb_campaign_data import db_insert_campaign_data
from airflow_files.tasks.data_load.db_register_fb_campaign import db_register_campaign
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log
# Load environment variables
path_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path=path_env)

db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

# Functions
def get_stores_from_db(db_url):
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    cursor.execute("SELECT id, store_name, yampi_alias, created_at, fb_ad_account FROM etl_schema.stores")
    stores = cursor.fetchall()
    conn.close()
    return stores

def create_day_ranges(first_date, last_date):
    date_ranges = []
    current_start_date = first_date
    while current_start_date <= last_date:
        current_end_date = current_start_date + timedelta(days=29)
        if current_end_date > last_date:
            current_end_date = last_date
        date_ranges.append((current_start_date, current_end_date))
        current_start_date = current_end_date + timedelta(days=1)
    return date_ranges

def generate_store_date_ranges(stores):
    store_date_ranges = {}
    last_date = datetime.now().date() - timedelta(days=1)
    for _, _, alias, created_at, _ in stores:
        first_date = created_at if isinstance(created_at, datetime) else datetime.strptime(str(created_at), '%Y-%m-%d').date()
        date_ranges = create_day_ranges(first_date, last_date)
        store_date_ranges[alias] = date_ranges
    return store_date_ranges

stores = get_stores_from_db(db_url)
store_date_ranges = generate_store_date_ranges(stores)
token_fb = os.getenv('FB_TOKEN')


# Python Callable functions
def fetch_facebook_data(store_id, token_fb, ad_account, start_date: datetime, end_date: datetime, **kwargs):
    all_data_path, unique_campaigns_path = fetch_facebook_campaigns(store_id, token_fb, ad_account, start_date, end_date)
    ti = kwargs['ti']
    ti.xcom_push(key='file_paths', value=(all_data_path, unique_campaigns_path, store_id))

def insert_campaigns(**kwargs):
    ti = kwargs['ti']
    #all_file_paths = [ti.xcom_pull(task_ids=task.task_id, key='file_paths') for task in kwargs['dag_run'].get_task_instances()]

    all_file_paths = []

    # Iterate through all task instances in the group to gather their file paths
    for task_instance in kwargs['dag_run'].get_task_instances():
        file_paths = task_instance.xcom_pull(task_ids=task_instance.task_id, key='file_paths')
        if file_paths:
            all_file_paths.append(file_paths)


    for _, path_campaigns_csv, store_id in all_file_paths:
        if path_campaigns_csv is None:
            continue
        df_campaigns = pd.read_csv(path_campaigns_csv, dtype={'campaign_name': 'str', 'campaign_id': 'str', 'objective': 'str'})
        db_register_campaign(df_campaigns, db_url, store_id)
        os.remove(path_campaigns_csv)

def insert_facebook_data(**kwargs):
    ti = kwargs['ti']
#    facebook_data_path = ti.xcom_pull(task_ids='fetch_facebook_group', key='file_paths', include_prior_dates=True)

    all_file_paths = []

    # Iterate through all task instances in the group to gather their file paths
    for task_instance in kwargs['dag_run'].get_task_instances():
        file_paths = task_instance.xcom_pull(task_ids=task_instance.task_id, key='file_paths')
        if file_paths:
            all_file_paths.append(file_paths)

    for facebook_data_path_csv,_ , store_id in all_file_paths:
        if facebook_data_path_csv is None:
            continue
        df_facebook = pd.read_csv(facebook_data_path_csv, dtype={'spend': 'float64', 'purchase': 'int64', 'cpm': 'float64', 'cpp': 'float64'})
        db_insert_campaign_data(df_facebook, db_url, store_id)
        os.remove(facebook_data_path_csv)

with DAG(
    'backpopulate_facebook_data',
    default_args={'owner': 'airflow', 'retries': 4, 'retry_delay': timedelta(seconds=30)},
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    with TaskGroup("fetch_facebook_group") as fetch_facebook_group:
        for store_id, store_name, alias, created_at, fb_ad_account in stores:
            date_ranges = store_date_ranges[alias]
            for start_date, end_date in date_ranges:
                PythonOperator(
                    task_id=f'{alias}_fetch_fb_data_task_{start_date.strftime("%Y-%m-%d")}_{end_date.strftime("%Y-%m-%d")}',
                    python_callable=fetch_facebook_data,
                    op_args=[store_id, token_fb, fb_ad_account, start_date, end_date],
                    provide_context=True,
                )

    insert_campaigns_task = PythonOperator(task_id='insert_campaigns', python_callable=insert_campaigns)
    insert_fb_data_task = PythonOperator(task_id='insert_fb_data', python_callable=insert_facebook_data)

    fetch_facebook_group >> insert_campaigns_task >> insert_fb_data_task
