import os
import sys
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

# Configurando o caminho do projeto para importar os scripts corretamente
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importando as funções utilizadas na DAG
from tasks.data_extraction.extract_all_yampi_orders import fetch_orders
from tasks.data_load.db_insert_customer import db_insert_customer
from tasks.data_transformation.prepare_orders_to_db import extract_order_info
from tasks.data_load.db_insert_orders import db_insert_orders

# Carrega as variáveis do arquivo .env
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Configurações de banco de dados e API
db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"
yampi_user_token = os.getenv('YAMPI_TOKEN')
yampi_secret_key = os.getenv('YAMPI_SECRET_KEY')
yampi_alias = os.getenv('YAMPI_ALIAS')

headers = {
    "content-type": "application/json",
    "User-Token": yampi_user_token,
    "User-Secret-Key": yampi_secret_key
}

# Define as datas para processamento
first_date = pd.to_datetime('2023-04-10', format="%Y-%m-%d")
yesterday = datetime.now() - timedelta(365)

week_ranges = []
current_start_date = first_date

while current_start_date <= yesterday:
    current_end_date = current_start_date + timedelta(days=6)
    if current_end_date > yesterday:
        current_end_date = yesterday
    week_ranges.append((current_start_date, current_end_date))
    current_start_date = current_end_date + timedelta(days=1)

# Função principal para processar os dados
def main():
    for start_date, end_date in week_ranges:
        print(f"\nProcessando período de {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")

        # Extrai os pedidos para o período
        print("Extraindo pedidos...")
        orders = fetch_orders(start_date, end_date, headers, yampi_alias)
        df_orders = pd.DataFrame(orders)
        print(df_orders.columns)

        # Insere os clientes no banco de dados
        print("Inserindo clientes no banco...")
        db_insert_customer(df_orders, db_url)

        # Extrai informações dos pedidos
        print("Extraindo informações dos pedidos...")
        df_extracted_orders = extract_order_info(df_orders, db_url)
        print(df_extracted_orders.head())

        # Insere os pedidos no banco de dados
        print("Inserindo pedidos no banco...")
        db_insert_orders(df_extracted_orders, db_url)

        print(f"Processamento do período {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')} concluído.")

if __name__ == "__main__":
    main()
