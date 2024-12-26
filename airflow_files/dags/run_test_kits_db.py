# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 20:16:57 2024

@author: giode
"""
import os
import sys
from dotenv import load_dotenv
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Carrega as variáveis do .env
env_path = Path(__file__).resolve().parent.parent / '.env'  # Caminho relativo ao .env
load_dotenv(dotenv_path=env_path)

import pandas as pd
from datetime import timedelta

from tasks.data_extraction.extract_sku import extract_sku_from_sheets
from tasks.data_load.db_insert_sku_product import db_insert_sku_products
from tasks.data_load.db_insert_sku_kits import db_insert_sku_kits
from tasks.data_load.db_insert_kit_composition import db_insert_kit_composition
from tasks.data_transformation.kits_composition_transformation import kits_composition_transformation


# Load environment variables
sheet_id = os.getenv('SKU_GOOGLE_SHEET_ID')
store = os.getenv('YAMPI_ALIAS')
db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

df_sku, sku_product_list = extract_sku_from_sheets(sheet_id, store)

#db_insert_sku_products(sku_product_list, db_url)

#db_insert_sku_kits(df_sku, db_url)

df_kit_composition = kits_composition_transformation(df_sku, db_url)
db_insert_kit_composition(df_kit_composition, db_url)

print('sucess')



def extract_kit_composition(df):
    # Lista para armazenar as composições extraídas
    compositions = []

    # Iterar sobre cada linha do DataFrame
    for _, row in df.iterrows():
        kit_sku = row['sku']
        # Dividir a string de composição em componentes individuais
        components = row['kit_composition'].split(',')
        for component in components:
            # Remover espaços extras e dividir em quantidade e produto
            parts = component.strip().split(' ', 1)
            if len(parts) == 2:
                quantity, product = parts
                compositions.append({'sku': kit_sku, 'product': product, 'quantity': int(quantity)})

    # Criar um novo DataFrame com as composições extraídas
    return pd.DataFrame(compositions)

# Aplicar a função para extrair as composições
df_compositions = extract_kit_composition(df_sku)

unit = df_compositions['product'].unique()
