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

from tasks.data_retrieve.extract_sku import extract_sku_from_sheets
from tasks.database_operations.db_insert_sku_product import db_insert_sku_products

# Load environment variables
sheet_id = os.getenv('SKU_GOOGLE_SHEET_ID')
store = os.getenv('YAMPI_ALIAS')
db_url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"

df_sku, sku_product_list = extract_sku_from_sheets(sheet_id, store)

db_insert_sku_products(sku_product_list, db_url)

print('sucess')
