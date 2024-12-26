import pandas as pd
import psycopg2
from psycopg2 import sql
import logging

def db_insert_kit_composition(df_kit_composition: pd.DataFrame, db_url: str) -> None:
    """
    Inserts a DataFrame of SKU products into the database.

    Args:
        sku_product_list (pd.DataFrame): DataFrame containing SKU products.
        db_url (str): Database connection URL.
    """

    null_products = df_kit_composition['product_id'].isnull()
    df_kit_composition = df_kit_composition[~null_products]
    logging.info(f"Removed {null_products.sum()} rows with null product_id")


    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    for _, row in df_kit_composition.iterrows():
        query = sql.SQL("""
            INSERT INTO app_schema.kits_content(kit_id, product_id, quantity)
            VALUES (%s, %s, %s)
            ON CONFLICT (kit_id, product_id) DO NOTHING
        """)
        cursor.execute(query, (row['kit_id'], row['product_id'], row['quantity']))

    conn.commit()
    cursor.close()
    conn.close()
