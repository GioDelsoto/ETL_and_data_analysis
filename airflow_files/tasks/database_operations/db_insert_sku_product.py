import pandas as pd
import psycopg2
from psycopg2 import sql


def db_insert_sku_products(sku_product_list: pd.DataFrame, db_url: str) -> None:
    """
    Inserts a DataFrame of SKU products into the database.

    Args:
        sku_product_list (pd.DataFrame): DataFrame containing SKU products.
        db_url (str): SQLAlchemy database connection URL.
    """

    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    for index, row in sku_product_list.iterrows():
        query = sql.SQL("""
            INSERT INTO app_schema.products (sku, name)
            VALUES (%s, %s)
        """)
        cursor.execute(query, (row['sku'], row['name']))

    conn.commit()
    cursor.close()
    conn.close()
