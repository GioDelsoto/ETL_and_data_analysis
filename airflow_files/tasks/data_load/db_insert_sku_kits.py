import pandas as pd
import psycopg2
from psycopg2 import sql


def db_insert_sku_kits(df_sku: pd.DataFrame, db_url: str) -> None:
    """
    Inserts a DataFrame of SKU kits into the database.

    Args:
        df_sku (pd.DataFrame): DataFrame containing SKU kits.
        db_url (str): Database connection URL.
    """

    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    for _, row in df_sku.iterrows():
        query = sql.SQL("""
            INSERT INTO app_schema.kits (sku, name)
            VALUES (%s, %s)
        """)
        cursor.execute(query, (row['sku'], row['name']))

    conn.commit()
    cursor.close()
    conn.close()
