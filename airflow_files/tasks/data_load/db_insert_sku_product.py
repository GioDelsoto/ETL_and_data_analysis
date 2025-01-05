import pandas as pd
import psycopg2
from psycopg2 import sql
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log


def db_insert_sku_products(sku_product_list: pd.DataFrame, db_url: str) -> None:
    """
    Inserts a DataFrame of SKU products into the database.

    Args:
        sku_product_list (pd.DataFrame): DataFrame containing SKU products.
        db_url (str): Database connection URL.
    """
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()

        for _, row in sku_product_list.iterrows():

            try:
                query = sql.SQL("""
                    INSERT INTO etl_schema.products (sku, name)
                    VALUES (%s, %s)
                """)
                cursor.execute(query, (row['sku'], row['name']))
            except psycopg2.Error as e:
                logger.error(f"Failed to insert/update product {row['sku']}: {e}", extra={
                "sql_state": e.pgcode,
                "pg_error": e.pgerror,
                })

        conn.commit()

    except Exception as conn_error:
        logger.critical(f"Database connection failed at db_insert_sku_product: {conn_error}", exc_info=True)

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
