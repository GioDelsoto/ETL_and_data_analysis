import pandas as pd
import psycopg2
from psycopg2 import sql
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log


def db_insert_kit_composition(df_kit_composition: pd.DataFrame, db_url: str) -> None:
    """
    Inserts a DataFrame of SKU products into the database.

    Args:
        sku_product_list (pd.DataFrame): DataFrame containing SKU products.
        db_url (str): Database connection URL.
    """
    try:
        null_products = df_kit_composition['product_sku'].isnull()
        df_kit_composition = df_kit_composition[~null_products]
        logger.info(f"Removed {null_products.sum()} rows with null product_sku")


        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        for _, row in df_kit_composition.iterrows():
            try:
                query = sql.SQL("""
                    INSERT INTO etl_schema.kits_content(kit_sku, product_sku, quantity)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (kit_sku, product_sku) DO NOTHING
                """)
                cursor.execute(query, (row['kit_sku'], row['product_sku'], row['quantity']))
            except psycopg2.Error as e:
                logger.error(f"Failed to insert/update kit component {row['kit_sku']} - {row['product_sku']}: {e}", extra={
                "sql_state": e.pgcode,
                "pg_error": e.pgerror,
                })

        conn.commit()

    except Exception as conn_error:
        logger.critical(f"Database connection failed at db_insert_kit_composition: {conn_error}", exc_info=True)

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
