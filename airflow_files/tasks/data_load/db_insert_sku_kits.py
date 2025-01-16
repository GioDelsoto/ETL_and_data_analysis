import pandas as pd
import psycopg2
from psycopg2 import sql
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log


def db_insert_sku_kits(df_sku: pd.DataFrame, db_url: str, store_id:int) -> None:
    """
    Inserts a DataFrame of SKU kits into the database.

    Args:
        df_sku (pd.DataFrame): DataFrame containing SKU kits.
        db_url (str): Database connection URL.
    """
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()

        for _, row in df_sku.iterrows():

            try:
                query = sql.SQL("""
                    INSERT INTO etl_schema.kits (sku, name, store_id)
                    VALUES (%s, %s, %s)
                """)
                cursor.execute(query, (row['sku'], row['name'], store_id))
            except psycopg2.Error as e:
                logger.error(f"Failed to insert/update kit {row['sku']}: {e}", extra={
                "sql_state": e.pgcode,
                "pg_error": e.pgerror,
                })

        conn.commit()

    except Exception as conn_error:
        logger.critical(f"Database connection failed at db_insert_sku_kits: {conn_error}", exc_info=True)

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
