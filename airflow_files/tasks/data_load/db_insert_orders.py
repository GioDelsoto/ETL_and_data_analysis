import pandas as pd
import psycopg2
from psycopg2 import sql
from airflow.utils.log.logging_mixin import LoggingMixin
from tasks.data_load.db_insert_sku_kits import db_insert_sku_kits

logger = LoggingMixin().log

def db_insert_orders(df_orders: pd.DataFrame, db_url: str) -> None:
    """
    Inserts a DataFrame of orders into the orders table in the database.
    """
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        for _, row in df_orders.iterrows():
            try:

                # Check if SKU exists in kits table
                sku_query = sql.SQL("""
                    SELECT 1 FROM etl_schema.kits WHERE sku = %s
                """)
                cursor.execute(sku_query, (row['kit_sku'],))
                sku_exists = cursor.fetchone()

                # If SKU doesn't exist, insert it
                if not sku_exists:
                    df_sku = pd.DataFrame({"sku": [row['kit_sku']], "name": [""]})
                    db_insert_sku_kits(df_sku, db_url)


                query = sql.SQL("""
                    INSERT INTO etl_schema.orders (
                        order_id, order_date, customer_id, status, payment_method, kit_sku, quantity,
                        total_value, total_product, total_shipment, coupom_code, coupom_value,
                        delivery_state, utm_source, utm_medium, utm_campaign, transaction_installments, transaction_value
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """)
                cursor.execute(query, (
                    row['order_id'], row['order_date'], row['customer_id'], row['status'], row['payment_method'],
                    row['kit_sku'], row['quantity'], row['total_value'], row['total_product'], row['total_shipment'],
                    row['coupom_code'], row['coupom_value'], row['delivery_state'], row['utm_source'], row['utm_medium'],
                    row['utm_campaign'], row['transaction_installments'], row['transaction_value']
                ))
            except psycopg2.Error as e:
                logger.error(f"Failed to insert order {row['order_id']} - Customer ID: {row['customer_id']}: {e}", extra={
                "sql_state": e.pgcode,
                "pg_error": e.pgerror,
                })

        conn.commit()

    except Exception as conn_error:
        logger.critical(f"Database connection failed at db_insert_orders: {conn_error}", exc_info=True)

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
