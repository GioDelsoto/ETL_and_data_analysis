import pandas as pd
import psycopg2
from psycopg2 import sql

def db_insert_orders(df_orders: pd.DataFrame, db_url: str) -> None:
    """
    Inserts a DataFrame of orders into the orders table in the database.
    """
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    for _, row in df_orders.iterrows():
        query = sql.SQL("""
            INSERT INTO app_schema.orders (
                order_id, order_date, customer_id, status, payment_method, kit_id, quantity,
                total_value, total_product, total_shipment, coupom_code, coupom_value,
                delivery_state, utm_source, utm_medium, utm_campaign, transaction_installments, transaction_value
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        cursor.execute(query, (
            row['order_id'], row['order_date'], row['customer_id'], row['status'], row['payment_method'],
            row['kit_id'], row['quantity'], row['total_value'], row['total_product'], row['total_shipment'],
            row['coupom_code'], row['coupom_value'], row['delivery_state'], row['utm_source'], row['utm_medium'],
            row['utm_campaign'], row['transaction_installments'], row['transaction_value']
        ))

    conn.commit()
    cursor.close()
    conn.close()
