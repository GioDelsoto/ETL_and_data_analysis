import pandas as pd
import psycopg2
from psycopg2 import sql

def db_insert_customer(df_orders: pd.DataFrame, db_url: str) -> None:
    """
    Inserts a DataFrame of orders to the customer table in the database.
    """
    df_customer = df_orders[['name', 'email', 'cpf', 'phone']].copy()

    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    for _, row in df_customer.iterrows():
        query = sql.SQL("""
            INSERT INTO app_schema.customers(name, email, cpf, phone)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (cpf) DO UPDATE
                SET email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    name = EXCLUDED.name
        """)
        cursor.execute(query, (row['name'], row['email'], row['cpf'], row['phone']))

    conn.commit()
    cursor.close()
    conn.close()
