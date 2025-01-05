import pandas as pd
import psycopg2
from psycopg2 import sql
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

def db_insert_customer(df_customer: pd.DataFrame, db_url: str) -> None:
  """
  Inserts a DataFrame of orders to the customer table in the database.
  """
  try:
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    for _, row in df_customer.iterrows():
      try:
        query = sql.SQL("""
          INSERT INTO etl_schema.customers(name, email, cpf, phone)
          VALUES (%s, %s, %s, %s)
          ON CONFLICT (cpf) DO UPDATE
          SET email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            name = EXCLUDED.name
        """)
        cursor.execute(query, (row['name'], row['email'], row['cpf'], row['phone']))

      except psycopg2.Error as e:
        # Utilizando logger.error para registrar as informações do erro
        logger.error(f"Failed to insert/update customer {row['cpf']} - {row['email']}: {e}", extra={
          "sql_state": e.pgcode,
          "pg_error": e.pgerror,
        })

    conn.commit()

  except Exception as conn_error:
    logger.critical(f"Database connection failed: {conn_error}", exc_info=True)

  finally:
    if 'cursor' in locals() and cursor:
      cursor.close()
    if 'conn' in locals() and conn:
      conn.close()
