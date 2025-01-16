import pandas as pd
import psycopg2
import os
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

def extract_order_info(path_orders_csv: str, db_url: str, store_id:int) -> pd.DataFrame:
    """
    Extract order and customer data based on CPF (Brazilian personal identifier).

    Args:
        path_orders_csv (str): String containing the path to the orders CSV file.
        db_url (str): Database connection URL.

    Returns:
        pd.DataFrame: DataFrame containing extracted order information.
    """

    try:
        # Defining data types for each column to ensure consistency
        dtypes = {
            'order_id': 'int64',
            'cpf': 'str',
            'status': 'str',
            'payment_method': 'str',
            'kit_sku': 'str',
            'quantity': 'int64',
            'total_value': 'float64',
            'total_product': 'float64',
            'total_shipment': 'float64',
            'coupom_code': 'str',
            'coupom_value': 'float64',
            'delivery_state': 'str',
            'delivery_city': 'str',
            'delivery_street': 'str',
            'delivery_number': 'str',
            'delivery_complement': 'str',
            'delivery_zipcode': 'str',
            'utm_source': 'str',
            'utm_medium': 'str',
            'utm_campaign': 'str',
            'transaction_installments': 'int64',
            'transaction_value': 'float64'
        }

        # Reading the CSV file
        df_orders = pd.read_csv(path_orders_csv, dtype=dtypes)

        # Selecting the columns of interest
        df_extracted_orders = df_orders[[
            'order_id', 'order_date', 'cpf', 'status', 'payment_method',
            'kit_sku', 'quantity', 'total_value', 'total_product', 'total_shipment',
            'coupom_code', 'coupom_value',
            'delivery_state','delivery_city', 'delivery_street','delivery_number','delivery_complement','delivery_zipcode',
            'utm_source', 'utm_medium',
            'utm_campaign', 'transaction_installments', 'transaction_value'
        ]].copy()

        # Connecting to the database to get customer IDs based on CPF
        try:
            conn = psycopg2.connect(db_url)
            cursor = conn.cursor()

            # Executing query to fetch CPF and customer IDs
            cursor.execute("SELECT cpf, store_id, id FROM etl_schema.customers")
            customers = cursor.fetchall()
            customers_ids_map = {(cpf, store_id): id for cpf, store_id, id in customers}

        except psycopg2.Error as e:
            logger.error(f"Failed to SELECT customers from database: {e}", extra={
                "sql_state": e.pgcode,
                "pg_error": e.pgerror,
            })
            # Returning None or handling appropriately in case of a connection error
            return None

        finally:
            # Ensuring that cursor and connection are closed
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        # Mapping CPFs to customer IDs
        df_extracted_orders['customer_id'] = df_extracted_orders.apply(lambda row: customers_ids_map.get((row['cpf'], store_id)), axis=1)
        # Checking if there are any missing customer IDs (CPFs not found in database)

        if df_extracted_orders['customer_id'].isna().sum() > 0:
            nan_cpf = df_extracted_orders[df_extracted_orders['customer_id'].isna()]
            logger.critical(f"Customers not found in the database: {nan_cpf}", exc_info=True)

        # Dropping the CPF column after mapping
        df_extracted_orders.drop(columns=['cpf'], inplace=True)

        # Saving the resulting DataFrame to a new CSV file
        base, ext = os.path.splitext(path_orders_csv)
        new_path_orders_csv = f"{base}_processed{ext}"
        df_extracted_orders.to_csv(new_path_orders_csv, index=False)

        # Returning the path to the processed CSV file
        return new_path_orders_csv

    except Exception as e:
        # Catching any other errors during the data processing
        logger.critical(f"Failed to transform orders data file: {path_orders_csv}: {e}", exc_info=True)
        return None
