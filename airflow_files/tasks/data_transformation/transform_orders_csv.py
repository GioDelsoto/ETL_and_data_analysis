import pandas as pd
import numpy as np
import psycopg2
import os

def extract_order_info(path_orders_csv: str, db_url: str) -> pd.DataFrame:
    """
    Extrai informações de pedidos e dados do cliente com base no CPF.

    Args:
        path_orders_csv (str): String containing the path to the orders CSV file.

    Returns:
        pd.DataFrame: DataFrame containing extracted order information.
    """


    dtypes = {
        'order_id': 'int64',
        #'order_date': 'str',
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
        'utm_source': 'str',
        'utm_medium': 'str',
        'utm_campaign': 'str',
        'transaction_installments': 'int64',
        'transaction_value': 'float64'
    }
    df_orders = pd.read_csv(path_orders_csv, dtype=dtypes)

    df_extracted_orders = df_orders[[
        'order_id', 'order_date', 'cpf', 'status', 'payment_method',
        'kit_sku', 'quantity', 'total_value', 'total_product', 'total_shipment',
        'coupom_code', 'coupom_value', 'delivery_state', 'utm_source', 'utm_medium',
        'utm_campaign', 'transaction_installments', 'transaction_value'
    ]].copy()


    #Get the product_id for each product_sku from database
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    cursor.execute("SELECT cpf, id FROM app_schema.customers")
    customers = cursor.fetchall()
    customers_ids_map = {cpf: id for cpf, id in customers}

    cursor.execute("SELECT sku, id FROM app_schema.kits")
    kit_skus = cursor.fetchall()
    kit_id_map = {sku: id for sku, id in kit_skus}

    cursor.close()
    conn.close()

    df_extracted_orders['kit_id'] = df_extracted_orders['kit_sku'].map(kit_id_map).replace(np.nan, 1)
    df_extracted_orders['customer_id'] = df_extracted_orders['cpf'].map(customers_ids_map).replace(np.nan, 1)

    print(df_extracted_orders['customer_id'], df_extracted_orders['cpf'])

    df_extracted_orders.drop(columns=['cpf', 'kit_sku'], inplace=True)

    base, ext = os.path.splitext(path_orders_csv)
    new_path_orders_csv = f"{base}_processed{ext}"

    df_extracted_orders.to_csv(new_path_orders_csv, index=False)

    return new_path_orders_csv
