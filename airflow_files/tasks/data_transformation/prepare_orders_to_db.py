import pandas as pd
import numpy as np
import psycopg2

def extract_order_info(df_orders: pd.DataFrame, db_url: str) -> pd.DataFrame:
    """
    Extrai informações de pedidos e dados do cliente com base no CPF.

    Args:
        df_orders (pd.DataFrame): DataFrame containing order information.

    Returns:
        pd.DataFrame: DataFrame containing extracted order information.
    """
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

    print(kit_id_map)
    return df_extracted_orders
