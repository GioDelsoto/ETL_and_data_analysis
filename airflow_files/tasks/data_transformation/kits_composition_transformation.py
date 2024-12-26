import pandas as pd
import psycopg2
from psycopg2 import sql
import numpy as np

def kits_composition_transformation(df_sku: pd.DataFrame, db_url: str) -> pd.DataFrame:
    """
    Extracts the composition of kits from a DataFrame of SKU products.

    Args:
        df_sku (pd.DataFrame): DataFrame containing SKU products.

    Returns:
        pd.DataFrame: DataFrame containing the composition of kits.
    """

    kits_composition = []

    for _, row in df_sku.iterrows():
        kit_sku = row['sku']

        kits_products = row['kit_composition'].split(',')
        for element in kits_products:

            element = element.strip()

            element_split = element.split(' ', 1)

            if len(element_split) == 1: #They forgot the quantity probably

                if len(element_split[0]) == 0: #Empty string
                    print(f'Invalid element in kit {kit_sku}: {element_split}')
                    continue

                if element_split[0].isdigit(): #If the first element is a digit, it is the quantity
                    quantity = int(element_split[0].strip())
                    product = element_split[1:].strip().replace(' ', '')
                else:
                    quantity = 1
                    product = element_split[0].strip().replace(' ', '')

            elif len(element_split) == 2:

                quantity = int(element_split[0].strip())
                product = element_split[1].strip().replace(' ', '')
            else:
                print(f'Invalid element in kit {kit_sku}: {element_split}')
                continue


            kits_composition.append({
                'kit_sku': kit_sku,
                'product_sku': product,
                'quantity': quantity
            })

    df_kits_composition = pd.DataFrame(kits_composition)


    #Get the product_id for each product_sku from database
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    cursor.execute("SELECT sku, id FROM app_schema.products")
    product_skus = cursor.fetchall()
    product_id_map = {sku: id for sku, id in product_skus}

    cursor.execute("SELECT sku, id FROM app_schema.kits")
    kit_skus = cursor.fetchall()
    kit_id_map = {sku: id for sku, id in kit_skus}

    cursor.close()
    conn.close()

    #Map the product_id and kit_id to the product_sku and kit_sku
    df_kits_composition['kit_id'] = df_kits_composition['kit_sku'].map(kit_id_map).replace(np.nan, 1)
    df_kits_composition['product_id'] = df_kits_composition['product_sku'].map(product_id_map).replace(np.nan, 1)

    return df_kits_composition
