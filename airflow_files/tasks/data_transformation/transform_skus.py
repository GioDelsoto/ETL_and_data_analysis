import pandas as pd
import psycopg2
from psycopg2 import sql
import numpy as np

from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log


def retrieve_products_sku(df_sku: pd.DataFrame, sku_product_list: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts the composition of kits from a DataFrame of SKU products.

    Args:
        df_sku (pd.DataFrame): DataFrame containing SKU products.

    Returns:
        pd.DataFrame: DataFrame containing the composition of kits.
    """

    products = []

    for _, row in df_sku.iterrows():
        kit_sku = row['sku']

        kits_products = row['kit_composition'].split(',')
        for element in kits_products:

            element = element.strip()

            element_split = element.split(' ', 1)

            if len(element_split) == 1: #They forgot the quantity probably

                if len(element_split[0]) == 0: #Empty string
                    logger.error(f'Invalid element in kit {kit_sku}: {element_split}')
                    continue

                if element_split[0].isdigit(): #If the first element is a digit, it is the quantity
                    product = element_split[1:].strip().replace(' ', '')
                else:
                    product = element_split[0].strip().replace(' ', '')

            elif len(element_split) == 2:
                product = element_split[1].strip().replace(' ', '')
            else:
                logger.error(f'Invalid element in kit {kit_sku}: {element_split}')
                continue


            products.append(product)
    products_df = pd.DataFrame(products, columns = ['sku'])
    sku_product_transformed = sku_product_list.merge(products_df.drop_duplicates(subset='sku'), on = 'sku', how = 'outer')

    return sku_product_transformed
