import pandas as pd
from typing import Tuple, List

def extract_sku_from_sheets(sheet_id: str, store: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extracts SKU data from Google Sheets for a specific store and processes it to generate
    a DataFrame of kit compositions and a list of individual SKUs with descriptions.

    Args:
        sheet_id (str): The ID of the Google Sheet containing SKU data.
        store (str): The store identifier.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]:
            - DataFrame of kit compositions with 'kit_composition' and 'Kit_SKU'.
            - DataFrame of unique SKUs with 'sku' and 'name'.
    """
    # Define sheet names based on the store
    store_sheets = {
        'pinkperfect': ('Kits Pink', 'Produtos Unitarios Pink'),
        'lemoritz': ('Kits LeMoritz', 'Produtos Unitarios LeMoritz'),
        'brainjuice': ('Kits BrainJuice', 'Produtos Unitarios BrainJuice'),
    }

    if store not in store_sheets:
        raise ValueError(f"Store '{store}' not recognized. Available options: {list(store_sheets.keys())}")

    sheet_name_kit, sheet_name_product = store_sheets[store]

    # Load data from Google Sheets
    url_kit = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name_kit}'.replace(" ", "%20")
    url_product = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name_product}'.replace(" ", "%20")
    df_sku = pd.read_csv(url_kit)
    df_sku_product = pd.read_csv(url_product)

    # Join and process data
    df_sku = df_sku[['SKU dos Componentes', 'SKU']]
    df_sku_product = df_sku_product[['SKU', 'Descrição']]
    df_sku_product['SKU dos Componentes'] = '1 ' + df_sku_product['SKU']
    df_sku_product.dropna(inplace=True)
    df_sku = pd.concat([df_sku, df_sku_product[['SKU dos Componentes', 'SKU']]])

    # Clean and rename columns
    df_sku['SKU dos Componentes'] = df_sku['SKU dos Componentes'].str.strip()
    df_sku = df_sku.rename(columns={'SKU dos Componentes': 'kit_composition', 'SKU': 'Kit_SKU'})

    # Process SKUs into unique list
    sku_product = df_sku['kit_composition'].str.replace(" ", "").str.split(",")
    sku_product_list = [item for sublist in sku_product for item in sublist if len(item) > 2]
    sku_product_list = [sku[1:] if sku[0].isdigit() else sku for sku in sku_product_list]
    sku_product_list = pd.DataFrame(list(set(sku_product_list)), columns=['SKU'])
    sku_product_list = sku_product_list.merge(df_sku_product[['Descrição', 'SKU']], on='SKU', how='left')
    sku_product_list = sku_product_list.rename(columns={'SKU':'sku','Descrição': 'name'})

    return df_sku, sku_product_list
