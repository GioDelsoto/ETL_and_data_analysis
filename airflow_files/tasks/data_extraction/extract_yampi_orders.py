import os
import requests
import pandas as pd
import json
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log


def fetch_orders(start_date, end_date, headers, yampi_alias):
    """
    Function to fetch orders between a start and end date, and save the customer and order data into CSV files.
    """

    # Convert dates to string format YYYY-MM-DD
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # Initialize the API URL with filters for status and date
    url = f"https://api.dooki.com.br/v2/{yampi_alias}/orders?&status_id[]=paid&limit=5000&date=created_at:{start_date_str}|{end_date_str}"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        response_decoded = response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching orders from Yampi API: {e}")
        raise

    total_pages = response_decoded["meta"]['pagination']['total_pages']
    all_orders = []

    for i in range(total_pages):
        orders = response_decoded["data"]

        # Iterating through the orders
        for order in orders:
            customer_data = order["customer"]['data']
            order_data = order["transactions"]["data"][0]
            order_info = {
                "name": customer_data["generic_name"],
                "email": customer_data["email"],
                "phone": customer_data["phone"]['full_number'],
                "cpf": customer_data["cpf"],
                "order_id": order['number'],
                "order_date": order_data["created_at"]['date'],
                "status": order['status']['data']['name'],
                "payment_method": order['payments'][0]['name'],
                "total_value": order["value_total"],
                "total_product": order["value_products"],
                "total_shipment": order["shipment_cost"],
                "delivery_state": order['shipping_address']['data']['state'],
                "utm_source": order["utm_source"],
                "utm_medium": order["utm_medium"],
                "utm_campaign": order["utm_campaign"],
                "transaction_installments": order_data['installments'],
                "transaction_value": order_data['amount'],
                "coupom_code": order['promocode']['data']['code'] if len(order['promocode']['data']) > 0 else None,
                "coupom_value": order['value_discount']
            }

            # Iterating through kits and duplicating order data
            for kit in order['spreadsheet']['data']:
                kit_data = order_info.copy()  # Copy order data
                kit_data.update({
                    "kit_sku": kit['sku'],
                    "kit_name": kit['product'],
                    "quantity": kit['quantity']
                })
                all_orders.append(kit_data)

        # If there are more pages, fetch the next page
        if i + 1 < total_pages:
            url = f"https://api.dooki.com.br/v2/{yampi_alias}/orders?&status_id[]=paid&limit=5000&date=created_at:{start_date_str}|{end_date_str}&page={i+2}"
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                response_decoded = response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching orders from Yampi API on page {i + 2}: {e}")
                raise

    # Creating the file paths for saving the data


    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Caminhos relativos para os diretórios de dados dentro do contêiner ou local
    orders_folder = os.path.join(base_dir, '../../temp_data/orders_data')
    customers_folder = os.path.join(base_dir, '../../temp_data/customers_data')

    # Ensure the folders exist
    os.makedirs(orders_folder, exist_ok=True)
    os.makedirs(customers_folder, exist_ok=True)

    # Create a DataFrame for the orders
    df_orders = pd.DataFrame(all_orders)
    df_orders['order_date'] = pd.to_datetime(df_orders['order_date'])
    # Saving the orders CSV file
    file_name_orders = f"orders_{start_date_str}_to_{end_date_str}.csv"
    file_path_orders = os.path.join(orders_folder, file_name_orders)
    print(file_path_orders)
    try:
        df_orders.drop(['name', 'phone', 'email'], axis=1).to_csv(file_path_orders, index=False)
        logger.info(f"Orders data saved to {file_path_orders}")
    except Exception as e:
        logger.error(f"Error saving orders CSV file: {e}")
        raise

    # Create the customers CSV file, excluding duplicates by CPF

    df_customers = df_orders.sort_values('order_date').drop_duplicates('cpf', keep='first').copy()
    df_customers = df_customers[['name', 'phone', 'email', 'cpf','order_date']]
    file_name_customers = f"customers_{start_date_str}_to_{end_date_str}.csv"
    file_path_customers = os.path.join(customers_folder, file_name_customers)
    try:
        df_customers.to_csv(file_path_customers, index=False)
        logger.info(f"Customers data saved to {file_path_customers}")
    except Exception as e:
        logger.error(f"Error saving customers CSV file: {e}")
        raise

    return file_path_customers, file_path_orders
