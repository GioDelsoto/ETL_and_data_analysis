import os
import requests
import json
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta


def fetch_orders(start_date, end_date, headers, yampi_alias):

    """
    This function is
    """

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")


    url = f"https://api.dooki.com.br/v2/{yampi_alias}/orders?&status_id[]=paid&limit=5000&date=created_at:{start_date_str}|{end_date_str}"
    response = requests.get(url, headers=headers)
    response_decoded = json.loads(response._content.decode("utf-8"))

    total_pages = response_decoded["meta"]['pagination']['total_pages']
    all_orders = []

    for i in range(total_pages):
        orders = response_decoded["data"]
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

            # Iterando sobre os kits e duplicando os dados do pedido
            for kit in order['spreadsheet']['data']:

                kit_data = order_info.copy()  # Copia os dados do pedido


                kit_data.update({
                    "kit_sku": kit['sku'],
                    "kit_name": kit['product'],
                    "quantity": kit['quantity']
                })
                all_orders.append(kit_data)

        #Go to next page
        url = f"https://api.dooki.com.br/v2/{yampi_alias}/orders?&status_id[]=paid&limit=5000&date=created_at:{start_date_str}|{end_date_str}&page={i+2}"
        response = requests.get(url, headers=headers)
        response_decoded = json.loads(response._content.decode("utf-8"))

    df = pd.DataFrame(all_orders)
    return df
