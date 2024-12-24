import os
import requests
import json
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta


path_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')

load_dotenv(dotenv_path=path_env)

yampi_user_token = os.getenv('YAMPI_TOKEN')
yampi_secret_key = os.getenv('YAMPI_SECRET_KEY')
yampi_alias = os.getenv('YAMPI_ALIAS')

headers = {
    "content-type": "application/json",
    "User-Token":yampi_user_token,
    "User-Secret-Key": yampi_secret_key
}

def backpopulate_yampi_data(start_date, end_date) -> None:
    """
    This function is
    """

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    url = f"https://api.dooki.com.br/v2/{yampi_alias}/orders?&status_id[]=paid&limit=5000&date=created_at:{start_date_str}|{end_date_str}"
    response = requests.get(url, headers=headers)
    response_decoded = json.loads(response._content.decode("utf-8"))

    total_pages = response_decoded["meta"]['pagination']['total_pages']
    for i in range(total_pages):
        orders = response_decoded["data"]
        for order in orders:
            name = order["customer"]['data']["generic_name"]
            email = order["customer"]['data']["email"]
            phone = order["customer"]['data']["phone"]
            cpf = order["customer"]['data']["cpf"]

            order_id = order['number']
            order_date = order["transactions"]["data"][0]["created_at"]['date']

            #customer_id
            status = order['status']['data']['name']
            payment_method = order['payments'][0]['name']

            kit_sku = []
            kit_name = []
            quantity = []

            for i in range(len(order['spreadsheet']['data'])):
                kit_sku.append(order['spreadsheet']['data'][0]['sku'])
                kit_name.append(order['spreadsheet']['data'][0]['product'])
                quantity.append(order['spreadsheet']['data'][0]['quantity'])

            total_value = order["value_total"]
            total_product = order["value_products"]
            total_ship = order["shipment_cost"]
            delivery_state = order['shipping_address']['data']['state']

            utm_source = order["utm_source"]
            utm_medium = order["utm_medium"]
            utm_campaign = order["utm_campaign"]

            if len(order['transactions']['data']) > 1:
                print('dar um look nas transações')
            transaction_installments = order['transactions']['data'][0]['installments']
            transaction_value = order['transactions']['data'][0]['amount']

            coupon_code = order['promocode']['data']['code'] if len(order['promocode']['data'])>0 else None
            coupon_value = order['value_discount']
            #coupon_percentage = order['promocode']['data']['price_products']

    return response_decoded


first_date = pd.to_datetime('2021-02-01', format = "%Y-%m-%d")
yesterday = datetime.now() - timedelta(1)

week_ranges = []

# Calculate the week ranges
current_start_date = first_date
while current_start_date <= yesterday:
    current_end_date = current_start_date + timedelta(days=6)
    if current_end_date > yesterday:
        current_end_date = yesterday
    week_ranges.append((current_start_date, current_end_date))
    current_start_date = current_end_date + timedelta(days=1)

count = 0
for i in week_ranges:
    response = backpopulate_yampi_data(i[0], i[1])
    print(f"{count/len(week_ranges)}% done")
    count += 1
