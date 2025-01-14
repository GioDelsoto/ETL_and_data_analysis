import pandas as pd
from backend.config import engine

class DatabaseService:
    @staticmethod
    def fetch_data(start_date=None, end_date=None):
        query = "SELECT order_date, product_name, quantity, total_price FROM orders"
        if start_date and end_date:
            query += f" WHERE order_date BETWEEN '{start_date}' AND '{end_date}'"
        return pd.read_sql(query, con=engine)
