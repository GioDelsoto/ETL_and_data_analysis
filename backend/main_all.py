from flask import Flask, request, render_template
from dash import Dash, html, dcc, Input, Output
import pandas as pd
from sqlalchemy import create_engine

# Flask app
flask_app = Flask(__name__)

# Connect to your database (update with your database credentials)
DATABASE_URI = 'postgresql://username:password@localhost:5432/your_database'
engine = create_engine(DATABASE_URI)

# Dash app
dash_app = Dash(__name__, server=flask_app, url_base_pathname='/dashboard/')

# Function to fetch data from the database
def fetch_data(start_date=None, end_date=None):
    query = "SELECT order_date, product_name, quantity, total_price FROM orders"
    if start_date and end_date:
        query += f" WHERE order_date BETWEEN '{start_date}' AND '{end_date}'"
    return pd.read_sql(query, con=engine)

# Layout for Dash
dash_app.layout = html.Div([
    html.H1("Sales Dashboard", style={'textAlign': 'center'}),

    dcc.DatePickerRange(
        id='date-picker',
        start_date_placeholder_text="Start Date",
        end_date_placeholder_text="End Date",
        style={'margin': '10px'}
    ),
    html.Button("Filter", id='filter-button', style={'margin': '10px'}),

    html.Div(id='metrics'),
    dcc.Graph(id='top-products-chart')
])

# Dash callback to update dashboard
@dash_app.callback(
    [Output('metrics', 'children'),
     Output('top-products-chart', 'figure')],
    [Input('filter-button', 'n_clicks')],
    [Input('date-picker', 'start_date'),
     Input('date-picker', 'end_date')]
)
def update_dashboard(n_clicks, start_date, end_date):
    data = fetch_data(start_date, end_date)
    if data.empty:
        return html.Div("No data found for the selected range."), {}

    # Metrics
    total_sales = data['quantity'].sum()
    total_revenue = data['total_price'].sum()
    top_products = data.groupby('product_name').sum().sort_values(by='quantity', ascending=False).head(5)

    metrics = html.Div([
        html.H3(f"Total Sales: {total_sales}"),
        html.H3(f"Total Revenue: ${total_revenue:,.2f}")
    ])

    # Top products chart
    figure = {
        'data': [
            {
                'x': top_products.index,
                'y': top_products['quantity'],
                'type': 'bar',
                'name': 'Top Products'
            }
        ],
        'layout': {
            'title': 'Top-Selling Products',
            'xaxis': {'title': 'Products'},
            'yaxis': {'title': 'Quantity Sold'}
        }
    }

    return metrics, figure

# Flask route for the main page
@flask_app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    flask_app.run(debug=True)
