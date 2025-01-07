from backend import create_app, create_dashboard
from backend.services.database_service import DatabaseService

app = create_app()
dash_app = create_dashboard(app)

# Dash app layout
dash_app.layout = dash_app.layout = html.Div([
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

# Dash app callbacks
@dash_app.callback(
    [Output('metrics', 'children'),
     Output('top-products-chart', 'figure')],
    [Input('filter-button', 'n_clicks')],
    [Input('date-picker', 'start_date'),
     Input('date-picker', 'end_date')]
)
def update_dashboard(n_clicks, start_date, end_date):
    data = DatabaseService.fetch_data(start_date, end_date)
    if data.empty:
        return html.Div("No data found for the selected range."), {}

    total_sales = data['quantity'].sum()
    total_revenue = data['total_price'].sum()
    top_products = data.groupby('product_name').sum().sort_values(by='quantity', ascending=False).head(5)

    metrics = html.Div([
        html.H3(f"Total Sales: {total_sales}"),
        html.H3(f"Total Revenue: ${total_revenue:,.2f}")
    ])

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

if __name__ == "__main__":
    app.run(debug=True)
