from flask import Flask
from dash import Dash

def create_app():
    app = Flask(__name__)
    with app.app_context():
        from backend.app.routes import blueprint as app_routes
        app.register_blueprint(app_routes)
    return app

def create_dashboard(server):
    dash_app = Dash(__name__, server=server, url_base_pathname='/dashboard/')
    return dash_app
