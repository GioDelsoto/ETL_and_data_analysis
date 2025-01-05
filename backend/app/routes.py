from flask import Blueprint, render_template
from backend.services.database_service import DatabaseService

blueprint = Blueprint('routes', __name__)

@blueprint.route('/')
def index():
    return render_template('index.html')
