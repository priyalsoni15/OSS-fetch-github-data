from flask import Flask
from flask_cors import CORS

def create_app():
    app = Flask(__name__)
    
    # Enable CORS
    CORS(app, resources={r"*": {"origins": "*"}})
    
    # Load configuration
    app.config.from_object('app.config.Config')

   # Register blueprints
    from app.routes import main_routes
    app.register_blueprint(main_routes)

    return app