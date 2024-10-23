from flask import Flask

def create_app():
    app = Flask(__name__)

    # Load configuration
    app.config.from_object('app.config.Config')

   # Register blueprints
    from app.routes import main_routes
    app.register_blueprint(main_routes)

    return app