from flask import Flask, jsonify

from harbor_common.db import close_db
from harbor_common.errors import HarborError


def create_app():
    app = Flask(__name__)
    app.config.from_pyfile("config.py")

    app.teardown_appcontext(close_db)

    from app.routes import bp
    from app.activation_routes import activation_bp
    from app.charles_routes import charles_bp

    app.register_blueprint(bp)
    app.register_blueprint(activation_bp)
    app.register_blueprint(charles_bp)

    @app.errorhandler(HarborError)
    def handle_harbor_error(e):
        return jsonify(e.to_dict()), e.status_code

    return app
