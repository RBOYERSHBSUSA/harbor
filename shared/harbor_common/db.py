import os

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import current_app, g


def get_db():
    if "db" not in g:
        db_url = current_app.config.get(
            "DATABASE_URL",
            os.environ.get("DATABASE_URL", "postgresql://harbor_app@localhost/harbor"),
        )
        g.db = psycopg2.connect(
            db_url,
            cursor_factory=RealDictCursor,
            options="-c search_path=harbor,public",
        )
    return g.db


def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()
