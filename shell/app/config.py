import os


DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://harbor_app@localhost/harbor"
)
