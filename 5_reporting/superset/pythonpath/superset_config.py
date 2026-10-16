from os import environ

SQLALCHEMY_DATABASE_URI = environ.get(
    "SUPERSET_DATABASE_URI",
    "sqlite:////app/superset_home/superset.db",
)
SECRET_KEY = environ.get("SUPERSET_SECRET_KEY", "change-me-in-production")
ENABLE_PROXY_FIX = True
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = True
FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": False,
}
