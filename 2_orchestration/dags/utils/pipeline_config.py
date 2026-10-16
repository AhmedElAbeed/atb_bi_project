"""Shared pipeline configuration for ATB BI Airflow DAGs."""
import os

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt")
DBT_TARGET = os.getenv("DBT_TARGET", "prod")

PROJECT_ROOT = os.getenv("ATB_PROJECT_ROOT", "/opt/airflow/atb_project")
ML_SRC_DIR = os.path.join(PROJECT_ROOT, "4_ml", "src")
ML_MODEL_DIR = os.path.join(PROJECT_ROOT, "4_ml", "models")

SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST", "host.docker.internal")
SQL_SERVER_PORT = os.getenv("SQL_SERVER_PORT", "1434")
SQL_SERVER_DATABASE = os.getenv("SQL_SERVER_DATABASE", "ATB_BI")
SQL_SERVER_USER = os.getenv("SQL_SERVER_USER", "airbyte_user")
SQL_SERVER_PASSWORD = os.getenv("SQL_SERVER_PASSWORD", "AZERTY123")

AIRBYTE_URL = os.getenv("AIRBYTE_URL", "http://host.docker.internal:8006")
AIRBYTE_SKIP = os.getenv("AIRBYTE_SKIP", "true").lower() in ("1", "true", "yes")

DBT_ENV_PREFIX = (
    f"cd {DBT_PROJECT_DIR} && "
    f"export DBT_PROFILES_DIR={DBT_PROFILES_DIR} && "
    f"export DBT_TARGET={DBT_TARGET} && "
)

ML_ENV_PREFIX = (
    f"export PYTHONPATH={ML_SRC_DIR} && "
    f"export ATB_SQL_SERVER={SQL_SERVER_HOST} && "
    f"export ATB_SQL_PORT={SQL_SERVER_PORT} && "
    f"export ATB_SQL_DATABASE={SQL_SERVER_DATABASE} && "
    f"export ATB_SQL_USERNAME={SQL_SERVER_USER} && "
    f"export ATB_SQL_PASSWORD={SQL_SERVER_PASSWORD} && "
)

DEFAULT_DAG_ARGS = {
    "owner": "ATB BI Team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}
