from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ML_ROOT = PROJECT_ROOT / "4_ml"
NOTEBOOK_DIR = ML_ROOT / "notebooks"
OUTPUT_DIR = ML_ROOT / "outputs"
MODEL_DIR = ML_ROOT / "models"

for directory in (OUTPUT_DIR, MODEL_DIR):
    directory.mkdir(parents=True, exist_ok=True)


def _load_env_files() -> None:
    """Load SQL credentials from the same .env files as the Streamlit dashboard."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for path in (
        PROJECT_ROOT / "5_reporting" / "streamlit" / ".env",
        ML_ROOT / ".env",
        PROJECT_ROOT / ".env",
    ):
        if path.exists():
            load_dotenv(path, override=False)


_load_env_files()

RANDOM_STATE = int(os.getenv("ATB_RANDOM_STATE", "42"))
RISK_TARGET_THRESHOLD = float(os.getenv("ATB_RISK_TARGET_THRESHOLD", "50"))
MLFLOW_EXPERIMENT_NAME = os.getenv("ATB_MLFLOW_EXPERIMENT", "atb_bi_customer_risk")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", f"file:///{(ML_ROOT / 'mlruns').as_posix()}")


@dataclass(frozen=True)
class WarehouseConnection:
    server: str
    database: str
    username: str
    password: str
    driver: str
    port: str
    schema: str
    ml_schema: str


def get_connection() -> WarehouseConnection:
    return WarehouseConnection(
        server=os.getenv("ATB_SQL_SERVER", "DESKTOP-B0PDEI7"),
        database=os.getenv("ATB_SQL_DATABASE", "ATB_BI"),
        username=os.getenv("ATB_SQL_USERNAME", "airbyte_user"),
        password=os.getenv("ATB_SQL_PASSWORD", ""),
        driver=os.getenv("ATB_SQL_DRIVER", "ODBC Driver 17 for SQL Server"),
        port=os.getenv("ATB_SQL_PORT", "1434"),
        schema=os.getenv("ATB_DWH_SCHEMA", "PFE_DWH"),
        ml_schema=os.getenv("ATB_ML_SCHEMA", "PFE_ML"),
    )


def build_sqlalchemy_uri(connection: WarehouseConnection | None = None) -> str:
    connection = connection or get_connection()
    if not connection.password:
        raise ValueError(
            "ATB_SQL_PASSWORD is not set. Copy 5_reporting/streamlit/.env "
            "or set ATB_SQL_PASSWORD before running ML scripts."
        )
    password_part = quote_plus(connection.password)
    driver_part = quote_plus(connection.driver)
    return (
        f"mssql+pyodbc://{connection.username}:{password_part}"
        f"@{connection.server}:{connection.port}/{connection.database}"
        f"?driver={driver_part}&TrustServerCertificate=yes&Encrypt=no"
    )
