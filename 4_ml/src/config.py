from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
from urllib.parse import quote_plus

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ML_ROOT = PROJECT_ROOT / "4_ml"
NOTEBOOK_DIR = ML_ROOT / "notebooks"
OUTPUT_DIR = ML_ROOT / "outputs"
MODEL_DIR = ML_ROOT / "models"

for directory in (OUTPUT_DIR, MODEL_DIR):
    directory.mkdir(parents=True, exist_ok=True)

RANDOM_STATE = int(os.getenv("ATB_RANDOM_STATE", "42"))
RISK_TARGET_THRESHOLD = float(os.getenv("ATB_RISK_TARGET_THRESHOLD", "50"))
MLFLOW_EXPERIMENT_NAME = os.getenv("ATB_MLFLOW_EXPERIMENT", "atb_bi_customer_risk")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", f"file:///{(ML_ROOT / 'mlruns').as_posix()}")


@dataclass(frozen=True)
class WarehouseConnection:
    server: str = os.getenv("ATB_SQL_SERVER", "localhost")
    database: str = os.getenv("ATB_SQL_DATABASE", "ATB_BI")
    username: str = os.getenv("ATB_SQL_USERNAME", "airbyte_user")
    password: str = os.getenv("ATB_SQL_PASSWORD", "")
    driver: str = os.getenv("ATB_SQL_DRIVER", "ODBC Driver 17 for SQL Server")
    port: str = os.getenv("ATB_SQL_PORT", "1434")
    schema: str = os.getenv("ATB_DWH_SCHEMA", "PFE_DWH")


def get_connection() -> WarehouseConnection:
    return WarehouseConnection()


def build_sqlalchemy_uri(connection: WarehouseConnection | None = None) -> str:
    connection = connection or get_connection()
    password_part = quote_plus(connection.password)
    driver_part = quote_plus(connection.driver)
    return (
        f"mssql+pyodbc://{connection.username}:{password_part}"
        f"@{connection.server}:{connection.port}/{connection.database}"
        f"?driver={driver_part}"
    )
