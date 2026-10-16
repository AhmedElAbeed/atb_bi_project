"""
DAG for ML pipeline — trains XGBoost risk model from PFE_DWH features.
Triggered manually or by atb_master_pipeline (weekly).
"""
from datetime import datetime, timedelta
import logging
import os
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.pipeline_config import DEFAULT_DAG_ARGS, ML_ENV_PREFIX, ML_SRC_DIR

logger = logging.getLogger(__name__)

default_args = {
    **DEFAULT_DAG_ARGS,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dag_ml_pipeline",
    default_args=default_args,
    description="ML DAG: train XGBoost customer risk model",
    schedule_interval=None,
    catchup=False,
    tags=["ATB", "ml", "prediction"],
)


def log_training_summary(**context) -> None:
    model_path = os.path.join(
        os.getenv("ATB_PROJECT_ROOT", "/opt/airflow/atb_project"),
        "4_ml",
        "models",
        "xgboost_best.pkl",
    )
    exists = os.path.isfile(model_path)
    size_kb = os.path.getsize(model_path) // 1024 if exists else 0
    logger.info("Model artifact: %s (exists=%s, size=%s KB)", model_path, exists, size_kb)
    if not exists:
        raise FileNotFoundError(f"Training did not produce model at {model_path}")


train_model = BashOperator(
    task_id="run_model_training",
    bash_command=(
        f"{ML_ENV_PREFIX} "
        f"cd {ML_SRC_DIR} && "
        "python run_training.py"
    ),
    dag=dag,
)

validate_artifact = PythonOperator(
    task_id="validate_model_artifact",
    python_callable=log_training_summary,
    dag=dag,
)

write_predictions = BashOperator(
    task_id="write_predictions_to_dwh",
    bash_command=(
        f"{ML_ENV_PREFIX} "
        f"cd {ML_SRC_DIR} && "
        "python run_predictions.py"
    ),
    dag=dag,
)

train_model >> validate_artifact >> write_predictions
