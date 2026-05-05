"""
DAG for ML pipeline layer - feature engineering, model training, prediction, and explainability
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import logging
import sys
import os

logger = logging.getLogger(__name__)

# ML source path (mounted in container)
ML_SRC_DIR = "/opt/airflow/ml_src"
sys.path.insert(0, ML_SRC_DIR)

# Default DAG arguments
default_args = {
    "owner": "ATB BI Team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["admin@atb.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "dag_ml_pipeline",
    default_args=default_args,
    description="ML DAG: feature engineering, training, evaluation, and prediction",
    schedule_interval=None,  # Triggered by full pipeline DAG
    catchup=False,
    tags=["ATB", "ml", "prediction"],
)


def run_feature_extraction(**context) -> dict:
    """
    Extract and engineer features from PFE_DWH
    
    This task:
    - Queries FAIT_CUSTOMER_RISK and dimensions from PFE_DWH
    - Constructs feature matrix: compliance, fragility, behavioral features
    - Handles missing values and outliers
    - Exits feature set as CSV or Parquet
    
    Returns: dict with feature matrix path and shape
    """
    logger.info("Starting feature extraction...")
    
    try:
        # TODO: Import actual feature extraction module when ML layer is ready
        # from ml_src.features import extract_features
        # features_df, metadata = extract_features()
        
        logger.warning("Feature extraction is a placeholder - implement ML layer")
        
        # Placeholder return
        return {
            "feature_matrix_path": "/opt/airflow/ml_outputs/features.parquet",
            "n_rows": 0,
            "n_features": 0,
            "status": "placeholder"
        }
    except Exception as e:
        logger.error(f"Feature extraction failed: {e}")
        raise


def run_model_training(**context) -> dict:
    """
    Train ML models (Random Forest baseline + XGBoost primary)
    
    This task:
    - Loads feature matrix from feature extraction task
    - Handles class imbalance (SMOTETomek)
    - Trains Random Forest (baseline) and XGBoost (primary)
    - Performs hyperparameter tuning
    - Serializes models (pickle)
    
    Returns: dict with model paths and cross-validation scores
    """
    logger.info("Starting model training...")
    
    try:
        # TODO: Import actual training module when ML layer is ready
        # from ml_src.train import train_models
        # models, cv_scores = train_models()
        
        logger.warning("Model training is a placeholder - implement ML layer")
        
        # Placeholder return
        return {
            "baseline_model_path": "/opt/airflow/ml_outputs/models/rf_baseline.pkl",
            "primary_model_path": "/opt/airflow/ml_outputs/models/xgb_primary.pkl",
            "cv_accuracy": 0.0,
            "cv_f1": 0.0,
            "cv_auc_roc": 0.0,
            "status": "placeholder"
        }
    except Exception as e:
        logger.error(f"Model training failed: {e}")
        raise


def evaluate_model_performance(**context) -> dict:
    """
    Evaluate models on test set and compute metrics
    
    This task:
    - Loads trained models and test set
    - Evaluates: Accuracy, F1, AUC-ROC, Brier Score, Precision, Recall
    - Saves evaluation report as JSON
    - Logs confusion matrix and classification report
    
    Returns: dict with test metrics
    """
    logger.info("Starting model evaluation...")
    
    try:
        # TODO: Import actual evaluation module when ML layer is ready
        # from ml_src.evaluate import evaluate_models
        # metrics = evaluate_models()
        
        logger.warning("Model evaluation is a placeholder - implement ML layer")
        
        # Placeholder return
        return {
            "test_accuracy": 0.0,
            "test_f1": 0.0,
            "test_auc_roc": 0.0,
            "test_brier_score": 0.0,
            "evaluation_report_path": "/opt/airflow/ml_outputs/evaluation_report.json",
            "status": "placeholder"
        }
    except Exception as e:
        logger.error(f"Model evaluation failed: {e}")
        raise


def compute_shap_explainability(**context) -> dict:
    """
    Compute SHAP values for global and local explainability
    
    This task:
    - Loads trained model and test set
    - Computes SHAP values (TreeExplainer for gradient boosting)
    - Generates global importance plots (bar, beeswarm)
    - Selects high-risk customers and computes local SHAP explanations
    - Saves outputs (PNG/HTML)
    
    Returns: dict with explainability output paths
    """
    logger.info("Starting SHAP explainability analysis...")
    
    try:
        # TODO: Import actual explainability module when ML layer is ready
        # from ml_src.explain import compute_shap_values
        # shap_data = compute_shap_values()
        
        logger.warning("SHAP analysis is a placeholder - implement ML layer")
        
        # Placeholder return
        return {
            "global_importance_plot": "/opt/airflow/ml_outputs/shap_global_importance.png",
            "shap_values_path": "/opt/airflow/ml_outputs/shap_values.pkl",
            "local_explanations_path": "/opt/airflow/ml_outputs/shap_local_explanations.html",
            "status": "placeholder"
        }
    except Exception as e:
        logger.error(f"SHAP explainability computation failed: {e}")
        raise


def write_predictions_to_dwh(**context) -> dict:
    """
    Generate predictions on full dataset and write to PFE_ML.ML_CUSTOMER_PREDICTIONS
    
    This task:
    - Loads trained model and full feature matrix
    - Generates predictions and probability scores
    - Maps predictions back to CUSTOMER_ID
    - Writes to PFE_ML.ML_CUSTOMER_PREDICTIONS with columns:
        - CUSTOMER_ID
        - SCORE_PROBA
        - PREDICTION_LABEL
        - MODEL_VERSION
        - PREDICTION_DATE
    - Tags high-risk customers for review
    
    Returns: dict with prediction statistics
    """
    logger.info("Starting prediction writing to database...")
    
    try:
        # TODO: Import actual prediction write module when ML layer is ready
        # from ml_src.predict import write_predictions
        # stats = write_predictions()
        
        logger.warning("Prediction writing is a placeholder - implement ML layer")
        
        # Placeholder return
        return {
            "total_predictions": 0,
            "high_risk_count": 0,
            "records_written": 0,
            "table_name": "PFE_ML.ML_CUSTOMER_PREDICTIONS",
            "status": "placeholder"
        }
    except Exception as e:
        logger.error(f"Prediction writing failed: {e}")
        raise


def log_ml_pipeline_summary(**context) -> None:
    """Log a summary of the ML pipeline results"""
    task_instance = context["task_instance"]
    
    logger.info("\n" + "="*80)
    logger.info("ML PIPELINE EXECUTION SUMMARY")
    logger.info("="*80)
    
    try:
        feature_output = task_instance.xcom_pull(task_ids="run_feature_extraction")
        logger.info(f"Feature Extraction: {feature_output}")
    except Exception as e:
        logger.warning(f"Could not retrieve feature extraction output: {e}")
    
    try:
        train_output = task_instance.xcom_pull(task_ids="run_model_training")
        logger.info(f"Model Training: {train_output}")
    except Exception as e:
        logger.warning(f"Could not retrieve training output: {e}")
    
    try:
        eval_output = task_instance.xcom_pull(task_ids="evaluate_model_performance")
        logger.info(f"Model Evaluation: {eval_output}")
    except Exception as e:
        logger.warning(f"Could not retrieve evaluation output: {e}")
    
    try:
        shap_output = task_instance.xcom_pull(task_ids="compute_shap_explainability")
        logger.info(f"SHAP Explainability: {shap_output}")
    except Exception as e:
        logger.warning(f"Could not retrieve SHAP output: {e}")
    
    try:
        pred_output = task_instance.xcom_pull(task_ids="write_predictions_to_dwh")
        logger.info(f"Prediction Writing: {pred_output}")
    except Exception as e:
        logger.warning(f"Could not retrieve prediction output: {e}")
    
    logger.info("="*80 + "\n")


# Feature engineering task
feature_task = PythonOperator(
    task_id="run_feature_extraction",
    python_callable=run_feature_extraction,
    dag=dag,
)

# Model training and evaluation task group
with TaskGroup("model_training", tooltip="Train and evaluate ML models", dag=dag) as training_group:
    
    train_task = PythonOperator(
        task_id="run_model_training",
        python_callable=run_model_training,
        dag=dag,
    )
    
    eval_task = PythonOperator(
        task_id="evaluate_model_performance",
        python_callable=evaluate_model_performance,
        dag=dag,
    )
    
    train_task >> eval_task

# Explainability task
shap_task = PythonOperator(
    task_id="compute_shap_explainability",
    python_callable=compute_shap_explainability,
    dag=dag,
)

# Prediction writing task
pred_task = PythonOperator(
    task_id="write_predictions_to_dwh",
    python_callable=write_predictions_to_dwh,
    dag=dag,
)

# Summary logging task
summary_task = PythonOperator(
    task_id="log_pipeline_summary",
    python_callable=log_ml_pipeline_summary,
    dag=dag,
)

# Task dependencies
feature_task >> training_group >> [shap_task, pred_task] >> summary_task

if __name__ == "__main__":
    dag.cli()
