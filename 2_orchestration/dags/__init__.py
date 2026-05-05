"""
ATB BI Project - Airflow DAGs
"""

__version__ = "1.0.0"
__author__ = "ATB BI Team"

# DAG imports for Airflow to discover
from . import dag_ingestion
from . import dag_transformation
from . import dag_ml_pipeline
from . import dag_full_pipeline

__all__ = [
    "dag_ingestion",
    "dag_transformation",
    "dag_ml_pipeline",
    "dag_full_pipeline",
]
