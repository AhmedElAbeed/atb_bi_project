"""Model deployment and registry management for ATB BI ML pipeline."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import joblib
import mlflow
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ModelMetadata:
    """Model metadata for registry and deployment."""
    name: str
    version: str
    model_type: str
    training_timestamp: str
    metrics: Dict[str, float]
    threshold: float
    feature_columns: list[str]
    numeric_features: list[str]
    categorical_features: list[str]
    mlflow_run_id: Optional[str] = None
    mlflow_model_uri: Optional[str] = None
    status: str = "ready"  # ready, deployed, archived


def save_model_metadata(
    metadata: ModelMetadata,
    output_dir: Path
) -> Path:
    """Save model metadata to JSON file."""
    metadata_dict = {
        "name": metadata.name,
        "version": metadata.version,
        "model_type": metadata.model_type,
        "training_timestamp": metadata.training_timestamp,
        "metrics": metadata.metrics,
        "threshold": metadata.threshold,
        "feature_columns": metadata.feature_columns,
        "numeric_features": metadata.numeric_features,
        "categorical_features": metadata.categorical_features,
        "mlflow_run_id": metadata.mlflow_run_id,
        "mlflow_model_uri": metadata.mlflow_model_uri,
        "status": metadata.status,
    }
    
    metadata_path = output_dir / f"{metadata.name}_metadata_v{metadata.version}.json"
    metadata_path.write_text(json.dumps(metadata_dict, indent=2))
    logger.info(f"Saved metadata to {metadata_path}")
    
    return metadata_path


def load_model_artifact(
    model_path: Path,
    artifact_type: str = "pickle"
) -> Dict[str, Any]:
    """Load model artifact from disk."""
    if artifact_type == "pickle":
        return joblib.load(model_path)
    elif artifact_type == "joblib":
        return joblib.load(model_path)
    else:
        raise ValueError(f"Unsupported artifact type: {artifact_type}")


def export_model_for_deployment(
    model: Any,
    metadata: ModelMetadata,
    export_dir: Path,
    include_preprocessor: bool = True
) -> Dict[str, Path]:
    """Export model with all necessary artifacts for deployment."""
    export_dir.mkdir(parents=True, exist_ok=True)
    
    artifacts = {}
    
    # Save model
    model_path = export_dir / f"{metadata.name}_model_v{metadata.version}.pkl"
    joblib.dump(model, model_path)
    artifacts["model"] = model_path
    logger.info(f"Exported model to {model_path}")
    
    # Save metadata
    metadata_path = save_model_metadata(metadata, export_dir)
    artifacts["metadata"] = metadata_path
    
    # Save inference config
    inference_config = {
        "model_type": metadata.model_type,
        "threshold": metadata.threshold,
        "feature_columns": metadata.feature_columns,
        "version": metadata.version,
        "timestamp": datetime.now().isoformat()
    }
    config_path = export_dir / f"{metadata.name}_inference_config_v{metadata.version}.json"
    config_path.write_text(json.dumps(inference_config, indent=2))
    artifacts["config"] = config_path
    logger.info(f"Exported inference config to {config_path}")
    
    return artifacts


def deploy_model_to_registry(
    model: Any,
    metadata: ModelMetadata,
    registry_name: str = "atb_risk_classifier"
) -> Dict[str, Any]:
    """Deploy model to MLflow Model Registry."""
    try:
        # Set MLflow tracking
        mlflow.set_experiment("atb_bi_customer_risk")
        
        with mlflow.start_run() as run:
            # Log model parameters
            mlflow.log_params({
                "model_type": metadata.model_type,
                "threshold": metadata.threshold,
                "n_features": len(metadata.feature_columns)
            })
            
            # Log metrics
            mlflow.log_metrics(metadata.metrics)
            
            # Log model
            mlflow.sklearn.log_model(
                model,
                artifact_path="model",
                registered_model_name=registry_name
            )
            
            run_id = run.info.run_id
            
            logger.info(f"Registered model {registry_name} with run_id {run_id}")
            
            return {
                "run_id": run_id,
                "model_name": registry_name,
                "status": "registered",
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        logger.warning(f"MLflow registration failed (non-critical): {e}")
        return {
            "run_id": None,
            "model_name": registry_name,
            "status": "warning",
            "error": str(e)
        }


def create_model_serving_endpoint(
    model: Any,
    metadata: ModelMetadata,
    output_dir: Path
) -> Path:
    """Create a simple Python module for serving the model."""
    serving_code = f'''"""
Model Serving Module - Auto-generated for {metadata.name} v{metadata.version}

This module provides inference capabilities for the trained model.
"""

import json
import pickle
import sys
from pathlib import Path
from typing import List, Dict, Any, Union

import numpy as np
import pandas as pd


class ModelServer:
    """Serve predictions from trained model."""
    
    def __init__(self, model_path: str, threshold: float = {metadata.threshold}):
        """Load model and configuration."""
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model = model_data.get('model')
        self.threshold = threshold
        self.feature_columns = {json.dumps(metadata.feature_columns)}
    
    def predict_proba(self, features: Union[pd.DataFrame, np.ndarray]) -> np.ndarray:
        """Get probability predictions."""
        return self.model.predict_proba(features)
    
    def predict(self, features: Union[pd.DataFrame, np.ndarray]) -> np.ndarray:
        """Get binary predictions using threshold."""
        proba = self.predict_proba(features)
        return (proba[:, 1] >= self.threshold).astype(int)
    
    def predict_with_confidence(
        self, 
        features: Union[pd.DataFrame, np.ndarray]
    ) -> Dict[str, Any]:
        """Get predictions with confidence scores."""
        proba = self.predict_proba(features)
        predictions = (proba[:, 1] >= self.threshold).astype(int)
        confidence = np.abs(proba[:, 1] - 0.5) * 2  # 0-1 confidence scale
        
        return {{
            "predictions": predictions.tolist(),
            "probabilities": proba[:, 1].tolist(),
            "confidence": confidence.tolist(),
            "threshold": self.threshold
        }}
    
    def batch_predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """Predict on a batch of records."""
        X = df[self.feature_columns]
        results = self.predict_with_confidence(X)
        
        output_df = df.copy()
        output_df['risk_prediction'] = results['predictions']
        output_df['risk_probability'] = results['probabilities']
        output_df['prediction_confidence'] = results['confidence']
        
        return output_df


def main():
    """Example usage of model server."""
    if len(sys.argv) < 2:
        print("Usage: python model_server.py <model_path> <input_csv> [output_csv]")
        sys.exit(1)
    
    model_path = sys.argv[1]
    input_csv = sys.argv[2]
    output_csv = sys.argv[3] if len(sys.argv) > 3 else "predictions.csv"
    
    # Load model
    server = ModelServer(model_path)
    
    # Load data
    df = pd.read_csv(input_csv)
    
    # Predict
    results = server.batch_predict(df)
    
    # Save results
    results.to_csv(output_csv, index=False)
    print(f"Saved predictions to {{output_csv}}")


if __name__ == "__main__":
    main()
'''
    
    serving_path = output_dir / f"{metadata.name}_server.py"
    serving_path.write_text(serving_code)
    logger.info(f"Created model serving module at {serving_path}")
    
    return serving_path


def create_batch_scoring_job(
    model: Any,
    metadata: ModelMetadata,
    feature_df: pd.DataFrame,
    output_dir: Path
) -> Dict[str, Any]:
    """Create a batch scoring job to predict on all features."""
    logger.info(f"Creating batch scoring predictions for {len(feature_df)} records...")
    
    try:
        # Get predictions
        X = feature_df[metadata.feature_columns]
        proba = model.predict_proba(X)
        predictions = (proba[:, 1] >= metadata.threshold).astype(int)
        
        # Create output dataframe
        scores_df = pd.DataFrame({
            'fact_customer_risk_sk': feature_df.get('fact_customer_risk_sk', range(len(feature_df))),
            'customer_id': feature_df.get('customer_id'),
            'risk_probability': proba[:, 1],
            'risk_prediction': predictions,
            'confidence': np.abs(proba[:, 1] - 0.5) * 2,
            'model_version': metadata.version,
            'model_type': metadata.model_type,
            'prediction_timestamp': pd.Timestamp.now(),
            'threshold': metadata.threshold
        })
        
        # Save outputs
        output_path = output_dir / f"batch_scores_{metadata.version}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        scores_df.to_csv(output_path, index=False)
        
        # Statistics
        high_risk_count = (scores_df['risk_prediction'] == 1).sum()
        logger.info(
            f"Batch scoring complete: {len(scores_df)} total, "
            f"{high_risk_count} high-risk ({100*high_risk_count/len(scores_df):.1f}%)"
        )
        
        return {
            "status": "success",
            "total_scored": len(scores_df),
            "high_risk_count": int(high_risk_count),
            "high_risk_pct": float(100 * high_risk_count / len(scores_df)),
            "output_path": str(output_path),
            "scores_df": scores_df
        }
    except Exception as e:
        logger.error(f"Batch scoring failed: {e}")
        raise
