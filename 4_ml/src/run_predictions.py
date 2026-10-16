"""Batch-score customers and write predictions to PFE_ML.ml_customer_predictions."""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "4_ml" / "src"
sys.path.insert(0, str(SRC))

from config import MODEL_DIR, OUTPUT_DIR, get_connection  # noqa: E402
from data_access import load_feature_frame, make_engine, test_sql_connection  # noqa: E402
from features import engineer_features, select_feature_columns  # noqa: E402


def ensure_ml_tables(engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}') "
                f"EXEC('CREATE SCHEMA [{schema}]')"
            )
        )
        conn.execute(
            text(
                f"""
                IF OBJECT_ID('{schema}.ml_customer_predictions', 'U') IS NULL
                BEGIN
                    CREATE TABLE [{schema}].[ml_customer_predictions] (
                        prediction_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                        customer_id BIGINT NOT NULL,
                        customer_sk BIGINT NULL,
                        risk_probability FLOAT NOT NULL,
                        risk_prediction INT NOT NULL,
                        rules_risk_tier NVARCHAR(20) NULL,
                        rules_global_score FLOAT NULL,
                        disagreement_flag INT NOT NULL DEFAULT 0,
                        is_pep INT NULL,
                        total_working_balance FLOAT NULL,
                        dao_area NVARCHAR(100) NULL,
                        model_name NVARCHAR(100) NOT NULL,
                        model_version NVARCHAR(50) NOT NULL,
                        prediction_threshold FLOAT NOT NULL,
                        prediction_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                        scoring_date DATE NULL
                    );
                    CREATE INDEX IX_ml_predictions_customer
                        ON [{schema}].[ml_customer_predictions](customer_id);
                    CREATE INDEX IX_ml_predictions_disagreement
                        ON [{schema}].[ml_customer_predictions](disagreement_flag);
                END
                """
            )
        )


def load_best_model(summary: dict) -> tuple[object, str, float]:
    best_name = summary["best_model"]
    entry = summary["results"][best_name]
    threshold = float(entry.get("threshold", entry.get("metrics", {}).get("threshold", 0.5)))

    artifact = entry.get("artifact")
    candidates = [
        Path(artifact) if artifact else None,
        MODEL_DIR / f"{best_name}_best.pkl",
        MODEL_DIR / f"{best_name}_best_model.joblib",
    ]
    model_path = next((p for p in candidates if p and p.exists()), None)
    if model_path is None:
        raise FileNotFoundError(f"No model artifact for {best_name}")

    return joblib.load(model_path), best_name, threshold


def build_predictions_frame(
    model,
    frame: pd.DataFrame,
    model_name: str,
    model_version: str,
    threshold: float,
) -> pd.DataFrame:
    engineered = engineer_features(frame.copy())
    feature_columns, _, _ = select_feature_columns(engineered)
    feature_columns = list(dict.fromkeys(feature_columns))
    for col in feature_columns:
        if col not in engineered.columns:
            engineered[col] = np.nan

    X = engineered[feature_columns]
    proba = model.predict_proba(X)[:, 1]
    pred = (proba >= threshold).astype(int)

    low_tiers = {"LOW", "MEDIUM", "UNKNOWN"}
    rules_tier = frame.get("risk_tier", pd.Series(dtype=str)).astype(str).str.upper()
    disagreement = (pred == 1) & rules_tier.isin(low_tiers)

    return pd.DataFrame({
        "customer_id": frame.get("customer_id"),
        "customer_sk": frame.get("customer_sk"),
        "risk_probability": proba,
        "risk_prediction": pred,
        "rules_risk_tier": frame.get("risk_tier"),
        "rules_global_score": frame.get("global_risk_score"),
        "disagreement_flag": disagreement.astype(int),
        "is_pep": frame.get("is_pep"),
        "total_working_balance": frame.get("total_working_balance"),
        "dao_area": frame.get("dao_area"),
        "model_name": model_name,
        "model_version": model_version,
        "prediction_threshold": threshold,
        "prediction_timestamp": datetime.utcnow(),
        "scoring_date": pd.to_datetime(frame.get("scoring_date"), errors="coerce").dt.date,
    })


def write_predictions(engine, schema: str, predictions: pd.DataFrame) -> int:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE [{schema}].[ml_customer_predictions]"))
    predictions.to_sql(
        "ml_customer_predictions",
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=2000,
    )
    return len(predictions)


def export_predictions_csv(predictions: pd.DataFrame) -> Path:
    out = OUTPUT_DIR / "ml_customer_predictions.csv"
    predictions.to_csv(out, index=False)
    return out


def main() -> None:
    summary_path = ROOT / "4_ml" / "outputs" / "training_summary.json"
    if not summary_path.exists():
        raise SystemExit(f"Missing training summary: {summary_path}. Run run_training.py first.")

    conn = get_connection()
    if not conn.password:
        raise SystemExit(
            "ATB_SQL_PASSWORD is missing. Ensure 5_reporting/streamlit/.env exists "
            "with your SQL Server credentials (same as the dashboard)."
        )
    print(f"SQL target: {conn.server}:{conn.port}/{conn.database} user={conn.username}")
    test_sql_connection()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    registry = summary.get("registry") or {}
    model_version = str(registry.get("version", "1"))

    print("Loading feature frame from warehouse...")
    frame = load_feature_frame()
    print(f"rows={len(frame)}")

    model, model_name, threshold = load_best_model(summary)
    print(f"model={model_name} threshold={threshold}")

    predictions = build_predictions_frame(model, frame, model_name, model_version, threshold)
    print(
        f"high_risk={int(predictions['risk_prediction'].sum())} "
        f"disagreements={int(predictions['disagreement_flag'].sum())}"
    )

    csv_path = export_predictions_csv(predictions)
    print(f"CSV backup: {csv_path}")

    engine = make_engine()
    ensure_ml_tables(engine, conn.ml_schema)
    rows = write_predictions(engine, conn.ml_schema, predictions)
    print(f"Wrote {rows} rows to {conn.ml_schema}.ml_customer_predictions")


if __name__ == "__main__":
    main()
