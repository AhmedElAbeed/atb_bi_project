"""
Lightweight MLflow test run — synthetic data, no SQL Server required.

Use for demos and to populate mlruns + training_summary.json when the warehouse
is unavailable.

  cd 4_ml/src
  python run_test_training.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "4_ml" / "src"
sys.path.insert(0, str(SRC))

from features import build_model_frame, select_feature_columns, split_temporal  # noqa: E402
import modeling  # noqa: E402


def _synthetic_frame(n: int = 1200) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    today = pd.Timestamp.today().normalize()
    sectors = ["AGR", "IND", "SRV", "BNK", "TRD"]
    dao_areas = ["TUNIS CENTRE", "SFAX", "SOUSSE", "NABEUL", "BIZERTE"]
    employment = ["EMPLOYED", "SELF_EMPLOYED", "RETIRED", "STUDENT"]

    data = {
        "customer_id": [f"C{i:05d}" for i in range(n)],
        "scoring_date": today - pd.to_timedelta(rng.integers(0, 365, n), unit="D"),
        "load_date": today,
        "global_risk_score": rng.integers(5, 95, n),
        "compliance_risk_index": rng.integers(0, 100, n),
        "financial_fragility_score": rng.integers(0, 100, n),
        "behavioral_risk_score": rng.integers(0, 100, n),
        "risk_tier": rng.choice(["LOW", "MEDIUM", "HIGH"], n),
        "total_working_balance": rng.uniform(-500, 120_000, n),
        "avg_working_balance": rng.uniform(-200, 40_000, n),
        "account_count": rng.integers(1, 6, n),
        "negative_balance_account_count": rng.integers(0, 3, n),
        "monthly_salary": rng.uniform(800, 4500, n),
        "number_of_dependents": rng.integers(0, 5, n),
        "customer_tenure_days": rng.integers(30, 4000, n),
        "is_kyc_complete": rng.integers(0, 2, n),
        "is_pep": rng.integers(0, 20, n).clip(0, 1),
        "is_compliance_flagged": rng.integers(0, 10, n).clip(0, 1),
        "has_compliance_decision": rng.integers(0, 10, n).clip(0, 1),
        "days_since_last_kyc_review": rng.integers(30, 800, n),
        "days_until_next_kyc_review": rng.integers(-60, 400, n),
        "customer_since_date": today - pd.to_timedelta(rng.integers(365, 4000, n), unit="D"),
        "oldest_account_opening_date": today - pd.to_timedelta(rng.integers(200, 3000, n), unit="D"),
        "newest_account_opening_date": today - pd.to_timedelta(rng.integers(0, 500, n), unit="D"),
        "last_kyc_review_date": today - pd.to_timedelta(rng.integers(30, 700, n), unit="D"),
        "next_kyc_review_date": today + pd.to_timedelta(rng.integers(30, 400, n), unit="D"),
        "employment_status": rng.choice(employment, n),
        "gender": rng.choice(["M", "F"], n),
        "marital_status": rng.choice(["SINGLE", "MARRIED", "DIVORCED"], n),
        "residence_status": rng.choice(["OWNER", "TENANT", "FAMILY"], n),
        "sector_code": rng.choice(sectors, n),
        "industry_code": rng.choice(["I01", "I02", "I03", "I04"], n),
        "target_code": rng.choice(["RET", "PRO", "VIP"], n),
        "segment_code": rng.choice(["A", "B", "C"], n),
        "dao_area": rng.choice(dao_areas, n),
        "client_nature_flag": rng.choice(["IND", "CORP"], n),
        "legal_capacity_flag": rng.choice(["FULL", "LIMITED"], n),
        "beneficial_owner_flag": rng.choice(["Y", "N"], n),
    }
    return pd.DataFrame(data)


def main() -> None:
    print("ATB ML — test training run (synthetic data)", flush=True)
    frame = _synthetic_frame(1200)
    model_frame = build_model_frame(frame)
    feature_columns, numeric_features, categorical_features = select_feature_columns(model_frame)

    def _uniq(seq):
        seen = set()
        out = []
        for v in seq:
            if v not in seen:
                seen.add(v)
                out.append(v)
        return out

    feature_columns = _uniq(feature_columns)
    cat_set = set(categorical_features)
    numeric_features = [c for c in _uniq(numeric_features) if c in feature_columns and c not in cat_set]
    categorical_features = [c for c in _uniq(categorical_features) if c in feature_columns]
    if not feature_columns:
        raise SystemExit("No features — aborting")

    train, test = split_temporal(model_frame)
    print(f"train={len(train)} test={len(test)} features={len(feature_columns)}", flush=True)

    X_train = train[feature_columns]
    y_train = train["risk_target"].astype(int)
    X_test = test[feature_columns]
    y_test = test["risk_target"].astype(int)

    specs = modeling.build_model_specs(numeric_features, categorical_features)
    results = {}

    for spec in specs[:1]:  # xgboost only for speed
        print(f"Tuning {spec.name} (test run)...", flush=True)
        search = modeling.tune_model(spec, X_train, y_train, n_iter=1)
        best = search.best_estimator_
        y_proba = best.predict_proba(X_test)[:, 1]
        threshold, _ = modeling.optimize_threshold(y_test, y_proba)
        metrics = modeling.evaluate_model(best, X_test, y_test, threshold=threshold)
        metrics["threshold"] = threshold
        print(f"{spec.name} metrics: {metrics}", flush=True)
        artifact = modeling.log_metrics_and_artifacts(
            model_name=spec.name,
            best_model=best,
            metrics=metrics,
            param_summary=search.best_params_,
            artifact_filename=f"{spec.name}_best.pkl",
        )
        results[spec.name] = {
            "metrics": metrics,
            "best_params": search.best_params_,
            "artifact": artifact,
            "threshold": threshold,
        }

    best_name = modeling.PRODUCTION_MODEL
    if best_name not in results:
        raise SystemExit(f"Production model {best_name!r} was not trained.")
    import joblib

    best_model = joblib.load(results[best_name]["artifact"])
    registry = modeling.register_model_to_production(
        model_name=best_name,
        best_model=best_model,
        metrics=results[best_name]["metrics"],
        param_summary=results[best_name]["best_params"],
        artifact_filename=f"{best_name}_best.pkl",
    )

    summary = {"best_model": best_name, "results": results, "registry": registry}
    out_dir = ROOT / "4_ml" / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "training_summary.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Done — summary: {out_path}", flush=True)
    print(f"MLflow registry: {registry['registry_name']} v{registry['version']} ({registry['stage']})", flush=True)


if __name__ == "__main__":
    main()
