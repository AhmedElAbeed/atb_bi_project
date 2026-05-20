from __future__ import annotations

import json
import os
import sys
from pathlib import Path


# ensure local src is importable when running from repo root
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "4_ml" / "src"
sys.path.insert(0, str(SRC))

from data_access import load_feature_frame
from features import build_model_frame, select_feature_columns, split_temporal
import modeling


def main():
    print("Loading feature frame...")
    frame = load_feature_frame()
    print(f"rows={len(frame)} columns={len(frame.columns)}")

    max_rows = int(os.getenv("ATB_TRAIN_MAX_ROWS", "5000"))
    if len(frame) > max_rows:
        frame = frame.sample(max_rows, random_state=42).sort_values("scoring_date").reset_index(drop=True)
        print(f"sampled_rows={len(frame)}", flush=True)

    model_frame = build_model_frame(frame)
    feature_columns, numeric_features, categorical_features = select_feature_columns(model_frame)
    # deduplicate while preserving order
    def _uniq(seq):
        seen = set()
        out = []
        for v in seq:
            if v not in seen:
                seen.add(v)
                out.append(v)
        return out

    feature_columns = _uniq(feature_columns)
    numeric_features = [c for c in _uniq(numeric_features) if c in feature_columns]
    categorical_features = [c for c in _uniq(categorical_features) if c in feature_columns]
    if len(feature_columns) == 0:
        raise SystemExit("No feature columns discovered; aborting")

    train, test = split_temporal(model_frame)
    print(f"train={len(train)} test={len(test)}")

    X_train = train[feature_columns]
    y_train = train["risk_target"].astype(int)
    X_test = test[feature_columns]
    y_test = test["risk_target"].astype(int)

    specs = modeling.build_model_specs(numeric_features, categorical_features)
    results = {}

    for spec in specs:
        print(f"Tuning {spec.name} (light search)...")
        search = modeling.tune_model(spec, X_train, y_train, n_iter=1)
        best = search.best_estimator_
        # find best threshold
        y_proba = best.predict_proba(X_test)[:, 1]
        threshold, best_f1 = modeling.optimize_threshold(y_test, y_proba)
        metrics = modeling.evaluate_model(best, X_test, y_test, threshold=threshold)
        print(f"{spec.name} metrics: {metrics}", flush=True)
        artifact = modeling.log_metrics_and_artifacts(
            model_name=spec.name,
            best_model=best,
            metrics=metrics,
            param_summary=search.best_params_,
            artifact_filename=f"{spec.name}_best.pkl",
        )
        results[spec.name] = {"metrics": metrics, "best_params": search.best_params_, "artifact": artifact}

    # choose best by f1
    best_model_name = max(results.keys(), key=lambda k: results[k]["metrics"]["f1"])
    summary = {"best_model": best_model_name, "results": results}

    out_dir = ROOT / "4_ml" / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "training_summary.json"
    out_path.write_text(json.dumps(summary, indent=2))
    print(f"Wrote summary to {out_path}")
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
