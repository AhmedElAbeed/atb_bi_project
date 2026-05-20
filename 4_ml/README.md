# ATB BI Project ML Workbench

Notebook order:

1. `notebooks/01_eda.ipynb` — data profiling and target balance
2. `notebooks/02_feature_engineering.ipynb` — leakage-safe features and train/test split
3. `notebooks/03_model_training.ipynb` — XGBoost + Random Forest with comparison charts
4. `notebooks/04_shap_explainability.ipynb` — global and local SHAP plots
5. `notebooks/05_model_selection_report.ipynb` — final metrics, ROC/PR, and model decision

Run all notebooks in order:

```bash
cd 4_ml
python scripts/run_all_notebooks.py
```

Shared code lives in `src/`.

The notebooks use the warehouse risk fact as the primary supervised-learning source, with:

- binary high-risk target derived from `global_risk_score >= 50`
- SMOTETomek for class imbalance
- XGBoost as the primary model
- Random Forest as the benchmark model
- MLflow file-based tracking by default unless `MLFLOW_TRACKING_URI` is set
