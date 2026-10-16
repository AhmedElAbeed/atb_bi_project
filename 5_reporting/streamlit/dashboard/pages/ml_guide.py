"""ML & risk scoring guide for PFE presentation."""
from __future__ import annotations

import streamlit as st

from dashboard import ml_service
from dashboard.ui import insight, page_header, section


def render() -> None:
    page_header(
        "ML & Risk Scoring Guide",
        "Understand customer risk (rules engine), KYC usage, F1 score, and the XGBoost model — for your PFE defense.",
    )

    section("1. What is fact_customer_risk?")
    st.markdown(
        """
**`PFE_DWH.fact_customer_risk`** is the **rules-based risk fact table** (not ML).

- **Grain:** one row per customer per scoring date  
- **Goal:** quantify how risky a customer is for compliance and financial monitoring  
- **Output:** 3 pillar scores + **global_risk_score** (0–100) + **risk_tier** (LOW → VERY_HIGH)

| Pillar | Weight | What it measures |
|--------|--------|------------------|
| **Compliance** | 40% | KYC, PEP, compliance flags, posting restrictions |
| **Financial fragility** | 35% | Salary, overdraft, low balances, unemployment |
| **Behavioral** | 25% | Tenure, overdraft behavior, stale KYC reviews |

**Formula:** `global_risk_score = compliance×0.40 + fragility×0.35 + behavioral×0.25`

**Business benefit:** Prioritize branch reviews, flag PEP/KYC gaps, segment portfolio risk without waiting for ML.
        """
    )

    section("2. Where is KYC used? (you wrote NYC — this is KYC)")
    st.markdown(
        """
**KYC = Know Your Customer** — regulatory identity verification.

### In the warehouse (dbt `int_customer_risk_score.sql`)

| KYC signal | Pillar | Points added |
|------------|--------|--------------|
| `is_kyc_complete = 0` | Compliance | **+35** |
| `last_kyc_review_date` null | Compliance | **+15** |
| KYC review > 365 days old | Compliance | **+10** |
| KYC review > 730 days / null | Behavioral | **+20** |

### In `dim_customer`
- `is_kyc_complete`, `last_kyc_review_date`, `next_kyc_review_date`, `kyc_score`

### In the ML model (`4_ml/src/features.py`)
- `is_kyc_complete` — direct feature  
- `days_since_last_kyc_review` — engineered feature  
- `is_kyc_stale` (>365 days), `is_kyc_very_stale` (>730 days) — engineered flags  

**Dashboard usage:** BI Studies (KYC vs risk), Customer dimension (KYC %), ML predictor form.
        """
    )

    section("3. What is F1 score? (not in your customer data)")
    st.markdown(
        """
**F1 score is a model evaluation metric** — it is **NOT a column in your SQL Server tables**.

| Term | Meaning in this project |
|------|-------------------------|
| **F1** | Harmonic mean of precision & recall on the **test set** |
| **ROC AUC** | How well the model ranks high-risk vs low-risk customers |
| **Precision** | Of customers flagged high-risk by ML, how many truly are |
| **Recall** | Of truly high-risk customers, how many the model catches |

### Where F1 is computed
- `4_ml/src/modeling.py` — `f1_score()` after training  
- `4_ml/src/run_model_validation.py` — compares train vs test F1 (overfitting check)  
- **Gate in pipeline:** F1 ≥ 0.70 and AUC ≥ 0.80 (`FULL_INTEGRATION_STATUS.md`)  
- **Dashboard:** ML risk model page → Model performance tab

### Target variable for ML
```python
risk_target = 1 if global_risk_score >= 50 else 0  # high risk binary label
```
The ML model learns to **predict the same high-risk label** from customer attributes (KYC, balances, sector…).
        """
    )

    section("4. What does the ML model do?")
    summary = ml_service.load_training_summary()
    model, model_info, _ = ml_service.load_model()

    st.markdown(
        """
| Aspect | Detail |
|--------|--------|
| **Algorithm** | XGBoost (also compared vs Random Forest) |
| **Input** | `dim_customer` + `fact_customer_risk` features (KYC, salary, balances, sector…) |
| **Output** | P(high risk) between 0 and 1 + binary label |
| **Goal** | Automate screening: flag customers likely to be high risk before manual review |
| **Difference from rules** | Rules are explicit points; ML finds non-linear patterns in data |

**Workflow:** ETL builds `fact_customer_risk` → ML trains on historical scores → Dashboard scores live customers.
        """
    )

    if summary:
        best = summary.get("best_model", "unknown")
        metrics = (summary.get("results") or {}).get(best, {}).get("metrics", {})
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Best model", best.upper())
        c2.metric("F1 (test)", f"{metrics.get('f1', 0):.3f}")
        c3.metric("ROC AUC", f"{metrics.get('roc_auc', 0):.3f}")
        c4.metric("Precision", f"{metrics.get('precision', 0):.3f}")
    elif model:
        st.info(model_info)
    else:
        st.warning("Model not trained yet. Run: `cd 4_ml && python src/run_training.py`")

    section("5. Is the model working perfectly?")
    insight(
        "<strong>Short answer: strong metrics, but verify before claiming perfection.</strong> "
        "If ROC AUC is ~0.99, check for data leakage (the model must NOT see global_risk_score "
        "during training — it is excluded in features.py). Always validate on <strong>fresh data</strong> "
        "and compare ML vs rules in the Batch scoring tab."
    )
    st.markdown(
        """
**Healthy signs**
- High ROC AUC and F1 on **test** set (not train)
- Small train–test F1 gap (see `05_model_report_overfitting_gap.png`)
- Agreement between ML and rules on obvious cases (PEP, no KYC)

**Red flags**
- Train F1 much higher than test F1 → overfitting
- ML disagrees with rules on many PEP/KYC-incomplete customers → needs review
- No `training_summary.json` or missing `.pkl` artifact → model not deployed

**For your PFE:** Say the model is a **decision-support tool**, not a replacement for compliance officers.
        """
    )

    section("6. Ideas to showcase ML in your presentation")
    st.markdown(
        """
1. **Live demo** — ML risk model → Risk predictor tab: score a customer by ID  
2. **Batch scoring** — Show ML vs DWH rule agreement %  
3. **ROC / PR curves** — Curves & matrix tab (proves model quality)  
4. **SHAP chart** — Display `4_ml/outputs/shap_summary.png` (top drivers: KYC, overdraft, salary)  
5. **Alert list** — Customers where ML = high risk but tier = LOW (future enhancement)  
6. **Score into warehouse** — Nightly table `PFE_ML.customer_risk_predictions`  
7. **Threshold slider** — What-if: change decision threshold without retraining  
8. **Side-by-side** — Rules score vs ML probability on same customer row  
        """
    )
