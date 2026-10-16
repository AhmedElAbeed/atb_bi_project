"""Executive PDF report generator for Overview page."""
from __future__ import annotations

import io
from datetime import datetime

from fpdf import FPDF


class ExecutiveReport(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 10, "ATB BI - Executive Report", ln=True, align="C")
        self.set_font("Helvetica", "", 9)
        self.cell(0, 6, f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}", ln=True, align="C")
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}} - PFE_DWH", align="C")


def build_executive_pdf(
    kpis: dict,
    overview: dict,
    risk_summary: list[dict] | None = None,
    freshness: dict | None = None,
    lang: str = "en",
) -> bytes:
    pdf = ExecutiveReport()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    title = "Portfolio KPIs" if lang == "en" else "Indicateurs portefeuille"
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, title, ln=True)
    pdf.set_font("Helvetica", "", 10)

    kpi_lines = [
        ("Customers / Clients", f"{kpis.get('customers', 0):,.0f}"),
        ("Accounts / Comptes", f"{kpis.get('accounts', 0):,.0f}"),
        ("Total balance (DT)", f"{kpis.get('total_balance', 0):,.0f}"),
        ("Avg risk score", f"{kpis.get('avg_risk', 0):.1f}"),
        ("High+ risk %", f"{kpis.get('high_risk_pct', 0):.1f}%"),
        ("Overdraft %", f"{kpis.get('overdraft_pct', 0):.1f}%"),
    ]
    for label, val in kpi_lines:
        pdf.cell(90, 7, label, border=0)
        pdf.cell(0, 7, val, ln=True)

    if freshness:
        pdf.ln(4)
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(0, 8, "Data freshness / Fraicheur ETL", ln=True)
        pdf.set_font("Helvetica", "", 10)
        for k, v in freshness.items():
            pdf.cell(90, 7, str(k), border=0)
            pdf.cell(0, 7, str(v), ln=True)

    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 8, "Warehouse inventory / Inventaire entrepot", ln=True)
    pdf.set_font("Helvetica", "", 9)
    for table, count in sorted(overview.items()):
        pdf.cell(90, 6, table, border=0)
        pdf.cell(0, 6, f"{count:,} rows", ln=True)

    if risk_summary:
        pdf.ln(4)
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(0, 8, "Risk tier summary / Repartition risque", ln=True)
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(40, 7, "Tier", border=1)
        pdf.cell(40, 7, "Customers", border=1)
        pdf.cell(40, 7, "Avg score", border=1, ln=True)
        pdf.set_font("Helvetica", "", 9)
        for row in risk_summary:
            pdf.cell(40, 6, str(row.get("risk_tier", "")), border=1)
            pdf.cell(40, 6, f"{row.get('customer_count', 0):,}", border=1)
            pdf.cell(40, 6, f"{row.get('avg_score', 0):.1f}", border=1, ln=True)

    pdf.ln(6)
    pdf.set_font("Helvetica", "I", 9)
    note = (
        "Rules-based risk (fact_customer_risk) + XGBoost ML screening. "
        "Source: PFE_DWH on SQL Server ATB_BI."
        if lang == "en"
        else "Risque par regles (fact_customer_risk) + criblage ML XGBoost. "
        "Source: PFE_DWH sur SQL Server ATB_BI."
    )
    pdf.multi_cell(0, 5, note)

    buf = io.BytesIO()
    pdf.output(buf)
    return buf.getvalue()


def build_alert_pack_pdf(
    disagreement_rows: list[dict],
    triple_rows: list[dict],
    ml_summary: dict | None = None,
    model_card: dict | None = None,
    lang: str = "en",
) -> bytes:
    """Compliance alert pack — ML≠rules disagreements + triple-flag cases."""
    pdf = ExecutiveReport()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    title = "Compliance Alert Pack" if lang == "en" else "Pack d'alertes conformité"
    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(0, 9, title, ln=True)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(
        0, 6,
        f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} · PFE_ML + PFE_DWH",
        ln=True,
    )
    pdf.ln(3)

    if model_card and model_card.get("available"):
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(0, 7, "ML model (MLflow Production)", ln=True)
        pdf.set_font("Helvetica", "", 9)
        for label, key in [
            ("Registry", "model_name"),
            ("Version", "version"),
            ("Stage", "stage"),
            ("Algorithm", "best_algorithm"),
            ("Trained", "trained_at"),
            ("Threshold", "threshold"),
        ]:
            pdf.cell(55, 6, label, border=0)
            pdf.cell(0, 6, str(model_card.get(key, "—")), ln=True)
        m = model_card.get("metrics") or {}
        pdf.cell(55, 6, "ROC AUC", border=0)
        pdf.cell(0, 6, f"{m.get('roc_auc', 0):.4f}" if m.get("roc_auc") else "—", ln=True)
        pdf.ln(2)

    if ml_summary:
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(0, 7, "Warehouse predictions snapshot", ln=True)
        pdf.set_font("Helvetica", "", 9)
        for label, key in [
            ("Total scored", "total_predictions"),
            ("ML high-risk", "ml_high_risk"),
            ("ML ≠ rules", "disagreements"),
            ("Last scored", "last_scored"),
        ]:
            pdf.cell(55, 6, label, border=0)
            pdf.cell(0, 6, str(ml_summary.get(key, "—")), ln=True)
        pdf.ln(2)

    pdf.set_font("Helvetica", "B", 11)
    hdr = (
        f"ML high-risk ≠ rules tier ({len(disagreement_rows)} cases)"
        if lang == "en"
        else f"ML haut risque ≠ règles ({len(disagreement_rows)} cas)"
    )
    pdf.cell(0, 7, hdr, ln=True)
    pdf.set_font("Helvetica", "B", 8)
    cols = ["Customer", "ML prob", "Rules tier", "Rules score", "Balance", "DAO"]
    widths = [28, 22, 24, 24, 28, 64]
    for w, c in zip(widths, cols):
        pdf.cell(w, 6, c, border=1)
    pdf.ln()
    pdf.set_font("Helvetica", "", 8)
    for row in disagreement_rows[:50]:
        pdf.cell(28, 5, str(row.get("customer_id", "")), border=1)
        prob = row.get("risk_probability")
        pdf.cell(22, 5, f"{float(prob):.3f}" if prob is not None else "—", border=1)
        pdf.cell(24, 5, str(row.get("rules_risk_tier", ""))[:10], border=1)
        score = row.get("rules_global_score")
        pdf.cell(24, 5, f"{float(score):.1f}" if score is not None else "—", border=1)
        bal = row.get("total_working_balance")
        pdf.cell(28, 5, f"{float(bal):,.0f}" if bal is not None else "—", border=1)
        pdf.cell(64, 5, str(row.get("dao_area", ""))[:28], border=1, ln=True)

    if triple_rows:
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 11)
        triple_hdr = (
            f"Triple-flag: PEP + high balance + ML high ({len(triple_rows)} cases)"
            if lang == "en"
            else f"Triple alerte: PEP + solde + ML ({len(triple_rows)} cas)"
        )
        pdf.cell(0, 7, triple_hdr, ln=True)
        pdf.set_font("Helvetica", "B", 8)
        for w, c in zip(widths, cols):
            pdf.cell(w, 6, c, border=1)
        pdf.ln()
        pdf.set_font("Helvetica", "", 8)
        for row in triple_rows[:40]:
            pdf.cell(28, 5, str(row.get("customer_id", "")), border=1)
            prob = row.get("risk_probability")
            pdf.cell(22, 5, f"{float(prob):.3f}" if prob is not None else "—", border=1)
            pdf.cell(24, 5, str(row.get("rules_risk_tier", ""))[:10], border=1)
            score = row.get("rules_global_score")
            pdf.cell(24, 5, f"{float(score):.1f}" if score is not None else "—", border=1)
            bal = row.get("total_working_balance")
            pdf.cell(28, 5, f"{float(bal):,.0f}" if bal is not None else "—", border=1)
            pdf.cell(64, 5, str(row.get("dao_area", ""))[:28], border=1, ln=True)

    buf = io.BytesIO()
    pdf.output(buf)
    return buf.getvalue()
