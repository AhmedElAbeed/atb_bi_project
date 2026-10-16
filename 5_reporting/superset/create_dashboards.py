"""
Deletes all existing charts, then recreates them with correct metric definitions
for Superset 4.x (expressionType must be 'SIMPLE' or 'SQL' only).
"""
import requests
import json
import sys

BASE_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin123"

session = requests.Session()

def login():
    r = session.post(f"{BASE_URL}/api/v1/security/login", json={
        "username": USERNAME, "password": PASSWORD,
        "provider": "db", "refresh": True
    })
    r.raise_for_status()
    token = r.json()["access_token"]
    session.headers.update({"Authorization": f"Bearer {token}"})
    r2 = session.get(f"{BASE_URL}/api/v1/security/csrf_token/")
    r2.raise_for_status()
    csrf = r2.json()["result"]
    session.headers.update({"X-CSRFToken": csrf, "Referer": BASE_URL})
    print("✅ Logged in")

def get_datasets():
    r = session.get(f"{BASE_URL}/api/v1/dataset/", params={"q": json.dumps({"page_size": 100})})
    r.raise_for_status()
    datasets = {d["table_name"]: d["id"] for d in r.json()["result"]}
    print(f"✅ Datasets: {list(datasets.keys())}")
    return datasets

def delete_all_charts():
    r = session.get(f"{BASE_URL}/api/v1/chart/", params={"q": json.dumps({"page_size": 100})})
    r.raise_for_status()
    charts = r.json()["result"]
    print(f"🗑️  Deleting {len(charts)} existing charts...")
    for c in charts:
        rd = session.delete(f"{BASE_URL}/api/v1/chart/{c['id']}")
        status = "✅" if rd.status_code == 200 else "⚠️"
        print(f"  {status} Deleted chart '{c['slice_name']}' (id={c['id']})")

def get_or_create_dashboard(title):
    r = session.get(f"{BASE_URL}/api/v1/dashboard/", params={"q": json.dumps({"page_size": 50})})
    r.raise_for_status()
    for d in r.json()["result"]:
        if d["dashboard_title"] == title:
            return d["id"]
    r2 = session.post(f"{BASE_URL}/api/v1/dashboard/", json={
        "dashboard_title": title,
        "published": True
    })
    if r2.status_code not in (200, 201):
        print(f"  ⚠️  Dashboard create failed: {r2.status_code} {r2.text[:200]}")
        return None
    did = r2.json()["id"]
    print(f"  ✅ Created dashboard '{title}' (id={did})")
    return did

# Valid metric builders for Superset 4.x
def simple_metric(column_name, aggregate, label=None):
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column_name},
        "aggregate": aggregate,
        "label": label or f"{aggregate}({column_name})"
    }

def sql_metric(sql, label):
    return {
        "expressionType": "SQL",
        "sqlExpression": sql,
        "label": label
    }

def create_chart(slice_name, viz_type, datasource_id, params, dashboard_id):
    payload = {
        "slice_name": slice_name,
        "viz_type": viz_type,
        "datasource_id": datasource_id,
        "datasource_type": "table",
        "params": json.dumps(params),
        "dashboards": [dashboard_id]
    }
    r = session.post(f"{BASE_URL}/api/v1/chart/", json=payload)
    if r.status_code not in (200, 201):
        print(f"  ⚠️  Failed '{slice_name}': {r.status_code} {r.text[:300]}")
        return None
    cid = r.json()["id"]
    print(f"  ✅ '{slice_name}' (id={cid})")
    return cid

def main():
    login()
    ds = get_datasets()

    exec_ds   = ds.get("vw_superset_executive_overview")
    risk_ds   = ds.get("vw_superset_risk_distribution")
    branch_ds = ds.get("vw_superset_branch_performance")
    client_ds = ds.get("vw_superset_client_risk_base")

    if not all([exec_ds, risk_ds, branch_ds, client_ds]):
        print("❌ Missing datasets. Aborting.")
        sys.exit(1)

    # Delete all old charts first
    delete_all_charts()
    print()

    # ── Dashboard 1: Executive Overview ──────────────────────────────────────
    print("📊 Building: ATB BI - Executive Overview")
    dash1 = get_or_create_dashboard("ATB BI - Executive Overview")

    create_chart("Total Customers", "big_number_total", exec_ds, {
        "metric": simple_metric("customer_count", "SUM"),
        "subheader": "Total Customers in Portfolio"
    }, dash1)

    create_chart("High Risk Customers", "big_number_total", exec_ds, {
        "metric": simple_metric("high_risk_customers", "SUM"),
        "subheader": "Customers at High or Very High Risk"
    }, dash1)

    create_chart("Avg Risk Score", "big_number_total", exec_ds, {
        "metric": simple_metric("avg_global_risk_score", "AVG"),
        "subheader": "Average Portfolio Risk Score"
    }, dash1)

    create_chart("Total Working Balance", "big_number_total", exec_ds, {
        "metric": simple_metric("total_working_balance", "SUM"),
        "subheader": "Total Portfolio Balance"
    }, dash1)

    create_chart("Risk Tier Distribution", "pie", risk_ds, {
        "groupby": ["risk_tier"],
        "metric": simple_metric("customer_count", "SUM"),
        "donut": True,
        "show_legend": True,
        "show_labels": True,
        "label_type": "key_percent"
    }, dash1)

    create_chart("Risk Trend by Date", "echarts_timeseries_line", risk_ds, {
        "x_axis": "scoring_date",
        "metrics": [simple_metric("customer_count", "SUM")],
        "groupby": ["risk_tier"],
        "time_grain_sqla": "P1D",
        "rich_tooltip": True
    }, dash1)

    # ── Dashboard 2: Branch Performance ──────────────────────────────────────
    print("\n📊 Building: ATB BI - Branch Performance")
    dash2 = get_or_create_dashboard("ATB BI - Branch Performance")

    create_chart("Risk Score by Branch Area", "echarts_bar", branch_ds, {
        "x_axis": "dao_area",
        "metrics": [simple_metric("avg_global_risk_score", "AVG")],
        "orientation": "horizontal"
    }, dash2)

    create_chart("Total Balance by Branch", "echarts_bar", branch_ds, {
        "x_axis": "dao_name",
        "metrics": [simple_metric("total_working_balance", "SUM")],
        "orientation": "horizontal"
    }, dash2)

    create_chart("High Risk by Branch", "echarts_bar", branch_ds, {
        "x_axis": "dao_area",
        "metrics": [simple_metric("high_risk_customers", "SUM")]
    }, dash2)

    create_chart("Customer Count by Branch", "echarts_bar", branch_ds, {
        "x_axis": "dao_name",
        "metrics": [simple_metric("customer_count", "SUM")]
    }, dash2)

    # ── Dashboard 3: Client Segmentation ─────────────────────────────────────
    print("\n📊 Building: ATB BI - Client Segmentation")
    dash3 = get_or_create_dashboard("ATB BI - Client Segmentation")

    create_chart("Customers by Gender", "pie", client_ds, {
        "groupby": ["gender"],
        "metric": sql_metric("COUNT(*)", "Count"),
        "donut": False,
        "show_legend": True,
        "show_labels": True
    }, dash3)

    create_chart("Customers by Employment Status", "echarts_bar", client_ds, {
        "x_axis": "employment_status",
        "metrics": [sql_metric("COUNT(*)", "Count")],
        "orientation": "horizontal"
    }, dash3)

    create_chart("Customers by Risk Tier", "echarts_bar", client_ds, {
        "x_axis": "risk_tier",
        "metrics": [sql_metric("COUNT(*)", "Count")]
    }, dash3)

    create_chart("Customers by Sector", "echarts_bar", client_ds, {
        "x_axis": "sector_description",
        "metrics": [sql_metric("COUNT(*)", "Count")],
        "orientation": "horizontal"
    }, dash3)

    create_chart("Avg Risk Score by Sector", "echarts_bar", client_ds, {
        "x_axis": "sector_description",
        "metrics": [simple_metric("global_risk_score", "AVG")],
        "orientation": "horizontal"
    }, dash3)

    create_chart("PEP Customers", "big_number_total", client_ds, {
        "metric": sql_metric("SUM(CAST(is_pep AS INT))", "PEP Customers"),
        "subheader": "Politically Exposed Persons"
    }, dash3)

    print("\n🎉 All done! Open http://localhost:8088/dashboard/list/")

if __name__ == "__main__":
    main()
