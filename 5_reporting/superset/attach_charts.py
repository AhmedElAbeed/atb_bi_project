"""
Attaches existing charts to their dashboards using the correct Superset API.
"""
import requests
import json

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

def get_all_charts():
    r = session.get(f"{BASE_URL}/api/v1/chart/", params={"q": json.dumps({"page_size": 100})})
    r.raise_for_status()
    return {c["slice_name"]: c["id"] for c in r.json()["result"]}

def get_all_dashboards():
    r = session.get(f"{BASE_URL}/api/v1/dashboard/", params={"q": json.dumps({"page_size": 50})})
    r.raise_for_status()
    return {d["dashboard_title"]: d["id"] for d in r.json()["result"]}

def add_chart_to_dashboard(chart_id, dashboard_id):
    """Use the chart update endpoint to add it to a dashboard."""
    # First get existing chart info
    r = session.get(f"{BASE_URL}/api/v1/chart/{chart_id}")
    r.raise_for_status()
    chart = r.json()["result"]
    existing_dashboards = [d["id"] for d in chart.get("dashboards", [])]
    if dashboard_id in existing_dashboards:
        return True  # already attached
    new_dashboards = existing_dashboards + [dashboard_id]
    r2 = session.put(f"{BASE_URL}/api/v1/chart/{chart_id}", json={
        "dashboards": new_dashboards
    })
    return r2.status_code in (200, 201)

def main():
    login()
    charts = get_all_charts()
    dashboards = get_all_dashboards()

    print(f"Found {len(charts)} charts, {len(dashboards)} dashboards")

    # Map: dashboard title -> list of chart names to attach
    assignments = {
        "ATB BI - Executive Overview": [
            "Total Customers",
            "High Risk Customers",
            "Avg Global Risk Score",
            "Total Working Balance",
            "Risk Tier Distribution",
            "Risk Trend by Date",
        ],
        "ATB BI - Branch Performance": [
            "Avg Risk Score by Branch",
            "Total Balance by Branch",
            "High Risk Customers by Branch",
            "Branch Customer Count",
        ],
        "ATB BI - Client Segmentation": [
            "Customers by Gender",
            "Customers by Employment Status",
            "Customers by Risk Tier",
            "Customers by Sector",
        ],
    }

    # Also attach Customer Count (id=1 from the first subagent session)
    assignments["ATB BI - Executive Overview"].append("Customer Count")

    for dash_title, chart_names in assignments.items():
        dash_id = dashboards.get(dash_title)
        if not dash_id:
            print(f"  ⚠️  Dashboard '{dash_title}' not found")
            continue
        print(f"\n📊 Dashboard: {dash_title} (id={dash_id})")
        for name in chart_names:
            chart_id = charts.get(name)
            if not chart_id:
                print(f"  ⚠️  Chart '{name}' not found")
                continue
            ok = add_chart_to_dashboard(chart_id, dash_id)
            print(f"  {'✅' if ok else '❌'} '{name}' -> dashboard {dash_id}")

    print("\n🎉 Done! Open http://localhost:8088/dashboard/list/")

if __name__ == "__main__":
    main()
