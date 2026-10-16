# Superset Setup Guide

## 1. Start the local stack

From this folder:

```bash
docker compose up -d
```

Open the UI at:

```text
http://localhost:8088
```

Default login:

- Username: `admin`
- Password: `admin123`

## 2. Initialize Superset

If the container is running but Superset is not initialized yet:

```bash
docker compose exec superset superset db upgrade
docker compose exec superset superset fab create-admin \
  --username admin \
  --firstname ATB \
  --lastname Admin \
  --email admin@atb.local \
  --password admin123
docker compose exec superset superset init
```

## 3. Add the warehouse connection

Create a Superset database connection to SQL Server with:

- Host: your SQL Server host
- Port: your SQL Server port
- Database: `ATB_BI`
- Username: warehouse user
- Password: warehouse password

Example SQLAlchemy URI:

```text
mssql+pyodbc://USER:PASSWORD@HOST:1434/ATB_BI?driver=ODBC+Driver+17+for+SQL+Server
```

## 4. Load the semantic layer

Run `superset_semantic_layer.sql` against the warehouse. It creates reporting-friendly views for the dashboards.

## 5. Register datasets

Create datasets from these views:

- `vw_superset_executive_overview`
- `vw_superset_client_risk_base`
- `vw_superset_account_portfolio`
- `vw_superset_branch_performance`
- `vw_superset_risk_distribution`
- `vw_superset_ml_predictions`

## 6. Create charts

Use the dashboard blueprint and keep naming consistent:

- `kpi_*` for KPI tiles
- `trend_*` for line charts
- `bar_*` for ranked bars
- `heatmap_*` for matrix charts
- `table_*` for watchlists

## 7. Publish dashboards

Recommended dashboard order:

1. Executive Overview
2. Client Segmentation
3. Financial Structure
4. Branch Performance
5. Risk and Compliance
6. ML Predictions
