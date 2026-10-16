# Apache Superset Reporting Kit

This folder contains the reporting layer for the ATB BI project using Apache Superset.

## What this gives you

- A local Superset stack for development
- Curated warehouse views for clean dashboarding
- A dashboard blueprint with layout and visual rules
- Setup instructions for datasets, charts, and publishing

## Recommended Reporting Stack

- Apache Superset for dashboards and exploration
- SQL Server warehouse tables in `PFE_DWH`
- Optional ML scoring table for predictions

## Why Superset

- It connects directly to the warehouse, so the dashboards stay aligned with dbt.
- It is better than a custom app for standard BI: filters, drilldowns, segments, and saved charts.
- It fits the analytics-first structure already used elsewhere in this project.

## Files in this kit

- `docker-compose.yml`: local Superset stack
- `superset_setup.md`: installation and connection steps
- `dashboard_blueprint.md`: design system and dashboard layout
- `superset_semantic_layer.sql`: reporting-friendly views

## Build Order

1. Start the Superset stack.
2. Run the semantic layer SQL in SQL Server.
3. Connect Superset to the warehouse.
4. Register datasets from the curated views.
5. Build charts following the blueprint.
6. Assemble and publish dashboards.

## Dashboard Set

1. Executive Overview
2. Client Segmentation
3. Financial Structure
4. Branch Performance
5. Risk and Compliance
6. ML Predictions
