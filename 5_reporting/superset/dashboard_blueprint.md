# Superset Dashboard Blueprint

This blueprint is designed for a clean banking analytics dashboard set in Apache Superset.

## Visual Direction

- Background: charcoal or dark slate
- Primary accent: teal or cyan
- Risk accents: amber and red
- Neutral text: soft gray and white
- Keep the palette restrained and consistent

## Layout Rules

- Use a 12-column grid.
- Keep KPIs in the top row.
- Put trends in the center.
- Put tables and watchlists at the bottom.
- Pin the filter rail on the left.

## Dashboard 1: Executive Overview

Purpose: one-screen summary for leadership.

Charts:

- KPI: total customers
- KPI: total accounts
- KPI: high-risk customers
- KPI: average risk score
- Line chart: risk trend by date
- Donut chart: risk tier split
- Table: top 20 customers by risk score

## Dashboard 2: Client Segmentation

Purpose: show who the customer base is.

Charts:

- Bar chart: customers by employment status
- Bar chart: customers by gender
- Bar chart: customers by marital status
- Bar chart: customers by residence status
- Treemap: sector and industry split
- Table: segment breakdown

## Dashboard 3: Financial Structure

Purpose: show balances and portfolio shape.

Charts:

- KPI: total working balance
- KPI: average working balance
- KPI: negative balance rate
- Time series: balance trend
- Bar chart: balance by currency
- Bar chart: balance by sector
- Scatter plot: account count vs balance

## Dashboard 4: Branch Performance

Purpose: compare DAO and branch performance.

Charts:

- Bar chart: average balance by DAO area
- Heatmap: branch risk by area and sector
- Ranked table: branch risk leaders
- Time series: branch risk trend
- KPI: high-risk branch count

## Dashboard 5: Risk and Compliance

Purpose: monitor exposure and controls.

Charts:

- KPI: compliance risk index average
- KPI: KYC gap rate
- KPI: PEP count
- Bar chart: risk tier distribution
- Bar chart: high-risk by sector
- Heatmap: risk tier by branch and sector
- Table: compliance watchlist

## Dashboard 6: ML Predictions

Purpose: operationalize the predictive layer.

Charts:

- KPI: predicted high-risk customers
- KPI: prediction coverage
- KPI: average prediction probability
- Line chart: predicted risk trend
- Bar chart: predictions by tier
- Table: customer prediction watchlist

## Interaction Rules

- Filters must apply consistently across all charts.
- Default date range should be the latest available scoring date.
- Top-N tables should stay small and actionable.
- Use the same LOW / MEDIUM / HIGH / VERY_HIGH thresholds everywhere.
- Avoid too many bright colors in the same view.
