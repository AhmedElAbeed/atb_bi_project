# ATB BI Dashboard - Streamlit Application

## Overview

This Streamlit dashboard provides comprehensive Business Intelligence analytics for the ATB (Arab Tunisian Bank) data warehouse. It connects directly to SQL Server (SSMS) and provides interactive visualizations for all data warehouse tables including risk analysis, account analytics, dimension analysis, and ML model monitoring.

## Features

### 📊 Warehouse Intelligence
- **KPI Dashboard**: Total customers, average risk scores, high-risk percentages, PEP flagged, KYC complete, compliance flags
- **Risk Tier Distribution**: Visual breakdown of LOW, MEDIUM, HIGH, VERY_HIGH risk customers
- **Global Risk Score Distribution**: Histogram showing risk score spread
- **Box Plot Analysis**: Risk scores distribution by tier
- **Three Pillars Analysis**: Compliance, Financial Fragility, and Behavioral risk scores
- **Top Risky Customers**: Table showing highest risk customers

### 💰 Account Analytics
- **Account KPIs**: Total accounts, total balance, average balance, overdraft rate
- **Balance by Sector**: Bar chart showing total balances per economic sector
- **Account Category Distribution**: Distribution of account types
- **Currency Distribution**: Balance breakdown by currency
- **Target Segment Analysis**: Accounts distribution by customer segment

### 🗂️ Dimension Analysis
- **Customer Dimension**: Gender distribution, employment status analysis
- **DAO/Branch Dimension**: Branch distribution by area
- **Sector Dimension**: Economic sector reference data
- **Industry Dimension**: Industry classification data
- **Target Segment Dimension**: Customer segmentation data
- **Currency Dimension**: Currency reference data
- **Date Dimension**: Date range and total days information
- **Risk Profile Dimension**: Risk tier summary and average scores by tier

### 📚 Data Dictionary
- **Table Overview**: All warehouse tables with row counts and sizes
- **Visual Table Sizes**: Bar chart showing table sizes
- **Table Explorer**: Interactive tool to browse any table's schema and sample data
- **Column Information**: Data types, nullability, and column order

### 🤖 ML Intelligence
- **Training Summary**: Best model information and metrics comparison
- **Model Scoring**: Live predictions with probability distributions
- **Feature Importance**: Model feature analysis
- **Scoring Sample**: Sample predictions with features

## Prerequisites

1. **SQL Server**: Ensure SQL Server is running and accessible
2. **Database**: The `ATB_BI` database must exist with the data warehouse schemas
3. **Python**: Python 3.8+ installed
4. **ODBC Driver**: ODBC Driver 18 for SQL Server installed

## Installation

### 1. Install Dependencies

```bash
cd 5_reporting/streamlit
pip install -r requirements.txt
```

### 2. Configure Connection

Edit the `.env` file with your SQL Server credentials:

```env
ATB_SQL_SERVER=your_server_name
ATB_SQL_DATABASE=ATB_BI
ATB_SQL_USERNAME=your_username
ATB_SQL_PASSWORD=your_password
ATB_SQL_PORT=1433
ATB_SQL_DRIVER=ODBC Driver 18 for SQL Server
ATB_DASHBOARD_SOURCE=PFE_DWH.fact_customer_risk
ATB_DASHBOARD_TOP_N=10000
```

### 3. Run the Dashboard

```bash
# Option 1: Direct execution
streamlit run app.py

# Option 2: With custom port
streamlit run app.py --server.port=8501

# Option 3: With external access
streamlit run app.py --server.address=0.0.0.0
```

### 4. Using Docker (Optional)

```bash
# Build and run with Docker Compose
docker-compose up -d

# View logs
docker-compose logs -f
```

## Database Schema Requirements

The dashboard expects the following schemas and tables in the `ATB_BI` database:

### PFE_DWH Schema (Data Warehouse)
- `fact_customer_risk` - Customer risk fact table
- `fact_account` - Account fact table
- `dim_customer` - Customer dimension
- `dim_dao` - Account Officer/Branch dimension
- `dim_sector` - Sector dimension
- `dim_industry` - Industry dimension
- `dim_target` - Target segment dimension
- `dim_currency` - Currency dimension
- `dim_date` - Date dimension
- `dim_risk_profile` - Risk profile dimension

### PFE_ODS Schema (Operational Data Store)
- Various ODS tables (automatically discovered)

## Navigation

Use the sidebar to switch between dedicated BI views:

| Page | Table(s) |
|------|----------|
| Overview | Cross-table KPIs + schema inventory |
| **BI Studies** | Multi-table joins & business analysis |
| Customer risk | `PFE_DWH.fact_customer_risk` |
| Accounts | `PFE_DWH.fact_account` |
| Customers | `PFE_DWH.dim_customer` |
| Branches (DAO) | `PFE_DWH.dim_dao` |
| Calendar | `PFE_DWH.dim_date` |
| Sectors | `PFE_DWH.dim_sector` |
| Industries | `PFE_DWH.dim_industry` |
| Target segments | `PFE_DWH.dim_target` |
| Currencies | `PFE_DWH.dim_currency` |
| Data explorer | Sample data + CSV export |
| ML risk model | XGBoost scoring & evaluation |

> Note: There is no `dim_account` in the warehouse — account analytics live in **fact_account**.

### ML model section

The **ML risk model** page uses the trained **XGBoost** classifier from `4_ml/` (target: high risk when `global_risk_score ≥ 50`).

1. Ensure the artifact exists: `4_ml/models/xgboost_best.pkl` (or run training).
2. Open sidebar → **ML risk model**.
3. Tabs: **Model performance** · **Curves & matrix** · **Batch scoring** · **Risk predictor** · **Roadmap**.

Retrain if needed:

```bash
cd 4_ml
python src/run_training.py
```

## Customization

### Changing Risk Tier Colors

Edit the `RISK_COLORS` dictionary in `app.py`:

```python
RISK_COLORS = {
    "LOW": "#2ecc71",      # Green
    "MEDIUM": "#f39c12",   # Orange
    "HIGH": "#e74c3c",     # Red
    "VERY_HIGH": "#8e44ad", # Purple
    "CRITICAL": "#c0392b",  # Dark Red
}
```

### Adjusting Data Load Limits

Modify environment variables:
- `ATB_DASHBOARD_TOP_N`: Maximum rows to load (default: 10000)

### Custom SQL Queries

Add custom queries in the data loading functions section of `app.py`.

## Troubleshooting

### Connection Issues

1. **Check SQL Server is running**:
   ```bash
   sqlcmd -S your_server_name -U your_username -P your_password
   ```

2. **Verify ODBC Driver is installed**:
   ```bash
   odbcinst -q -d -n "ODBC Driver 18 for SQL Server"
   ```

3. **Test connection from Python**:
   ```python
   import pyodbc
   conn = pyodbc.connect(
       "DRIVER={ODBC Driver 18 for SQL Server};"
       "SERVER=your_server_name;"
       "DATABASE=ATB_BI;"
       "UID=your_username;"
       "PWD=your_password"
   )
   ```

### Missing Tables

If tables are missing, run the dbt transformation pipeline:

```bash
cd 3_transformation
dbt run
dbt test
```

### Performance Issues

- Reduce `ATB_DASHBOARD_TOP_N` to load fewer rows
- Increase cache TTL values in the `@st.cache_data` decorators
- Consider creating indexed views for frequently queried data

## Architecture

```
┌─────────────────┐
│   Streamlit     │
│   Dashboard     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   SQLAlchemy    │
│   (pyodbc)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   SQL Server    │
│   (ATB_BI)      │
├─────────────────┤
│ PFE_DWH         │ ← Data Warehouse (Star Schema)
│ PFE_ODS         │ ← Operational Data Store
│ PFE_ML          │ ← ML Predictions
└─────────────────┘
```

## Contributing

When adding new features:

1. Follow the existing code structure
2. Add appropriate caching decorators
3. Include error handling for database queries
4. Update this README with new features

## License

This project is part of the ATB BI Project and is intended for internal use at Arab Tunisian Bank.

## Support

For issues or questions:
- Check the project documentation in the root `README.md`
- Review the data warehouse documentation in `docs/`
- Contact the data engineering team

---

**Version**: 3.0  
**Last Updated**: 2026-06-02

### Project layout

```
streamlit/
  app.py                 # Entry point
  dashboard/
    config.py            # Theme & table names
    db.py                # SQL Server connection
    loaders.py           # Aggregated SQL queries
    charts.py / ui.py    # Plotly & layout helpers
    pages/               # One module per BI view
```