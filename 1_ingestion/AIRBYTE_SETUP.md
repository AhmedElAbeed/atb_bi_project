# Airbyte Setup Guide for ATB BI Project

## Status

**Current Ingestion (Python ETL):**
- ✅ ODS_CURRENCY: 22 rows
- ✅ ODS_DAO: 150 rows
- ✅ ODS_INDUSTRY: 663 rows
- ✅ ODS_SECTOR: 45 rows
- ✅ ODS_TARGET: 11 rows
- ⏳ ODS_ACCOUNT: Pending (145,284 rows)
- ⏳ ODS_CUSTOMER: Pending (136,676 rows)

**Total loaded: 891 rows**

---

## When to Use Airbyte (Future)

Airbyte is recommended for:
1. **Repeatable ingestion** - scheduled daily/weekly syncs
2. **CDC (Change Data Capture)** - incremental updates only
3. **Enterprise production** - monitoring, logging, connectors library
4. **Multiple data sources** - APIs, databases, warehouses

---

## Airbyte Docker Compose Setup

The project includes a `docker-compose.yml` with Airbyte service. To start:

```powershell
# Start Airbyte + PostgreSQL metadata DB
docker-compose up -d airbyte airbyte-db

# Wait 30-60 seconds for startup
Start-Sleep -Seconds 60

# Access Airbyte web UI
Start-Process https://localhost:8000
```

---

## Airbyte Configuration Steps (Once Running)

### 1. Create CSV Source Connector

1. Open http://localhost:8000 in browser
2. Click **+ Create Connection**
3. Select **Source Type**: "File" or "Local File"
4. Configuration:
   - **Source Name**: `ATB_CSVs`
   - **Provider**: Local File or S3 (if using cloud)
   - **File Format**: CSV
   - **Separator**: Pipe (`|`)
   - **Path**: `/data/raw/*.csv`

### 2. Create SQL Server Destination

1. Click **+ New Destination**
2. Select **Destination Type**: "Microsoft SQL Server"
3. Configuration:
   ```
   Host: DESKTOP-B0PDEI7
   Port: 1433
   Database: ATB_BI
   Schema: PFE_ODS
   Username: sa (or your user)
   Password: [Your SQL password]
   ```

### 3. Create Connections for Each CSV

Create 7 separate connections (one per CSV):

| Source | Destination Table | Frequency |
|--------|-------------------|-----------|
| account.csv | ODS_ACCOUNT | Daily |
| customer.csv | ODS_CUSTOMER | Daily |
| currency.csv | ODS_CURRENCY | Weekly |
| dao.csv | ODS_DAO | Weekly |
| industry.csv | ODS_INDUSTRY | Weekly |
| sector.csv | ODS_SECTOR | Weekly |
| target.csv | ODS_TARGET | Weekly |

---

## Manual CSV Load (Alternative)

If Airbyte bandwidth is an issue, use the Python scripts:

```powershell
# Load all CSVs
python 1_ingestion\ingest_csv_to_sql.py

# Or fix specific tables
python 1_ingestion\fix_empty_tables.py
```

---

## Validation Checklist

After ingestion (any method):

```sql
-- Check all tables exist
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='PFE_ODS'

-- Check row counts
SELECT 
  TABLE_NAME, 
  (SELECT COUNT(*) FROM PFE_ODS.???) AS RowCount
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA='PFE_ODS'

-- Sample data
SELECT TOP 5 * FROM [PFE_ODS].[ODS_CUSTOMER]
SELECT TOP 5 * FROM [PFE_ODS].[ODS_ACCOUNT]
```

---

## Troubleshooting

**Airbyte won't start:**
- Check Docker: `docker ps`
- Rebuild: `docker-compose down && docker-compose up -d airbyte airbyte-db`
- Check logs: `docker logs airbyte`

**CSV not imported:**
- Verify separator is pipe (`|`), not comma
- Ensure file paths are absolute in config
- Check data types match SQL Server columns

**Connection failed to SQL Server:**
- Verify DESKTOP-B0PDEI7 is reachable: `ping DESKTOP-B0PDEI7`
- Check SQL Server is running: Open SSMS
- Verify credentials and database exists

---

## Next Steps

1. Validate all 7 tables are populated in PFE_ODS
2. Move to **dbt transformation layer** (3_transformation/)
3. Run dbt models to create PFE_DWH constellation schema
4. Then proceed to ML and reporting

---

Documents:   - `1_ingestion/ingest_csv_to_sql.py` - Full Python ingestion
- `1_ingestion/fix_empty_tables.py` - Targeted fix for individual tables
- `docker-compose.yml` - Airbyte + Airflow stack
