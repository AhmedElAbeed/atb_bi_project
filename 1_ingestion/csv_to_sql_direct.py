#!/usr/bin/env python3
"""
Direct CSV to SQL Server ingestion script
Bypasses Airbyte and loads CSVs directly to ATB_BI.PFE_ODS schema
"""

import os
import glob
import pandas as pd
from pathlib import Path
import pyodbc
from datetime import datetime

# Configuration
CSV_FOLDER = r"C:\Users\Ahmed\Desktop\atb_bi_project\Data\raw"
SQLSERVER_HOST = "DESKTOP-B0PDEI7"
SQLSERVER_DATABASE = "ATB_BI"
SQLSERVER_SCHEMA = "PFE_ODS"
DRIVER = "ODBC Driver 17 for SQL Server"

# CSV files to process
CSV_FILES = {
    "F1_2025-04-14_ACCOUNT.csv": "ODS_ACCOUNT",
    "F1_2025-04-14_CUSTOMER.csv": "ODS_CUSTOMER",
    "F1_2025-04-14_CURRENCY.csv": "ODS_CURRENCY",
    "F1_2025-04-14_DAO.csv": "ODS_DAO",
    "F1_2025-04-14_INDUSTRY.csv": "ODS_INDUSTRY",
    "F1_2025-04-14_SECTOR.csv": "ODS_SECTOR",
    "F1_2025-04-14_TARGET.csv": "ODS_TARGET"
}

def connect_sql_server():
    """Create Windows authentication connection to SQL Server"""
    connection_string = (
        f"Driver={DRIVER};"
        f"Server={SQLSERVER_HOST};"
        f"Database={SQLSERVER_DATABASE};"
        f"Trusted_Connection=yes;"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(connection_string)
    return conn

def load_csv_to_sql(csv_file, table_name):
    """Load CSV file to SQL Server table"""
    csv_path = os.path.join(CSV_FOLDER, csv_file)
    
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        return False
    
    try:
        # Read CSV with pipe delimiter
        print(f"📖 Reading {csv_file}...")
        df = pd.read_csv(csv_path, delimiter='|', encoding='utf-8', low_memory=False)
        
        row_count = len(df)
        print(f"   ✓ Loaded {row_count:,} rows")
        
        # Connect to SQL Server
        conn = connect_sql_server()
        cursor = conn.cursor()
        
        # Drop existing table if it exists
        print(f"🗑️  Checking table {SQLSERVER_SCHEMA}.{table_name}...")
        cursor.execute(f"IF OBJECT_ID('{SQLSERVER_SCHEMA}.{table_name}') IS NOT NULL DROP TABLE [{SQLSERVER_SCHEMA}].[{table_name}]")
        conn.commit()
        
        # Insert data using SQLAlchemy for better handling
        from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, DateTime
        
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect=Driver={DRIVER};Server={SQLSERVER_HOST};Database={SQLSERVER_DATABASE};Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;"
        )
        
        table_ref = f"{SQLSERVER_SCHEMA}.{table_name}"
        
        # Convert to approx data types
        df.to_sql(
            table_name,
            engine,
            schema=SQLSERVER_SCHEMA,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        conn.close()
        engine.dispose()
        
        print(f"✅ Successfully loaded {table_name}: {row_count:,} rows\n")
        return True
        
    except Exception as e:
        print(f"❌ Error loading {csv_file}: {str(e)}\n")
        return False

def main():
    print("=" * 70)
    print("CSV → SQL Server Direct Ingestion")
    print("=" * 70)
    print(f"Target: {SQLSERVER_HOST}\\{SQLSERVER_DATABASE}.{SQLSERVER_SCHEMA}")
    print(f"CSV Folder: {CSV_FOLDER}\n")
    
    # Verify schema exists
    try:
        conn = connect_sql_server()
        cursor = conn.cursor()
        cursor.execute(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='{SQLSERVER_SCHEMA}') CREATE SCHEMA [{SQLSERVER_SCHEMA}]")
        conn.commit()
        conn.close()
        print(f"✓ Schema {SQLSERVER_SCHEMA} ready\n")
    except Exception as e:
        print(f"❌ Cannot connect to SQL Server: {str(e)}")
        return
    
    # Load each CSV
    results = {}
    for csv_file, table_name in CSV_FILES.items():
        results[table_name] = load_csv_to_sql(csv_file, table_name)
    
    # Summary
    print("=" * 70)
    print("INGESTION SUMMARY")
    print("=" * 70)
    for table, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"{status}: {table}")
    
    total = len(results)
    success_count = sum(1 for v in results.values() if v)
    print(f"\n{success_count}/{total} tables successfully loaded")
    
    # Verify row counts
    try:
        conn = connect_sql_server()
        cursor = conn.cursor()
        print(f"\n📊 ROW COUNTS IN {SQLSERVER_SCHEMA}:")
        print("-" * 70)
        
        cursor.execute(f"""
        SELECT 
            TABLE_NAME,
            SUM(p.rows) AS row_count
        FROM sys.tables t
        JOIN sys.partitions p ON t.object_id = p.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '{SQLSERVER_SCHEMA}'
        GROUP BY TABLE_NAME
        ORDER BY TABLE_NAME
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[0]:20} {row[1]:>12,} rows")
        
        conn.close()
    except Exception as e:
        print(f"Could not verify row counts: {str(e)}")

if __name__ == "__main__":
    main()
