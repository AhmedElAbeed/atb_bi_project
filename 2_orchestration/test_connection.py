#!/usr/bin/env python3
"""Test SQL Server connection from inside Docker container"""
import pyodbc
import os

server = os.getenv("SQL_SERVER_HOST", "host.docker.internal")
port = os.getenv("SQL_SERVER_PORT", "1434")
database = os.getenv("SQL_SERVER_DATABASE", "ATB_BI")
user = os.getenv("DBT_USER", "airbyte_user")
password = os.getenv("DBT_PASSWORD", "AZERTY123")

conn_str = (
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server={server},{port};"
    f"Database={database};"
    f"UID={user};"
    f"PWD={password};"
    f"Encrypt=no;"
    f"TrustServerCertificate=yes;"
    f"Connection Timeout=10;"
)

print(f"Attempting connection to {server},{port} / {database} as {user}...")
try:
    conn = pyodbc.connect(conn_str, timeout=10)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN ('PFE_ODS','PFE_DWH')")
    count = cursor.fetchone()[0]
    print(f"SUCCESS: Found {count} tables in PFE_ODS/PFE_DWH")
    
    cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN ('PFE_ODS','PFE_DWH') ORDER BY TABLE_SCHEMA, TABLE_NAME")
    for row in cursor.fetchall():
        print(f"  {row[0]}.{row[1]}")
    
    conn.close()
    print("Connection closed successfully")
except Exception as e:
    print(f"ERROR: {e}")
