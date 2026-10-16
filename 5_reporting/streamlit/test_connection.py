"""Test script to verify SQL Server connection."""
import pyodbc
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("=" * 60)
print("Testing SQL Server Connection")
print("=" * 60)

# Get connection parameters
server = os.getenv("ATB_SQL_SERVER", "DESKTOP-B0PDEI7")
database = os.getenv("ATB_SQL_DATABASE", "ATB_BI")
username = os.getenv("ATB_SQL_USERNAME", "airbyte_user")
password = os.getenv("ATB_SQL_PASSWORD", "AZERTY123")
port = os.getenv("ATB_SQL_PORT", "1434")

print(f"\nConnection Details:")
print(f"  Server: {server}")
print(f"  Database: {database}")
print(f"  Username: {username}")
print(f"  Port: {port}")

# Get driver from environment or use default
driver = os.getenv("ATB_SQL_DRIVER", "ODBC Driver 17 for SQL Server")

# Test connection string with TrustServerCertificate
conn_str = (
    f"DRIVER={{{driver}}};"
    f"SERVER={server},{port};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    f"TrustServerCertificate=yes;"
    f"Encrypt=no;"
)

try:
    print("\nAttempting connection...")
    conn = pyodbc.connect(conn_str)
    print("✅ Connection successful!")
    
    # Test query
    cursor = conn.cursor()
    cursor.execute("SELECT @@VERSION")
    version = cursor.fetchone()
    print(f"\nSQL Server Version:\n{version[0][:100]}...")
    
    # List available schemas
    cursor.execute("""
    SELECT DISTINCT s.name 
    FROM sys.schemas s
    WHERE s.name IN ('PFE_ODS', 'PFE_DWH', 'PFE_ML', 'dbo')
    ORDER BY s.name
    """)
    schemas = cursor.fetchall()
    print(f"\nAvailable Schemas:")
    for schema in schemas:
        print(f"  - {schema.name}")
    
    # Check for key tables
    cursor.execute("""
    SELECT 
        s.name as schema_name,
        t.name as table_name,
        p.rows as row_count
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
    WHERE s.name IN ('PFE_DWH', 'PFE_ODS')
    ORDER BY s.name, t.name
    """)
    tables = cursor.fetchall()
    print(f"\nKey Tables:")
    for table in tables[:10]:  # Show first 10
        print(f"  - {table.schema_name}.{table.table_name}: {table.row_count:,} rows")
    
    conn.close()
    print("\n✅ All tests passed! Connection is working properly.")
    
except Exception as e:
    print(f"\n❌ Connection failed!")
    print(f"Error: {e}")
    print("\nTroubleshooting steps:")
    print("1. Make sure SQL Server is running")
    print("2. Check if the server name is correct")
    print("3. Verify the database exists")
    print("4. Ensure the user has proper permissions")
    print("5. Check firewall settings")

print("\n" + "=" * 60)