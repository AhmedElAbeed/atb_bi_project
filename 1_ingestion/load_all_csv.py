"""
Robust CSV Loader - handles mixed types and NULL values
"""
import pandas as pd
import pyodbc
from pathlib import Path

# Connection setup
DRIVER = "ODBC Driver 17 for SQL Server"
SERVER = "DESKTOP-B0PDEI7"
DATABASE = "ATB_BI"
conn_str = f"Driver={{{DRIVER}}};Server={SERVER};Database={DATABASE};Trusted_Connection=yes;"

def connect_sql():
    """Create SQL connection."""
    conn = pyodbc.connect(conn_str)
    conn.setencoding(encoding='utf-8')
    return conn

def load_csv_bulk(csv_file, table_name, schema="PFE_ODS"):
    """Load CSV using BCP-style bulk insert via pyodbc."""
    try:
        csv_path = Path(__file__).parent.parent / "data" / "raw" / csv_file
        
        if not csv_path.exists():
            print(f"❌ {csv_file} not found")
            return False
        
        # Read CSV with minimal type conversion
        df = pd.read_csv(csv_path, sep="|", dtype=str)
        df = df.fillna("")  # Replace NaN with empty string
        
        print(f"📖 {csv_file}: {len(df)} rows, {len(df.columns)} cols")
        
        conn = connect_sql()
        cursor = conn.cursor()
        
        # Drop existing table
        drop_sql = f"IF EXISTS (SELECT 1 FROM sys.objects WHERE OBJECT_ID = OBJECT_ID('{schema}.{table_name}')) DROP TABLE [{schema}].[{table_name}]"
        cursor.execute(drop_sql)
        conn.commit()
        
        # Create table with all VARCHAR columns
        cols = ", ".join([f"[{col}] VARCHAR(max)" for col in df.columns])
        create_sql = f"CREATE TABLE [{schema}].[{table_name}] ({cols})"
        cursor.execute(create_sql)
        conn.commit()
        print(f"✅ Created table {table_name}")
        
        # Bulk insert in chunks
        chunk_size = 5000
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            
            # Prepare INSERT VALUES
            placeholders = ", ".join(["?" for _ in df.columns])
            col_names = ", ".join([f"[{col}]" for col in df.columns])
            sql = f"INSERT INTO [{schema}].[{table_name}] ({col_names}) VALUES ({placeholders})"
            
            # Convert rows to tuples
            data = [tuple(row) for _, row in chunk.iterrows()]
            
            cursor.executemany(sql, data)
            conn.commit()
            print(f"  ✓ Inserted rows {i+1}-{min(i+chunk_size, len(df))}")
        
        conn.close()
        
        # Verify
        conn = connect_sql()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table_name}]")
        count = cursor.fetchone()[0]
        print(f"✅ {table_name}: {count} rows successfully loaded\n")
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading {csv_file}: {e}\n")
        return False

def main():
    """Load all CSV files."""
    print("="*60)
    print("ROBUST CSV INGESTION")
    print("="*60 + "\n")
    
    files = {
        "account.csv": "ODS_ACCOUNT",
        "customer.csv": "ODS_CUSTOMER",
        "currency.csv": "ODS_CURRENCY",
        "dao.csv": "ODS_DAO",
        "industry.csv": "ODS_INDUSTRY",
        "sector.csv": "ODS_SECTOR",
        "target.csv": "ODS_TARGET",
    }
    
    results = {}
    for csv_file, table_name in files.items():
        results[csv_file] = load_csv_bulk(csv_file, table_name)
    
    # Summary
    print("="*60)
    print("SUMMARY")
    print("="*60)
    success = sum(1 for v in results.values() if v)
    print(f"✅ {success}/{len(files)} files loaded successfully")
    for csv, result in results.items():
        status = "✅" if result else "❌"
        print(f"{status} {csv}")
    print("="*60)

if __name__ == "__main__":
    main()
