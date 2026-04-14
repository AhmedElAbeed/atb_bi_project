"""
CSV Ingestion Script - Load CSV files to SQL Server PFE_ODS schema
This is a lightweight alternative to Airbyte for low-bandwidth environments.
"""

import pandas as pd
import pyodbc
import os
from pathlib import Path

# Configuration
SQL_SERVER = "DESKTOP-B0PDEI7"
DATABASE = "ATB_BI"
SCHEMA = "PFE_ODS"
DATA_PATH = Path(__file__).parent.parent / "data" / "raw"

# CSV files to load
CSV_FILES = {
    "account.csv": "ODS_ACCOUNT",
    "customer.csv": "ODS_CUSTOMER",
    "currency.csv": "ODS_CURRENCY",
    "dao.csv": "ODS_DAO",
    "industry.csv": "ODS_INDUSTRY",
    "sector.csv": "ODS_SECTOR",
    "target.csv": "ODS_TARGET",
}

def get_connection():
    """Create SQL Server connection using Windows authentication."""
    conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={SQL_SERVER};Database={DATABASE};Trusted_Connection=yes;"
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        print(f"Error connecting to SQL Server: {e}")
        raise

def load_csv_to_sql(csv_file, table_name, conn):
    """Load CSV file into SQL Server table."""
    csv_path = DATA_PATH / csv_file
    
    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        return False
    
    try:
        # Read CSV (pipe-delimited)
        df = pd.read_csv(csv_path, sep="|")
        print(f"📖 Loaded {csv_file}: {len(df)} rows, {len(df.columns)} columns")
        
        # Create fully qualified table name
        full_table_name = f"{SCHEMA}.{table_name}"
        
        # Load to SQL Server
        from sqlalchemy import create_engine
        engine = create_engine(
            f"mssql+pyodbc://@{SQL_SERVER}/{DATABASE}?"
            f"driver=ODBC+Driver+17+for+SQL+Server&"
            f"Trusted_Connection=yes"
        )
        
        df.to_sql(
            table_name,
            con=engine,
            schema=SCHEMA,
            if_exists="replace",  # Drop and recreate if exists
            index=False,
            method="multi",
            chunksize=1000
        )
        
        print(f"✅ {table_name}: {len(df)} rows inserted")
        return True
        
    except Exception as e:
        print(f"❌ Error loading {csv_file}: {e}")
        return False

def validate_ingestion(conn):
    """Validate all tables were created in PFE_ODS."""
    try:
        cursor = conn.cursor()
        query = f"""
        SELECT TABLE_NAME, 
               (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA='[PFE_ODS]' AND TABLE_NAME=t.TABLE_NAME) AS col_count
        FROM INFORMATION_SCHEMA.TABLES t
        WHERE TABLE_SCHEMA = 'PFE_ODS'
        ORDER BY TABLE_NAME
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print("\n" + "="*60)
        print("VALIDATION SUMMARY")
        print("="*60)
        print(f"{'Table Name':<25} {'Columns':<10} {'Rows':<10}")
        print("-"*60)
        
        total_rows = 0
        for table_name, col_count in results:
            # Get row count for each table
            row_query = f"SELECT COUNT(*) FROM [PFE_ODS].[{table_name}]"
            row_cursor = conn.cursor()
            row_cursor.execute(row_query)
            row_count = row_cursor.fetchone()[0]
            
            print(f"{table_name:<25} {col_count:<10} {row_count:<10}")
            total_rows += row_count
        
        print("-"*60)
        print(f"Total tables: {len(results)}")
        print(f"Total rows loaded: {total_rows}")
        print("="*60)
        
    except Exception as e:
        print(f"Error validating ingestion: {e}")

def main():
    """Main ETL process."""
    print("\n" + "="*60)
    print("CSV INGESTION TO SQL SERVER")
    print("="*60)
    
    # Connect to SQL Server
    try:
        conn = get_connection()
        cursor = conn.cursor()
        print(f"✅ Connected to {SQL_SERVER}\\{DATABASE}")
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        return
    
    # Load each CSV
    print(f"\nLoading CSVs from: {DATA_PATH}\n")
    loaded_count = 0
    
    for csv_file, table_name in CSV_FILES.items():
        if load_csv_to_sql(csv_file, table_name, conn):
            loaded_count += 1
    
    print(f"\n{loaded_count}/{len(CSV_FILES)} files loaded successfully")
    
    # Validate
    print("\nValidating tables in PFE_ODS...")
    validate_ingestion(conn)
    
    conn.close()
    print("\n✅ Ingestion complete!")

if __name__ == "__main__":
    main()
