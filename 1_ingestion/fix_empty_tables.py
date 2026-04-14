"""Quick fix for ODS_ACCOUNT and ODS_CUSTOMER - direct load"""
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

engine = create_engine(
    "mssql+pyodbc://@DESKTOP-B0PDEI7/ATB_BI?"
    "driver=ODBC+Driver+17+for+SQL+Server&"
    "Trusted_Connection=yes"
)

base_path = Path(__file__).parent.parent / "data" / "raw"

# Fix ODS_ACCOUNT
df_account = pd.read_csv(base_path / "account.csv", sep="|")
print(f"Account: {len(df_account)} rows, {len(df_account.columns)} columns")
df_account.to_sql("ODS_ACCOUNT", engine, schema="PFE_ODS", if_exists="replace", index=False, method="multi", chunksize=5000)

# Fix ODS_CUSTOMER  
df_customer = pd.read_csv(base_path / "customer.csv", sep="|")
print(f"Customer: {len(df_customer)} rows, {len(df_customer.columns)} columns")
df_customer.to_sql("ODS_CUSTOMER", engine, schema="PFE_ODS", if_exists="replace", index=False, method="multi", chunksize=5000)

print("✅ Fixed empty tables!")
