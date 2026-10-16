import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

server = 'localhost' # Since we are on the host
port = os.getenv('SQL_SERVER_PORT', '1434')
database = os.getenv('SQL_SERVER_DATABASE', 'ATB_BI')
username = os.getenv('SQL_SERVER_USER', 'airbyte_user')
password = os.getenv('SQL_SERVER_PASSWORD', 'AZERTY123')

conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    views_to_check = [
        'vw_superset_executive_overview',
        'vw_superset_client_risk_base',
        'vw_superset_account_portfolio',
        'vw_superset_branch_performance',
        'vw_superset_risk_distribution',
        'vw_superset_ml_predictions'
    ]
    
    print("Checking views in database:")
    for view in views_to_check:
        cursor.execute(f"SELECT COUNT(*) FROM sys.views WHERE name = '{view}'")
        exists = cursor.fetchone()[0] > 0
        print(f"{view}: {'EXISTS' if exists else 'MISSING'}")
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
