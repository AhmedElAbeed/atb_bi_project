import pyodbc
import re
sql_file = r"C:\Users\Ahmed\Desktop\atb_bi_project\5_reporting\superset\superset_semantic_layer.sql"
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-B0PDEI7,1434;DATABASE=ATB_BI;UID=airbyte_user;PWD=AZERTY123;Encrypt=yes;TrustServerCertificate=yes;'
print('Using connection string:', conn_str)
conn = pyodbc.connect(conn_str, autocommit=True)
cur = conn.cursor()
print('Connected to SQL Server')
with open(sql_file, 'r', encoding='utf-8') as f:
    sql = f.read()
# split on semicolon followed by optional whitespace/newline
parts = [p.strip() for p in re.split(r';\s*\n', sql) if p.strip()]
print(f'Found {len(parts)} SQL parts')
for i, part in enumerate(parts, 1):
    print(f'Executing part {i}/{len(parts)} (len={len(part)})')
    try:
        cur.execute(part)
    except Exception as e:
        print('Error executing part', i, e)
print('Done')
conn.close()
