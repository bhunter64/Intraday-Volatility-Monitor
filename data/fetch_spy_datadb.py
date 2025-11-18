import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
 
load_dotenv()
database_url = os.getenv("DATABASE_URL")
table_name = "SPY_Data"
 
try:
    conn = psycopg2.connect(database_url)
    query = f'SELECT * FROM "{table_name}" ORDER BY timestamp DESC;'
 
    df = pd.read_sql(query, conn)
 
    print(f"Fetched {len(df)} rows from '{table_name}':\n")
    print(df.head())
 
    df.to_csv(f"{table_name}full.csv", index=False)
    print("done")
except Exception as e:
    print(f"error fetchi g")
finally:
    if 'conn' in locals():
        conn.close()