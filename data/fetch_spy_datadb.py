import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
 
load_dotenv()
database_url = os.getenv("DATABASE_URL2")
table_name = "SPY_DATA_V2"
 
 
try:
    conn = psycopg2.connect(database_url)
    #query = f'SELECT * FROM "{table_name}" ORDER BY time DESC;' 
    #full csv with all data

    #top 100 rows only, modify as needed
    query = f'SELECT * FROM "{table_name}" ORDER BY time ASC;'

 
    df = pd.read_sql(query, conn)
 
    print(f"Fetched {len(df)} rows from '{table_name}':\n")
    print(df.head())
 
    df.to_csv(f"{table_name}full.csv", index=False)
    print("done")
except Exception as e:
    print(f"error fetching data: {e}")
finally:
    if 'conn' in locals():
        conn.close()