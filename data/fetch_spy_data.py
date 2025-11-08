import os
import requests
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from typing import Optional
from datetime import datetime

load_dotenv()

api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
database_url = os.getenv("DATABASE_URL")

LOG_FILE = "data_log.csv"

if os.path.exists(LOG_FILE):
    data_log = pd.read_csv(LOG_FILE)
else:
    data_log = pd.DataFrame(columns=["Timestamp", "Step", "Description", "Row Count", "Status", "Error Message"])



#deals with data log
def log_step(step: str, description: str, row_count: Optional[int] = None, status: str = "OK", error_msg: Optional[str] = ""):
    """Add a new log entry."""
    global data_log
    entry = {
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Step": step,
        "Description": description,
        "Row Count": row_count if row_count is not None else "",
        "Status": status,
        "Error Message": error_msg
    }
    data_log = pd.concat([data_log, pd.DataFrame([entry])], ignore_index=True)
    data_log.to_csv(LOG_FILE, index=False)

#get latest timestamp from db to avoid duplicates
def get_latest_timestamp_from_db(db_url: str, table_name: str = "SPY_Data"):
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute(f'SELECT MAX(timestamp) FROM "{table_name}"')
        result = cur.fetchone()[0]
        cur.close()
        conn.close()
        log_step("DB_CHECK", f"Checked latest timestamp in {table_name}", status="OK")
        if result:
            return pd.Timestamp(result, tz="UTC")
        return None
    except Exception as e:
        log_step("DB_CHECK", f"Failed to check timestamp in {table_name}", status="ERROR", error_msg=str(e))
        print(f"Could not get timestamp (table may not exist): {e}")
        return None

#fetch data with time range and deduplication
def fetch_spy_data(symbol: str, api_key: str, time_range: str = 'month', db_url: str = None):
    try:
        valid_ranges = ['min', 'week', 'month', 'max']
        if time_range not in valid_ranges:
            raise ValueError(f"time_range must be one of {valid_ranges}")
        
        outputsize = 'compact' if time_range == 'min' else 'full'
        
        latest_in_db = None
        if db_url:
            latest_in_db = get_latest_timestamp_from_db(db_url)
            if latest_in_db:
                print(f"Latest timestamp in database: {latest_in_db}")
        
        url = 'https://www.alphavantage.co/query'
        params = {
            'function': 'TIME_SERIES_INTRADAY',  
            'symbol': symbol,
            'interval': '1min',
            'apikey': api_key,
            'outputsize': outputsize,
            'datatype': 'json',
            #'adjusted': 'true',         
            'extended_hours': 'false', #only want regular trading hours, saves api costs
   
        }
        
        print(f"Fetching {symbol} data (outputsize={outputsize}, range={time_range}) from Alpha Vantage...")
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            raise Exception(f"API request failed with status {response.status_code}")
        
        data = response.json()
        
        if 'Error Message' in data:
            raise Exception(f"API Error: {data['Error Message']}")
        if 'Note' in data:
            raise Exception(f"API Limit: {data['Note']}")
        
        #getting what we need from the response
        time_series = data["Time Series (1min)"]
        df = pd.DataFrame.from_dict(time_series, orient="index")
        df.reset_index(inplace=True)
        df.rename(columns={"index": "timestamp"}, inplace=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize("UTC")
        df["symbol"] = symbol
        df["price"] = df["4. close"].astype(float)
        df = df[["timestamp", "symbol", "price"]]
        
        log_step("API_FETCH", f"Fetched {len(df)} rows from Alpha Vantage", len(df))
        
        #filter based on time range
        now = pd.Timestamp.now(tz="UTC")
        if time_range == 'week':
            cutoff = now - pd.Timedelta(days=7)
            df = df[df["timestamp"] >= cutoff]
        elif time_range == 'month':
            cutoff = now - pd.Timedelta(days=30)
            df = df[df["timestamp"] >= cutoff]
        
        log_step("FILTER", f"Filtered data for range={time_range}", len(df))
        
        if latest_in_db is not None:
            before = len(df)
            df = df[df["timestamp"] > latest_in_db]
            removed = before - len(df)
            log_step("DEDUP_DB", f"Removed {removed} duplicate rows already in DB", len(df))
        
        if len(df) == 0:
            log_step("DATA_EMPTY", "No new rows to insert", 0)
        
        return df

    except Exception as e:
        log_step("FETCH_ERROR", "Failed to fetch or process data", status="ERROR", error_msg=str(e))
        raise

#insert data into database
def insert_data_to_db(df: pd.DataFrame, db_url: str, table_name: str = "SPY_Data"):
    if len(df) == 0:
        print("No data to insert, skipping database operation.")
        return
    print(f"Inserting {len(df)} rows into table {table_name}...")
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.executemany(
            f"""
            INSERT INTO "{table_name}" (timestamp, symbol, price)
            VALUES (%s, %s, %s)
            ON CONFLICT (timestamp, symbol) DO NOTHING
            """,
            df[["timestamp", "symbol", "price"]].values.tolist()
        )
        conn.commit()
        cur.close()
        conn.close()
        log_step("DB_INSERT", f"Inserted {len(df)} new rows into {table_name}", len(df))
        print("Database insertion completed successfully.")
    except Exception as e:
        log_step("DB_INSERT", f"Database insertion failed for {table_name}", status="ERROR", error_msg=str(e))
        print(f"Error inserting data: {e}")
        raise

#get user time range
def get_user_time_range():
    while True:
        time_range = input("\nWhat data range do you want? (min/week/month/max): ").strip().lower()
        if time_range in ['min', 'week', 'month', 'max']:
            return time_range
        else:
            print(f"Invalid input: '{time_range}'")
            print("Please enter 'min', 'week', 'month', or 'max'")

#main function
def main():
    symbol = "SPY"
    time_range = get_user_time_range()
    
    print(f"\nStarting SPY data fetch for range: {time_range}\n")
    
    try:
        df = fetch_spy_data(symbol, api_key, time_range, database_url)
        insert_data_to_db(df, database_url)
        log_step("COMPLETE", f"Process completed successfully for {symbol}", len(df))
    except Exception as e:
        log_step("FATAL", "Fatal error in main process", status="ERROR", error_msg=str(e))
        print(f"Fatal error: {e}")
    
    print("\nProcess completed!\n")
    log_step("PROCESS_END", "Data fetch and insert process ended")

if __name__ == "__main__":
    main()
