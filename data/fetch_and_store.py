import os
import requests
import pandas as pd
from dotenv import load_dotenv

import psycopg2
#using to connect to db

load_dotenv()

#setting up environment variables
api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

database_url = os.getenv("DATABASE_URL")

#part 1 fetch data from alpha vantage and convert to dataframe

#getting 1 minute interval data for SPY
def fetch_spy_Data(symbol, api_key):


    #for transparency using requests to get data from alpha vantage rathwer than alpha vantage api 
    url = f'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': symbol,
        'interval': '1min',
        'apikey': api_key,
        'outputsize': 'full',
        'datatype': 'json'
    }
    
    print(f"Fetching {symbol} minute data from Alpha Vantage")
    response = requests.get(url, params=params)
    

    #getting data
    if response.status_code != 200:
        raise Exception(f"API request failed with status {response.status_code}")
    
    data = response.json()
    
    if 'Error Message' in data:
        raise Exception(f"API Error: {data['Error Message']}")
    if 'Note' in data:
        raise Exception(f"API Limit: {data['Note']}")
    
    time_series = data["Time Series (1min)"]
    #converting to dataframe
    df = pd.DataFrame.from_dict(time_series, orient="index")
    df.reset_index(inplace=True)
    df.rename(columns={"index": "timestamp"}, inplace=True)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
    #fix timezone time stamp mismatch issue

    df["symbol"] = symbol
    df["price"] = df["4. close"].astype(float)
    df = df[["timestamp", "symbol", "price"]]

    #filtering to last 7 days
 

    # filter to last 7 days
    one_week_ago = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)
    df = df[df["timestamp"] >= one_week_ago]

    print(f"Got {len(df)} rows for {symbol} (last 7 days)")
    return df


#part 2 insert data into database
def insert_data_to_db(df: pd.DataFrame, db_url: str, table_name: str = "SPY_Data"):
    print(f"Inserting {len(df)} rows into table {table_name}...")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    print("Database connection established.")
    try:
        cur.executemany(
            f"""
            INSERT INTO "{table_name}" (timestamp, symbol, price)
            VALUES (%s, %s, %s)
            """,
            df[["timestamp", "symbol", "price"]].values.tolist()
        )
        conn.commit()
        print(f"{len(df)} rows inserted into {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {e}")

    finally:
        cur.close()
        conn.close()
        print("Database connection closed.")


def main():
    symbol = "SPY"
    df = fetch_spy_Data(symbol, api_key)
    insert_data_to_db(df, database_url)


if __name__ == "__main__":
    main()