import databento as db
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2.extras import execute_values
import logging

logging.basicConfig(
    filename="spy_ingestion.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


load_dotenv()

client = db.Historical(os.getenv("DATABENTO_API_KEY"))
database_url = os.getenv("DATABASE_URL2")


def fetch_from_databento_and_clean(client, symbol="SPY", days_back=7, start_date=None, end_date=None):
    # Determine date range
    if start_date and end_date:
        start = start_date
        end = end_date

    else:
        today_utc = datetime.now(timezone.utc)
        start_utc = today_utc - timedelta(days=days_back)

        # Set end to midnight today to avoid requesting future data
        end_utc = today_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        start = start_utc.isoformat()
        end = end_utc.isoformat()

    logger.info(f"Date range: {start} → {end}")


    # Fetch data from Databento
    data = client.timeseries.get_range(
        dataset="XNAS.ITCH",
        symbols=[symbol],
        schema="ohlcv-1m",
        start=start,
        end=end,
    )

    # Convert to DataFrame
    df = data.to_df().reset_index()
    logger.info(f"Fetched {len(df)} raw rows")

    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["time"] = df["ts_event"].dt.tz_convert("US/Eastern")
    
    # Filter to regular trading hours (09:30–16:00 ET)
    df = (
        df.set_index("time")
        .between_time("09:30", "16:00", inclusive="both")
        .reset_index()
    )
    

    # Rename and select columns
    df = df.rename(columns={"close": "price"})
    df["time"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S %Z")
    df = df[["time", "symbol", "price"]]
    df = df.sort_values("time", ascending=True).reset_index(drop=True)

    logger.info(f"Cleaned data to {len(df)} rows after filtering trading hours.")

    return df

def insert_data_to_db(df: pd.DataFrame, db_url: str, table_name: str):
    if df is None or df.empty:
        logger.warning("No data to insert — skipping DB insert")

        return
    


    rows = df[["time", "symbol", "price"]].values.tolist()
    logger.info(f"Inserting {len(rows)} rows into {table_name}")

    sql = f'''
        INSERT INTO "{table_name}" ("time", "symbol", "price")
        VALUES %s
        ON CONFLICT ("time", "symbol") DO NOTHING
    '''

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=10_000)
        conn.commit()
        logger.info("Database insert committed successfully")
    except Exception as e:
        logger.exception("Database insert failed")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    logger.info("Starting SPY ingestion job")

    df_cleaned = fetch_from_databento_and_clean(client, days_back=7)
    insert_data_to_db(df_cleaned, database_url, table_name="SPY_DATA_V2")
    logger.info("SPY ingestion job completed")

    

