import os
from unittest import result
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from src.ivtool.detectors.cusum import main_cusum_run
from src.ivtool.detectors.bocpe import main_bocpe_run
from src.ivtool.detectors.page_hinkley import run_page_hinkley

def get_data():
    load_dotenv()
    print("Loading data from database...")
    database_url = os.getenv("DATABASE_URL2")
    table_name = "SPY_DATA_V2"

    if database_url is None:
        raise ValueError("DATABASE_URL2 not found in environment variables")
    query = f'SELECT * FROM "{table_name}" ORDER BY time ASC;'
    with psycopg2.connect(database_url) as conn:
        df = pd.read_sql(query, conn)
    print("Data loaded successfully.")
    return df


def detect_events(df: pd.DataFrame) -> pd.DataFrame:
    print("Running CUSUM...")
    flagged_cusum = main_cusum_run(df)
    print("Running BOCPE...")
    flagged_bocpe = main_bocpe_run(df)
    print("Running Page-Hinkley...")
    flagged_high_ph, flag_low_ph = run_page_hinkley(df)
   
    return flagged_cusum, flagged_bocpe, flagged_high_ph, flag_low_ph


def page_hinkley_high_risk_regimes(flagged_high_ph, flagged_low_ph):
    high_timestamps = sorted(flagged_high_ph["timestamp"].tolist())
    low_timestamps = sorted(flagged_low_ph["timestamp"].tolist())
    all_flagged = []

    # NYSE trading hours in UTC (13:30 - 21:00 UTC), Monday to Friday (did 13:30 - 21:00 to be safe as need to take into account dst)
    market_open = pd.Timestamp("13:30").time()
    market_close = pd.Timestamp("21:00").time()

    for high_ts in high_timestamps:
        next_low = next((low for low in low_timestamps if low > high_ts), None)
        end_ts = flagged_high_ph["timestamp"].max() if next_low is None else next_low

        minute_range = pd.date_range(start=high_ts, end=end_ts, freq="1min", tz="UTC")

        for ts in minute_range:
            if ts.weekday() < 5 and market_open <= ts.time() <= market_close:
                all_flagged.append({"timestamp": ts, "regime": "high volatility"})

    if not all_flagged:
        print("No high volatility regions found.")
        return pd.DataFrame(columns=["timestamp", "regime"])

    page_hinkley_result = pd.DataFrame(all_flagged).drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)
    print(f"High volatility minutes identified in Page-Hinkley: {len(page_hinkley_result)}")
    return page_hinkley_result



def bocpe_high_risk_regimes(flagged_bocpe):
    high_timestamps = sorted(flagged_bocpe[flagged_bocpe["new_regime"] == "High Volatility"]["timestamp"].tolist())
    low_timestamps = sorted(flagged_bocpe[flagged_bocpe["new_regime"] == "Low Volatility"]["timestamp"].tolist())
    
    all_flagged = []

    # NYSE trading hours in UTC (13:30 - 21:00 UTC), Monday to Friday
    market_open = pd.Timestamp("13:30").time()
    market_close = pd.Timestamp("21:00").time()
    


    for high_ts in high_timestamps:
        next_low = next((low for low in low_timestamps if low > high_ts), None)
        end_ts = flagged_bocpe["timestamp"].max() if next_low is None else next_low
        
        for ts in pd.date_range(start=high_ts, end=end_ts, freq="1min", tz="UTC"):
            if ts.weekday() < 5 and market_open <= ts.time() <= market_close:
                all_flagged.append({"timestamp": ts, "regime": "high volatility"})
    
    if not all_flagged:
        print("No high volatility regions found.")
        return pd.DataFrame(columns=["timestamp", "regime"])
    
    result = pd.DataFrame(all_flagged).drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)
    print(f"High volatility minutes identified in BOCPE: {len(result)}")
    return result




def high_risk_regimes(flagged_cusum, bocpe_result, page_hinkley_result):
    combined = pd.concat([
        flagged_cusum["timestamp"],
        bocpe_result["timestamp"],
        page_hinkley_result["timestamp"]
    ])
    counts = combined.value_counts().reset_index()
    counts.columns = ["timestamp", "detector_count"]
    high_risk = counts[counts["detector_count"] >= 2][["timestamp"]].copy()
    high_risk["regime"] = "high risk"
    high_risk = high_risk.sort_values("timestamp").reset_index(drop=True)
    print(f"High risk regimes identified: {len(high_risk)} regimes.")
    return high_risk


def main():
    df = get_data()
    flagged_cusum, flagged_bocpe, flagged_high_ph, flagged_low_ph = detect_events(df)
    page_hinkley_result = page_hinkley_high_risk_regimes(flagged_high_ph, flagged_low_ph)
    bocpe_result = bocpe_high_risk_regimes(flagged_bocpe)
    high_risk = high_risk_regimes(flagged_cusum, bocpe_result, page_hinkley_result)
    high_risk.to_csv("high_risk_regimes.csv", index=False)
    return high_risk

if __name__ == "__main__":
    main()


