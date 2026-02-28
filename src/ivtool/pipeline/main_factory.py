import os
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
    print("Running event detection algorithms...")
    print("Running CUSUM...")
    flagged_cusum = main_cusum_run(df)
    print("Running BOCPE...")
    flagged_bocpe = main_bocpe_run(df)
    print("Running Page-Hinkley...")
    flagged_high_ph, flagged_low_ph = run_page_hinkley(df)
    return flagged_cusum, flagged_bocpe, flagged_high_ph, flagged_low_ph


def high_risk_regimes(flagged_cusum, flagged_bocpe, flagged_high_ph, flagged_low_ph):
    print("Identifying high risk regimes...")
    combined = pd.concat([
        flagged_cusum["timestamp"],
        flagged_bocpe["timestamp"],
        flagged_low_ph["timestamp"]
    ])
    counts = combined.value_counts().reset_index()
    counts.columns = ["timestamp", "detector_count"]
    overlap_high = counts[counts["detector_count"] >= 2][["timestamp"]].copy()
    overlap_high["regime"] = "high risk"
    always_high = flagged_high_ph[["timestamp"]].copy()
    always_high["regime"] = "high risk"
    high_risk = pd.concat([overlap_high, always_high]).drop_duplicates("timestamp")
    high_risk = high_risk.sort_values("timestamp").reset_index(drop=True)
    print(f"High risk regimes identified: {len(high_risk)} regimes.")
    return high_risk


def main():
    df = get_data()
    flagged_cusum, flagged_bocpe, flagged_high_ph, flagged_low_ph = detect_events(df)
    high_risk = high_risk_regimes(flagged_cusum, flagged_bocpe, flagged_high_ph, flagged_low_ph)
    high_risk.to_csv("high_risk.csv", index=False)
    return high_risk

if __name__ == "__main__":
    main()


