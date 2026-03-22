import os
from dataclasses import dataclass
from itertools import product

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from src.ivtool.detectors.bocpe import main_bocpe_run
from src.ivtool.detectors.cusum import main_cusum_run
from src.ivtool.detectors.page_hinkley import run_page_hinkley


@dataclass(frozen=True)
class CalibrationChoice:
    name: str
    params: dict
    day_flags: set[pd.Timestamp]
    minute_count: int
    output: pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]


DEFAULT_CUSUM_PARAMS = {"k": 0.00005, "h": 0.0023}
DEFAULT_BOCPE_PARAMS = {
    "hazard": 1 / (390 * 3),
    "threshold": 0.5,
    "vol_threshold": 0.0003,
    "max_run_length": 1200,
}
DEFAULT_PAGE_HINKLEY_PARAMS = {"alarm_threshold": 250.0}

CALIBRATION_GRID = {
    "cusum": [{"k": DEFAULT_CUSUM_PARAMS["k"], "h": h} for h in [0.0018, 0.0021, 0.0023, 0.0026, 0.003]],
    "bocpe": [
        {
            "hazard": DEFAULT_BOCPE_PARAMS["hazard"],
            "threshold": threshold,
            "vol_threshold": vol_threshold,
            "max_run_length": DEFAULT_BOCPE_PARAMS["max_run_length"],
        }
        for threshold, vol_threshold in product([0.4, 0.5, 0.6], [0.0002, 0.0003, 0.0004])
    ],
    "page_hinkley": [{"alarm_threshold": threshold} for threshold in [180.0, 220.0, 250.0, 280.0, 320.0]],
}


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


def _timestamps_to_utc(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.to_datetime(series, utc=True)


def _timestamps_to_day_flags(series: pd.Series) -> set[pd.Timestamp]:
    timestamps = _timestamps_to_utc(series)
    if timestamps.empty:
        return set()
    return set(timestamps.dt.normalize().tolist())


def _high_regime_minutes(high_timestamps: list[pd.Timestamp], low_timestamps: list[pd.Timestamp], final_timestamp) -> pd.DataFrame:
    all_flagged: list[dict[str, pd.Timestamp | str]] = []
    market_open = pd.Timestamp("13:30").time()
    market_close = pd.Timestamp("21:00").time()

    for high_ts in high_timestamps:
        next_low = next((low for low in low_timestamps if low > high_ts), None)
        end_ts = final_timestamp if next_low is None else next_low

        for ts in pd.date_range(start=high_ts, end=end_ts, freq="1min", tz="UTC"):
            if ts.weekday() < 5 and market_open <= ts.time() <= market_close:
                all_flagged.append({"timestamp": ts, "regime": "high volatility"})

    if not all_flagged:
        return pd.DataFrame(columns=["timestamp", "regime"])

    return pd.DataFrame(all_flagged).drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)


def page_hinkley_high_risk_regimes(flagged_high_ph: pd.DataFrame, flagged_low_ph: pd.DataFrame) -> pd.DataFrame:
    high_timestamps = sorted(_timestamps_to_utc(flagged_high_ph.get("timestamp", pd.Series(dtype=object))).tolist())
    low_timestamps = sorted(_timestamps_to_utc(flagged_low_ph.get("timestamp", pd.Series(dtype=object))).tolist())

    if not high_timestamps:
        print("No high volatility regions found.")
        return pd.DataFrame(columns=["timestamp", "regime"])

    final_timestamp = max(high_timestamps + low_timestamps)
    page_hinkley_result = _high_regime_minutes(high_timestamps, low_timestamps, final_timestamp)
    print(f"High volatility minutes identified in Page-Hinkley: {len(page_hinkley_result)}")
    return page_hinkley_result



def bocpe_high_risk_regimes(flagged_bocpe: pd.DataFrame) -> pd.DataFrame:
    high_mask = flagged_bocpe.get("new_regime", pd.Series(dtype=object)) == "High Volatility"
    low_mask = flagged_bocpe.get("new_regime", pd.Series(dtype=object)) == "Low Volatility"

    high_timestamps = sorted(_timestamps_to_utc(flagged_bocpe.loc[high_mask, "timestamp"] if "timestamp" in flagged_bocpe else pd.Series(dtype=object)).tolist())
    low_timestamps = sorted(_timestamps_to_utc(flagged_bocpe.loc[low_mask, "timestamp"] if "timestamp" in flagged_bocpe else pd.Series(dtype=object)).tolist())

    if not high_timestamps:
        print("No high volatility regions found.")
        return pd.DataFrame(columns=["timestamp", "regime"])

    final_timestamp = max(high_timestamps + low_timestamps)
    result = _high_regime_minutes(high_timestamps, low_timestamps, final_timestamp)
    print(f"High volatility minutes identified in BOCPE: {len(result)}")
    return result



def _evaluate_cusum_candidate(df: pd.DataFrame, params: dict) -> CalibrationChoice:
    flagged_cusum = main_cusum_run(df, **params)
    day_flags = _timestamps_to_day_flags(flagged_cusum["timestamp"])
    return CalibrationChoice(
        name="cusum",
        params=params,
        day_flags=day_flags,
        minute_count=len(flagged_cusum),
        output=flagged_cusum,
    )



def _evaluate_bocpe_candidate(df: pd.DataFrame, params: dict) -> CalibrationChoice:
    flagged_bocpe = main_bocpe_run(df, **params)
    bocpe_result = bocpe_high_risk_regimes(flagged_bocpe)
    day_flags = _timestamps_to_day_flags(bocpe_result["timestamp"])
    return CalibrationChoice(
        name="bocpe",
        params=params,
        day_flags=day_flags,
        minute_count=len(bocpe_result),
        output=flagged_bocpe,
    )



def _evaluate_page_hinkley_candidate(df: pd.DataFrame, params: dict) -> CalibrationChoice:
    flagged_high_ph, flagged_low_ph = run_page_hinkley(df, **params)
    page_hinkley_result = page_hinkley_high_risk_regimes(flagged_high_ph, flagged_low_ph)
    day_flags = _timestamps_to_day_flags(page_hinkley_result["timestamp"])
    return CalibrationChoice(
        name="page_hinkley",
        params=params,
        day_flags=day_flags,
        minute_count=len(page_hinkley_result),
        output=(flagged_high_ph, flagged_low_ph),
    )



def _calibration_score(day_flag_sets: list[set[pd.Timestamp]], minute_counts: list[int]) -> float:
    day_counts = np.array([len(flags) for flags in day_flag_sets], dtype=float)
    if np.any(day_counts == 0):
        return float("inf")

    average_day_count = float(day_counts.mean())
    spread_penalty = float((day_counts.max() - day_counts.min()) / average_day_count)

    union_days = set().union(*day_flag_sets)
    if not union_days:
        return float("inf")

    disagreement_count = 0
    for day in union_days:
        states = [day in flags for flags in day_flag_sets]
        disagreement_count += len(set(states)) > 1
    disagreement_penalty = disagreement_count / len(union_days)

    minutes = np.array(minute_counts, dtype=float)
    minute_penalty = float((minutes.max() - minutes.min()) / max(minutes.mean(), 1.0))

    return spread_penalty + disagreement_penalty + (0.15 * minute_penalty)



def calibrate_detectors(df: pd.DataFrame) -> dict[str, CalibrationChoice]:
    print("Calibrating detector thresholds so the three models behave comparably...")
    cusum_choices = [_evaluate_cusum_candidate(df, params) for params in CALIBRATION_GRID["cusum"]]
    bocpe_choices = [_evaluate_bocpe_candidate(df, params) for params in CALIBRATION_GRID["bocpe"]]
    page_hinkley_choices = [_evaluate_page_hinkley_candidate(df, params) for params in CALIBRATION_GRID["page_hinkley"]]

    best_combo: tuple[CalibrationChoice, CalibrationChoice, CalibrationChoice] | None = None
    best_score = float("inf")

    for combo in product(cusum_choices, bocpe_choices, page_hinkley_choices):
        score = _calibration_score(
            [choice.day_flags for choice in combo],
            [choice.minute_count for choice in combo],
        )
        if score < best_score:
            best_score = score
            best_combo = combo

    if best_combo is None:
        raise RuntimeError("Unable to calibrate detector thresholds")

    calibrated = {choice.name: choice for choice in best_combo}
    for name, choice in calibrated.items():
        print(
            f"Selected {name} parameters {choice.params} with {len(choice.day_flags)} flagged days and {choice.minute_count} flagged minutes/events."
        )
    return calibrated



def detect_events(df: pd.DataFrame):
    calibrated = calibrate_detectors(df)
    flagged_cusum = calibrated["cusum"].output
    flagged_bocpe = calibrated["bocpe"].output
    flagged_high_ph, flagged_low_ph = calibrated["page_hinkley"].output

    return {
        "calibration": {name: choice.params for name, choice in calibrated.items()},
        "flagged_cusum": flagged_cusum,
        "flagged_bocpe": flagged_bocpe,
        "flagged_high_ph": flagged_high_ph,
        "flagged_low_ph": flagged_low_ph,
    }



def high_risk_regimes(flagged_cusum: pd.DataFrame, bocpe_result: pd.DataFrame, page_hinkley_result: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat(
        [
            flagged_cusum["timestamp"],
            bocpe_result["timestamp"],
            page_hinkley_result["timestamp"],
        ]
    )
    counts = combined.value_counts().reset_index()
    counts.columns = ["timestamp", "detector_count"]
    high_risk = counts[counts["detector_count"] >= 2][["timestamp"]].copy()
    high_risk["regime"] = "high risk"
    high_risk = high_risk.sort_values("timestamp").reset_index(drop=True)
    print(f"High risk regimes identified: {len(high_risk)} regimes.")
    return high_risk



def disagreement_days(flagged_cusum: pd.DataFrame, bocpe_result: pd.DataFrame, page_hinkley_result: pd.DataFrame) -> pd.DataFrame:
    cusum_days = _timestamps_to_day_flags(flagged_cusum["timestamp"])
    bocpe_days = _timestamps_to_day_flags(bocpe_result["timestamp"])
    page_hinkley_days = _timestamps_to_day_flags(page_hinkley_result["timestamp"])

    all_days = sorted(cusum_days | bocpe_days | page_hinkley_days)
    rows = []
    for day in all_days:
        cusum_flag = day in cusum_days
        bocpe_flag = day in bocpe_days
        page_hinkley_flag = day in page_hinkley_days
        disagreement = len({cusum_flag, bocpe_flag, page_hinkley_flag}) > 1
        if disagreement:
            rows.append(
                {
                    "date": day.date().isoformat(),
                    "cusum_flag": cusum_flag,
                    "bocpe_flag": bocpe_flag,
                    "page_hinkley_flag": page_hinkley_flag,
                    "models_flagging": int(cusum_flag) + int(bocpe_flag) + int(page_hinkley_flag),
                }
            )

    result = pd.DataFrame(rows)
    print(f"Model disagreement days identified: {len(result)} days.")
    return result



def main():
    df = get_data()
    detection_results = detect_events(df)
    flagged_cusum = detection_results["flagged_cusum"]
    flagged_bocpe = detection_results["flagged_bocpe"]
    flagged_high_ph = detection_results["flagged_high_ph"]
    flagged_low_ph = detection_results["flagged_low_ph"]

    page_hinkley_result = page_hinkley_high_risk_regimes(flagged_high_ph, flagged_low_ph)
    bocpe_result = bocpe_high_risk_regimes(flagged_bocpe)
    high_risk = high_risk_regimes(flagged_cusum, bocpe_result, page_hinkley_result)
    disagreement = disagreement_days(flagged_cusum, bocpe_result, page_hinkley_result)

    high_risk.to_csv("high_risk_regimes.csv", index=False)
    disagreement.to_csv("model_flag_disagreements.csv", index=False)
    pd.DataFrame(
        [
            {"model": model, **params}
            for model, params in detection_results["calibration"].items()
        ]
    ).to_csv("detector_calibration.csv", index=False)
    return high_risk, disagreement


if __name__ == "__main__":
    main()
