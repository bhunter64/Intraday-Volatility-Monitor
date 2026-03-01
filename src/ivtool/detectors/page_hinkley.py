from __future__ import annotations

import math
import numpy as np
import pandas as pd
from typing import Optional


class Page_Hinkley:

    def __init__(self):
        self.t = 0
        self.high_list = []
        self.low_list = []
        self.reset()

    def reset(self) -> None:
        self.s = 0.0
        self.g_pos = 0.0
        self.g_neg = 0.0
        self.min = 0.0
        self.max = 0.0

    def get_f(self, x_std):
        sigma = 0.625188
        mu = -8.288934
        f_x = 1 / (x_std * sigma * (pow(2 * math.pi, 1 / 2)))
        f_low_ln = -pow(math.log(x_std) - (mu - 0.2), 2)
        f_high_ln = -pow(math.log(x_std) - (mu + 0.2), 2)
        f_low = f_x * pow(math.e, f_low_ln / (2 * (pow(sigma, 2))))
        f_high = f_x * pow(math.e, f_high_ln / (2 * (pow(sigma, 2))))
        return (f_low, f_high)

    def update(self, x_std: float, timestamp) -> Optional[str]:
        self.t += 1
        (f0, f1) = self.get_f(x_std)
        big_x = math.log(f1) - math.log(f0)
        self.s += big_x
        self.min = min(self.min, self.s)
        self.max = max(self.max, self.s)

        self.g_pos = self.s - self.min
        self.g_neg = self.max - self.s

        if self.g_pos > 250:
            self.high_list.append(timestamp)
            self.reset()
            return "high"

        elif self.g_neg > 250:
            self.low_list.append(timestamp)
            self.reset()
            return "low"

        return None


def run_page_hinkley(df: pd.DataFrame):

    df = df.sort_values("time").reset_index(drop=True)
    prices = df["price"].astype(float)

    returns = np.log(prices / prices.shift(1))
    returns = returns.dropna().reset_index(drop=True)

    std_series = returns.rolling(window=30, min_periods=30).std().dropna()
    timestamps = df["time"].iloc[std_series.index]

    detector = Page_Hinkley()
    for x_std, ts in zip(std_series, timestamps):
        detector.update(float(x_std), ts)

    flagged_high = pd.DataFrame({
        "timestamp": detector.high_list,
        "alarm": "high"
    }).reset_index(drop=True)

    flagged_low = pd.DataFrame({
        "timestamp": detector.low_list,
        "alarm": "low"
    }).reset_index(drop=True)
    print("Page-Hinkley run complete. Number of high volatility regimes detected:", len(flagged_high))
    
    return flagged_high, flagged_low





