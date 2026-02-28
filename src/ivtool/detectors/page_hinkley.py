from typing import Dict
import numpy as np
import pandas as pd
import math
import matplotlib.pyplot as plt
from datetime import datetime

# Read CSV
df = pd.read_csv(
    "/Users/saanvi.sood/quantt/Intraday-Volatility-Monitor/src/ivtool/detectors/SPY_Datafull.csv"
)

df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("timestamp").reset_index(
    drop=True
)  # Sort prices in ascending time order

prices = df["price"].astype(float)

# Compute returns
returns = np.log(prices / prices.shift(1))
returns = returns.dropna().reset_index(drop=True)  # Drop first NaN return


class Page_Hinkley:

    def __init__(
        self,
        t: int = 0,
        high_indices: list = None,
        low_indices: list = None,
        x_std: float = 0.0,
        big_x: float = 0.0,
        s: float = 0.0,
        g_pos: float = 0.0,  # High regime
        g_neg: float = 0.0,  # Low regime
        min: float = 0.0,
        max: float = 0.0,
        s_list: list = None,
        high_list: list = None,
        low_list: list = None,
    ):
        self.t = int(t)
        self.high_indices = [] if high_indices is None else list(high_indices)
        self.low_indices = [] if low_indices is None else list(low_indices)
        self.x_std = float(x_std)
        self.big_x = float(big_x)
        self.s = float(s)
        self.g_pos = float(g_pos)
        self.g_neg = float(g_neg)
        self.min = float(min)
        self.max = float(max)
        self.s_list = [] if s_list is None else list(s_list)
        self.high_list = [] if high_list is None else list(high_list)
        self.low_list = [] if low_list is None else list(low_list)
        self.reset()

    def reset(self) -> None:
        self.x_std = 0.0
        self.s = 0.0
        self.g_pos = 0.0
        self.g_neg = 0.0
        self.min = 0.0
        self.max = 0.0

    def get_f(self, x_std):
        # Constants
        sigma = 0.625188
        mu = -8.288934
        f_x = 1 / (x_std * sigma * (pow(2 * math.pi, 1 / 2)))
        f_low_ln = -pow(math.log(x_std) - (mu - 0.2), 2)
        f_high_ln = -pow(math.log(x_std) - (mu + 0.2), 2)
        f_low = f_x * pow(math.e, f_low_ln / (2 * (pow(sigma, 2))))
        f_high = f_x * pow(math.e, f_high_ln / (2 * (pow(sigma, 2))))
        return (f_low, f_high)

    def update(self, x_std: float, timestamp) -> bool:
        self.t += 1
        (f0, f1) = self.get_f(x_std)
        self.big_x = math.log(f1) - math.log(f0)
        self.s += self.big_x
        self.s_list.append(self.s)
        self.min = min(self.min, self.s)
        self.max = max(self.max, self.s)

        self.g_pos = self.s - self.min
        self.g_neg = self.max - self.s

        # Check for alarms
        if self.g_pos > 250:
            self.high_indices.append(self.t - 1)
            self.reset()
            self.high_list.append(timestamp)
            return True

        elif self.g_neg > 250:
            self.reset()
            self.low_indices.append(self.t - 1)
            self.low_list.append(timestamp)
            return False


def run_ph(returns):
    detector = Page_Hinkley()

    std_list = returns.rolling(window=30, min_periods=30).std().dropna()

    timestamps = df.loc[std_list.index, "timestamp"]

    for x, ts in zip(std_list.dropna(), timestamps):
        detector.update(float(x), ts)

    # Testing

    for i in range(len(detector.high_list)):
        print(len(detector.high_list))
        print(f"{detector.high_list[i]}\n")
        matching_row = df[df["timestamp"] == detector.high_list[i]]
        print(matching_row)

    print("\n")

    for i in range(len(detector.low_list)):
        print(len(detector.low_list))
        print(f"{detector.low_list[i]}\n")
        matching_row = df[df["timestamp"] == detector.low_list[i]]
        print(matching_row)

    return detector


# Plotting

detector = run_ph(returns)

plt.figure()
plt.plot(detector.s_list)
plt.title("Page-Hinkley High vs. Low Regime Flags")
plt.xlabel("Datapoint Number")
plt.ylabel("S")
plt.axhline(y=0, linestyle="dashed", color="r", label="Zero")

plt.scatter(
    detector.high_indices,
    [detector.s_list[i] for i in detector.high_indices],
    marker="o",
    color="r",
    zorder=5,
    label="High flag",
)

plt.scatter(
    detector.low_indices,
    [detector.s_list[i] for i in detector.low_indices],
    marker="x",
    color="r",
    zorder=5,
    label="Low flag",
)

plt.legend()
plt.show()
