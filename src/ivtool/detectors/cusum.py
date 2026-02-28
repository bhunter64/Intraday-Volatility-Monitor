from typing import Dict
import numpy as np
import pandas as pd

class CUSUM:
    """
    Two-sided CUSUM for mean shifts on a streaming returns x.
    Parameters:
      k: reference value (noise)
      h: decision threshold
    """

    def __init__(self, k: float = 0.5, h: float = 5.0, mu: float = 0.0):
        self.k = float(k)
        self.h = float(h)
        self.reset()
        self.mu = mu

    def reset(self) -> None:
        self.gp = 0.0
        self.gn = 0.0
        self.t = 0

    def update(self, x: float) -> bool:
        self.t += 1
        self.gp = max(0.0, self.gp + x - (self.mu + self.k))
        self.gn = min(0.0, self.gn + x - (self.mu - self.k))
        if self.gp > self.h or self.gn < -self.h:
            self.reset()
            return True
        return False

    def state(self) -> Dict[str, float]:
        return {"gp": self.gp, "gn": self.gn, "t": float(self.t)}


def run_cusum(returns, k, h, mu=0.0):
    detector = CUSUM(k=k, h=h, mu=mu)
    alarms = []
    for x in returns:
        alarms.append(detector.update(float(x)))
    return pd.Series(alarms, index=returns.index)


def main_cusum_run(df: pd.DataFrame, k: float = 0.00005, h: float = 0.0023) -> pd.DataFrame:

    df = df.sort_values("time").reset_index(drop=True)
    prices = df["price"].astype(float)

    returns = np.log(prices / prices.shift(1))
    returns = returns.dropna().reset_index(drop=True)

    timestamps = df["time"].iloc[1:].reset_index(drop=True)
    alarms = run_cusum(returns, k=k, h=h)

    flagged = pd.DataFrame({
        "timestamp": timestamps[alarms],
        "alarm": True
    }).reset_index(drop=True)

    #print("CUSUM run complete. Number of change points detected:", len(flagged))

    return flagged