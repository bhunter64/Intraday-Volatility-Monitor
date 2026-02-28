from typing import Dict
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Read CSV
df = pd.read_csv("SPY_Datafull.csv")

df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("timestamp").reset_index(
    drop=True
)  # Sort prices in ascending time order

prices = df["price"].astype(float)

# Compute returns
returns = np.log(prices / prices.shift(1))
returns = returns.dropna().reset_index(drop=True)  # Drop first NaN return

# Wasn't sure how to define a true event, used 2 stds for now
threshold = 2 * returns.std()
event_mask = returns.abs() > threshold
event_mask = event_mask.astype(bool)

# The default k and h values in the class are just placeholders.
# We'll need to add the tuned values from Matt and Gavin (I think)


class CUSUM:
    """
    Two-sided CUSUM for mean shifts on a streaming returns x.
    Parameters:
      k: reference value (noise)
      h: decision threshold
    """

    def __init__(self, k: float = 0.5, h: float = 5.0, mu: float = 0.0):
        self.k = float(k)  # drift
        self.h = float(h)  # threshold
        self.reset()
        self.mu = mu  # mean return

    def reset(self) -> None:
        self.gp = 0.0  # positive (up) cumulative sum
        self.gn = 0.0  # negative (down) cumulative sum
        self.t = 0

    def update(self, x: float) -> bool:
        """
        Takes a single data point 'x'.
        Returns True if the Volatility Spike (gp) triggers the alarm.
        """
        self.t += 1

        # Compare input 'x' against the target 'mu'
        self.gp = max(0.0, self.gp + x - (self.mu + self.k))

        self.gn = min(0.0, self.gn + x - (self.mu - self.k))

        # Check for Alarms
        # Trigger if volatility spikes or crashes
        if self.gp > self.h or self.gn < -self.h:
            self.reset()
            return True

        return False

    def state(self) -> Dict[str, float]:
        """
        Returns the internal state.
        Required so the system can log what the detector is thinking.
        """
        return {"gp": self.gp, "gn": self.gn, "t": float(self.t)}


# Plotting/tuning (Saanvi)


# Run CUSUM function to loop through all returns in CSV
# Will produce array of triggered alarms (True = alarm, False = none)
def run_cusum(returns, k, h, mu=0.0):
    detector = CUSUM(k=k, h=h, mu=mu)
    alarms = []
    for x in returns:
        alarms.append(detector.update(float(x)))
    return pd.Series(alarms, index=returns.index)


def evaluate_hits(alarms, event_mask):
    alarms = alarms.astype(bool)
    event_mask = event_mask.astype(bool)

    # tp = true positive, fp = false positive, fn = false negative, tn = true negative
    tp = ((alarms == True) & (event_mask == True)).sum()
    fp = ((alarms == True) & (event_mask == False)).sum()
    fn = ((alarms == False) & (event_mask == True)).sum()
    tn = ((alarms == False) & (event_mask == False)).sum()

    # Calculate hit rate
    if (tp + fn) > 0:
        hit_rate = tp / (tp + fn)
    else:
        hit_rate = 0

    # False alarm rate
    if (fp + tn) > 0:
        false_alarm_rate = fp / (fp + tn)
    else:
        false_alarm_rate = 0

    return hit_rate, false_alarm_rate


# Parameter tuning
results = []

k_values = [0.00001, 0.00005, 0.0001, 0.0005]
h_values = [0.0002, 0.0005, 0.001, 0.005]

for k in k_values:
    for h in h_values:
        alarms = run_cusum(returns, k=k, h=h)
        hit, far = evaluate_hits(alarms, event_mask)
        results.append((k, h, hit, far))

df = pd.DataFrame(results, columns=["k", "h", "hit_rate", "false_alarm_rate"])

# Trade-off curve
df_sorted = df.sort_values("false_alarm_rate")
plt.figure(figsize=(10, 7))
plt.scatter(df["false_alarm_rate"], df["hit_rate"])

# False-alarm target line
plt.axvline(
    0.05, color="red", linestyle="--", label="False-alarm target (5%)", linewidth=2
)

plt.plot(
    df_sorted["false_alarm_rate"],
    df_sorted["hit_rate"],
    "-o",
    alpha=0.8,
    markersize=6,
    label="CUSUM performance curve",
)

# Axis labels and formatting
plt.xlabel("False Alarm Rate", fontsize=14)
plt.ylabel("Hit Rate", fontsize=14)
plt.title("CUSUM Trade-Off Curve", fontsize=16)
plt.grid(alpha=0.3)
plt.legend()
plt.tight_layout()
plt.show()
