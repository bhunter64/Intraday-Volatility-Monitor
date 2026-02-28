import matplotlib.pyplot as plt
import numpy as np

# Mocking the detectors for visualization testing
class PageHinkleyMock:
    def __init__(self, threshold=0.05):
        self.threshold = threshold
        self.sum_diff = 0.0
    def update(self, x):
        # Simulates a slow buildup of evidence
        self.sum_diff += (x * 0.05) 
        return self.sum_diff > self.threshold

class CUSUMMock:
    def __init__(self, threshold=0.2):
        self.threshold = threshold
        self.s = 0.0
    def update(self, x):
        # Simulates a sudden spike detector
        self.s += x
        return self.s > self.threshold

def generate_slow_heat_data(n=100):
    """Generates 50 mins of noise, then 50 mins of gradual volatility increase."""
    quiet = np.random.normal(0, 0.01, n // 2)
    drift = [np.random.normal(0, 0.01 + (i * 0.002)) for i in range(n // 2)]
    return np.concatenate([quiet, drift])

def run_comparison_test():
    data = generate_slow_heat_data()
    ph = PageHinkleyMock(threshold=0.08)
    cusum = CUSUMMock(threshold=0.5)

    ph_alarms, cu_alarms = [], []
    ph_sums, cu_sums = [], []

    for i, val in enumerate(data):
        # Track Page-Hinkley
        if ph.update(val): ph_alarms.append(i)
        ph_sums.append(ph.sum_diff)
        
        # Track CUSUM
        if cusum.update(val): cu_alarms.append(i)
        cu_sums.append(cusum.s)

    # Visualization
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)

    # Top: Volatility & Alarm Points
    ax1.plot(data, label="Volatility Metric", color='gray', alpha=0.5)
    if ph_alarms: ax1.axvline(ph_alarms[0], color='green', linestyle='--', label='PH First Alarm')
    if cu_alarms: ax1.axvline(cu_alarms[0], color='red', linestyle='--', label='CUSUM First Alarm')
    ax1.set_title("Detection Delay Comparison: Page-Hinkley vs. CUSUM")
    ax1.legend()

    # Bottom: Cumulative Evidence Lines
    ax2.plot(ph_sums, label="PH Evidence (sum_diff)", color='green')
    ax2.plot(cu_sums, label="CUSUM Evidence (S)", color='red')
    ax2.set_ylabel("Accumulated Evidence")
    ax2.legend()

    plt.tight_layout()
    plt.savefig("research/ph_vs_cusum_delay.png") # Save figure per 'Definition of Done'
    plt.show()

if __name__ == "__main__":
    run_comparison_test()