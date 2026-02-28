from __future__ import annotations

from dataclasses import dataclass
from math import exp, pi, sqrt
from typing import Dict, List, Optional
import numpy as np
import pandas as pd


@dataclass
class _PosteriorState:
    run_length_probs: List[float]
    mean_posteriors: List[float]
    precision_posteriors: List[float]


class BOCPE:
    """Bayesian Online Change Point Estimation."""

    def __init__(
        self,
        hazard: float = 1.0 / 250.0,
        threshold: float = 0.2,
        prior_mean: float = 0.0,
        prior_precision: float = 1.0,
        obs_var: float = 1.0,
        max_run_length: Optional[int] = None,
    ) -> None:
        if not (0.0 < hazard < 1.0):
            raise ValueError("hazard must be in (0, 1)")
        if not (0.0 < threshold < 1.0):
            raise ValueError("threshold must be in (0, 1)")
        if prior_precision <= 0.0:
            raise ValueError("prior_precision must be positive")
        if obs_var <= 0.0:
            raise ValueError("obs_var must be positive")
        if max_run_length is not None and max_run_length < 1:
            raise ValueError("max_run_length must be >= 1")

        self.hazard = float(hazard)
        self.threshold = float(threshold)
        self.prior_mean = float(prior_mean)
        self.prior_precision = float(prior_precision)
        self.obs_var = float(obs_var)
        self.max_run_length = max_run_length
        self.reset()

    def reset(self) -> None:
        self.t = 0
        self._state = _PosteriorState(
            run_length_probs=[1.0],
            mean_posteriors=[self.prior_mean],
            precision_posteriors=[self.prior_precision],
        )
        self._cp_prob = 0.0
        self._map_run_length = 0

    def _normal_pdf(self, x: float, mean: float, var: float) -> float:
        coeff = 1.0 / sqrt(2.0 * pi * var)
        exponent = -0.5 * ((x - mean) ** 2) / var
        return coeff * exp(exponent)

    def _predictive_density(self, x: float) -> List[float]:
        pred: List[float] = []
        for mean, precision in zip(self._state.mean_posteriors, self._state.precision_posteriors):
            pred_var = self.obs_var + (1.0 / precision)
            pred.append(self._normal_pdf(x, mean, pred_var))
        return pred

    def update(self, x: float) -> bool:
        x = float(x)
        prev_probs = self._state.run_length_probs
        pred = self._predictive_density(x)

        growth = [p * q * (1.0 - self.hazard) for p, q in zip(prev_probs, pred)]
        cp_prob_unnorm = sum(p * q * self.hazard for p, q in zip(prev_probs, pred))

        new_probs = [cp_prob_unnorm] + growth
        evidence = sum(new_probs)
        if evidence <= 0.0:
            raise FloatingPointError("numerical instability in BOCPE update")
        new_probs = [p / evidence for p in new_probs]

        obs_prec = 1.0 / self.obs_var
        prev_means = self._state.mean_posteriors
        prev_precs = self._state.precision_posteriors

        new_precs = [self.prior_precision + obs_prec]
        new_means = [
            (self.prior_precision * self.prior_mean + obs_prec * x) / new_precs[0]
        ]

        for mean, prec in zip(prev_means, prev_precs):
            post_prec = prec + obs_prec
            post_mean = (prec * mean + obs_prec * x) / post_prec
            new_precs.append(post_prec)
            new_means.append(post_mean)

        if self.max_run_length is not None and len(new_probs) > self.max_run_length + 1:
            keep = self.max_run_length + 1
            new_probs = new_probs[:keep]
            total = sum(new_probs)
            if total <= 0.0:
                raise FloatingPointError("numerical instability after truncation")
            new_probs = [p / total for p in new_probs]
            new_precs = new_precs[:keep]
            new_means = new_means[:keep]

        self._state = _PosteriorState(
            run_length_probs=new_probs,
            mean_posteriors=new_means,
            precision_posteriors=new_precs,
        )
        new_map = max(range(len(new_probs)), key=lambda idx: new_probs[idx])
        new_cp_prob = new_probs[0]
        triggered = (new_cp_prob >= self.threshold) or (self.t > 0 and new_map < self._map_run_length)

        self._cp_prob = new_cp_prob
        self._map_run_length = new_map
        self.t += 1
        return triggered

    def state(self) -> Dict[str, float]:
        probs = self._state.run_length_probs
        return {
            "t": float(self.t),
            "cp_prob": float(self._cp_prob),
            "map_run_length": float(self._map_run_length),
            "posterior_peak_prob": float(probs[self._map_run_length]),
        }


def run_bocpe(returns, hazard=1.0/250.0, threshold=0.5, obs_var=1.0):
    #print("Running BOCPE on returns...")
    detector = BOCPE(hazard=hazard, threshold=threshold, obs_var=obs_var)
    alarms = []
    for x in returns:
        alarms.append(detector.update(float(x)))
    return pd.Series(alarms, index=returns.index)


def main_bocpe_run(df: pd.DataFrame, hazard: float = 1.0/250.0, threshold: float = 0.2, obs_var: float = 1.0) -> pd.DataFrame:
    """
    Main entry point. Takes a df with 'timestamp' and 'price' columns.
    Returns a dataframe of flagged timestamps where change points were detected.
    """
    print("Running BOCPE change point detection...")
    df = df.sort_values("time").reset_index(drop=True)
    prices = df["price"].astype(float)

    returns = np.log(prices / prices.shift(1))
    returns = returns.dropna().reset_index(drop=True)

    timestamps = df["time"].iloc[1:].reset_index(drop=True)
    alarms = run_bocpe(returns, hazard=hazard, threshold=threshold, obs_var=obs_var)

    flagged = pd.DataFrame({
        "timestamp": timestamps[alarms],
        "alarm": True
    }).reset_index(drop=True)
    #print("BOCPE run complete. Number of change points detected:", len(flagged))
    return flagged