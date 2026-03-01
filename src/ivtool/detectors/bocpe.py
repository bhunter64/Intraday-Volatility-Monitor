from __future__ import annotations
 
import numpy as np
import pandas as pd
from dataclasses import dataclass
from math import exp, log, pi, lgamma
from typing import Dict, List, Optional, Tuple
 
 
@dataclass
class _PosteriorState:
    run_length_probs: List[float]
    alpha_posteriors: List[float]
    beta_posteriors: List[float]
 
 
class VolatilityBOCPE:
    """Bayesian Online Change Point Estimation (Normal-Gamma variance-shift model)."""
 
    def __init__(
        self,
        hazard: float = 1.0 / 250.0,
        threshold: float = 0.5,
        prior_alpha: float = 2.0,   
        prior_beta: float = 0.01,
        vol_threshold: float = 0.02, 
        max_run_length: Optional[int] = None,
    ) -> None:
        if not (0.0 < hazard < 1.0):
            raise ValueError("hazard must be in (0, 1)")
        if not (0.0 < threshold < 1.0):
            raise ValueError("threshold must be in (0, 1)")
        if prior_alpha <= 1.0:
            raise ValueError("prior_alpha must be > 1.0 for defined expected variance")
        if prior_beta <= 0.0:
            raise ValueError("prior_beta must be positive")
        if vol_threshold <= 0.0:
            raise ValueError("vol_threshold must be positive")
        if max_run_length is not None and max_run_length < 1:
            raise ValueError("max_run_length must be >= 1")
 
        self.hazard = float(hazard)
        self.threshold = float(threshold)
        self.prior_alpha = float(prior_alpha)
        self.prior_beta = float(prior_beta)
        self.vol_threshold = float(vol_threshold)
        self.max_run_length = max_run_length
 
        self.reset()
 
    def reset(self) -> None:
        self.t = 0
        self._state = _PosteriorState(
            run_length_probs=[1.0],
            alpha_posteriors=[self.prior_alpha],
            beta_posteriors=[self.prior_beta],
        )
        self._cp_prob = 0.0
        self._map_run_length = 0
        self._current_regime = "Low Volatility"
 
    def _student_t_pdf(self, x: float, alpha: float, beta: float) -> float:
        log_pdf = (
            lgamma(alpha + 0.5)
            - lgamma(alpha)
            - 0.5 * log(2.0 * pi * beta)
            - (alpha + 0.5) * log(1.0 + (x ** 2) / (2.0 * beta))
        )
        return exp(log_pdf)
 
    def _predictive_density(self, x: float) -> List[float]:
        pred: List[float] = []
        for alpha, beta in zip(self._state.alpha_posteriors, self._state.beta_posteriors):
            pred.append(self._student_t_pdf(x, alpha, beta))
        return pred
 
    def update(self, x: float) -> Tuple[bool, str]:
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
 
        prev_alphas = self._state.alpha_posteriors
        prev_betas = self._state.beta_posteriors
 
        new_alphas = [self.prior_alpha + 0.5]
        new_betas = [self.prior_beta + 0.5 * (x ** 2)]
 
        for alpha, beta in zip(prev_alphas, prev_betas):
            new_alphas.append(alpha + 0.5)
            new_betas.append(beta + 0.5 * (x ** 2))
 
        if self.max_run_length is not None and len(new_probs) > self.max_run_length + 1:
            keep = self.max_run_length + 1
            new_probs = new_probs[:keep]
            total = sum(new_probs)
            if total <= 0.0:
                raise FloatingPointError("numerical instability after truncation")
            new_probs = [p / total for p in new_probs]
            new_alphas = new_alphas[:keep]
            new_betas = new_betas[:keep]
 
        self._state = _PosteriorState(
            run_length_probs=new_probs,
            alpha_posteriors=new_alphas,
            beta_posteriors=new_betas,
        )
 
        new_map = max(range(len(new_probs)), key=lambda idx: new_probs[idx])
        new_cp_prob = new_probs[0]
        triggered = (new_cp_prob >= self.threshold) or (self.t > 0 and new_map < self._map_run_length)
 
        map_alpha = new_alphas[new_map]
        map_beta = new_betas[new_map]
        expected_variance = map_beta / (map_alpha - 1.0) 
        regime_label = "High Volatility" if expected_variance > self.vol_threshold else "Low Volatility"
 
        self._cp_prob = new_cp_prob
        self._map_run_length = new_map
        self._current_regime = regime_label
        self.t += 1
        return triggered, regime_label
 
    def state(self) -> Dict[str, float | str]:
        probs = self._state.run_length_probs
        map_run_length = self._map_run_length
        return {
            "t": float(self.t),
            "cp_prob": float(self._cp_prob),
            "map_run_length": float(map_run_length),
            "posterior_peak_prob": float(probs[map_run_length]),
            "current_regime": self._current_regime,
        }
 
 
def run_bocpe(returns: pd.Series, hazard: float = 1.0/250.0, threshold: float = 0.5, vol_threshold: float = 0.02, max_run_length: int = 1200) -> Tuple[pd.Series, pd.Series]:
    print("Running Volatility BOCPE on returns...")
    detector = VolatilityBOCPE(hazard=hazard, threshold=threshold, vol_threshold=vol_threshold, max_run_length=max_run_length)
    alarms = []
    regimes = []
    for x in returns:
        triggered, regime = detector.update(float(x))
        alarms.append(triggered)
        regimes.append(regime)
    return pd.Series(alarms, index=returns.index), pd.Series(regimes, index=returns.index)
 
 
def main_bocpe_run(df: pd.DataFrame, hazard: float = 1/(390*3), threshold: float = 0.5, vol_threshold: float = 0.0003, max_run_length: int = 1200) -> pd.DataFrame:
    """
    Main entry point. Takes a df with 'time' and 'price' columns.
    Returns a dataframe of flagged timestamps where change points were detected,
    along with their identified volatility regime.
    """
    print("Running Volatility BOCPE change point detection...")
    df = df.sort_values("time").reset_index(drop=True)
    prices = df["price"].astype(float)
 
    returns = np.log(prices / prices.shift(1))
    returns = returns.dropna().reset_index(drop=True)
 
    timestamps = df["time"].iloc[1:].reset_index(drop=True)
    # Unpack both alarms and regimes
    alarms, regimes = run_bocpe(returns, hazard=hazard, threshold=threshold, vol_threshold=vol_threshold, max_run_length=max_run_length)
 
    # Filter for only the points where an alarm was triggered
    flagged = pd.DataFrame({
        "timestamp": timestamps[alarms],
        "alarm": True,
        "new_regime": regimes[alarms]
    }).reset_index(drop=True)
    print("BOCPE run complete. Number of change points detected:", len(flagged))
    return flagged