from typing import Dict
import numpy as np
import pandas as pd

class CUSUM:
    """
    Two-sided CUSUM adapted for volatility and directional movement detection.
    This implementation follows the IVM project specs for accumulating evidence 
    measures based on return differences rather than a global mean.
    """
    def __init__(self, k: float = 0.5, h: float = 5.0):
        self.k = float(k)          # Volatility-scaled tolerance (epsilon)
        self.h = float(h)          # Decision threshold
        self.reset()

    def reset(self) -> None:
        """Resets the cumulative sums and timer."""
        self.gp = 0.0              # S+: Positive (upward) evidence measure
        self.gn = 0.0              # S-: Negative (downward) evidence measure
        self.t = 0

    def update(self, x: float) -> bool:
        """
        Processes a single return difference 'x'.
        Returns True if the combined evidence (S+ + S-) triggers an alarm.
        """
        self.t += 1
        
        # 1. Accumulate evidence as positive values
        # self.k acts as the epsilon tolerance factor 
        self.gp = max(0.0, self.gp + x + self.k)
        self.gn = max(0.0, -(x + self.k)) 

        # 2. Trigger alarm if the sum of evidence sets exceeds threshold h 
        if (self.gp + self.gn) >= self.h:
            self.reset()
            return True
            
        return False
    
    def state(self) -> Dict[str, float]:
        """Returns the internal state for logging and monitoring."""
        return {
            "gp": self.gp, 
            "gn": self.gn, 
            "t": float(self.t)
        }