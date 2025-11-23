from typing import Dict
import numpy as np
import pandas as pd

#The default k and h values in the class are just placeholders. 
#We'll need to add the tuned values from Matt and Gavin (I think)

class CUSUM:
    """
    Two-sided CUSUM for mean shifts on a streaming returns x.
    Parameters:
      k: reference value (noise)
      h: decision threshold
    
    """
    def __init__(self, k: float = 0.5, h: float = 5.0, mu: float = 0.0):
        self.k = float(k) #drift
        self.h = float(h) #threshold
        self.reset()
        self.mu = mu #mean return
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
        
        #Compare input 'x' against the target 'mu'
        self.gp = max(0.0, self.gp + x - (self.mu + self.k))
        
        self.gn = min(0.0, self.gn + x - (self.mu - self.k))

        #Check for Alarms
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
        return {
            "gp": self.gp, 
            "gn": self.gn, 
            "t": float(self.t)
        }
  