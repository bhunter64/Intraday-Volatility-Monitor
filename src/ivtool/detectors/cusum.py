from typing import Dict

class CUSUM:
    """
    Two-sided CUSUM for mean shifts on a streaming returns x.
    Parameters:
      k: reference value (noise)
      h: decision threshold
    """
    def __init__(self, k: float = 0.5, h: float = 5.0):
        self.k = float(k)
        self.h = float(h)
        self.reset()
