from typing import Dict

class PageHinkley:
    def __init__(self, delta: float = 0.005, lambda_: float = 50.0, alpha: float = 0.999):
        self.delta = float(delta)
        self.lambda_ = float(lambda_)
        self.alpha = float(alpha)
        self.reset()

    def reset(self) -> None:
        self.t = 0
        self.mean = 0.0
        self.m_t = 0.0
        self.M_t = 0.0
