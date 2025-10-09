from .cusum import CUSUM
from .page_hinkley import PageHinkley
from .bocpe import BOCPE

def make(name: str, **params):
    name = name.lower()
    if name in {"cusum"}:
        return CUSUM(**params)
    if name in {"page_hinkley"}:
        return PageHinkley(**params)
    if name in {"bocpe"}:
        return BOCPE(**params)
    raise ValueError(f"Unknown detector: {name}")
