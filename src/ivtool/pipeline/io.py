from typing import Iterator, Tuple, Dict, Any
import csv
from pathlib import Path

def stream_prices(cfg: Dict[str, Any]) -> Iterator[Tuple[str, float]]:
    """
    Minimal local CSV streamer used for tests/demos.
    Expects cfg like: {"source": "data/samples/prices.csv"}
    CSV columns: timestamp,symbol,price
    """
    src = cfg.get("source", "data/samples/prices.csv")
    p = Path(src)
    if not p.exists():
        raise FileNotFoundError(f"Data source not found: {p.resolve()}")
    with p.open() as f:
        r = csv.DictReader(f)
        for row in r:
            yield row["timestamp"], float(row["price"])
