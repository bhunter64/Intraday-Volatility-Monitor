# Data policy
- Do **not** commit raw or large data.
- Put tiny samples into `data/samples/` for docs/tests.
- Real data lives in cloud/object storage or databases; point to it via config (YAML) and environment variables (e.g., `DATA_S3_URI`).

## Structure
- `data/samples/`: Tiny datasets for docs, and tests.

## Usage
- Experiments pull data paths from `experiments/configs/`.
- Sample CSVs in `data/samples/` are safe for quick demos.
- Expected format includes:
  - `timestamp`
  - `ticker`
  - `price`
  - `volume`