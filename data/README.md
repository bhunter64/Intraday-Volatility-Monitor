# Data policy
- Do **not** commit raw or large data.
- Put tiny samples into `data/samples/` for docs/tests.
- Real data lives in cloud/object storage or databases; point to it via config (YAML) and environment variables (e.g., `DATA_S3_URI`).
