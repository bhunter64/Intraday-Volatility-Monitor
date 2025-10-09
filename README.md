# Intraday-Volatility-Monitor
A 3-model intrady volatility monitoring project, centered around using statistical methods to solve quantitative problems in finance.
Built with a dedicated team of undergraduate research students in Math, CS and Engineering.

This is a QUANTT (Queen's University Algorithmic Network Trading Team) project. It is designed as a tool for Quant Traders and Developers
to inform decisions related to volatility (ie storms, crashes, etc.). Although it's primary purpose in internal utilization, to test it's
effectiveness, it is implemented here with a tiliting portfolio strategy.

To improve the statistical and mathematical rigor of the project, a research paper is also written in tandem with the tool, to demonstrate
acurate findings and a passion for mathematical research amongst the team.

## Quickstart (local)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r infra/env/requirements.txt
pip install -e .
python experiments/run.py --config experiments/configs/cusum_baseline.yaml
