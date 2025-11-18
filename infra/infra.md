# Infrastructure

This folder contains the environment and CI setup.

## Overview
Ensures experiments and tests run consistently across:
- Local development
- Continuous Integration (CI)
- Cloud environments

## Environment
- Python 3.11
- Dependencies: `infra/env/requirements.txt`
  - Core scientific stack (numpy, pandas)
  - Visualization (matplotlib, pyyaml)
  - Test tools (pytest, ruff, mypy)

## Purpose
Standardize the development environment and enforce code quality.

## Outputs
- Reproducible installs via `requirements.txt`
- CI reports
