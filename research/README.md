# Research

This directory contains analytical notes, experiment results, and validation cases used to benchmark the Intraday Volatility Monitor algorithms.

## Overview

We evaluate multiple change detection algorithms under controlled conditions to assess:

-   **Detection delay** after a mean shift
-   **False alarm rate** under stationary data
-   **Robustness** to gradual drift

## Validation Cases

-   **CUSUM:** Expected detection delay under step mean shift; tests typical `k/h` settings
-   **Page-Hinkley:** Evaluates robustness to gradual drifts with `delta/lambda` guidance
-   **BOCPE:** Contains formulation notes and acceptance criteria

## Purpose

These research notes guide the parameter tuning and performance benchmarking of detectors implemented in `src/ivtool/detectors/`.

## Outputs

-   Performance plots (mean shift vs detection delay)
-   Benchmark tables
-   Parameter recommendations
