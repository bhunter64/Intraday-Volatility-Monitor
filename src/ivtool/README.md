# Source / IVTool

This package implements the core logic for the Intraday Volatility Monitor, including change-point detection algorithms and data-processing pipelines.

## Modules Overview

## 1. `detectors.base`

Defines a **base interface** for all detectors.

### 2. `detectors.cusum`

Implements the Cumulative Sum Control Chart (CUSUM) algorithm, used to detect shifts in the mean level of a time series.

Parameters:

-   `k`: Reference value (sensitivity)
-   `h`: Decision interval (threshold)
-   Direction of detection (up/down/both)

### 3. `detectors.page_hinkley`

Implements the Page-Hinkley Test, a sequential change detection method robust to small fluctuations.

Parameters:

-   `delta`: Tolerance to minor changes
-   `lambda`: Threshold for detection

### 4. `detectors.bocpe`

Implements Bayesian Online Change Point Estimation (BOCPE), a probabalistic model for real-time change detection.

### 5. `detectors.factory`

Provides a simple factory pattern for detector instantiation.

### 6. `pipeline.io`

Defines input/output operations for the detector pipeline including reading market data streams and applying preprocessing.
