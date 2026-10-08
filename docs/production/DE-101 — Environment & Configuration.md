# DE-101 — DEV / PROD Environment Isolation

## Problem

The original Databricks pipeline used a single hard-coded environment:

- `aviation.bronze.telemetry`
- `aviation.silver.telemetry`
- `aviation.gold.*`
- `/Volumes/aviation/raw/landing/...`

This meant development and production workloads could share data,
Auto Loader checkpoints, schema state, and Delta tables.

## Solution

Refactored the pipeline to support environment-based configuration:

```python
env = dbutils.widgets.get("env").strip().lower()
catalog = f"aviation_{env}"
```
