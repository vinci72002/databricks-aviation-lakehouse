# DE-101 Learning Notes — Environment Isolation & Configuration

## 1. What I Built

In DE-101, I changed my Databricks Aviation Lakehouse from a
single-environment pipeline into a pipeline that can run independently
in DEV and PROD.

Before:

    aviation
       |
       ├── bronze
       ├── silver
       ├── quarantine
       └── gold

After:

                     Same Pipeline Code
                            |
                       env parameter
                      /             \
                    DEV             PROD
                     |               |
              aviation_dev     aviation_prod
                     |               |
                   Bronze           Bronze
                     |               |
                   Silver           Silver
                  /      \         /      \
           Quarantine   Gold  Quarantine   Gold
                     |               |
                    Ops             Ops

The key idea is:

> Same code, different configuration, isolated state.

---

## 2. Why Do We Need DEV and PROD?

My original pipeline used hard-coded resources such as:

    aviation.bronze.telemetry
    aviation.silver.telemetry
    aviation.gold.fact_telemetry_hourly

and:

    /Volumes/aviation/raw/landing/telemetry

This works for a demo, but it is dangerous for a production-style
system.

If developers test new code using the same tables and checkpoints as
production, a development run could:

- overwrite production tables
- process production files
- change Auto Loader progress
- corrupt or change production results

Therefore DEV and PROD must be isolated.

---

## 3. Environment Parameter

Instead of creating separate DEV and PROD versions of every notebook,
I added an environment parameter:

```python
dbutils.widgets.text("env", "dev", "Environment")

env = dbutils.widgets.get("env").strip().lower()

if env not in {"dev", "prod"}:
    raise ValueError("Allowed values: dev, prod")

catalog = f"aviation_{env}"
```

18. What I Should Remember for Interviews
    Q: Why separate DEV and PROD?
    Because development workloads should not modify production data or
    production processing state.
    Q: Why not just create different table names?
    Because a pipeline also contains state such as Auto Loader checkpoints
    and schema metadata. Those resources must also be isolated.
    Q: Why use the same code for both environments?
    It reduces code duplication and configuration drift. Environment-specific
    values should come from configuration rather than separate implementations.
    Q: What does an Auto Loader checkpoint do?
    It maintains processing progress/state so Auto Loader knows what has
    already been processed and can continue safely across runs.
    Q: Why are DEV and PROD checkpoints different?
    Because sharing a checkpoint would allow one environment's processing
    progress to affect the other environment.
    Q: Is this pipeline fully production-ready now?
    No.
    DE-101 solves environment isolation.
    The next production gaps include incremental processing, processing
    watermarks, generic late-data handling, idempotent Gold updates, CI/CD,
    monitoring, and stronger recovery behavior.
