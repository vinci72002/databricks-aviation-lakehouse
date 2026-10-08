# Databricks notebook source
# ============================================================
# 05 - Data Quality Gate & Audit
#
# Audit states: RUNNING -> PASSED / FAILED
#
# This task validates existing outputs.
# It does not commit or roll back upstream data or watermarks.
# ============================================================

import uuid

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# 1. Parameters

dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.dropdown(
    "inject_failure",
    "false",
    ["false", "true"],
    "Inject Failure",
)

env = dbutils.widgets.get("env").strip().lower()
failure_value = dbutils.widgets.get("inject_failure").strip().lower()

if env not in {"dev", "prod"}:
    raise ValueError(f"Invalid environment: {env!r}. Allowed: dev, prod")

if failure_value not in {"false", "true"}:
    raise ValueError(f"Invalid inject_failure value: {failure_value!r}")

inject_failure = failure_value == "true"


# 2. Environment-specific tables

catalog = f"aviation_{env}"

SILVER_TABLE = f"{catalog}.silver.telemetry"
HOURLY_TABLE = f"{catalog}.gold.fact_telemetry_hourly"
DAILY_TABLE = f"{catalog}.gold.fact_telemetry_daily"
DIM_TABLE = f"{catalog}.gold.dim_aircraft"
AUDIT_TABLE = f"{catalog}.ops.pipeline_audit"

batch_id = str(uuid.uuid4())

print("=" * 60)
print("DATA QUALITY GATE")
print("=" * 60)
print(f"Environment    : {env.upper()}")
print(f"Audit table    : {AUDIT_TABLE}")
print(f"Batch ID       : {batch_id}")
print(f"Inject failure : {inject_failure}")


# 3. Create audit table
#
# Compatible with the existing audit table schema.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
    batch_id STRING,
    status STRING,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message STRING
)
USING DELTA
""")

audit_start_df = spark.range(1).select(
    F.lit(batch_id).alias("batch_id"),
    F.lit("RUNNING").alias("status"),
    F.current_timestamp().alias("started_at"),
    F.lit(None).cast("timestamp").alias("completed_at"),
    F.lit(None).cast("string").alias("error_message"),
)

(audit_start_df.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE))

audit = DeltaTable.forName(spark, AUDIT_TABLE)


# 4. Audit update helper
#
# Use literal columns instead of building SQL strings
# from exception messages.


def update_audit(status, error_message=None):
    audit.update(
        condition=F.col("batch_id") == F.lit(batch_id),
        set={
            "status": F.lit(status),
            "completed_at": F.current_timestamp(),
            "error_message": F.lit(error_message).cast("string"),
        },
    )


# 5. DQ helpers


def duplicate_group_count(df, keys):
    return df.groupBy(*keys).count().filter(F.col("count") > 1).count()


def invalid_key_condition(keys):
    condition = F.lit(False)

    for key in keys:
        condition = condition | (
            F.col(key).isNull() | (F.length(F.trim(F.col(key).cast("string"))) == 0)
        )

    return condition


# 6. Execute DQ checks

try:
    for table in (
        SILVER_TABLE,
        HOURLY_TABLE,
        DAILY_TABLE,
        DIM_TABLE,
    ):
        if not spark.catalog.tableExists(table):
            raise RuntimeError(f"Required table is missing: {table}")

    silver_df = spark.table(SILVER_TABLE)
    hourly_df = spark.table(HOURLY_TABLE)
    daily_df = spark.table(DAILY_TABLE)
    dim_df = spark.table(DIM_TABLE)

    results = {}

    results["silver_invalid_keys"] = silver_df.filter(
        invalid_key_condition(["event_id", "aircraft_id", "flight_id"])
        | F.col("event_time").isNull()
        | F.col("_ingest_ts").isNull()
    ).count()

    results["silver_duplicate_events"] = duplicate_group_count(silver_df, ["event_id"])

    gold_definitions = (
        (
            "hourly",
            hourly_df,
            ["aircraft_id", "flight_id", "telemetry_hour"],
        ),
        (
            "daily",
            daily_df,
            ["aircraft_id", "flight_id", "telemetry_date"],
        ),
    )

    for name, df, keys in gold_definitions:
        results[f"{name}_duplicate_grains"] = duplicate_group_count(df, keys)

        results[f"{name}_invalid_rows"] = df.filter(
            invalid_key_condition(keys)
            | F.col("sample_count").isNull()
            | (F.col("sample_count") <= 0)
        ).count()

    results["scd2_duplicate_versions"] = duplicate_group_count(
        dim_df, ["aircraft_id", "effective_from"]
    )

    results["scd2_invalid_rows"] = dim_df.filter(
        invalid_key_condition(["aircraft_id"])
        | F.col("effective_from").isNull()
        | F.col("effective_to").isNull()
        | F.col("is_current").isNull()
        | F.col("aircraft_sk").isNull()
        | (F.col("effective_from") >= F.col("effective_to"))
    ).count()

    results["scd2_invalid_current_count"] = (
        dim_df.groupBy("aircraft_id")
        .agg(F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias("current_count"))
        .filter(F.col("current_count") != 1)
        .count()
    )

    print("\nDQ RESULTS")
    print("-" * 60)

    for name, count in results.items():
        print(f"{name:<36}: {count}")

    failed_checks = {name: count for name, count in results.items() if count > 0}

    if failed_checks:
        raise RuntimeError(f"DQ gate failed: {failed_checks}")

    # Inject failure after real checks, before marking PASSED.
    if inject_failure:
        raise RuntimeError(
            "CONTROLLED FAILURE: injected in the DQ task " "before recording PASSED."
        )

    update_audit("PASSED")

    print("\nDQ GATE: PASSED")
    print("Audit recorded successfully.")

except Exception as error:
    # Preserve the original task error if audit recording fails.
    try:
        update_audit("FAILED", str(error)[:8000])
    except Exception as audit_error:
        print(
            "Could not record FAILED audit status:",
            str(audit_error),
        )

    print("\nDQ GATE: FAILED")
    print(str(error))
    print(f"Batch ID: {batch_id}")

    raise


# 7. Display recent audit records

display(spark.table(AUDIT_TABLE).orderBy(F.col("started_at").desc()).limit(10))
