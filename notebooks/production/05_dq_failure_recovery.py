# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# DE-101 - Environment Configuration
#
# Same notebook supports:
#   env=dev  -> aviation_dev
#   env=prod -> aviation_prod

dbutils.widgets.text("env", "dev", "Environment")

env = dbutils.widgets.get("env").strip().lower()

if env not in {"dev", "prod"}:
    raise ValueError(
        f"Invalid environment: '{env}'. "
        "Allowed values: dev, prod"
    )

catalog = f"aviation_{env}"


# COMMAND ----------
# Imports

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime
import uuid


# COMMAND ----------
# Environment-specific tables

SILVER_TABLE = f"{catalog}.silver.telemetry"

HOURLY_TABLE = (
    f"{catalog}.gold.fact_telemetry_hourly"
)

DAILY_TABLE = (
    f"{catalog}.gold.fact_telemetry_daily"
)

DIM_TABLE = (
    f"{catalog}.gold.dim_aircraft"
)

AUDIT_TABLE = (
    f"{catalog}.ops.pipeline_audit"
)


# COMMAND ----------
# Environment isolation safety check

expected_table_prefix = f"{catalog}."

tables = [
    SILVER_TABLE,
    HOURLY_TABLE,
    DAILY_TABLE,
    DIM_TABLE,
    AUDIT_TABLE
]

for table in tables:
    if not table.startswith(expected_table_prefix):
        raise RuntimeError(
            f"Environment isolation failed: {table}"
        )


print("=" * 60)
print("DQ & FAILURE RECOVERY")
print("=" * 60)

print(f"Environment : {env.upper()}")
print(f"Catalog     : {catalog}")
print(f"Silver      : {SILVER_TABLE}")
print(f"Hourly      : {HOURLY_TABLE}")
print(f"Daily       : {DAILY_TABLE}")
print(f"Dimension   : {DIM_TABLE}")
print(f"Audit       : {AUDIT_TABLE}")

print("=" * 60)
print("Environment isolation check: PASSED")


# COMMAND ----------
# Job parameter
#
# false = normal run
# true  = controlled failure test

dbutils.widgets.text(
    "inject_failure",
    "false",
    "Inject Failure"
)

inject_failure = (
    dbutils.widgets
    .get("inject_failure")
    .strip()
    .lower()
    == "true"
)

batch_id = str(uuid.uuid4())

print()
print(f"batch_id       : {batch_id}")
print(f"inject_failure : {inject_failure}")


# COMMAND ----------
# Create environment-specific audit table

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

print(f"Audit table ready: {AUDIT_TABLE}")


# COMMAND ----------
# Record pipeline start

audit_start = spark.createDataFrame(
    [
        (
            batch_id,
            "RUNNING",
            datetime.now(),
            None,
            None
        )
    ],
    """
    batch_id STRING,
    status STRING,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message STRING
    """
)

(
    audit_start.write
    .mode("append")
    .saveAsTable(AUDIT_TABLE)
)

audit = DeltaTable.forName(
    spark,
    AUDIT_TABLE
)


# COMMAND ----------
# Execute DQ Gate

try:

    # ---------------------------------------------------------
    # Controlled failure test
    # ---------------------------------------------------------

    if inject_failure:

        raise Exception(
            "CONTROLLED FAILURE: "
            "simulated pipeline crash"
        )


    # ---------------------------------------------------------
    # DQ Check 1
    # Gold Hourly must contain unique grains
    # ---------------------------------------------------------

    hourly_duplicates = (
        spark.table(HOURLY_TABLE)
        .groupBy(
            "aircraft_id",
            "flight_id",
            "telemetry_hour"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )


    # ---------------------------------------------------------
    # DQ Check 2
    # Gold Daily must contain unique grains
    # ---------------------------------------------------------

    daily_duplicates = (
        spark.table(DAILY_TABLE)
        .groupBy(
            "aircraft_id",
            "flight_id",
            "telemetry_date"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )


    # ---------------------------------------------------------
    # DQ Check 3
    # Silver business keys cannot be NULL
    # ---------------------------------------------------------

    silver_null_keys = (
        spark.table(SILVER_TABLE)
        .filter(
            F.col("event_id").isNull()
            | F.col("aircraft_id").isNull()
            | F.col("flight_id").isNull()
        )
        .count()
    )


    # ---------------------------------------------------------
    # DQ Check 4
    # SCD2: maximum one current row per aircraft
    # ---------------------------------------------------------

    scd2_violations = (
        spark.table(DIM_TABLE)
        .filter(
            F.col("is_current") == True
        )
        .groupBy(
            "aircraft_id"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )


    # ---------------------------------------------------------
    # Display DQ results
    # ---------------------------------------------------------

    print()
    print("DQ RESULTS")
    print("-" * 60)

    print(
        "Hourly duplicate grains:",
        hourly_duplicates
    )

    print(
        "Daily duplicate grains :",
        daily_duplicates
    )

    print(
        "Silver null keys        :",
        silver_null_keys
    )

    print(
        "SCD2 violations         :",
        scd2_violations
    )


    # ---------------------------------------------------------
    # DQ Gate
    # ---------------------------------------------------------

    dq_failed = (
        hourly_duplicates > 0
        or daily_duplicates > 0
        or silver_null_keys > 0
        or scd2_violations > 0
    )

    if dq_failed:
        raise Exception(
            "DQ Gate FAILED"
        )


    # ---------------------------------------------------------
    # Commit audit status
    # ---------------------------------------------------------

    audit.update(
        condition=f"batch_id = '{batch_id}'",
        set={
            "status": "'COMMITTED'",
            "completed_at":
                "current_timestamp()"
        }
    )

    print()
    print("DQ Gate PASSED")
    print("Pipeline COMMITTED")


# COMMAND ----------
# Failure handling

except Exception as e:

    error = str(e).replace(
        "'",
        "''"
    )

    audit.update(
        condition=f"batch_id = '{batch_id}'",
        set={
            "status": "'FAILED'",
            "completed_at":
                "current_timestamp()",
            "error_message":
                f"'{error}'"
        }
    )

    print()
    print(
        "Pipeline FAILED:",
        str(e)
    )

    raise


# COMMAND ----------
# Final audit validation

print()
print("=" * 60)
print("LATEST PIPELINE AUDIT")
print("=" * 60)

display(
    spark.table(AUDIT_TABLE)
    .orderBy(
        F.col("started_at").desc()
    )
    .limit(10)
)