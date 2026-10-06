# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime
import uuid

# COMMAND ----------
# Job parameter
# false = normal run
# true  = controlled failure test

dbutils.widgets.text("inject_failure", "false")

inject_failure = (
    dbutils.widgets.get("inject_failure").lower()
    == "true"
)

batch_id = str(uuid.uuid4())

print("batch_id:", batch_id)
print("inject_failure:", inject_failure)

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS aviation.gold.pipeline_audit (
    batch_id STRING,
    status STRING,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message STRING
)
USING DELTA
""")

# COMMAND ----------

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

audit_start.write.mode("append").saveAsTable(
    "aviation.gold.pipeline_audit"
)

audit = DeltaTable.forName(
    spark,
    "aviation.gold.pipeline_audit"
)

# COMMAND ----------

try:

    if inject_failure:
        raise Exception(
            "CONTROLLED FAILURE: simulated pipeline crash"
        )

    hourly_duplicates = (
        spark.table(
            "aviation.gold.fact_telemetry_hourly"
        )
        .groupBy(
            "aircraft_id",
            "flight_id",
            "telemetry_hour"
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    daily_duplicates = (
        spark.table(
            "aviation.gold.fact_telemetry_daily"
        )
        .groupBy(
            "aircraft_id",
            "flight_id",
            "telemetry_date"
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    silver_null_keys = (
        spark.table("aviation.silver.telemetry")
        .filter(
            F.col("event_id").isNull()
            | F.col("aircraft_id").isNull()
            | F.col("flight_id").isNull()
        )
        .count()
    )

    scd2_violations = (
        spark.table("aviation.gold.dim_aircraft")
        .filter(F.col("is_current") == True)
        .groupBy("aircraft_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print("Hourly duplicate grains:", hourly_duplicates)
    print("Daily duplicate grains :", daily_duplicates)
    print("Silver null keys        :", silver_null_keys)
    print("SCD2 violations         :", scd2_violations)

    dq_failed = (
        hourly_duplicates > 0
        or daily_duplicates > 0
        or silver_null_keys > 0
        or scd2_violations > 0
    )

    if dq_failed:
        raise Exception("DQ Gate FAILED")

    (
        audit.update(
            condition=f"batch_id = '{batch_id}'",
            set={
                "status": "'COMMITTED'",
                "completed_at": "current_timestamp()"
            }
        )
    )

    print("DQ Gate PASSED")
    print("Pipeline COMMITTED")

except Exception as e:

    error = str(e).replace("'", "''")

    (
        audit.update(
            condition=f"batch_id = '{batch_id}'",
            set={
                "status": "'FAILED'",
                "completed_at": "current_timestamp()",
                "error_message": f"'{error}'"
            }
        )
    )

    print("Pipeline FAILED:", str(e))

    raise