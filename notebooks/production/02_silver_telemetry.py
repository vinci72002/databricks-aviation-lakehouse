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
# Imports and environment-specific tables

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_TABLE = f"{catalog}.bronze.telemetry"
SILVER_TABLE = f"{catalog}.silver.telemetry"
QUARANTINE_TABLE = f"{catalog}.quarantine.telemetry"


# COMMAND ----------
# Environment isolation safety check

expected_table_prefix = f"{catalog}."

tables = [
    BRONZE_TABLE,
    SILVER_TABLE,
    QUARANTINE_TABLE
]

for table in tables:
    if not table.startswith(expected_table_prefix):
        raise RuntimeError(
            f"Environment isolation failed: {table}"
        )

print("=" * 60)
print("SILVER TELEMETRY PIPELINE")
print("=" * 60)
print(f"Environment : {env.upper()}")
print(f"Catalog     : {catalog}")
print(f"Bronze      : {BRONZE_TABLE}")
print(f"Silver      : {SILVER_TABLE}")
print(f"Quarantine  : {QUARANTINE_TABLE}")
print("=" * 60)
print("Environment isolation check: PASSED")


# COMMAND ----------
# Read Bronze

bronze_df = spark.table(BRONZE_TABLE)


# COMMAND ----------
# Type conversion

typed_df = (
    bronze_df
    .withColumn(
        "event_time",
        F.to_timestamp("event_time")
    )
    .withColumn(
        "altitude_ft",
        F.col("altitude_ft").cast("long")
    )
    .withColumn(
        "ground_speed_kts",
        F.col("ground_speed_kts").cast("double")
    )
    .withColumn(
        "engine_temp_c",
        F.col("engine_temp_c").cast("double")
    )
    .withColumn(
        "fuel_remaining_kg",
        F.col("fuel_remaining_kg").cast("double")
    )
)


# COMMAND ----------
# Data Quality validation

validated_df = (
    typed_df
    .withColumn(
        "_dq_reason",

        F.when(
            F.col("event_id").isNull(),
            "missing_event_id"
        )

        .when(
            F.col("aircraft_id").isNull(),
            "missing_aircraft_id"
        )

        .when(
            F.col("flight_id").isNull(),
            "missing_flight_id"
        )

        .when(
            F.col("event_time").isNull(),
            "invalid_event_time"
        )

        .when(
            F.col("altitude_ft") < 0,
            "negative_altitude"
        )

        .when(
            F.col("fuel_remaining_kg") < 0,
            "negative_fuel"
        )

        .when(
            (F.col("engine_temp_c") < -80)
            | (F.col("engine_temp_c") > 1200),
            "invalid_engine_temp"
        )
    )
)


# COMMAND ----------
# Deduplication
#
# Keep the earliest ingested record for each event_id.

dedup_window = (
    Window
    .partitionBy("event_id")
    .orderBy(
        F.col("_ingest_ts").asc()
    )
)

dedup_df = (
    validated_df
    .withColumn(
        "_row_number",
        F.row_number().over(dedup_window)
    )
)


# COMMAND ----------
# Split valid / invalid records

valid_df = (
    dedup_df
    .filter(
        F.col("_dq_reason").isNull()
        & (F.col("_row_number") == 1)
    )
    .drop(
        "_dq_reason",
        "_row_number"
    )
)


invalid_df = (
    dedup_df
    .filter(
        F.col("_dq_reason").isNotNull()
        | (F.col("_row_number") > 1)
    )
    .withColumn(
        "_reject_reason",

        F.when(
            F.col("_row_number") > 1,
            F.lit("duplicate_event")
        )
        .otherwise(
            F.col("_dq_reason")
        )
    )
    .drop(
        "_dq_reason",
        "_row_number"
    )
)


# COMMAND ----------
# Write Silver

(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        SILVER_TABLE
    )
)


# COMMAND ----------
# Write Quarantine

(
    invalid_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        QUARANTINE_TABLE
    )
)


# COMMAND ----------
# Validation

bronze_count = bronze_df.count()
silver_count = valid_df.count()
quarantine_count = invalid_df.count()

print()
print("=" * 60)
print("SILVER PIPELINE COMPLETED")
print("=" * 60)

print(f"Environment : {env.upper()}")
print(f"Bronze      : {bronze_count}")
print(f"Silver      : {silver_count}")
print(f"Quarantine  : {quarantine_count}")

print()
print(f"Silver table     : {SILVER_TABLE}")
print(f"Quarantine table : {QUARANTINE_TABLE}")


# COMMAND ----------
# Display rejected records

display(
    spark.table(QUARANTINE_TABLE)
    .select(
        "event_id",
        "_reject_reason"
    )
    .orderBy("event_id")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM aviation_dev.bronze.telemetry;
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC FROM aviation_prod.silver.telemetry;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM aviation_prod.quarantine.telemetry
# MAGIC ORDER BY event_id;