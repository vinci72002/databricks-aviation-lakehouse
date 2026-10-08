# Databricks notebook source
# ============================================================
# 02 - Silver Telemetry
#
# Bronze -> Type conversion -> DQ -> Dedup
#        -> Silver / Quarantine
#
# Processing mode:
#   Full Bronze snapshot, overwrite both output tables.
#
# Dedup policy:
#   Keep the earliest valid record for each event_id.
#   Use record content to break ingestion-time ties.
#
# Preserve _ingest_ts for DE-102 incremental Gold processing.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Environment configuration

dbutils.widgets.text("env", "dev", "Environment")

env = dbutils.widgets.get("env").strip().lower()

if env not in {"dev", "prod"}:
    raise ValueError(f"Invalid environment: {env!r}. " "Allowed values: dev, prod")

catalog = f"aviation_{env}"

BRONZE_TABLE = f"{catalog}.bronze.telemetry"
SILVER_TABLE = f"{catalog}.silver.telemetry"
QUARANTINE_TABLE = f"{catalog}.quarantine.telemetry"

for table in (
    BRONZE_TABLE,
    SILVER_TABLE,
    QUARANTINE_TABLE,
):
    if not table.startswith(f"{catalog}."):
        raise RuntimeError(f"Environment isolation failed: {table}")

print("=" * 60)
print("SILVER TELEMETRY PIPELINE")
print("=" * 60)
print(f"Environment : {env.upper()}")
print(f"Bronze      : {BRONZE_TABLE}")
print(f"Silver      : {SILVER_TABLE}")
print(f"Quarantine  : {QUARANTINE_TABLE}")


# 2. Read Bronze and check required columns

if not spark.catalog.tableExists(BRONZE_TABLE):
    raise RuntimeError(
        f"Missing Bronze table: {BRONZE_TABLE}. " "Run notebook 01 first."
    )

bronze_df = spark.table(BRONZE_TABLE)

required_columns = {
    "event_id",
    "aircraft_id",
    "flight_id",
    "event_time",
    "altitude_ft",
    "ground_speed_kts",
    "engine_temp_c",
    "fuel_remaining_kg",
    "_ingest_ts",
}

missing_columns = required_columns - set(bronze_df.columns)

if missing_columns:
    raise RuntimeError(
        f"Bronze is missing required columns: " f"{sorted(missing_columns)}"
    )


# 3. Safe type conversion
#
# Preserve the original row as JSON for rejected-record analysis
# and deterministic tie-breaking.

raw_columns = sorted(bronze_df.columns)

typed_df = bronze_df.withColumn(
    "_raw_record",
    F.to_json(
        F.struct(*[F.col(name) for name in raw_columns]),
        options={"ignoreNullFields": "false"},
    ),
)

target_types = {
    "event_id": "STRING",
    "aircraft_id": "STRING",
    "flight_id": "STRING",
    "event_time": "TIMESTAMP",
    "altitude_ft": "BIGINT",
    "ground_speed_kts": "DOUBLE",
    "engine_temp_c": "DOUBLE",
    "fuel_remaining_kg": "DOUBLE",
    "_ingest_ts": "TIMESTAMP",
}

for column, target_type in target_types.items():
    typed_df = typed_df.withColumn(
        column,
        F.expr(f"try_cast(`{column}` AS {target_type})"),
    )


# 4. Data quality rules
#
# First matching reason wins.
# Numeric fields are required under this version's policy.


def missing_id(column):
    return F.col(column).isNull() | (F.length(F.trim(F.col(column))) == 0)


def invalid_number(column):
    return (
        F.col(column).isNull()
        | F.isnan(F.col(column).cast("double"))
        | (F.abs(F.col(column).cast("double")) == F.lit(float("inf")))
    )


validated_df = typed_df.withColumn(
    "_dq_reason",
    F.when(
        missing_id("event_id"),
        "missing_event_id",
    )
    .when(
        missing_id("aircraft_id"),
        "missing_aircraft_id",
    )
    .when(
        missing_id("flight_id"),
        "missing_flight_id",
    )
    .when(
        F.col("event_time").isNull(),
        "invalid_event_time",
    )
    .when(
        F.col("_ingest_ts").isNull(),
        "invalid_ingest_ts",
    )
    .when(
        invalid_number("altitude_ft"),
        "invalid_altitude",
    )
    .when(
        F.col("altitude_ft") < 0,
        "negative_altitude",
    )
    .when(
        invalid_number("ground_speed_kts"),
        "invalid_ground_speed",
    )
    .when(
        F.col("ground_speed_kts") < 0,
        "negative_ground_speed",
    )
    .when(
        invalid_number("fuel_remaining_kg"),
        "invalid_fuel",
    )
    .when(
        F.col("fuel_remaining_kg") < 0,
        "negative_fuel",
    )
    .when(
        invalid_number("engine_temp_c")
        | (F.col("engine_temp_c") < -80)
        | (F.col("engine_temp_c") > 1200),
        "invalid_engine_temp",
    ),
)

# Reject rescued input instead of silently ignoring it.
if "_rescued_data" in bronze_df.columns:
    validated_df = validated_df.withColumn(
        "_dq_reason",
        F.when(
            F.col("_dq_reason").isNotNull(),
            F.col("_dq_reason"),
        ).when(
            F.col("_rescued_data").isNotNull(),
            F.lit("rescued_data"),
        ),
    )


# 5. Deduplicate only quality-approved records

quality_valid_df = validated_df.filter(F.col("_dq_reason").isNull())

dedup_window = Window.partitionBy("event_id").orderBy(
    F.col("_ingest_ts").asc(),
    F.col("_raw_record").asc(),
)

ranked_df = quality_valid_df.withColumn(
    "_row_number",
    F.row_number().over(dedup_window),
)

valid_df = ranked_df.filter(F.col("_row_number") == 1).drop(
    "_row_number", "_dq_reason", "_raw_record"
)

quality_rejected_df = (
    validated_df.filter(F.col("_dq_reason").isNotNull())
    .withColumn("_reject_reason", F.col("_dq_reason"))
    .drop("_dq_reason")
)

duplicate_rejected_df = (
    ranked_df.filter(F.col("_row_number") > 1)
    .withColumn("_reject_reason", F.lit("duplicate_event"))
    .drop("_row_number", "_dq_reason")
)

invalid_df = quality_rejected_df.unionByName(duplicate_rejected_df)


# 6. Validate outputs before writing
#
# Full counts are suitable for this small learning dataset.

bronze_count = bronze_df.count()
silver_count = valid_df.count()
quarantine_count = invalid_df.count()

if bronze_count != silver_count + quarantine_count:
    raise RuntimeError(
        "Row reconciliation failed: "
        f"Bronze={bronze_count}, "
        f"Silver={silver_count}, "
        f"Quarantine={quarantine_count}"
    )

duplicate_groups = (
    valid_df.groupBy("event_id").count().filter(F.col("count") > 1).count()
)

if duplicate_groups:
    raise RuntimeError("Silver validation failed: duplicate event_id")

print("Row reconciliation: PASSED")
print("Silver uniqueness: PASSED")


# 7. Write full snapshots
#
# Each Delta write is independent.
# If the second write fails, the task fails; rerun both writes.
# Run this task without concurrent Bronze updates for the lab.

(
    valid_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

(
    invalid_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(QUARANTINE_TABLE)
)


# 8. Verify persisted row counts

actual_silver_count = spark.table(SILVER_TABLE).count()
actual_quarantine_count = spark.table(QUARANTINE_TABLE).count()

if actual_silver_count != silver_count or actual_quarantine_count != quarantine_count:
    raise RuntimeError("Persisted row counts do not match expected outputs.")


# 9. Final status

print()
print("=" * 60)
print("SILVER PIPELINE: SUCCESS")
print("=" * 60)
print(f"Environment : {env.upper()}")
print(f"Bronze      : {bronze_count}")
print(f"Silver      : {actual_silver_count}")
print(f"Quarantine  : {actual_quarantine_count}")

print("\nReject reason summary:")

display(
    spark.table(QUARANTINE_TABLE)
    .groupBy("_reject_reason")
    .count()
    .orderBy("_reject_reason")
)
