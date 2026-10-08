# Databricks notebook source
# ============================================================
# 03 - Incremental Gold Telemetry
#
# Detect new arrivals using _ingest_ts.
# Recompute complete affected hourly / daily grains.
# MERGE -> DQ -> optional failure injection -> watermark commit.
#
# Run with max concurrent runs = 1.
# ============================================================

from datetime import datetime

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


# 2. Configuration

catalog = f"aviation_{env}"

SILVER_TABLE = f"{catalog}.silver.telemetry"
HOURLY_TABLE = f"{catalog}.gold.fact_telemetry_hourly"
DAILY_TABLE = f"{catalog}.gold.fact_telemetry_daily"
CONTROL_TABLE = f"{catalog}.ops.pipeline_control"

PIPELINE_NAME = "telemetry_gold"
INITIAL_WATERMARK = datetime(1970, 1, 1)

for table in (
    SILVER_TABLE,
    HOURLY_TABLE,
    DAILY_TABLE,
    CONTROL_TABLE,
):
    if not table.startswith(f"{catalog}."):
        raise RuntimeError(f"Environment isolation failed: {table}")

print("=" * 60)
print("INCREMENTAL GOLD PIPELINE")
print("=" * 60)
print(f"Environment    : {env.upper()}")
print(f"Silver         : {SILVER_TABLE}")
print(f"Hourly         : {HOURLY_TABLE}")
print(f"Daily          : {DAILY_TABLE}")
print(f"Control        : {CONTROL_TABLE}")
print(f"Inject failure : {inject_failure}")


# 3. Validate Silver prerequisites

if not spark.catalog.tableExists(SILVER_TABLE):
    raise RuntimeError(f"Missing Silver table: {SILVER_TABLE}. Run 02 first.")

silver_df = spark.table(SILVER_TABLE)

required_columns = {
    "event_id",
    "aircraft_id",
    "flight_id",
    "event_time",
    "_ingest_ts",
    "altitude_ft",
    "ground_speed_kts",
    "engine_temp_c",
    "fuel_remaining_kg",
}

missing_columns = required_columns - set(silver_df.columns)

if missing_columns:
    raise RuntimeError(f"Missing Silver columns: {sorted(missing_columns)}")

invalid_keys = (
    silver_df.filter(
        F.col("event_id").isNull()
        | F.col("aircraft_id").isNull()
        | F.col("flight_id").isNull()
        | F.col("event_time").isNull()
        | F.col("_ingest_ts").isNull()
    )
    .limit(1)
    .count()
)

if invalid_keys:
    raise RuntimeError("Silver contains null keys or timestamps. Check 02.")


# 4. Initialize control table and read watermark

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
    pipeline_name STRING,
    last_success_watermark TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
MERGE INTO {CONTROL_TABLE} AS target
USING (
    SELECT
        '{PIPELINE_NAME}' AS pipeline_name,
        TIMESTAMP('1970-01-01 00:00:00')
            AS last_success_watermark,
        current_timestamp() AS updated_at
) AS source
ON target.pipeline_name = source.pipeline_name
WHEN NOT MATCHED THEN INSERT *
""")

control_rows = (
    spark.table(CONTROL_TABLE)
    .filter(F.col("pipeline_name") == PIPELINE_NAME)
    .select("last_success_watermark")
    .collect()
)

if len(control_rows) != 1:
    raise RuntimeError(
        f"Expected one control record for {PIPELINE_NAME}, "
        f"found {len(control_rows)}."
    )

last_watermark = control_rows[0]["last_success_watermark"]

if last_watermark is None:
    raise RuntimeError("Processing watermark is null.")

print(f"Last watermark : {last_watermark}")


# 5. Check first-run / recovery state
#
# Never silently create empty Gold tables when the watermark
# indicates historical data has already been processed.

missing_gold_tables = [
    table
    for table in (HOURLY_TABLE, DAILY_TABLE)
    if not spark.catalog.tableExists(table)
]

if missing_gold_tables and last_watermark != INITIAL_WATERMARK:
    raise RuntimeError(
        "Gold tables are missing but the watermark has advanced. "
        f"Missing: {missing_gold_tables}. "
        "Restore Gold or perform a controlled full rebuild. "
        "The watermark has not been reset."
    )

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {HOURLY_TABLE} (
    aircraft_id STRING,
    flight_id STRING,
    telemetry_hour TIMESTAMP,
    sample_count BIGINT,
    avg_altitude_ft DOUBLE,
    avg_ground_speed_kts DOUBLE,
    avg_engine_temp_c DOUBLE,
    max_engine_temp_c DOUBLE,
    min_fuel_remaining_kg DOUBLE
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DAILY_TABLE} (
    aircraft_id STRING,
    flight_id STRING,
    telemetry_date DATE,
    sample_count BIGINT,
    avg_altitude_ft DOUBLE,
    avg_ground_speed_kts DOUBLE,
    avg_engine_temp_c DOUBLE,
    max_engine_temp_c DOUBLE,
    min_fuel_remaining_kg DOUBLE
)
USING DELTA
""")


# 6. Validate Gold grains


def validate_gold():
    definitions = (
        (
            HOURLY_TABLE,
            ["aircraft_id", "flight_id", "telemetry_hour"],
        ),
        (
            DAILY_TABLE,
            ["aircraft_id", "flight_id", "telemetry_date"],
        ),
    )

    for table, keys in definitions:
        df = spark.table(table)

        duplicate_found = (
            df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
        )

        if duplicate_found:
            raise RuntimeError(f"DQ FAILED: duplicate grains in {table}")

        invalid = F.col("sample_count").isNull() | (F.col("sample_count") <= 0)

        for key in keys:
            invalid = invalid | F.col(key).isNull()

        if df.filter(invalid).limit(1).count():
            raise RuntimeError(
                f"DQ FAILED: null grain keys or invalid " f"sample_count in {table}"
            )

    print("Gold DQ validation: PASSED")


# Check existing targets before MERGE.
validate_gold()


# 7. Fix the processing window

upper_watermark = silver_df.agg(F.max("_ingest_ts").alias("upper_watermark")).first()[
    "upper_watermark"
]

if upper_watermark is None:
    print("Silver is empty. Watermark remains unchanged.")
    dbutils.notebook.exit("NO_NEW_DATA")

changed_df = silver_df.filter(
    (F.col("_ingest_ts") > F.lit(last_watermark))
    & (F.col("_ingest_ts") <= F.lit(upper_watermark))
)

changed_count = changed_df.count()

print(f"Upper watermark: {upper_watermark}")
print(f"Changed rows   : {changed_count}")

if changed_count == 0:
    print("No new arrivals. Watermark remains unchanged.")
    dbutils.notebook.exit("NO_NEW_DATA")

# Use a bounded source for all recomputations in this run.
batch_source_df = silver_df.filter(F.col("_ingest_ts") <= F.lit(upper_watermark))


# 8. Recompute complete affected grains


def recompute_grains(grain_column, grain_expression):
    keys = ["aircraft_id", "flight_id", grain_column]

    affected_df = (
        changed_df.withColumn(grain_column, grain_expression).select(*keys).distinct()
    )

    complete_source_df = batch_source_df.withColumn(
        grain_column, grain_expression
    ).join(affected_df, on=keys, how="inner")

    updates_df = complete_source_df.groupBy(*keys).agg(
        F.count("*").alias("sample_count"),
        F.avg("altitude_ft").alias("avg_altitude_ft"),
        F.avg("ground_speed_kts").alias("avg_ground_speed_kts"),
        F.avg("engine_temp_c").alias("avg_engine_temp_c"),
        F.max("engine_temp_c").alias("max_engine_temp_c"),
        F.min("fuel_remaining_kg").alias("min_fuel_remaining_kg"),
    )

    return updates_df, keys


hourly_updates_df, hourly_keys = recompute_grains(
    "telemetry_hour",
    F.date_trunc("hour", F.col("event_time")),
)

daily_updates_df, daily_keys = recompute_grains(
    "telemetry_date",
    F.to_date("event_time"),
)

hourly_grain_count = hourly_updates_df.count()
daily_grain_count = daily_updates_df.count()

print(f"Affected hourly grains: {hourly_grain_count}")
print(f"Affected daily grains : {daily_grain_count}")


# 9. MERGE Gold
#
# Replace each affected grain's metrics.
# Do not add the new count to the existing count.


def merge_gold(table, updates_df, keys):
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)

    (
        DeltaTable.forName(spark, table)
        .alias("target")
        .merge(updates_df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"MERGE completed: {table}")


merge_gold(HOURLY_TABLE, hourly_updates_df, hourly_keys)
merge_gold(DAILY_TABLE, daily_updates_df, daily_keys)


# 10. Validate Gold before watermark commit

validate_gold()


# 11. Optional failure injection

if inject_failure:
    raise RuntimeError(
        "TEST FAILURE: injected after Gold MERGE and DQ, "
        "before watermark commit. "
        "Gold writes may already be present; rerun safely "
        "with inject_failure=false."
    )


# 12. Commit watermark using a typed DataFrame
#
# No timestamp-to-SQL-string conversion is required.

commit_df = spark.createDataFrame(
    [(PIPELINE_NAME, upper_watermark)],
    schema=("pipeline_name STRING, " "last_success_watermark TIMESTAMP"),
).withColumn("updated_at", F.current_timestamp())

(
    DeltaTable.forName(spark, CONTROL_TABLE)
    .alias("target")
    .merge(
        commit_df.alias("source"),
        "target.pipeline_name = source.pipeline_name",
    )
    .whenMatchedUpdate(
        set={
            "last_success_watermark": "source.last_success_watermark",
            "updated_at": "source.updated_at",
        }
    )
    .execute()
)

committed_watermark = (
    spark.table(CONTROL_TABLE)
    .filter(F.col("pipeline_name") == PIPELINE_NAME)
    .select("last_success_watermark")
    .first()["last_success_watermark"]
)

if committed_watermark != upper_watermark:
    raise RuntimeError("Watermark verification failed after commit.")


# 13. Final status

print()
print("=" * 60)
print("INCREMENTAL GOLD PIPELINE: SUCCESS")
print("=" * 60)
print(f"Environment    : {env.upper()}")
print(f"Changed rows   : {changed_count}")
print(f"Hourly grains  : {hourly_grain_count}")
print(f"Daily grains   : {daily_grain_count}")
print(f"Old watermark  : {last_watermark}")
print(f"New watermark  : {committed_watermark}")
