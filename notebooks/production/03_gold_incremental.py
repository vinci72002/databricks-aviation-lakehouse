# Databricks notebook source
# Databricks notebook source

# ============================================================
# DE-102 - Production Incremental Gold Pipeline
#
# Stage 1:
#   1. Support DEV / PROD environments
#   2. Create processing control table
#   3. Read last successful watermark
#   4. Determine current batch upper watermark
#   5. Detect Silver rows not yet processed by Gold
#
# IMPORTANT:
#   This stage does NOT update the processing watermark.
#   The watermark will only be advanced after:
#
#       Gold MERGE
#           ↓
#       DQ validation
#           ↓
#       Successful commit
#
# ============================================================


# COMMAND ----------
# Environment Configuration

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


# COMMAND ----------
# Environment-specific tables

SILVER_TABLE = (
    f"{catalog}.silver.telemetry"
)

HOURLY_TABLE = (
    f"{catalog}.gold.fact_telemetry_hourly"
)

DAILY_TABLE = (
    f"{catalog}.gold.fact_telemetry_daily"
)

CONTROL_TABLE = (
    f"{catalog}.ops.pipeline_control"
)

PIPELINE_NAME = "telemetry_gold"


# COMMAND ----------
# Environment isolation safety check

expected_table_prefix = f"{catalog}."

tables = [
    SILVER_TABLE,
    HOURLY_TABLE,
    DAILY_TABLE,
    CONTROL_TABLE
]

for table in tables:
    if not table.startswith(expected_table_prefix):
        raise RuntimeError(
            f"Environment isolation failed: {table}"
        )


print("=" * 60)
print("DE-102 - INCREMENTAL GOLD PIPELINE")
print("=" * 60)

print(f"Environment : {env.upper()}")
print(f"Catalog     : {catalog}")
print(f"Silver      : {SILVER_TABLE}")
print(f"Gold Hourly : {HOURLY_TABLE}")
print(f"Gold Daily  : {DAILY_TABLE}")
print(f"Control     : {CONTROL_TABLE}")

print("=" * 60)
print("Environment isolation check: PASSED")


# COMMAND ----------
# Create processing control table
#
# Auto Loader checkpoint:
#   Tracks Raw -> Bronze ingestion progress.
#
# pipeline_control:
#   Tracks Silver -> Gold processing progress.
#
# These are two different types of state.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
    pipeline_name STRING,
    last_success_watermark TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
""")

print(
    f"Control table ready: {CONTROL_TABLE}"
)


# COMMAND ----------
# Initialize processing watermark
#
# This operation is idempotent.
#
# The record is created only if it does not already exist.
#
# 1970-01-01 means:
#   No Silver data has been successfully processed yet.

spark.sql(f"""
MERGE INTO {CONTROL_TABLE} AS target

USING (
    SELECT
        '{PIPELINE_NAME}'
            AS pipeline_name,

        TIMESTAMP('1970-01-01 00:00:00')
            AS last_success_watermark,

        current_timestamp()
            AS updated_at
) AS source

ON target.pipeline_name = source.pipeline_name

WHEN NOT MATCHED THEN

    INSERT (
        pipeline_name,
        last_success_watermark,
        updated_at
    )

    VALUES (
        source.pipeline_name,
        source.last_success_watermark,
        source.updated_at
    )
""")


# COMMAND ----------
# Display current control state

print("Current processing control state:")

display(
    spark.table(CONTROL_TABLE)
    .filter(
        F.col("pipeline_name")
        == PIPELINE_NAME
    )
)


# COMMAND ----------
# Read last successful processing watermark

control_row = (
    spark.table(CONTROL_TABLE)
    .filter(
        F.col("pipeline_name")
        == PIPELINE_NAME
    )
    .select(
        "last_success_watermark"
    )
    .first()
)

if control_row is None:
    raise RuntimeError(
        f"Missing control record "
        f"for pipeline: {PIPELINE_NAME}"
    )

last_watermark = (
    control_row["last_success_watermark"]
)

print(
    f"Last successful watermark: "
    f"{last_watermark}"
)


# COMMAND ----------
# Read Silver
#
# Silver is the source of truth for Gold aggregation.

silver_df = spark.table(
    SILVER_TABLE
)

silver_count = silver_df.count()

print(
    f"Silver input rows: "
    f"{silver_count}"
)


# COMMAND ----------
# Validate required incremental-processing column

required_columns = {
    "event_id",
    "aircraft_id",
    "flight_id",
    "event_time",
    "_ingest_ts"
}

missing_columns = (
    required_columns
    - set(silver_df.columns)
)

if missing_columns:
    raise RuntimeError(
        "Silver table is missing required columns: "
        f"{sorted(missing_columns)}"
    )

print(
    "Required Silver columns: PASSED"
)


# COMMAND ----------
# Determine current batch upper watermark
#
# We create a fixed processing window:
#
#   last_success_watermark
#            <
#       _ingest_ts
#            <=
#       upper_watermark
#
# upper_watermark is the maximum ingestion timestamp
# currently available in Silver.

upper_watermark_row = (
    silver_df
    .agg(
        F.max("_ingest_ts")
        .alias("upper_watermark")
    )
    .first()
)

upper_watermark = (
    upper_watermark_row[
        "upper_watermark"
    ]
)

print()
print("=" * 60)
print("PROCESSING WINDOW")
print("=" * 60)

print(
    f"Last watermark : "
    f"{last_watermark}"
)

print(
    f"Upper watermark: "
    f"{upper_watermark}"
)


# COMMAND ----------
# Detect Silver rows not yet processed by Gold
#
# IMPORTANT:
#
# We use _ingest_ts instead of event_time.
#
# A late-arriving event may have:
#
#   event_time = yesterday
#   _ingest_ts = today
#
# It must still be detected as newly arrived data.

if upper_watermark is None:

    changed_df = (
        silver_df
        .limit(0)
    )

else:

    changed_df = (
        silver_df
        .filter(
            (
                F.col("_ingest_ts")
                > F.lit(last_watermark)
            )
            &
            (
                F.col("_ingest_ts")
                <= F.lit(upper_watermark)
            )
        )
    )


# COMMAND ----------
# Count changed Silver rows

changed_count = (
    changed_df.count()
)

# No new data -> finish safely

if changed_count == 0:
    print("=" * 60)
    print("NO NEW DATA")
    print("=" * 60)
    print("No new Silver rows detected.")
    print("Gold processing is not required.")
    print("Watermark remains unchanged.")

    dbutils.notebook.exit("NO_NEW_DATA")


    


print()
print("=" * 60)
print("INCREMENTAL DETECTION RESULT")
print("=" * 60)

print(
    f"Silver rows         : "
    f"{silver_count}"
)

print(
    f"Changed Silver rows : "
    f"{changed_count}"
)

print(
    f"Last watermark      : "
    f"{last_watermark}"
)

print(
    f"Upper watermark     : "
    f"{upper_watermark}"
)


# COMMAND ----------
# Display changed rows

display(
    changed_df
    .select(
        "event_id",
        "aircraft_id",
        "flight_id",
        "event_time",
        "_ingest_ts"
    )
    .orderBy(
        "_ingest_ts",
        "event_id"
    )
)


# COMMAND ----------
# Stage 1 validation
#
# IMPORTANT:
#
# DO NOT update last_success_watermark here.
#
# At this point we have only DETECTED the data.
#
# The production sequence must be:
#
#   Detect changed rows
#       ↓
#   Find affected grains
#       ↓
#   Recompute complete affected grains
#       ↓
#   MERGE Gold
#       ↓
#   DQ validation
#       ↓
#   COMMITTED
#       ↓
#   Advance watermark
#
# If processing fails before COMMITTED,
# the watermark must remain unchanged.

print()
print("=" * 60)
print("DE-102 STAGE 1 COMPLETED")
print("=" * 60)

if upper_watermark is None:

    print(
        "No Silver data is available."
    )

elif changed_count == 0:

    print(
        "No new Silver rows detected."
    )

else:

    print(
        f"{changed_count} Silver rows "
        "are waiting for Gold processing."
    )

print()
print(
    "Processing watermark has NOT been advanced."
)

print(
    "Next step: determine affected "
    "hourly and daily grains."
)

# COMMAND ----------

# COMMAND ----------
# DE-102 Stage 2.1
# Find affected hourly grains

affected_hourly_df = (
    changed_df
    .withColumn(
        "telemetry_hour",
        F.date_trunc(
            "hour",
            F.col("event_time")
        )
    )
    .select(
        "aircraft_id",
        "flight_id",
        "telemetry_hour"
    )
    .distinct()
)

affected_hourly_count = affected_hourly_df.count()

print(
    f"Affected hourly grains: "
    f"{affected_hourly_count}"
)

display(
    affected_hourly_df
    .orderBy(
        "aircraft_id",
        "flight_id",
        "telemetry_hour"
    )
)

# COMMAND ----------

# COMMAND ----------
# DE-102 Stage 2.2
# Read complete Silver rows for affected hourly grains

silver_hourly_df = (
    silver_df
    .withColumn(
        "telemetry_hour",
        F.date_trunc(
            "hour",
            F.col("event_time")
        )
    )
)

affected_hourly_source_df = (
    silver_hourly_df
    .join(
        affected_hourly_df,
        on=[
            "aircraft_id",
            "flight_id",
            "telemetry_hour"
        ],
        how="inner"
    )
)

source_count = affected_hourly_source_df.count()

print(
    f"Complete Silver rows for affected hours: "
    f"{source_count}"
)

display(
    affected_hourly_source_df
    .select(
        "event_id",
        "aircraft_id",
        "flight_id",
        "event_time",
        "telemetry_hour",
        "engine_temp_c"
    )
    .orderBy(
        "aircraft_id",
        "flight_id",
        "event_time"
    )
)

# COMMAND ----------

# COMMAND ----------
# DE-102 Stage 2.3
# Recompute complete affected hourly grains

hourly_updates_df = (
    affected_hourly_source_df
    .groupBy(
        "aircraft_id",
        "flight_id",
        "telemetry_hour"
    )
    .agg(
        F.count("*").alias("sample_count"),

        F.avg("altitude_ft").alias(
            "avg_altitude_ft"
        ),

        F.avg("ground_speed_kts").alias(
            "avg_ground_speed_kts"
        ),

        F.avg("engine_temp_c").alias(
            "avg_engine_temp_c"
        ),

        F.max("engine_temp_c").alias(
            "max_engine_temp_c"
        ),

        F.min("fuel_remaining_kg").alias(
            "min_fuel_remaining_kg"
        )
    )
)

print(
    f"Recomputed hourly grains: "
    f"{hourly_updates_df.count()}"
)

display(
    hourly_updates_df
    .orderBy(
        "aircraft_id",
        "flight_id",
        "telemetry_hour"
    )
)

# COMMAND ----------

# COMMAND ----------
# DE-102 Stage 2.4
# MERGE affected hourly grains into Gold

from delta.tables import DeltaTable

hourly_gold = DeltaTable.forName(
    spark,
    HOURLY_TABLE
)

(
    hourly_gold.alias("target")
    .merge(
        hourly_updates_df.alias("source"),
        """
        target.aircraft_id = source.aircraft_id
        AND target.flight_id = source.flight_id
        AND target.telemetry_hour = source.telemetry_hour
        """
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("Hourly Gold MERGE completed.")

display(
    spark.table(HOURLY_TABLE)
    .orderBy(
        "aircraft_id",
        "flight_id",
        "telemetry_hour"
    )
)

# COMMAND ----------

# COMMAND ----------
# DE-102 Stage 2.5
# Find affected daily grains

affected_daily_df = (
    changed_df
    .withColumn(
        "telemetry_date",
        F.to_date(
            F.col("event_time")
        )
    )
    .select(
        "aircraft_id",
        "flight_id",
        "telemetry_date"
    )
    .distinct()
)

affected_daily_count = affected_daily_df.count()

print(
    f"Affected daily grains: "
    f"{affected_daily_count}"
)

display(
    affected_daily_df
    .orderBy(
        "aircraft_id",
        "flight_id",
        "telemetry_date"
    )
)

# COMMAND ----------

# COMMAND ----------
# DE-102 Stage 2.6
# Recompute and MERGE affected daily grains


# 1. Add daily grain key to complete Silver data

silver_daily_df = (
    silver_df
    .withColumn(
        "telemetry_date",
        F.to_date(
            F.col("event_time")
        )
    )
)


# 2. Read complete Silver rows
#    belonging to affected daily grains

affected_daily_source_df = (
    silver_daily_df
    .join(
        affected_daily_df,
        on=[
            "aircraft_id",
            "flight_id",
            "telemetry_date"
        ],
        how="inner"
    )
)


# 3. Recompute complete affected daily grains

daily_updates_df = (
    affected_daily_source_df
    .groupBy(
        "aircraft_id",
        "flight_id",
        "telemetry_date"
    )
    .agg(
        F.count("*").alias("sample_count"),

        F.avg("altitude_ft").alias(
            "avg_altitude_ft"
        ),

        F.avg("ground_speed_kts").alias(
            "avg_ground_speed_kts"
        ),

        F.avg("engine_temp_c").alias(
            "avg_engine_temp_c"
        ),

        F.max("engine_temp_c").alias(
            "max_engine_temp_c"
        ),

        F.min("fuel_remaining_kg").alias(
            "min_fuel_remaining_kg"
        )
    )
)


# 4. MERGE into Daily Gold

daily_gold = DeltaTable.forName(
    spark,
    DAILY_TABLE
)

(
    daily_gold.alias("target")
    .merge(
        daily_updates_df.alias("source"),
        """
        target.aircraft_id = source.aircraft_id
        AND target.flight_id = source.flight_id
        AND target.telemetry_date = source.telemetry_date
        """
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)


print("Daily Gold MERGE completed.")

display(
    spark.table(DAILY_TABLE)
    .orderBy(
        "aircraft_id",
        "flight_id",
        "telemetry_date"
    )
)

# COMMAND ----------

# COMMAND ----------
# DE-102 Stage 3.1
# Validate Gold before committing processing watermark


# Check duplicate Hourly grains

hourly_duplicate_count = (
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


# Check duplicate Daily grains

daily_duplicate_count = (
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


print(
    f"Hourly duplicate grains: "
    f"{hourly_duplicate_count}"
)

print(
    f"Daily duplicate grains : "
    f"{daily_duplicate_count}"
)


if hourly_duplicate_count > 0:
    raise RuntimeError(
        "DQ FAILED: duplicate hourly grains"
    )

if daily_duplicate_count > 0:
    raise RuntimeError(
        "DQ FAILED: duplicate daily grains"
    )


print("Gold DQ validation: PASSED")

# COMMAND ----------

# COMMAND ----------
# DE-102 Validation 3
# Failure injection before watermark commit

dbutils.widgets.dropdown(
    "inject_failure",
    "false",
    ["false", "true"],
    "Inject Failure"
)

inject_failure = (
    dbutils.widgets
    .get("inject_failure")
    .lower() == "true"
)

if inject_failure:
    raise RuntimeError(
        "DE-102 TEST FAILURE: "
        "Failure injected before watermark commit."
    )

print("Failure injection: OFF")

# COMMAND ----------

# COMMAND ----------
# DE-102 Stage 3.2
# Commit successful processing watermark

if upper_watermark is not None:

    spark.sql(f"""
        UPDATE {CONTROL_TABLE}

        SET
            last_success_watermark =
                TIMESTAMP('{upper_watermark}'),

            updated_at =
                current_timestamp()

        WHERE pipeline_name =
            '{PIPELINE_NAME}'
    """)

    print(
        "Processing watermark committed:"
    )

    print(
        f"{last_watermark}"
        f"  ->  "
        f"{upper_watermark}"
    )

else:

    print(
        "No Silver data available. "
        "Watermark unchanged."
    )


# COMMAND ----------
# Verify control state

display(
    spark.table(CONTROL_TABLE)
    .filter(
        F.col("pipeline_name")
        == PIPELINE_NAME
    )
)

# COMMAND ----------

# COMMAND ----------
# DE-102 Validation 1
# Check for Silver rows after committed watermark

current_watermark = (
    spark.table(CONTROL_TABLE)
    .filter(
        F.col("pipeline_name") == PIPELINE_NAME
    )
    .select("last_success_watermark")
    .first()["last_success_watermark"]
)

new_rows_df = (
    spark.table(SILVER_TABLE)
    .filter(
        F.col("_ingest_ts") > F.lit(current_watermark)
    )
)

new_row_count = new_rows_df.count()

print(f"Current watermark : {current_watermark}")
print(f"New Silver rows   : {new_row_count}")

# COMMAND ----------

display(
    spark.table(HOURLY_TABLE)
    .orderBy(
        "aircraft_id",
        "flight_id",
        "telemetry_hour"
    )
)

# COMMAND ----------

display(
    spark.table(CONTROL_TABLE)
    .filter(F.col("pipeline_name") == PIPELINE_NAME)
)

# COMMAND ----------

display(
    spark.table(CONTROL_TABLE)
    .filter(F.col("pipeline_name") == PIPELINE_NAME)
    .select("last_success_watermark", "updated_at")
)