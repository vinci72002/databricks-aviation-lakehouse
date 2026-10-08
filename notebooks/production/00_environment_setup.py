# Databricks notebook source
# Databricks notebook source
# ============================================================
# DE-101 - Environment & Configuration
# Aviation Lakehouse Production Simulation
#
# Purpose:
#   1. Create isolated DEV / PROD environments
#   2. Use the same code for both environments
#   3. Parameterize catalog, tables, volumes and checkpoints
#
# Environment mapping:
#   env=dev  -> aviation_dev
#   env=prod -> aviation_prod
# ============================================================


# COMMAND ----------
# 1. Runtime parameter

dbutils.widgets.text("env", "dev", "Environment")

env = dbutils.widgets.get("env").strip().lower()

ALLOWED_ENVS = {"dev", "prod"}

if env not in ALLOWED_ENVS:
    raise ValueError(
        f"Invalid environment: '{env}'. "
        f"Allowed values: {sorted(ALLOWED_ENVS)}"
    )

catalog = f"aviation_{env}"

print("=" * 60)
print("AVIATION LAKEHOUSE")
print("=" * 60)
print(f"Environment : {env.upper()}")
print(f"Catalog     : {catalog}")
print("=" * 60)


# COMMAND ----------
# 2. Environment definitions

SCHEMAS = [
    "raw",
    "bronze",
    "silver",
    "quarantine",
    "gold",
    "ops"
]

print("Schemas:")
for schema in SCHEMAS:
    print(f"  {catalog}.{schema}")


# COMMAND ----------
# 3. Create catalog

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalog}
""")

print(f"Catalog ready: {catalog}")


# COMMAND ----------
# 4. Create schemas

for schema in SCHEMAS:

    spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
    """)

    print(f"Schema ready: {catalog}.{schema}")


# COMMAND ----------
# 5. Create landing volume
#
# In this free Production Lab the Unity Catalog Volume
# simulates an external cloud landing zone such as ADLS Gen2.

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {catalog}.raw.landing
""")

print(f"Volume ready: {catalog}.raw.landing")


# COMMAND ----------
# 6. Standard storage paths
#
# IMPORTANT:
# DEV and PROD must never share:
#   - source path
#   - Auto Loader schemaLocation
#   - checkpointLocation

RAW_VOLUME = f"/Volumes/{catalog}/raw/landing"

TELEMETRY_SOURCE_PATH = (
    f"{RAW_VOLUME}/telemetry"
)

TELEMETRY_SCHEMA_PATH = (
    f"{RAW_VOLUME}/_schemas/telemetry"
)

TELEMETRY_CHECKPOINT_PATH = (
    f"{RAW_VOLUME}/_checkpoints/bronze_telemetry"
)


# COMMAND ----------
# 7. Standard table names

BRONZE_TELEMETRY_TABLE = (
    f"{catalog}.bronze.telemetry"
)

SILVER_TELEMETRY_TABLE = (
    f"{catalog}.silver.telemetry"
)

SILVER_AIRCRAFT_TABLE = (
    f"{catalog}.silver.aircraft"
)

QUARANTINE_TELEMETRY_TABLE = (
    f"{catalog}.quarantine.telemetry"
)

GOLD_HOURLY_TABLE = (
    f"{catalog}.gold.fact_telemetry_hourly"
)

GOLD_DAILY_TABLE = (
    f"{catalog}.gold.fact_telemetry_daily"
)

DIM_AIRCRAFT_TABLE = (
    f"{catalog}.gold.dim_aircraft"
)

PIPELINE_AUDIT_TABLE = (
    f"{catalog}.ops.pipeline_audit"
)

# DE-102 will use this table for incremental processing.
PIPELINE_CONTROL_TABLE = (
    f"{catalog}.ops.pipeline_control"
)


# COMMAND ----------
# 8. Display resolved configuration

print()
print("=" * 60)
print("RESOLVED ENVIRONMENT CONFIGURATION")
print("=" * 60)

print(f"""
Environment
-----------
env                     = {env}
catalog                 = {catalog}

Storage
-------
raw_volume              = {RAW_VOLUME}
telemetry_source        = {TELEMETRY_SOURCE_PATH}
schema_location         = {TELEMETRY_SCHEMA_PATH}
checkpoint_location     = {TELEMETRY_CHECKPOINT_PATH}

Bronze
------
bronze_telemetry        = {BRONZE_TELEMETRY_TABLE}

Silver
------
silver_telemetry        = {SILVER_TELEMETRY_TABLE}
silver_aircraft         = {SILVER_AIRCRAFT_TABLE}

Quarantine
----------
quarantine_telemetry    = {QUARANTINE_TELEMETRY_TABLE}

Gold
----
gold_hourly             = {GOLD_HOURLY_TABLE}
gold_daily              = {GOLD_DAILY_TABLE}
dim_aircraft            = {DIM_AIRCRAFT_TABLE}

Operations
----------
pipeline_audit          = {PIPELINE_AUDIT_TABLE}
pipeline_control        = {PIPELINE_CONTROL_TABLE}
""")


# COMMAND ----------
# 9. Safety validation
#
# Prevent accidental cross-environment paths.

expected_prefix = f"/Volumes/aviation_{env}/"

paths_to_validate = [
    RAW_VOLUME,
    TELEMETRY_SOURCE_PATH,
    TELEMETRY_SCHEMA_PATH,
    TELEMETRY_CHECKPOINT_PATH
]

for path in paths_to_validate:

    if not path.startswith(expected_prefix):

        raise RuntimeError(
            "Environment isolation validation failed.\n"
            f"Environment : {env}\n"
            f"Path        : {path}\n"
            f"Expected    : {expected_prefix}"
        )


tables_to_validate = [
    BRONZE_TELEMETRY_TABLE,
    SILVER_TELEMETRY_TABLE,
    SILVER_AIRCRAFT_TABLE,
    QUARANTINE_TELEMETRY_TABLE,
    GOLD_HOURLY_TABLE,
    GOLD_DAILY_TABLE,
    DIM_AIRCRAFT_TABLE,
    PIPELINE_AUDIT_TABLE,
    PIPELINE_CONTROL_TABLE
]

expected_catalog_prefix = f"aviation_{env}."

for table in tables_to_validate:

    if not table.startswith(expected_catalog_prefix):

        raise RuntimeError(
            "Environment isolation validation failed.\n"
            f"Environment : {env}\n"
            f"Table       : {table}\n"
            f"Expected    : {expected_catalog_prefix}"
        )

print("Environment isolation validation: PASSED")


# COMMAND ----------
# 10. Validate catalog objects

print()
print("=" * 60)
print("ENVIRONMENT OBJECTS")
print("=" * 60)

display(
    spark.sql(
        f"SHOW SCHEMAS IN {catalog}"
    )
)


# COMMAND ----------
# 11. Validate landing volume

display(
    spark.sql(
        f"SHOW VOLUMES IN {catalog}.raw"
    )
)


# COMMAND ----------
# 12. Final status

print()
print("=" * 60)
print("DE-101 ENVIRONMENT SETUP: SUCCESS")
print("=" * 60)

print(f"""
Active environment : {env.upper()}
Active catalog     : {catalog}

The pipeline is configured to use:

{TELEMETRY_SOURCE_PATH}

and write only to:

{catalog}.*

No DEV/PROD data is shared through the configured
table, schema-state or checkpoint paths.
""")

# COMMAND ----------

display(
    spark.table("aviation_dev.bronze.telemetry")
    .select(
        "event_id",
        "aircraft_id",
        "flight_id",
        "event_time",
        "altitude_ft",
        "ground_speed_kts",
        "engine_temp_c",
        "fuel_remaining_kg"
    )
    .orderBy("event_time")
)

# COMMAND ----------

from pyspark.sql import functions as F

display(
    spark.table("aviation_dev.bronze.telemetry")
    .filter(F.col("event_id") == "EVT-004")
)

# COMMAND ----------

test_data = """{"aircraft_id":"AC-101","altitude_ft":34000,"engine_temp_c":650.0,"event_id":"EVT-NEW-001","event_time":"2026-10-04T13:00:00","flight_id":"FL-001","fuel_remaining_kg":4500.0,"ground_speed_kts":460}
{"aircraft_id":"AC-102","altitude_ft":31000,"engine_temp_c":630.0,"event_id":"EVT-LATE-002","event_time":"2026-10-02T11:45:00","flight_id":"FL-002","fuel_remaining_kg":4700.0,"ground_speed_kts":450}
"""

file_path = (
    "/Volumes/aviation_dev/raw/landing/"
    "telemetry/telemetry_005_incremental.jsonl"
)

dbutils.fs.put(
    file_path,
    test_data,
    overwrite=True
)

print(file_path)

# COMMAND ----------

from pyspark.sql import functions as F

display(
    spark.table("aviation_dev.bronze.telemetry")
    .filter(
        F.col("event_id").isin(
            "EVT-NEW-001",
            "EVT-LATE-002"
        )
    )
    .select(
        "event_id",
        "aircraft_id",
        "flight_id",
        "event_time",
        "_ingest_ts"
    )
)

# COMMAND ----------

display(
    spark.table("aviation_dev.gold.fact_telemetry_hourly")
    .orderBy("aircraft_id", "flight_id", "telemetry_hour")
)

# COMMAND ----------

test_data = """{"aircraft_id":"AC-101","altitude_ft":35000,"engine_temp_c":660.0,"event_id":"EVT-FAIL-001","event_time":"2026-10-04T13:10:00","flight_id":"FL-001","fuel_remaining_kg":4400.0,"ground_speed_kts":465}
"""

file_path = (
    "/Volumes/aviation_dev/raw/landing/"
    "telemetry/telemetry_006_failure_test.jsonl"
)

dbutils.fs.put(
    file_path,
    test_data,
    overwrite=True
)

from pyspark.sql import functions as F
print(file_path)

# COMMAND ----------


from pyspark.sql import functions as F

display(
    spark.table(CONTROL_TABLE)
    .filter(F.col("pipeline_name") == PIPELINE_NAME)
)