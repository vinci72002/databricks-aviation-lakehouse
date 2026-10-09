# Databricks notebook source
# ============================================================
# 00 - Environment Setup
# DE-101 / DE-103 - Aviation Lakehouse
#
# Responsibilities:
#   1. Validate the environment parameter
#   2. Resolve and validate environment-specific configuration
#   3. Create catalog, schemas, volume and source directory
#   4. Verify environment objects
#
# Environment mapping:
#   dev  -> aviation_dev
#   prod -> aviation_prod
#
# This notebook does not initialize business tables.
# ============================================================


# COMMAND ----------
# 1. Runtime parameter

dbutils.widgets.text("env", "dev", "Environment")

env = dbutils.widgets.get("env").strip().lower()

ALLOWED_ENVS = {"dev", "prod"}

if env not in ALLOWED_ENVS:
    raise ValueError(
        f"Invalid environment: {env!r}. " f"Allowed values: {sorted(ALLOWED_ENVS)}"
    )


# COMMAND ----------
# 2. Resolve configuration
#
# Configuration is kept local for now so this notebook can run
# independently. We will connect common/config.py when the
# repository import and deployment structure is established.

catalog = f"aviation_{env}"

SCHEMAS = (
    "raw",
    "bronze",
    "silver",
    "quarantine",
    "gold",
    "ops",
)

VOLUME_NAME = "landing"
VOLUME_FULL_NAME = f"{catalog}.raw.{VOLUME_NAME}"

RAW_VOLUME = f"/Volumes/{catalog}/raw/{VOLUME_NAME}"

TELEMETRY_SOURCE_PATH = f"{RAW_VOLUME}/telemetry"
TELEMETRY_SCHEMA_PATH = f"{RAW_VOLUME}/_schemas/telemetry"
TELEMETRY_CHECKPOINT_PATH = f"{RAW_VOLUME}/_checkpoints/bronze_telemetry"

BRONZE_TELEMETRY_TABLE = f"{catalog}.bronze.telemetry"

SILVER_TELEMETRY_TABLE = f"{catalog}.silver.telemetry"
SILVER_AIRCRAFT_TABLE = f"{catalog}.silver.aircraft"

QUARANTINE_TELEMETRY_TABLE = f"{catalog}.quarantine.telemetry"

GOLD_HOURLY_TABLE = f"{catalog}.gold.fact_telemetry_hourly"
GOLD_DAILY_TABLE = f"{catalog}.gold.fact_telemetry_daily"
DIM_AIRCRAFT_TABLE = f"{catalog}.gold.dim_aircraft"

PIPELINE_AUDIT_TABLE = f"{catalog}.ops.pipeline_audit"
PIPELINE_CONTROL_TABLE = f"{catalog}.ops.pipeline_control"

STORAGE_PATHS = {
    "raw_volume": RAW_VOLUME,
    "telemetry_source": TELEMETRY_SOURCE_PATH,
    "schema_location": TELEMETRY_SCHEMA_PATH,
    "checkpoint_location": TELEMETRY_CHECKPOINT_PATH,
}

TABLE_NAMES = {
    "bronze_telemetry": BRONZE_TELEMETRY_TABLE,
    "silver_telemetry": SILVER_TELEMETRY_TABLE,
    "silver_aircraft": SILVER_AIRCRAFT_TABLE,
    "quarantine_telemetry": QUARANTINE_TELEMETRY_TABLE,
    "gold_hourly": GOLD_HOURLY_TABLE,
    "gold_daily": GOLD_DAILY_TABLE,
    "dim_aircraft": DIM_AIRCRAFT_TABLE,
    "pipeline_audit": PIPELINE_AUDIT_TABLE,
    "pipeline_control": PIPELINE_CONTROL_TABLE,
}


# COMMAND ----------
# 3. Validate configuration before creating objects

expected_volume_root = f"/Volumes/{catalog}/raw/{VOLUME_NAME}"

for name, path in STORAGE_PATHS.items():
    if not (
        path == expected_volume_root or path.startswith(f"{expected_volume_root}/")
    ):
        raise RuntimeError(
            f"Environment isolation failed for {name}: {path}. "
            f"Expected volume root: {expected_volume_root}"
        )

    if any(part in {".", ".."} for part in path.split("/")):
        raise RuntimeError(f"Unexpected relative path component in {name}: {path}")

for name, table in TABLE_NAMES.items():
    parts = table.split(".")

    if (
        len(parts) != 3
        or parts[0] != catalog
        or parts[1] not in SCHEMAS
        or not parts[2]
    ):
        raise RuntimeError(f"Invalid environment-specific table for {name}: {table}")

if len(set(STORAGE_PATHS.values())) != len(STORAGE_PATHS):
    raise RuntimeError("Storage paths must be distinct.")

print("Configuration validation: PASSED")


# COMMAND ----------
# 4. Display resolved configuration
#
# Table names below are configuration values.
# Their existence is not checked by this notebook.

print("=" * 60)
print("AVIATION LAKEHOUSE - ENVIRONMENT SETUP")
print("=" * 60)
print(f"Environment : {env.upper()}")
print(f"Catalog     : {catalog}")
print(f"Volume      : {VOLUME_FULL_NAME}")

print("\nStorage paths:")
for name, path in STORAGE_PATHS.items():
    print(f"  {name:<24} = {path}")

print("\nConfigured table names:")
for name, table in TABLE_NAMES.items():
    print(f"  {name:<24} = {table}")


# COMMAND ----------
# 5. Ensure catalog
#
# CREATE CATALOG requires Metastore privilege CREATE CATALOG.
# The prod job runs as a service principal, which should not have
# that privilege. CREATE CATALOG IF NOT EXISTS still checks it,
# even when the catalog already exists.
#
# Create aviation_prod once as a Metastore admin, then grant the
# service principal USE CATALOG / CREATE SCHEMA on that catalog.

existing_catalogs = {
    row["catalog"] if "catalog" in row.asDict() else row[0]
    for row in spark.sql("SHOW CATALOGS").collect()
}

if catalog in existing_catalogs:
    print(f"Catalog already exists: {catalog}")
else:
    try:
        spark.sql(f"CREATE CATALOG `{catalog}`")
        print(f"Catalog created: {catalog}")
    except Exception as exc:
        raise RuntimeError(
            f"Catalog {catalog} does not exist and this identity cannot "
            f"create it on the Metastore. A Metastore admin must run "
            f"CREATE CATALOG {catalog} and grant this job identity "
            f"USE CATALOG and CREATE SCHEMA on {catalog}."
        ) from exc


# COMMAND ----------
# 6. Create schemas

for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    print(f"Schema ready: {catalog}.{schema}")


# COMMAND ----------
# 7. Create managed landing volume
#
# Uses the managed storage configured in Unity Catalog.

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS
        `{catalog}`.`raw`.`{VOLUME_NAME}`
    """)

print(f"Volume ready: {VOLUME_FULL_NAME}")


# COMMAND ----------
# 8. Create source directory
#
# Auto Loader manages its schema and checkpoint state.
# Only the input directory is created here.

created = dbutils.fs.mkdirs(TELEMETRY_SOURCE_PATH)

if not created:
    raise RuntimeError(
        f"Could not create source directory: " f"{TELEMETRY_SOURCE_PATH}"
    )

print(f"Source directory ready: {TELEMETRY_SOURCE_PATH}")


# COMMAND ----------
# 9. Verify schemas

schema_rows = spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()

# SHOW SCHEMAS returns the schema name in its first column.
actual_schemas = {str(row[0]) for row in schema_rows}

missing_schemas = set(SCHEMAS) - actual_schemas

if missing_schemas:
    raise RuntimeError(
        f"Environment setup failed. "
        f"Missing schemas in {catalog}: "
        f"{sorted(missing_schemas)}"
    )

print(f"Schema verification: PASSED ({len(SCHEMAS)} required)")


# COMMAND ----------
# 10. Verify landing volume

volume_rows = spark.sql(f"SHOW VOLUMES IN `{catalog}`.`raw`").collect()

actual_volumes = {row["volume_name"] for row in volume_rows}

if VOLUME_NAME not in actual_volumes:
    raise RuntimeError(
        f"Environment setup failed. " f"Missing volume: {VOLUME_FULL_NAME}"
    )

print("Volume verification: PASSED")


# COMMAND ----------
# 11. Verify source directory access
#
# Listing an empty directory is valid.
# Listing failure raises an exception and fails the task.

source_entries = dbutils.fs.ls(TELEMETRY_SOURCE_PATH)

print("Source directory access: PASSED")
print(f"Source directory entries: {len(source_entries)}")


# COMMAND ----------
# 12. Final status

print()
print("=" * 60)
print("ENVIRONMENT SETUP: SUCCESS")
print("=" * 60)

print(f"""
Environment : {env.upper()}
Catalog     : {catalog}
Volume      : {VOLUME_FULL_NAME}
Source      : {TELEMETRY_SOURCE_PATH}

Verified:
  - Required schemas exist
  - Landing volume exists
  - Source directory is accessible
  - Configured paths and table names belong to {catalog}

Next:
  - Prepare input data
  - Initialize required business tables
  - Run the processing tasks with env={env}

Each processing task must resolve its own configuration.
""")
