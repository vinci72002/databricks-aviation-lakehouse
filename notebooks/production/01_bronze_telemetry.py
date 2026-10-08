# Databricks notebook source
# ============================================================
# 01 - Bronze Telemetry
# DE-103 - Aviation Lakehouse
#
# JSON files -> Auto Loader -> Bronze Delta table
#
# Prerequisites:
#   - 00_environment_setup completed
#   - Input JSON files prepared
#
# Keep schema and checkpoint paths stable between runs.
# ============================================================

from pyspark.sql import functions as F

# 1. Runtime parameter

dbutils.widgets.text("env", "dev", "Environment")

env = dbutils.widgets.get("env").strip().lower()

if env not in {"dev", "prod"}:
    raise ValueError(f"Invalid environment: {env!r}. " "Allowed values: dev, prod")


# 2. Resolve configuration independently

catalog = f"aviation_{env}"

RAW_VOLUME = f"/Volumes/{catalog}/raw/landing"

SOURCE_PATH = f"{RAW_VOLUME}/telemetry"
SCHEMA_PATH = f"{RAW_VOLUME}/_schemas/telemetry"
CHECKPOINT_PATH = f"{RAW_VOLUME}/_checkpoints/bronze_telemetry"

BRONZE_TABLE = f"{catalog}.bronze.telemetry"


# 3. Validate configuration before processing

paths = {
    "source": SOURCE_PATH,
    "schema": SCHEMA_PATH,
    "checkpoint": CHECKPOINT_PATH,
}

for name, path in paths.items():
    if not path.startswith(f"{RAW_VOLUME}/"):
        raise RuntimeError(f"Environment isolation failed for {name}: {path}")

    if any(part in {".", ".."} for part in path.split("/")):
        raise RuntimeError(f"Unexpected relative path component: {path}")

if len(set(paths.values())) != len(paths):
    raise RuntimeError("Source, schema and checkpoint paths must be distinct.")

if BRONZE_TABLE != f"{catalog}.bronze.telemetry":
    raise RuntimeError(f"Unexpected Bronze target: {BRONZE_TABLE}")

print("Configuration validation: PASSED")


# 4. Verify prerequisites
#
# Fail if the Bronze schema or source directory is unavailable.
# An empty directory is allowed here, but the first Auto Loader
# run needs input files to infer the schema.

spark.sql(f"DESCRIBE SCHEMA `{catalog}`.`bronze`").collect()

source_entries = dbutils.fs.ls(SOURCE_PATH)

print("=" * 60)
print("BRONZE TELEMETRY PIPELINE")
print("=" * 60)
print(f"Environment      : {env.upper()}")
print(f"Source           : {SOURCE_PATH}")
print(f"Schema location  : {SCHEMA_PATH}")
print(f"Checkpoint       : {CHECKPOINT_PATH}")
print(f"Target           : {BRONZE_TABLE}")
print(f"Source entries   : {len(source_entries)}")

if not source_entries:
    print(
        "Source directory is empty. "
        "Initial schema inference requires input JSON files."
    )


# 5. Read JSON files with Auto Loader
#
# Preserve the existing schema inference behavior.

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .load(SOURCE_PATH)
    .withColumn("_ingest_ts", F.current_timestamp())
)


# 6. Write Bronze and wait for completion
#
# Exceptions propagate so the Databricks task fails visibly.

query = (
    bronze_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()


# 7. Verify the output table

if not spark.catalog.tableExists(BRONZE_TABLE):
    raise RuntimeError(f"Bronze table was not created: {BRONZE_TABLE}")

bronze_df = spark.table(BRONZE_TABLE)

if "_ingest_ts" not in bronze_df.columns:
    raise RuntimeError(f"Missing ingestion timestamp in {BRONZE_TABLE}")

# Suitable for this small learning dataset.
# Avoid a full-table count on every run at production scale.
bronze_count = bronze_df.count()


# 8. Final status

print()
print("=" * 60)
print("BRONZE PIPELINE: SUCCESS")
print("=" * 60)
print(f"Environment       : {env.upper()}")
print(f"Target            : {BRONZE_TABLE}")
print(f"Total Bronze rows : {bronze_count}")
print(f"Checkpoint        : {CHECKPOINT_PATH}")
