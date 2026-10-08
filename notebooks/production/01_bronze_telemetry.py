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

print("=" * 60)
print("BRONZE TELEMETRY PIPELINE")
print("=" * 60)
print(f"Environment : {env.upper()}")
print(f"Catalog     : {catalog}")


# COMMAND ----------
# Environment-specific configuration

from pyspark.sql import functions as F

RAW_VOLUME = f"/Volumes/{catalog}/raw/landing"

SOURCE_PATH = f"{RAW_VOLUME}/telemetry"
SCHEMA_PATH = f"{RAW_VOLUME}/_schemas/telemetry"
CHECKPOINT_PATH = f"{RAW_VOLUME}/_checkpoints/bronze_telemetry"

BRONZE_TABLE = f"{catalog}.bronze.telemetry"


# COMMAND ----------
# Environment isolation safety check

expected_volume_prefix = f"/Volumes/{catalog}/"
expected_table_prefix = f"{catalog}."

for path in [
    SOURCE_PATH,
    SCHEMA_PATH,
    CHECKPOINT_PATH
]:
    if not path.startswith(expected_volume_prefix):
        raise RuntimeError(
            f"Environment isolation failed: {path}"
        )

if not BRONZE_TABLE.startswith(expected_table_prefix):
    raise RuntimeError(
        f"Environment isolation failed: {BRONZE_TABLE}"
    )

print()
print("Resolved configuration")
print("-" * 60)
print(f"Source     : {SOURCE_PATH}")
print(f"Schema     : {SCHEMA_PATH}")
print(f"Checkpoint : {CHECKPOINT_PATH}")
print(f"Target     : {BRONZE_TABLE}")
print("-" * 60)
print("Environment isolation check: PASSED")


# COMMAND ----------
# Auto Loader
#
# Business logic remains unchanged from the original pipeline.

bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .load(SOURCE_PATH)
    .withColumn("_ingest_ts", F.current_timestamp())
)


# COMMAND ----------
# Write Bronze Delta table

query = (
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()


# COMMAND ----------
# Validation

bronze_count = spark.table(BRONZE_TABLE).count()

print()
print("=" * 60)
print("BRONZE PIPELINE COMPLETED")
print("=" * 60)
print(f"Environment : {env.upper()}")
print(f"Target      : {BRONZE_TABLE}")
print(f"Bronze rows : {bronze_count}")

display(
    spark.table(BRONZE_TABLE)
    .orderBy("event_id")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM aviation_dev.bronze.telemetry;
# MAGIC
# MAGIC SHOW TABLES IN aviation_prod.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM aviation_prod.bronze.telemetry;