# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_PATH = "/Volumes/aviation/raw/landing/telemetry"
SCHEMA_PATH = "/Volumes/aviation/raw/landing/_schemas/telemetry"
CHECKPOINT_PATH = "/Volumes/aviation/raw/landing/_checkpoints/bronze_telemetry"

BRONZE_TABLE = "aviation.bronze.telemetry"

# COMMAND ----------

bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .load(SOURCE_PATH)
    .withColumn("_ingest_ts", F.current_timestamp())
)

# COMMAND ----------

query = (
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()

# COMMAND ----------

bronze_count = spark.table(BRONZE_TABLE).count()

print(f"Bronze rows: {bronze_count}")

display(
    spark.table(BRONZE_TABLE)
    .orderBy("event_id")
)