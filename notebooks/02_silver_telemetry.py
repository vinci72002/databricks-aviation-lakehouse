# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_TABLE = "aviation.bronze.telemetry"
SILVER_TABLE = "aviation.silver.telemetry"
QUARANTINE_TABLE = "aviation.quarantine.telemetry"

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)

# COMMAND ----------

typed_df = (
    bronze_df
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("altitude_ft", F.col("altitude_ft").cast("long"))
    .withColumn("ground_speed_kts", F.col("ground_speed_kts").cast("double"))
    .withColumn("engine_temp_c", F.col("engine_temp_c").cast("double"))
    .withColumn("fuel_remaining_kg", F.col("fuel_remaining_kg").cast("double"))
)

# COMMAND ----------

validated_df = (
    typed_df
    .withColumn(
        "_dq_reason",
        F.when(F.col("event_id").isNull(), "missing_event_id")
        .when(F.col("aircraft_id").isNull(), "missing_aircraft_id")
        .when(F.col("flight_id").isNull(), "missing_flight_id")
        .when(F.col("event_time").isNull(), "invalid_event_time")
        .when(F.col("altitude_ft") < 0, "negative_altitude")
        .when(F.col("fuel_remaining_kg") < 0, "negative_fuel")
        .when(
            (F.col("engine_temp_c") < -80)
            | (F.col("engine_temp_c") > 1200),
            "invalid_engine_temp"
        )
    )
)

# COMMAND ----------

dedup_window = (
    Window
    .partitionBy("event_id")
    .orderBy(F.col("_ingest_ts").asc())
)

dedup_df = (
    validated_df
    .withColumn(
        "_row_number",
        F.row_number().over(dedup_window)
    )
)

# COMMAND ----------

valid_df = (
    dedup_df
    .filter(
        F.col("_dq_reason").isNull()
        & (F.col("_row_number") == 1)
    )
    .drop("_dq_reason", "_row_number")
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
        ).otherwise(F.col("_dq_reason"))
    )
    .drop("_dq_reason", "_row_number")
)

# COMMAND ----------

(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

(
    invalid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(QUARANTINE_TABLE)
)

# COMMAND ----------

print("Bronze:", bronze_df.count())
print("Silver:", valid_df.count())
print("Quarantine:", invalid_df.count())

display(
    spark.table(QUARANTINE_TABLE)
    .select("event_id", "_reject_reason")
    .orderBy("event_id")
)