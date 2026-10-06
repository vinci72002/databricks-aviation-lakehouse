# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_TABLE = "aviation.silver.telemetry"
HOURLY_TABLE = "aviation.gold.fact_telemetry_hourly"
DAILY_TABLE = "aviation.gold.fact_telemetry_daily"

silver_df = spark.table(SILVER_TABLE)

# COMMAND ----------

hourly_df = (
    silver_df
    .withColumn(
        "telemetry_hour",
        F.date_trunc("hour", "event_time")
    )
    .groupBy(
        "aircraft_id",
        "flight_id",
        "telemetry_hour"
    )
    .agg(
        F.count("*").alias("sample_count"),
        F.avg("altitude_ft").alias("avg_altitude_ft"),
        F.avg("ground_speed_kts").alias("avg_ground_speed_kts"),
        F.avg("engine_temp_c").alias("avg_engine_temp_c"),
        F.max("engine_temp_c").alias("max_engine_temp_c"),
        F.min("fuel_remaining_kg").alias("min_fuel_remaining_kg")
    )
)

(
    hourly_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(HOURLY_TABLE)
)

# COMMAND ----------

daily_df = (
    silver_df
    .withColumn(
        "telemetry_date",
        F.to_date("event_time")
    )
    .groupBy(
        "aircraft_id",
        "flight_id",
        "telemetry_date"
    )
    .agg(
        F.count("*").alias("sample_count"),
        F.avg("altitude_ft").alias("avg_altitude_ft"),
        F.avg("ground_speed_kts").alias("avg_ground_speed_kts"),
        F.avg("engine_temp_c").alias("avg_engine_temp_c"),
        F.max("engine_temp_c").alias("max_engine_temp_c"),
        F.min("fuel_remaining_kg").alias("min_fuel_remaining_kg")
    )
)

(
    daily_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DAILY_TABLE)
)

# COMMAND ----------

display(
    spark.table(HOURLY_TABLE)
    .orderBy("aircraft_id", "telemetry_hour")
)

display(
    spark.table(DAILY_TABLE)
    .orderBy("aircraft_id", "telemetry_date")
)