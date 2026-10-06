# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

DIM_TABLE = "aviation.gold.dim_aircraft"
SILVER_AIRCRAFT = "aviation.silver.aircraft"

# COMMAND ----------
# Initial aircraft source

aircraft_data = [
    ("AC-101", "A320", "SkyEast", "CFM56", "2020-01-01"),
    ("AC-102", "A320neo", "PacificAir", "LEAP-1A", "2021-06-01")
]

aircraft_df = (
    spark.createDataFrame(
        aircraft_data,
        [
            "aircraft_id",
            "model",
            "operator",
            "engine_type",
            "effective_date"
        ]
    )
    .withColumn(
        "effective_date",
        F.col("effective_date").cast("date")
    )
)

(
    aircraft_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_AIRCRAFT)
)

# COMMAND ----------
# Initialize SCD2 dimension only if it does not exist

if not spark.catalog.tableExists(DIM_TABLE):

    initial_dim = (
        aircraft_df
        .withColumn("effective_from", F.col("effective_date"))
        .withColumn(
            "effective_to",
            F.lit("9999-12-31").cast("date")
        )
        .withColumn("is_current", F.lit(True))
        .withColumn(
            "aircraft_sk",
            F.xxhash64("aircraft_id", "effective_from")
        )
        .drop("effective_date")
    )

    (
        initial_dim.write
        .format("delta")
        .saveAsTable(DIM_TABLE)
    )

# COMMAND ----------
# AC-101 changes operator on 2026-10-03

changes = (
    spark.createDataFrame(
        [
            (
                "AC-101",
                "A320",
                "NorthAir",
                "CFM56",
                "2026-10-03"
            )
        ],
        [
            "aircraft_id",
            "model",
            "operator",
            "engine_type",
            "effective_date"
        ]
    )
    .withColumn(
        "effective_date",
        F.col("effective_date").cast("date")
    )
)

# COMMAND ----------
# Close old current version only when attributes changed

dim = DeltaTable.forName(spark, DIM_TABLE)

(
    dim.alias("target")
    .merge(
        changes.alias("source"),
        """
        target.aircraft_id = source.aircraft_id
        AND target.is_current = true
        """
    )
    .whenMatchedUpdate(
        condition="""
            target.operator <> source.operator
            OR target.model <> source.model
            OR target.engine_type <> source.engine_type
        """,
        set={
            "effective_to": "source.effective_date",
            "is_current": "false"
        }
    )
    .execute()
)

# COMMAND ----------
# Insert new version idempotently

new_versions = (
    changes
    .withColumn(
        "effective_from",
        F.col("effective_date")
    )
    .withColumn(
        "effective_to",
        F.lit("9999-12-31").cast("date")
    )
    .withColumn("is_current", F.lit(True))
    .withColumn(
        "aircraft_sk",
        F.xxhash64("aircraft_id", "effective_from")
    )
    .drop("effective_date")
)

dim = DeltaTable.forName(spark, DIM_TABLE)

(
    dim.alias("target")
    .merge(
        new_versions.alias("source"),
        """
        target.aircraft_id = source.aircraft_id
        AND target.effective_from = source.effective_from
        """
    )
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

display(
    spark.table(DIM_TABLE)
    .orderBy("aircraft_id", "effective_from")
)

# COMMAND ----------
# Late-arriving telemetry handling
#
# EVT-LATE-001 must already have passed:
# Raw -> 01 Bronze -> 02 Silver
#
# We discover the affected grain from Silver rather than
# hard-coding aggregate values.

late_df = (
    spark.table("aviation.silver.telemetry")
    .filter(F.col("event_id") == "EVT-LATE-001")
)

if late_df.count() > 0:

    affected = (
        late_df
        .select(
            "aircraft_id",
            "flight_id",
            F.date_trunc(
                "hour",
                "event_time"
            ).alias("telemetry_hour"),
            F.to_date(
                "event_time"
            ).alias("telemetry_date")
        )
        .first()
    )

    silver = spark.table("aviation.silver.telemetry")

    # Recompute COMPLETE affected hour from Silver
    recomputed_hour = (
        silver
        .withColumn(
            "telemetry_hour",
            F.date_trunc("hour", "event_time")
        )
        .filter(
            (F.col("aircraft_id") == affected.aircraft_id)
            & (F.col("flight_id") == affected.flight_id)
            & (
                F.col("telemetry_hour")
                == F.lit(affected.telemetry_hour)
            )
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

    hourly = DeltaTable.forName(
        spark,
        "aviation.gold.fact_telemetry_hourly"
    )

    (
        hourly.alias("target")
        .merge(
            recomputed_hour.alias("source"),
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

    # Recompute COMPLETE affected day
    recomputed_day = (
        silver
        .withColumn(
            "telemetry_date",
            F.to_date("event_time")
        )
        .filter(
            (F.col("aircraft_id") == affected.aircraft_id)
            & (F.col("flight_id") == affected.flight_id)
            & (
                F.col("telemetry_date")
                == F.lit(affected.telemetry_date)
            )
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

    daily = DeltaTable.forName(
        spark,
        "aviation.gold.fact_telemetry_daily"
    )

    (
        daily.alias("target")
        .merge(
            recomputed_day.alias("source"),
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

    print("Late-arriving event processed.")

else:
    print("EVT-LATE-001 not found. No late-data correction required.")