# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# DE-102
# Aircraft Dimension - SCD Type 2
#
# Responsibility:
#   - Maintain aircraft dimension history
#   - Close previous current version when tracked attributes change
#   - Insert new current version
#   - Support DEV / PROD environments
#
# Gold telemetry incremental / late-data handling
# is owned by 03_gold_incremental.


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
from delta.tables import DeltaTable


# COMMAND ----------
# Environment-specific tables

SILVER_AIRCRAFT = f"{catalog}.silver.aircraft"
DIM_TABLE = f"{catalog}.gold.dim_aircraft"


# COMMAND ----------
# Environment isolation safety check

expected_table_prefix = f"{catalog}."

tables = [
    SILVER_AIRCRAFT,
    DIM_TABLE
]

for table in tables:
    if not table.startswith(expected_table_prefix):
        raise RuntimeError(
            f"Environment isolation failed: {table}"
        )


print("=" * 60)
print("AIRCRAFT SCD2 PIPELINE")
print("=" * 60)

print(f"Environment     : {env.upper()}")
print(f"Catalog         : {catalog}")
print(f"Silver Aircraft : {SILVER_AIRCRAFT}")
print(f"Aircraft Dim    : {DIM_TABLE}")

print("=" * 60)
print("Environment isolation check: PASSED")


# COMMAND ----------
# Demo aircraft source
#
# This source is intentionally kept simple for the portfolio lab.
# Production source ingestion can be separated later.

aircraft_data = [
    (
        "AC-101",
        "A320",
        "SkyEast",
        "CFM56",
        "2020-01-01"
    ),
    (
        "AC-102",
        "A320neo",
        "PacificAir",
        "LEAP-1A",
        "2021-06-01"
    )
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


# COMMAND ----------
# Write aircraft source into current environment

(
    aircraft_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_AIRCRAFT)
)

print(
    f"Aircraft source written: "
    f"{SILVER_AIRCRAFT}"
)


# COMMAND ----------
# Initialize SCD2 dimension only if it does not exist

if not spark.catalog.tableExists(DIM_TABLE):

    initial_dim = (
        aircraft_df
        .withColumn(
            "effective_from",
            F.col("effective_date")
        )
        .withColumn(
            "effective_to",
            F.lit("9999-12-31").cast("date")
        )
        .withColumn(
            "is_current",
            F.lit(True)
        )
        .withColumn(
            "aircraft_sk",
            F.xxhash64(
                "aircraft_id",
                "effective_from"
            )
        )
        .drop("effective_date")
    )

    (
        initial_dim.write
        .format("delta")
        .saveAsTable(DIM_TABLE)
    )

    print(
        f"SCD2 dimension initialized: "
        f"{DIM_TABLE}"
    )

else:

    print(
        f"SCD2 dimension already exists: "
        f"{DIM_TABLE}"
    )


# COMMAND ----------
# Demo aircraft change event
#
# AC-101 changes operator:
# SkyEast -> NorthAir
#
# Effective date:
# 2026-10-03

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
# Close old current SCD2 version
#
# Only close the current version when
# one of the tracked attributes changes.

dim = DeltaTable.forName(
    spark,
    DIM_TABLE
)

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
# Build new SCD2 versions

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
    .withColumn(
        "is_current",
        F.lit(True)
    )
    .withColumn(
        "aircraft_sk",
        F.xxhash64(
            "aircraft_id",
            "effective_from"
        )
    )
    .drop("effective_date")
)


# COMMAND ----------
# Insert new SCD2 version idempotently
#
# Merge key:
#   aircraft_id + effective_from
#
# Rerunning the notebook will therefore
# not create another copy of the same version.

dim = DeltaTable.forName(
    spark,
    DIM_TABLE
)

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
# Validate SCD2 invariant
#
# Each aircraft must have at most
# one current version.

current_duplicates = (
    spark.table(DIM_TABLE)
    .filter(F.col("is_current") == True)
    .groupBy("aircraft_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if current_duplicates > 0:
    raise RuntimeError(
        "SCD2 validation failed: "
        "aircraft has multiple current versions."
    )

print("SCD2 current-version validation: PASSED")


# COMMAND ----------
# Display dimension history

display(
    spark.table(DIM_TABLE)
    .orderBy(
        "aircraft_id",
        "effective_from"
    )
)


# COMMAND ----------
# Final status

print()
print("=" * 60)
print("SCD2 PIPELINE COMPLETED")
print("=" * 60)

print(f"Environment : {env.upper()}")
print(f"Dimension   : {DIM_TABLE}")
print(
    "Late-arriving telemetry handling: "
    "03_gold_incremental"
)

print("=" * 60)

# COMMAND ----------

# Validate 04_scd2_aircraft result

display(
    spark.table("aviation_dev.gold.dim_aircraft")
    .select(
        "aircraft_id",
        "model",
        "operator",
        "engine_type",
        "effective_from",
        "effective_to",
        "is_current",
        "aircraft_sk"
    )
    .orderBy(
        "aircraft_id",
        "effective_from"
    )
)