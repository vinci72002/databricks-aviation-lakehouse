# Databricks notebook source
# ============================================================
# 04 - Aircraft SCD Type 2
#
# Source: silver.aircraft
# Target: gold.dim_aircraft
#
# Policy:
#   - One source row per aircraft per run
#   - New changes must move forward in effective_date
#   - Exact historical replays are ignored
#   - Conflicting historical changes fail explicitly
#   - Source omission does not delete aircraft
#
# Validity intervals: [effective_from, effective_to)
# Run without concurrent source updates or dimension writers.
# ============================================================

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Environment

dbutils.widgets.text("env", "dev", "Environment")
env = dbutils.widgets.get("env").strip().lower()

if env not in {"dev", "prod"}:
    raise ValueError(f"Invalid environment: {env!r}. Allowed: dev, prod")

catalog = f"aviation_{env}"

SOURCE_TABLE = f"{catalog}.silver.aircraft"
DIM_TABLE = f"{catalog}.gold.dim_aircraft"

ATTRIBUTES = ["model", "operator", "engine_type"]
SOURCE_COLUMNS = [
    "aircraft_id",
    *ATTRIBUTES,
    "effective_date",
]

print("=" * 60)
print("AIRCRAFT SCD2 PIPELINE")
print("=" * 60)
print(f"Environment : {env.upper()}")
print(f"Source      : {SOURCE_TABLE}")
print(f"Dimension   : {DIM_TABLE}")


# 2. Validate source

if not spark.catalog.tableExists(SOURCE_TABLE):
    raise RuntimeError(
        f"Missing aircraft source: {SOURCE_TABLE}. " "Prepare aircraft data separately."
    )

raw_df = spark.table(SOURCE_TABLE)

missing_columns = set(SOURCE_COLUMNS) - set(raw_df.columns)

if missing_columns:
    raise RuntimeError(f"Missing aircraft columns: {sorted(missing_columns)}")

source_df = raw_df.select(
    F.col("aircraft_id").cast("string").alias("aircraft_id"),
    *[F.col(name).cast("string").alias(name) for name in ATTRIBUTES],
    F.expr("try_cast(effective_date AS DATE)").alias("effective_date"),
)

invalid_source = (
    source_df.filter(
        F.col("aircraft_id").isNull()
        | (F.length(F.trim(F.col("aircraft_id"))) == 0)
        | F.col("effective_date").isNull()
        | (F.col("effective_date") >= F.lit("9999-12-31").cast("date"))
    )
    .limit(1)
    .count()
)

if invalid_source:
    raise RuntimeError("Aircraft source contains invalid IDs or effective dates.")

if source_df.groupBy("aircraft_id").count().filter(F.col("count") > 1).limit(1).count():
    raise RuntimeError(
        "Source must contain one row per aircraft per run. "
        "Multiple changes require ordered processing."
    )

print(f"Source rows : {source_df.count()}")


# 3. Create an empty dimension if needed

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DIM_TABLE} (
    aircraft_id STRING,
    model STRING,
    operator STRING,
    engine_type STRING,
    effective_from DATE,
    effective_to DATE,
    is_current BOOLEAN,
    aircraft_sk BIGINT
)
USING DELTA
""")


# 4. Validate dimension history


def fail_if_rows(df, message):
    if df.limit(1).count():
        raise RuntimeError(message)


def validate_dimension():
    df = spark.table(DIM_TABLE)

    fail_if_rows(
        df.filter(
            F.col("aircraft_id").isNull()
            | (F.length(F.trim(F.col("aircraft_id"))) == 0)
            | F.col("effective_from").isNull()
            | F.col("effective_to").isNull()
            | F.col("is_current").isNull()
            | F.col("aircraft_sk").isNull()
            | (F.col("effective_from") >= F.col("effective_to"))
        ),
        "SCD2 DQ FAILED: invalid keys or validity intervals.",
    )

    fail_if_rows(
        df.groupBy("aircraft_id", "effective_from").count().filter(F.col("count") > 1),
        "SCD2 DQ FAILED: duplicate historical versions.",
    )

    fail_if_rows(
        df.groupBy("aircraft_sk").count().filter(F.col("count") > 1),
        "SCD2 DQ FAILED: duplicate surrogate keys.",
    )

    fail_if_rows(
        df.groupBy("aircraft_id")
        .agg(F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias("current_count"))
        .filter(F.col("current_count") != 1),
        "SCD2 DQ FAILED: each aircraft must have one current row.",
    )

    history_window = Window.partitionBy("aircraft_id").orderBy("effective_from")

    history_df = df.withColumn(
        "_next_from",
        F.lead("effective_from").over(history_window),
    )

    sentinel = F.lit("9999-12-31").cast("date")

    fail_if_rows(
        history_df.filter(
            (
                F.col("_next_from").isNotNull()
                & (F.col("is_current") | (F.col("effective_to") != F.col("_next_from")))
            )
            | (
                F.col("_next_from").isNull()
                & ((~F.col("is_current")) | (F.col("effective_to") != sentinel))
            )
        ),
        "SCD2 DQ FAILED: gaps, overlaps or invalid current interval.",
    )

    print("Dimension history validation: PASSED")


validate_dimension()


# 5. Identify historical replays and conflicts

history_df = spark.table(DIM_TABLE)

same_date_df = source_df.alias("s").join(
    history_df.alias("h"),
    (F.col("s.aircraft_id") == F.col("h.aircraft_id"))
    & (F.col("s.effective_date") == F.col("h.effective_from")),
    "inner",
)

same_attributes = F.lit(True)

for name in ATTRIBUTES:
    same_attributes = same_attributes & (
        F.col(f"s.{name}").eqNullSafe(F.col(f"h.{name}"))
    )

fail_if_rows(
    same_date_df.filter(~same_attributes),
    "Historical conflict: same aircraft and effective date "
    "have different attributes. Correction requires a separate workflow.",
)

replay_keys_df = same_date_df.select(
    F.col("s.aircraft_id").alias("aircraft_id"),
    F.col("s.effective_date").alias("effective_date"),
)

replay_count = replay_keys_df.count()

pending_df = source_df.join(
    replay_keys_df,
    on=["aircraft_id", "effective_date"],
    how="left_anti",
)

print(f"Historical replay rows: {replay_count}")


# 6. Compare pending rows with current versions

current_df = history_df.filter(F.col("is_current")).select(
    F.col("aircraft_id").alias("_current_id"),
    F.col("effective_from").alias("_current_from"),
    *[F.col(name).alias(f"_current_{name}") for name in ATTRIBUTES],
)

comparison_df = pending_df.join(
    current_df,
    pending_df["aircraft_id"] == current_df["_current_id"],
    "left",
)

fail_if_rows(
    comparison_df.filter(
        F.col("_current_id").isNotNull()
        & (F.col("effective_date") <= F.col("_current_from"))
    ),
    "Out-of-order aircraft change: effective date must be later "
    "than the current version. Historical insertion is unsupported.",
)

attributes_equal = F.lit(True)

for name in ATTRIBUTES:
    attributes_equal = attributes_equal & (
        F.col(name).eqNullSafe(F.col(f"_current_{name}"))
    )

new_aircraft_df = comparison_df.filter(F.col("_current_id").isNull()).select(
    *SOURCE_COLUMNS
)

changed_aircraft_df = comparison_df.filter(
    F.col("_current_id").isNotNull() & (~attributes_equal)
).select(*SOURCE_COLUMNS)

new_count = new_aircraft_df.count()
changed_count = changed_aircraft_df.count()

print(f"New aircraft     : {new_count}")
print(f"Changed aircraft : {changed_count}")


# 7. Stage and apply a single SCD2 MERGE
#
# Changed aircraft contribute two source rows:
#   real merge key -> close current version
#   null merge key -> insert new version
#
# New aircraft contribute one insert row.

if new_count + changed_count > 0:
    match_rows_df = new_aircraft_df.unionByName(changed_aircraft_df).withColumn(
        "_merge_key", F.col("aircraft_id")
    )

    insert_rows_df = changed_aircraft_df.withColumn(
        "_merge_key",
        F.lit(None).cast("string"),
    )

    staged_df = match_rows_df.unionByName(insert_rows_df).withColumn(
        "aircraft_sk",
        F.xxhash64("aircraft_id", "effective_date"),
    )

    (
        DeltaTable.forName(spark, DIM_TABLE)
        .alias("target")
        .merge(
            staged_df.alias("source"),
            """
            target.aircraft_id = source._merge_key
            AND target.is_current = true
            """,
        )
        .whenMatchedUpdate(
            set={
                "effective_to": "source.effective_date",
                "is_current": "false",
            }
        )
        .whenNotMatchedInsert(
            values={
                "aircraft_id": "source.aircraft_id",
                "model": "source.model",
                "operator": "source.operator",
                "engine_type": "source.engine_type",
                "effective_from": "source.effective_date",
                "effective_to": "DATE '9999-12-31'",
                "is_current": "true",
                "aircraft_sk": "source.aircraft_sk",
            }
        )
        .execute()
    )

    print("SCD2 MERGE completed.")
else:
    print("No new aircraft or attribute changes. MERGE skipped.")


# 8. Validate and display results

validate_dimension()

print()
print("=" * 60)
print("AIRCRAFT SCD2 PIPELINE: SUCCESS")
print("=" * 60)
print(f"Environment : {env.upper()}")
print(f"Dimension   : {DIM_TABLE}")

display(
    spark.table(DIM_TABLE)
    .select(
        "aircraft_id",
        "model",
        "operator",
        "engine_type",
        "effective_from",
        "effective_to",
        "is_current",
        "aircraft_sk",
    )
    .orderBy("aircraft_id", "effective_from")
)
