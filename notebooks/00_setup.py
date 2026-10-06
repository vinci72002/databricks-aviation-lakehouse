# Databricks notebook source
# ============================================================
# Databricks Aviation Lakehouse
# Clean test environment
# ============================================================

spark.sql("CREATE CATALOG IF NOT EXISTS aviation")

for schema in ["raw", "bronze", "silver", "quarantine", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS aviation.{schema}")

spark.sql("""
CREATE VOLUME IF NOT EXISTS aviation.raw.landing
""")

# Drop generated tables from previous experiments
tables = [
    "aviation.bronze.telemetry",
    "aviation.silver.telemetry",
    "aviation.silver.aircraft",
    "aviation.quarantine.telemetry",
    "aviation.gold.fact_telemetry_hourly",
    "aviation.gold.fact_telemetry_daily",
    "aviation.gold.dim_aircraft",
    "aviation.gold.pipeline_audit"
]

for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

# Remove previous test files / Auto Loader state
base = "/Volumes/aviation/raw/landing"

for path in [
    f"{base}/telemetry",
    f"{base}/_schemas",
    f"{base}/_checkpoints"
]:
    try:
        dbutils.fs.rm(path, True)
    except:
        pass

dbutils.fs.mkdirs(f"{base}/telemetry")

print("Environment reset complete.")

# COMMAND ----------

import json

base = "/Volumes/aviation/raw/landing/telemetry"

telemetry_001 = [
    {
        "event_id": "EVT-001",
        "aircraft_id": "AC-101",
        "flight_id": "FL-001",
        "event_time": "2026-10-01T10:00:00",
        "altitude_ft": 32000,
        "ground_speed_kts": 450,
        "engine_temp_c": 650.0,
        "fuel_remaining_kg": 5200.0
    },
    {
        "event_id": "EVT-002",
        "aircraft_id": "AC-101",
        "flight_id": "FL-001",
        "event_time": "2026-10-01T10:01:00",
        "altitude_ft": 32500,
        "ground_speed_kts": 455,
        "engine_temp_c": 655.0,
        "fuel_remaining_kg": 5150.0
    }
]

telemetry_002 = [
    {
        "event_id": "EVT-003",
        "aircraft_id": "AC-102",
        "flight_id": "FL-002",
        "event_time": "2026-10-02T11:00:00",
        "altitude_ft": 30000,
        "ground_speed_kts": 440,
        "engine_temp_c": 640.0,
        "fuel_remaining_kg": 4900.0
    },
    {
        "event_id": "EVT-004",
        "aircraft_id": "AC-102",
        "flight_id": "FL-002",
        "event_time": "2026-10-02T11:01:00",
        "altitude_ft": 30500,
        "ground_speed_kts": 445,
        "engine_temp_c": 645.0,
        "fuel_remaining_kg": 4850.0
    }
]

def write_jsonl(path, rows):
    content = "\n".join(json.dumps(row) for row in rows)
    dbutils.fs.put(path, content, True)

write_jsonl(f"{base}/telemetry_001.jsonl", telemetry_001)
write_jsonl(f"{base}/telemetry_002.jsonl", telemetry_002)

display(dbutils.fs.ls(base))

# COMMAND ----------

import json

late_event = {
    "event_id": "EVT-LATE-001",
    "aircraft_id": "AC-102",
    "flight_id": "FL-002",
    "event_time": "2026-10-02T11:30:00",
    "altitude_ft": 31500,
    "ground_speed_kts": 448,
    "engine_temp_c": 600.0,
    "fuel_remaining_kg": 4750.0
}

dbutils.fs.put(
    "/Volumes/aviation/raw/landing/telemetry/telemetry_004_late.jsonl",
    json.dumps(late_event),
    True
)

display(
    dbutils.fs.ls(
        "/Volumes/aviation/raw/landing/telemetry"
    )
)

# COMMAND ----------

# ============================================================
# RESET GENERATED DATA
# Keep ALL raw JSONL files
# ============================================================

# 1. Drop generated Delta tables
tables = [
    "aviation.bronze.telemetry",
    "aviation.silver.telemetry",
    "aviation.silver.aircraft",
    "aviation.quarantine.telemetry",
    "aviation.gold.fact_telemetry_hourly",
    "aviation.gold.fact_telemetry_daily",
    "aviation.gold.dim_aircraft",
    "aviation.gold.pipeline_audit"
]

for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped: {table}")


# 2. Reset Auto Loader state
# IMPORTANT:
# Raw telemetry JSONL files are NOT deleted.

state_paths = [
    "/Volumes/aviation/raw/landing/_schemas/telemetry",
    "/Volumes/aviation/raw/landing/_checkpoints/bronze_telemetry"
]

for path in state_paths:
    try:
        dbutils.fs.rm(path, True)
        print(f"Removed state: {path}")
    except Exception as e:
        print(f"Skip: {path} - {e}")


print("RESET COMPLETE")
print("Raw data was preserved.")