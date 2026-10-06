-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Final validation
-- MAGIC
-- MAGIC Run after notebooks `00`–`05`. Each cell is a separate SQL statement.

-- COMMAND ----------

SHOW TABLES IN aviation.raw;
SHOW TABLES IN aviation.bronze;
SHOW TABLES IN aviation.silver;
SHOW TABLES IN aviation.quarantine;
SHOW TABLES IN aviation.gold;

-- COMMAND ----------

SELECT 'bronze.telemetry' AS table_name, COUNT(*) AS rows
FROM aviation.bronze.telemetry

UNION ALL

SELECT 'silver.telemetry', COUNT(*)
FROM aviation.silver.telemetry

UNION ALL

SELECT 'quarantine.telemetry', COUNT(*)
FROM aviation.quarantine.telemetry

UNION ALL

SELECT 'silver.aircraft', COUNT(*)
FROM aviation.silver.aircraft

UNION ALL

SELECT 'gold.dim_aircraft', COUNT(*)
FROM aviation.gold.dim_aircraft

UNION ALL

SELECT 'gold.fact_telemetry_hourly', COUNT(*)
FROM aviation.gold.fact_telemetry_hourly

UNION ALL

SELECT 'gold.fact_telemetry_daily', COUNT(*)
FROM aviation.gold.fact_telemetry_daily

UNION ALL

SELECT 'gold.pipeline_audit', COUNT(*)
FROM aviation.gold.pipeline_audit;

-- COMMAND ----------

SELECT
    event_id,
    aircraft_id,
    flight_id,
    event_time,
    engine_temp_c,
    fuel_remaining_kg,
    _ingest_ts
FROM aviation.bronze.telemetry
ORDER BY event_id;

-- COMMAND ----------

SELECT
    event_id,
    aircraft_id,
    flight_id,
    event_time,
    engine_temp_c,
    fuel_remaining_kg
FROM aviation.silver.telemetry
ORDER BY event_id;

-- COMMAND ----------

SELECT
    event_id,
    flight_id,
    fuel_remaining_kg,
    _reject_reason
FROM aviation.quarantine.telemetry
ORDER BY event_id;

-- COMMAND ----------

SELECT *
FROM aviation.gold.fact_telemetry_hourly
ORDER BY aircraft_id, flight_id, telemetry_hour;

-- COMMAND ----------

SELECT *
FROM aviation.gold.fact_telemetry_daily
ORDER BY aircraft_id, flight_id, telemetry_date;

-- COMMAND ----------

SELECT *
FROM aviation.gold.fact_telemetry_hourly
WHERE aircraft_id = 'AC-102'
  AND flight_id = 'FL-002'
ORDER BY telemetry_hour;

-- COMMAND ----------

SELECT
    aircraft_id,
    model,
    operator,
    engine_type,
    effective_from,
    effective_to,
    is_current
FROM aviation.gold.dim_aircraft
ORDER BY aircraft_id, effective_from;

-- COMMAND ----------

SELECT
    aircraft_id,
    COUNT(*) AS current_count
FROM aviation.gold.dim_aircraft
WHERE is_current = true
GROUP BY aircraft_id
HAVING COUNT(*) <> 1;

-- COMMAND ----------

SELECT
    aircraft_id,
    flight_id,
    telemetry_hour,
    COUNT(*) AS cnt
FROM aviation.gold.fact_telemetry_hourly
GROUP BY aircraft_id, flight_id, telemetry_hour
HAVING COUNT(*) > 1;

-- COMMAND ----------

SELECT
    aircraft_id,
    flight_id,
    telemetry_date,
    COUNT(*) AS cnt
FROM aviation.gold.fact_telemetry_daily
GROUP BY aircraft_id, flight_id, telemetry_date
HAVING COUNT(*) > 1;

-- COMMAND ----------

SELECT
    batch_id,
    status,
    started_at,
    completed_at,
    error_message
FROM aviation.gold.pipeline_audit
ORDER BY started_at DESC;

-- COMMAND ----------

SELECT * FROM aviation.gold.fact_telemetry_daily;
