# Data dictionary

Unity Catalog: `aviation`

Source of truth is the notebooks. This file only names tables, grains, and columns that those notebooks create.

## Layers

| Schema | Role |
| --- | --- |
| `raw` | Volume `aviation.raw.landing` for JSONL. No tables. |
| `bronze` | Auto Loader append of telemetry JSON |
| `silver` | Typed, validated, deduplicated telemetry; aircraft snapshot |
| `quarantine` | DQ failures and duplicate `event_id`s |
| `gold` | Hourly/daily facts, SCD2 dimension, pipeline audit |

Volume path: `/Volumes/aviation/raw/landing/telemetry/`

Auto Loader state (not in Git):

- schema: `/Volumes/aviation/raw/landing/_schemas/telemetry`
- checkpoint: `/Volumes/aviation/raw/landing/_checkpoints/bronze_telemetry`

## `aviation.bronze.telemetry`

Append-only JSON plus `_ingest_ts`.

| Column | Notes |
| --- | --- |
| `event_id` | Event business key |
| `aircraft_id` | Aircraft natural key |
| `flight_id` | Flight natural key |
| `event_time` | Event time as landed |
| `altitude_ft` | Altitude |
| `ground_speed_kts` | Ground speed |
| `engine_temp_c` | Engine temperature |
| `fuel_remaining_kg` | Remaining fuel |
| `_ingest_ts` | Ingest timestamp added by Auto Loader |

## `aviation.silver.telemetry`

Same payload after casts. Grain: `event_id` (first ingest wins).

DQ reasons written to quarantine:

- `missing_event_id` / `missing_aircraft_id` / `missing_flight_id`
- `invalid_event_time`
- `negative_altitude`
- `negative_fuel`
- `invalid_engine_temp` (below -80 or above 1200)
- `duplicate_event`

## `aviation.quarantine.telemetry`

Rejected bronze rows plus `_reject_reason`.

## `aviation.silver.aircraft`

Current source snapshot used to seed / drive SCD2: `aircraft_id`, `model`, `operator`, `engine_type`, `effective_date`.

## `aviation.gold.fact_telemetry_hourly`

Grain: `aircraft_id` + `flight_id` + `telemetry_hour`

Metrics: `sample_count`, `avg_altitude_ft`, `avg_ground_speed_kts`, `avg_engine_temp_c`, `max_engine_temp_c`, `min_fuel_remaining_kg`

## `aviation.gold.fact_telemetry_daily`

Grain: `aircraft_id` + `flight_id` + `telemetry_date`

Same metrics as hourly.

## `aviation.gold.dim_aircraft`

SCD Type 2. Grain: `aircraft_id` + `effective_from`

| Column | Notes |
| --- | --- |
| `aircraft_sk` | `xxhash64(aircraft_id, effective_from)` |
| `aircraft_id` | Natural key |
| `model` / `operator` / `engine_type` | Type 2 attributes |
| `effective_from` | Inclusive start |
| `effective_to` | End date; current rows use `9999-12-31` |
| `is_current` | Open version flag |

## `aviation.gold.pipeline_audit`

One row per DQ-gate run: `batch_id`, `status` (`RUNNING` / `COMMITTED` / `FAILED`), `started_at`, `completed_at`, `error_message`.
