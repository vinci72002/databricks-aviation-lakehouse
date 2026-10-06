# Sample telemetry payloads

These JSONL files match the rows that `notebooks/00_setup.py` writes into the Unity Catalog Volume:

`/Volumes/aviation/raw/landing/telemetry/`

| File | Events |
| --- | --- |
| `telemetry/telemetry_001.jsonl` | EVT-001, EVT-002 (AC-101 / FL-001, 2026-10-01 10:00–10:01) |
| `telemetry/telemetry_002.jsonl` | EVT-003, EVT-004 (AC-102 / FL-002, 2026-10-02 11:00–11:01) |
| `telemetry_late/telemetry_004_late.jsonl` | EVT-LATE-001 (AC-102 / FL-002, 2026-10-02 11:30) |

Aircraft dimension rows are created in `notebooks/04_scd2_incremental_late.py`, not as landing files.
