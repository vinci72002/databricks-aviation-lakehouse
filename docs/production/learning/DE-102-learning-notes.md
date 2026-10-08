# DE-102 Learning Notes — Incremental Processing & Failure Recovery

## 1. What I Built

In DE-102, I changed the Gold layer from a full-rebuild process into an incremental pipeline that can run repeatedly and recover safely from failures.

Before:

```text
Silver
  |
Read all data
  |
Recompute all Gold
  |
Overwrite Gold
```

After:

```text
Silver
  |
Processing Watermark
  |
Detect Changed Rows
  |
Find Affected Grains
  |
Recompute Complete Grains
  |
Delta MERGE
  |
DQ Validation
  |
Commit Watermark
```

The key idea is:

> Incrementally discover what changed, then recompute the complete affected grain.

---

## 2. Processing Watermark

I created a control table:

```text
aviation_<env>.ops.pipeline_control
```

It stores:

```text
pipeline_name
last_success_watermark
updated_at
```

The watermark is based on:

```text
_ingest_ts
```

instead of:

```text
event_time
```

because late-arriving data may have an old event time but a new ingestion time.

Example:

```text
event_time = 2026-10-02 11:45
_ingest_ts = 2026-10-07 09:36
```

The incremental window is:

```text
last_success_watermark < _ingest_ts <= upper_watermark
```

The important rule is:

> The watermark represents how far the pipeline successfully processed, not how far it has seen.

---

## 3. Affected Grain Reprocessing

Gold is aggregated by business grain.

Hourly grain:

```text
aircraft_id
flight_id
telemetry_hour
```

Daily grain:

```text
aircraft_id
flight_id
telemetry_date
```

When a new or late record arrives, I do not aggregate only the changed row.

Instead:

```text
Changed Row
   |
Find Affected Grain
   |
Read Complete Grain from Silver
   |
Recompute Aggregate
   |
MERGE into Gold
```

Example:

```text
Existing 11:00 grain:
3 records
sample_count = 3

Late record arrives:
11:45
```

The pipeline rereads the complete 11:00 grain and recalculates:

```text
sample_count = 4
```

This allows late-arriving data to update historical Gold results correctly.

---

## 4. Delta MERGE and Idempotency

Gold uses Delta `MERGE` instead of append or full overwrite.

Behavior:

```text
Existing grain -> UPDATE
New grain      -> INSERT
```

This also makes retry safe.

During failure testing, the same batch was processed again after a simulated failure.

The result remained:

```text
sample_count = 2
```

instead of becoming:

```text
sample_count = 3
```

So repeated execution does not create duplicate business results.

---

## 5. Failure Recovery

The processing sequence is:

```text
Detect
  |
Process
  |
MERGE
  |
Validate
  |
Commit Watermark
```

I added a failure injection before the watermark commit.

When the pipeline failed:

```text
Gold MERGE completed
DQ passed
Failure occurred
Watermark stayed unchanged
```

On retry:

```text
The same batch was detected again
The affected grain was recomputed
MERGE produced the same result
The watermark advanced only after success
```

This provides safe retry behavior.

It is important to remember:

> This is not one atomic transaction across all Gold tables.

Recovery works because the watermark is not advanced on failure and the MERGE logic is idempotent.

---

## 6. No-New-Data Handling

For scheduled execution, no new data should be treated as a normal condition.

The notebook exits safely when:

```text
changed_count = 0
```

So a scheduled pipeline can behave like:

```text
10:00 -> new data -> process
10:15 -> no data  -> exit
10:30 -> no data  -> exit
10:45 -> new data -> process
```

This makes `03_gold_incremental` suitable for repeated scheduled runs.

---

## 7. What I Should Remember for Interviews

**Why use `_ingest_ts` instead of `event_time` for the processing watermark?**  
Because late-arriving records may have an old event time but a new ingestion time.

**How do you handle late-arriving data?**  
I identify the affected grain, reread the complete grain from Silver, recompute it, and MERGE the result into Gold.

**Why use MERGE?**  
It supports both inserts and updates and makes retry idempotent.

**What happens if the pipeline fails before watermark commit?**  
The watermark remains unchanged, so the same batch can be processed again safely.

**Is the pipeline transactional across multiple Gold tables?**  
No. Recovery is based on an unchanged watermark plus idempotent recomputation and MERGE.

**One-sentence summary:**

> In DE-102, I built an incremental Gold processing pattern using processing watermarks, affected-grain recomputation, Delta MERGE, and safe retry for late data and failures.
