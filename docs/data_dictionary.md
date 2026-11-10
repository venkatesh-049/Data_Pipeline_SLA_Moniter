# Data Dictionary and Entity Rules

## Conventions and assumptions

- CSV headers and database columns use `snake_case`.
- IDs are stable strings generated from dataset names or deterministic generator indexes.
- Every timestamp is ISO-8601 with an explicit timezone; the monitor normalizes timestamps to UTC and writes UTC `Z` in reports.
- One monitoring batch represents one expected delivery per `dataset_id`. A production scheduler that tracks multiple intervals should add `scheduled_at` and key results by (`dataset_id`, `scheduled_at`).
- `expected_arrival_at` is the latest acceptable time for source arrival. The completion deadline is calculated, not stored: expected arrival plus the completion SLA.
- Freshness is measured at the supplied monitoring `as_of` instant as minutes since completion. Exact SLA boundaries pass.

## Source entity: dataset schedule (`datasets.csv`)

**Grain:** one expected delivery in the batch per dataset.  
**Primary key:** `dataset_id`. Duplicate IDs are rejected; the first valid schedule row is retained.  
**Relationship:** one schedule may have zero or one accepted load event for this batch.

| Column | Type | Nullable | Rules |
|---|---|---:|---|
| `dataset_id` | string | No | Stable, non-empty identifier; primary key |
| `dataset_name` | string | No | Human-readable label |
| `expected_arrival_at` | ISO-8601 timestamp | No | Must include timezone |
| `completion_sla_minutes` | positive integer | No | Completion deadline is expected arrival plus this many minutes |
| `freshness_sla_minutes` | positive integer | No | Maximum age at monitoring time |

## Source entity: observed load event (`load_events.csv`)

**Grain:** one producer-reported load event.  
**Candidate key:** (`dataset_id`, `load_id`); duplicate keys are rejected. Only one event per dataset is accepted per batch.  
**Foreign key:** `dataset_id` must exist in the valid schedule.

| Column | Type | Nullable | Rules |
|---|---|---:|---|
| `dataset_id` | string | No | Must reference a scheduled dataset |
| `load_id` | string | No | Stable source event identifier; combined key must be unique |
| `arrived_at` | ISO-8601 timestamp | No | Must include timezone |
| `completed_at` | ISO-8601 timestamp | No | Must include timezone; cannot precede arrival or be after `as_of` |
| `row_count` | non-negative integer | No | Source-reported count; zero is valid |
| `source_file` | string | No | Producer/source object name retained for traceability |

Records with missing required values, malformed timestamps/numbers, a duplicate key, extra CSV values, invalid reference, multiple delivery events, or impossible event ordering are quarantined with the source line and raw record.

## Derived target: `dataset_results`

**Grain/key:** one row per (`run_id`, `dataset_id`). `run_id` identifies input contents plus the monitoring timestamp.

| Column | Type | Meaning |
|---|---|---|
| `expected_arrival_at` | UTC timestamp | Scheduled deadline |
| `arrived_at`, `completed_at` | UTC timestamp / null | Accepted event times; null if no accepted load |
| `row_count` | integer / null | Source-reported count for accepted event |
| `arrival_status` | enum | `on_time`, `late`, `missing`, `invalid`, or `not_due` |
| `completion_status` | enum | `within_sla`, `breached`, or `not_applicable` |
| `freshness_status` | enum | `fresh`, `stale`, or `not_applicable` |
| `freshness_minutes` | non-negative decimal / null | `as_of - completed_at` in minutes |

`invalid` means no valid load was accepted and at least one record for that scheduled dataset was rejected. If there are no events or relevant rejects, the result is `missing` only after the expected time; before that it is `not_due` and does not alert. Invalid configuration rows do not create a result because no valid schedule exists.

## Operational entities

- The SQLite schema is created by SQLAlchemy when the application opens the configured local database. Foreign keys are enabled on each connection.
- **`runs`** — one record per deterministic run ID; stores start/end, as-of, state, input/expected/loaded/output/reject counts, and batch failure reason. Successful runs store `no failure`; failed runs store the failure message.
- **`rejects`** — quarantined schedule/event records with run, source file, CSV line, optional dataset ID, reason code/text, and raw record JSON.
- **`alerts`** — actionable missing, invalid, late-arrival, completion-breach, stale-data, and rejected-record notifications. Each has a severity, code, message, and optional dataset ID.

## Classification and reconciliation rules

1. An event arriving after the scheduled time produces `arrival_status=late`; equality is on time.
2. Completion after `expected_arrival_at + completion_sla_minutes` produces `completion_status=breached`; equality is within SLA.
3. Freshness age greater than the dataset freshness SLA produces `freshness_status=stale`; equality remains fresh.
4. A missing valid event after its due time produces a critical `missing_load` alert. A future scheduled delivery has `arrival_status=not_due`, with no missing alert. A rejected event for a scheduled dataset produces `invalid_load` if no other event was accepted; each reject has a `rejected_record` alert. Late, completion, and freshness alerts are independently emitted.
5. For a successful run, the number of result rows equals the number of valid schedules. Accepted load count plus missing/invalid/not-due outcomes explain the expected population; rejected source records are separately counted and traceable.
