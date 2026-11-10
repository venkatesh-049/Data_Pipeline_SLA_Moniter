# Design Decisions

## Database layout

The CLI, dashboard, and tests use SQLite through SQLAlchemy Core. No database server, credentials, or network connection are required. The database path defaults to `database/pipeline.db` and can be overridden with `SQLITE_DB_PATH` in `.env` or `--db` on the CLI.

`database/schema.sql` documents the SQLite table and index definitions. The application initializes missing tables through SQLAlchemy metadata when it opens the database. Keep the SQL script consistent with definitions in `sla_monitor/database.py`.

## Run identity and idempotency

A run ID is derived from the software version, the two input file contents, and the UTC monitoring time. Repeating a completed batch returns its stored results; changing the input, time, or pipeline version produces a new historical run. Failed runs can be retried under the same ID.

## Validation and quarantine

The pipeline validates the CSV header as a batch-level contract and validates individual records separately. A batch schema error marks the run failed. A record-level problem is written to `rejects` with source line, source file, raw record, and reason while valid records continue through processing.

## SLA semantics

Arrival is late only when it is strictly after the expected time. Completion is breached only when strictly after expected time plus the configured completion SLA. Freshness is stale only when its age at `as_of` is strictly greater than the freshness SLA. This makes equality at a configured boundary pass.

## Known scope limits

- One schedule row and at most one accepted load are supported per dataset per monitoring batch. Multiple schedule intervals should add a schedule key such as `(dataset_id, scheduled_at)`.
- Alerts are persisted and displayed; sending email, chat, or pager notifications is an integration step outside this local project.
- SQLite is suitable for the local single-process workload. A production multi-host deployment with concurrent writers should evaluate a server database and migration strategy.
