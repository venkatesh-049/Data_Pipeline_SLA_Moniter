PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    as_of TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    input_rows INTEGER NOT NULL DEFAULT 0,
    expected_count INTEGER NOT NULL DEFAULT 0,
    loaded_count INTEGER NOT NULL DEFAULT 0,
    output_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    failure_reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dataset_results (
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    dataset_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    expected_arrival_at TEXT NOT NULL,
    arrived_at TEXT,
    completed_at TEXT,
    row_count INTEGER,
    arrival_status TEXT NOT NULL,
    completion_status TEXT NOT NULL,
    freshness_status TEXT NOT NULL,
    freshness_minutes REAL,
    PRIMARY KEY (run_id, dataset_id)
);

CREATE TABLE IF NOT EXISTS rejects (
    reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    source_file TEXT NOT NULL,
    source_line INTEGER NOT NULL,
    dataset_id TEXT,
    reason_code TEXT NOT NULL,
    reason TEXT NOT NULL,
    raw_record TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    dataset_id TEXT,
    severity TEXT NOT NULL CHECK (severity IN ('critical', 'high', 'medium')),
    alert_code TEXT NOT NULL,
    message TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_results_run_status
    ON dataset_results(run_id, arrival_status, completion_status);
CREATE INDEX IF NOT EXISTS idx_rejects_run ON rejects(run_id);
CREATE INDEX IF NOT EXISTS idx_alerts_run ON alerts(run_id);
