"""SQLAlchemy database setup and portable monitoring table definitions."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import (
    Column,
    CheckConstraint,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    event,
)
from sqlalchemy.engine import Engine


metadata = MetaData()

runs = Table(
    "runs",
    metadata,
    Column("run_id", String(24), primary_key=True),
    Column("started_at", String(32), nullable=False),
    Column("ended_at", String(32)),
    Column("as_of", String(32), nullable=False),
    Column("status", String(16), nullable=False),
    Column("input_rows", Integer, nullable=False, default=0),
    Column("expected_count", Integer, nullable=False, default=0),
    Column("loaded_count", Integer, nullable=False, default=0),
    Column("output_count", Integer, nullable=False, default=0),
    Column("rejected_count", Integer, nullable=False, default=0),
    Column("failure_reason", Text, nullable=False),
    CheckConstraint(
        "status IN ('running', 'succeeded', 'failed')", name="ck_runs_status"
    ),
)

dataset_results = Table(
    "dataset_results",
    metadata,
    Column("run_id", String(24), ForeignKey("runs.run_id"), primary_key=True),
    Column("dataset_id", String(255), primary_key=True),
    Column("dataset_name", Text, nullable=False),
    Column("expected_arrival_at", String(32), nullable=False),
    Column("arrived_at", String(32)),
    Column("completed_at", String(32)),
    Column("row_count", Integer),
    Column("arrival_status", String(24), nullable=False),
    Column("completion_status", String(24), nullable=False),
    Column("freshness_status", String(24), nullable=False),
    Column("freshness_minutes", Float),
)

rejects = Table(
    "rejects",
    metadata,
    Column("reject_id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(24), ForeignKey("runs.run_id"), nullable=False),
    Column("source_file", Text, nullable=False),
    Column("source_line", Integer, nullable=False),
    Column("dataset_id", String(255)),
    Column("reason_code", String(64), nullable=False),
    Column("reason", Text, nullable=False),
    Column("raw_record", Text, nullable=False),
)

alerts = Table(
    "alerts",
    metadata,
    Column("alert_id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(24), ForeignKey("runs.run_id"), nullable=False),
    Column("dataset_id", String(255)),
    Column("severity", String(16), nullable=False),
    Column("alert_code", String(64), nullable=False),
    Column("message", Text, nullable=False),
    CheckConstraint(
        "severity IN ('critical', 'high', 'medium')", name="ck_alerts_severity"
    ),
)

Index(
    "idx_results_run_status",
    dataset_results.c.run_id,
    dataset_results.c.arrival_status,
    dataset_results.c.completion_status,
)
Index("idx_rejects_run", rejects.c.run_id)
Index("idx_alerts_run", alerts.c.run_id)


def create_database_engine(database: str | Path) -> Engine:
    """Create a SQLite engine from a database URL or file path."""
    if isinstance(database, Path):
        database.parent.mkdir(parents=True, exist_ok=True)
        url = f"sqlite:///{database.resolve().as_posix()}"
    else:
        value = database.strip()
        if "://" in value:
            url = value
        else:
            path = Path(value)
            path.parent.mkdir(parents=True, exist_ok=True)
            url = f"sqlite:///{path.resolve().as_posix()}"

    if not url.startswith("sqlite:"):
        raise ValueError("only SQLite database URLs are supported")

    engine = create_engine(url, future=True)

    @event.listens_for(engine, "connect")
    def _enable_sqlite_foreign_keys(dbapi_connection, _connection_record):
        del _connection_record
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    return engine


def initialize_database(engine: Engine) -> None:
    """Create monitoring tables and indexes if they do not yet exist."""
    metadata.create_all(engine)
