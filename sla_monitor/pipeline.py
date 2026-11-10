"""Batch validation, SLA evaluation, and SQLAlchemy persistence."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from . import __version__
from .database import create_database_engine, initialize_database

logger = logging.getLogger(__name__)

DATASET_FIELDS = (
    "dataset_id",
    "dataset_name",
    "expected_arrival_at",
    "completion_sla_minutes",
    "freshness_sla_minutes",
)
LOAD_FIELDS = (
    "dataset_id",
    "load_id",
    "arrived_at",
    "completed_at",
    "row_count",
    "source_file",
)


class PipelineError(Exception):
    """A batch-level failure that prevents a trustworthy report."""


@dataclass(frozen=True)
class Dataset:
    dataset_id: str
    dataset_name: str
    expected_arrival_at: datetime
    completion_sla_minutes: int
    freshness_sla_minutes: int


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _read_csv(path: Path, fields: tuple[str, ...]) -> list[tuple[int, dict[str, Any]]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames is None:
                raise PipelineError(f"{path}: CSV is empty and has no header")
            if len(reader.fieldnames) != len(set(reader.fieldnames)):
                raise PipelineError(f"{path}: duplicate column names in header")
            missing = sorted(set(fields) - set(reader.fieldnames))
            unexpected = sorted(set(reader.fieldnames) - set(fields))
            if missing or unexpected:
                details = []
                if missing:
                    details.append(f"missing columns: {', '.join(missing)}")
                if unexpected:
                    details.append(f"unexpected columns: {', '.join(unexpected)}")
                raise PipelineError(f"{path}: schema mismatch ({'; '.join(details)})")
            rows = []
            for line_number, row in enumerate(reader, start=2):
                extra = row.pop(None, None)
                if extra:
                    row["__row_error"] = "too many values for the declared columns"
                rows.append((line_number, row))
            return rows
    except OSError as exc:
        raise PipelineError(f"cannot read {path}: {exc}") from exc
    except csv.Error as exc:
        raise PipelineError(f"{path}: malformed CSV: {exc}") from exc


def _positive_integer(value: str, field: str, *, allow_zero: bool = False) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if parsed < 0 or (parsed == 0 and not allow_zero):
        bound = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{field} must be {bound}")
    return parsed


def _run_id(datasets_path: Path, loads_path: Path, as_of: datetime) -> str:
    digest = hashlib.sha256()
    digest.update(__version__.encode("utf-8"))
    digest.update(_iso(as_of).encode("utf-8"))
    for path in (datasets_path, loads_path):
        try:
            contents = path.read_bytes()
        except OSError:
            digest.update(b"\0unreadable-input\0")
            digest.update(str(path.resolve()).encode("utf-8"))
        else:
            digest.update(b"\0input\0")
            digest.update(len(contents).to_bytes(8, byteorder="big"))
            digest.update(contents)
    return digest.hexdigest()[:24]


def _add_alert(
    alerts: list[dict[str, Any]],
    dataset_id: str | None,
    severity: str,
    code: str,
    message: str,
) -> None:
    alerts.append(
        {
            "dataset_id": dataset_id,
            "severity": severity,
            "alert_code": code,
            "message": message,
        }
    )


def run_pipeline(
    datasets_path: str | Path,
    loads_path: str | Path,
    db_path: str | Path,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """Validate a batch, persist monitoring results, and return a report.

    Identical source bytes and an identical as-of timestamp share one run ID.
    A completed ID is returned unchanged; failed IDs can be retried.
    """
    datasets_path = Path(datasets_path)
    loads_path = Path(loads_path)
    as_of = as_of or _utc_now()
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise PipelineError("as_of must include a timezone")
    as_of = as_of.astimezone(timezone.utc)
    try:
        run_id = _run_id(datasets_path, loads_path, as_of)
    except OSError as exc:
        raise PipelineError(f"cannot fingerprint input files: {exc}") from exc
    logger.info("Starting SLA monitor run_id=%s as_of=%s", run_id, _iso(as_of))

    engine: Engine | None = None
    try:
        engine = create_database_engine(db_path)
        initialize_database(engine)
        conn = engine.connect()
    except (SQLAlchemyError, ValueError) as exc:
        dialect = engine.dialect.name if engine is not None else "configured"
        if engine is not None:
            engine.dispose()
        raise PipelineError(f"cannot initialize {dialect} database: {exc}") from exc
    try:
        existing = conn.execute(
            text("SELECT status FROM runs WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).mappings().first()
        if existing and existing["status"] == "succeeded":
            report = get_report(conn, run_id)
            logger.info(
                "Run succeeded run_id=%s expected=%s loaded=%s rejected=%s",
                run_id,
                report["run"]["expected_count"],
                report["run"]["loaded_count"],
                report["run"]["rejected_count"],
            )
            return report
        conn.rollback()
        started_at = _iso(_utc_now())
        with conn.begin():
            if existing:
                conn.execute(
                    text(
                        """
                        UPDATE runs SET started_at = :started_at, ended_at = NULL,
                            as_of = :as_of, status = 'running', input_rows = 0,
                            expected_count = 0, loaded_count = 0, output_count = 0,
                            rejected_count = 0, failure_reason = 'no failure'
                        WHERE run_id = :run_id
                        """
                    ),
                    {"run_id": run_id, "started_at": started_at, "as_of": _iso(as_of)},
                )
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO runs
                            (run_id, started_at, as_of, status, input_rows,
                             expected_count, loaded_count, output_count, rejected_count,
                             failure_reason)
                        VALUES (:run_id, :started_at, :as_of, 'running', 0,
                                0, 0, 0, 0, 'no failure')
                        """
                    ),
                    {"run_id": run_id, "started_at": started_at, "as_of": _iso(as_of)},
                )
            for table_name in ("dataset_results", "rejects", "alerts"):
                conn.execute(
                    text(f"DELETE FROM {table_name} WHERE run_id = :run_id"),
                    {"run_id": run_id},
                )
        try:
            dataset_rows = _read_csv(datasets_path, DATASET_FIELDS)
            load_rows = _read_csv(loads_path, LOAD_FIELDS)
            datasets: dict[str, Dataset] = {}
            rejects: list[dict[str, Any]] = []
            for line, row in dataset_rows:
                raw = json.dumps(row, sort_keys=True)
                dataset_id = (row.get("dataset_id") or "").strip() or None
                try:
                    if row.get("__row_error"):
                        raise ValueError(row["__row_error"])
                    for field in DATASET_FIELDS:
                        if not (row.get(field) or "").strip():
                            raise ValueError(f"{field} is required")
                    dataset_id = row["dataset_id"].strip()
                    if dataset_id in datasets:
                        raise ValueError("duplicate dataset_id")
                    item = Dataset(
                        dataset_id=dataset_id,
                        dataset_name=row["dataset_name"].strip(),
                        expected_arrival_at=_parse_timestamp(
                            row["expected_arrival_at"], "expected_arrival_at"
                        ),
                        completion_sla_minutes=_positive_integer(
                            row["completion_sla_minutes"], "completion_sla_minutes"
                        ),
                        freshness_sla_minutes=_positive_integer(
                            row["freshness_sla_minutes"], "freshness_sla_minutes"
                        ),
                    )
                    datasets[dataset_id] = item
                except ValueError as exc:
                    rejects.append(
                        {
                            "source_file": str(datasets_path),
                            "source_line": line,
                            "dataset_id": dataset_id,
                            "reason_code": "invalid_dataset_config",
                            "reason": str(exc),
                            "raw_record": raw,
                        }
                    )

            valid_loads: dict[str, dict[str, Any]] = {}
            seen_keys: set[tuple[str, str]] = set()
            rejected_dataset_ids: set[str] = set()
            for line, row in load_rows:
                raw = json.dumps(row, sort_keys=True)
                dataset_id = (row.get("dataset_id") or "").strip() or None
                reason_code = "invalid_load"
                try:
                    if row.get("__row_error"):
                        raise ValueError(row["__row_error"])
                    for field in LOAD_FIELDS:
                        if not (row.get(field) or "").strip():
                            raise ValueError(f"{field} is required")
                    dataset_id = row["dataset_id"].strip()
                    load_id = row["load_id"].strip()
                    if dataset_id not in datasets:
                        reason_code = "unknown_dataset"
                        raise ValueError("dataset_id is not present in the dataset schedule")
                    key = (dataset_id, load_id)
                    if key in seen_keys:
                        reason_code = "duplicate_load"
                        raise ValueError("duplicate (dataset_id, load_id)")
                    arrived_at = _parse_timestamp(row["arrived_at"], "arrived_at")
                    completed_at = _parse_timestamp(row["completed_at"], "completed_at")
                    if completed_at < arrived_at:
                        reason_code = "completion_before_arrival"
                        raise ValueError("completed_at is earlier than arrived_at")
                    if completed_at > as_of:
                        reason_code = "completion_in_future"
                        raise ValueError("completed_at is later than the batch as_of timestamp")
                    row_count = _positive_integer(
                        row["row_count"], "row_count", allow_zero=True
                    )
                    if dataset_id in valid_loads:
                        reason_code = "multiple_loads_for_schedule"
                        raise ValueError("more than one load supplied for this scheduled dataset")
                    seen_keys.add(key)
                    valid_loads[dataset_id] = {
                        "load_id": load_id,
                        "arrived_at": arrived_at,
                        "completed_at": completed_at,
                        "row_count": row_count,
                    }
                except ValueError as exc:
                    if dataset_id:
                        rejected_dataset_ids.add(dataset_id)
                    rejects.append(
                        {
                            "source_file": (row.get("source_file") or str(loads_path)).strip(),
                            "source_line": line,
                            "dataset_id": dataset_id,
                            "reason_code": reason_code,
                            "reason": str(exc),
                            "raw_record": raw,
                        }
                    )

            alerts: list[dict[str, Any]] = []
            results: list[dict[str, Any]] = []
            for dataset_id, dataset in datasets.items():
                load = valid_loads.get(dataset_id)
                if load is None:
                    arrival_status = (
                        "invalid"
                        if dataset_id in rejected_dataset_ids
                        else (
                            "not_due"
                            if dataset.expected_arrival_at > as_of
                            else "missing"
                        )
                    )
                    completion_status = "not_applicable"
                    freshness_status = "not_applicable"
                    freshness_minutes = None
                    arrived_at = completed_at = None
                    row_count = None
                    if arrival_status == "missing":
                        _add_alert(
                            alerts,
                            dataset_id,
                            "critical",
                            "missing_load",
                            f"No valid load was received for {dataset.dataset_name}.",
                        )
                    elif arrival_status == "invalid":
                        _add_alert(
                            alerts,
                            dataset_id,
                            "high",
                            "invalid_load",
                            f"Load records for {dataset.dataset_name} were rejected.",
                        )
                else:
                    arrived = load["arrived_at"]
                    completed = load["completed_at"]
                    arrived_at = _iso(arrived)
                    completed_at = _iso(completed)
                    row_count = load["row_count"]
                    arrival_status = (
                        "late" if arrived > dataset.expected_arrival_at else "on_time"
                    )
                    deadline = dataset.expected_arrival_at + timedelta(
                        minutes=dataset.completion_sla_minutes
                    )
                    completion_status = "breached" if completed > deadline else "within_sla"
                    freshness_minutes = round(
                        (as_of - completed).total_seconds() / 60, 2
                    )
                    freshness_status = (
                        "stale"
                        if freshness_minutes > dataset.freshness_sla_minutes
                        else "fresh"
                    )
                    if arrival_status == "late":
                        _add_alert(
                            alerts,
                            dataset_id,
                            "medium",
                            "late_arrival",
                            f"Load arrived after { _iso(dataset.expected_arrival_at) }.",
                        )
                    if completion_status == "breached":
                        _add_alert(
                            alerts,
                            dataset_id,
                            "high",
                            "completion_sla_breached",
                            f"Load completed after its {dataset.completion_sla_minutes}-minute SLA.",
                        )
                    if freshness_status == "stale":
                        _add_alert(
                            alerts,
                            dataset_id,
                            "medium",
                            "stale_data",
                            f"Data freshness is {freshness_minutes} minutes; "
                            f"the limit is {dataset.freshness_sla_minutes} minutes.",
                        )
                results.append(
                    {
                        "dataset_id": dataset_id,
                        "dataset_name": dataset.dataset_name,
                        "expected_arrival_at": _iso(dataset.expected_arrival_at),
                        "arrived_at": arrived_at,
                        "completed_at": completed_at,
                        "row_count": row_count,
                        "arrival_status": arrival_status,
                        "completion_status": completion_status,
                        "freshness_status": freshness_status,
                        "freshness_minutes": freshness_minutes,
                    }
                )
            for reject in rejects:
                _add_alert(
                    alerts,
                    reject["dataset_id"],
                    "medium" if reject["dataset_id"] in datasets else "high",
                    "rejected_record",
                    f"Rejected {reject['source_file']} line {reject['source_line']}: "
                    f"{reject['reason']}",
                )

            with conn.begin():
                if rejects:
                    conn.execute(
                        text(
                            """
                            INSERT INTO rejects
                                (run_id, source_file, source_line, dataset_id,
                                 reason_code, reason, raw_record)
                            VALUES (:run_id, :source_file, :source_line, :dataset_id,
                                    :reason_code, :reason, :raw_record)
                            """
                        ),
                        [{"run_id": run_id, **item} for item in rejects],
                    )
                if results:
                    conn.execute(
                        text(
                            """
                            INSERT INTO dataset_results
                                (run_id, dataset_id, dataset_name, expected_arrival_at,
                                 arrived_at, completed_at, row_count, arrival_status,
                                 completion_status, freshness_status, freshness_minutes)
                            VALUES (:run_id, :dataset_id, :dataset_name,
                                    :expected_arrival_at, :arrived_at, :completed_at,
                                    :row_count, :arrival_status, :completion_status,
                                    :freshness_status, :freshness_minutes)
                            """
                        ),
                        [{"run_id": run_id, **item} for item in results],
                    )
                if alerts:
                    conn.execute(
                        text(
                            """
                            INSERT INTO alerts
                                (run_id, dataset_id, severity, alert_code, message)
                            VALUES (:run_id, :dataset_id, :severity, :alert_code, :message)
                            """
                        ),
                        [{"run_id": run_id, **item} for item in alerts],
                    )
                conn.execute(
                    text(
                        """
                        UPDATE runs SET ended_at = :ended_at, status = 'succeeded',
                            input_rows = :input_rows, expected_count = :expected_count,
                            loaded_count = :loaded_count, output_count = :output_count,
                            rejected_count = :rejected_count,
                            failure_reason = 'no failure'
                        WHERE run_id = :run_id
                        """
                    ),
                    {
                        "ended_at": _iso(_utc_now()),
                        "input_rows": len(dataset_rows) + len(load_rows),
                        "expected_count": len(datasets),
                        "loaded_count": len(valid_loads),
                        "output_count": len(results),
                        "rejected_count": len(rejects),
                        "run_id": run_id,
                    },
                )
            return get_report(conn, run_id)
        except Exception as exc:
            conn.rollback()
            try:
                with conn.begin():
                    conn.execute(
                        text(
                            """
                            UPDATE runs
                            SET ended_at = :ended_at, status = 'failed',
                                failure_reason = :failure_reason
                            WHERE run_id = :run_id
                            """
                        ),
                        {
                            "ended_at": _iso(_utc_now()),
                            "failure_reason": str(exc),
                            "run_id": run_id,
                        },
                    )
            except SQLAlchemyError as persist_error:
                raise PipelineError(
                    f"run {run_id} failed: {exc}; could not persist failure reason: "
                    f"{persist_error}"
                ) from persist_error
            logger.error("Run failed run_id=%s reason=%s", run_id, exc)
            if isinstance(exc, PipelineError):
                raise PipelineError(f"run {run_id} failed: {exc}") from exc
            raise PipelineError(f"run {run_id} failed: {exc}") from exc
    finally:
        conn.close()
        engine.dispose()


def get_report(conn: Connection, run_id: str) -> dict[str, Any]:
    run = conn.execute(
        text("SELECT * FROM runs WHERE run_id = :run_id"), {"run_id": run_id}
    ).mappings().first()
    if run is None:
        raise PipelineError(f"run not found: {run_id}")
    results = conn.execute(
        text(
            """
        SELECT dataset_id, dataset_name, expected_arrival_at, arrived_at, completed_at,
               row_count, arrival_status, completion_status, freshness_status,
               freshness_minutes
        FROM dataset_results WHERE run_id = :run_id ORDER BY dataset_id
        """
        ),
        {"run_id": run_id},
    ).mappings().all()
    rejects = conn.execute(
        text(
            """
        SELECT source_file, source_line, dataset_id, reason_code, reason, raw_record
        FROM rejects WHERE run_id = :run_id ORDER BY reject_id
        """
        ),
        {"run_id": run_id},
    ).mappings().all()
    alerts = conn.execute(
        text(
            """
        SELECT dataset_id, severity, alert_code, message
        FROM alerts WHERE run_id = :run_id ORDER BY alert_id
        """
        ),
        {"run_id": run_id},
    ).mappings().all()
    run_data = dict(run)
    if run_data.get("failure_reason") is None:
        run_data["failure_reason"] = "no failure"
    return {
        "run": run_data,
        "results": [dict(row) for row in results],
        "rejects": [dict(row) for row in rejects],
        "alerts": [dict(row) for row in alerts],
    }


def latest_run_id(db_path: str | Path) -> str:
    if isinstance(db_path, Path) and not db_path.exists():
        raise PipelineError(f"database does not exist: {db_path}")
    engine = create_database_engine(db_path)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 1")
            ).mappings().first()
            if row is None:
                raise PipelineError("database contains no pipeline runs")
            return str(row["run_id"])
    finally:
        engine.dispose()


def report_for_run(db_path: str | Path, run_id: str | None = None) -> dict[str, Any]:
    if isinstance(db_path, Path) and not db_path.exists():
        raise PipelineError(f"database does not exist: {db_path}")
    engine = create_database_engine(db_path)
    try:
        with engine.connect() as conn:
            selected = run_id
            if selected is None:
                row = conn.execute(
                    text("SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 1")
                ).mappings().first()
                if row is None:
                    raise PipelineError("database contains no pipeline runs")
                selected = str(row["run_id"])
            return get_report(conn, selected)
    finally:
        engine.dispose()
