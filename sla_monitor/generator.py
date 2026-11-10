"""Deterministic synthetic input generation for demos and scale tests."""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .pipeline import DATASET_FIELDS, LOAD_FIELDS, _iso

SAMPLE_AS_OF = datetime(2025, 1, 1, 5, 0, tzinfo=timezone.utc)


def _write_rows(path: Path, fields: tuple[str, ...], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def generate_sample(output_dir: str | Path) -> tuple[Path, Path]:
    """Create a small fixed example containing good and deliberately bad data."""
    output_dir = Path(output_dir)
    expected = datetime(2025, 1, 1, 2, 0, tzinfo=timezone.utc)
    definitions = [
        ("sales_daily", "Daily sales", expected, 60, 180),
        ("inventory_hourly", "Hourly inventory", expected, 30, 60),
        ("payments_daily", "Daily payments", expected, 30, 120),
        ("customers_daily", "Daily customers", expected, 60, 180),
        ("events_hourly", "Hourly events", expected, 60, 180),
    ]
    datasets = [
        {
            "dataset_id": dataset_id,
            "dataset_name": name,
            "expected_arrival_at": _iso(due),
            "completion_sla_minutes": str(completion_sla),
            "freshness_sla_minutes": str(freshness_sla),
        }
        for dataset_id, name, due, completion_sla, freshness_sla in definitions
    ]
    loads = [
        {
            "dataset_id": "sales_daily",
            "load_id": "sales-20250101",
            "arrived_at": "2025-01-01T01:55:00Z",
            "completed_at": "2025-01-01T02:25:00Z",
            "row_count": "1200",
            "source_file": "sales_20250101.csv",
        },
        {
            "dataset_id": "inventory_hourly",
            "load_id": "inventory-20250101",
            "arrived_at": "2025-01-01T02:10:00Z",
            "completed_at": "2025-01-01T02:50:00Z",
            "row_count": "540",
            "source_file": "inventory_20250101.csv",
        },
        {
            "dataset_id": "payments_daily",
            "load_id": "payments-20250101",
            "arrived_at": "2025-01-01T02:05:00Z",
            "completed_at": "2025-01-01T02:45:00Z",
            "row_count": "802",
            "source_file": "payments_20250101.csv",
        },
        {
            "dataset_id": "events_hourly",
            "load_id": "events-20250101",
            "arrived_at": "2025-01-01T02:00:00Z",
            "completed_at": "2025-01-01T02:15:00Z",
            "row_count": "-1",
            "source_file": "events_20250101.csv",
        },
        {
            "dataset_id": "ghost_dataset",
            "load_id": "unknown-1",
            "arrived_at": "2025-01-01T02:00:00Z",
            "completed_at": "2025-01-01T02:10:00Z",
            "row_count": "10",
            "source_file": "unknown.csv",
        },
        {
            "dataset_id": "sales_daily",
            "load_id": "sales-20250101",
            "arrived_at": "2025-01-01T01:55:00Z",
            "completed_at": "2025-01-01T02:25:00Z",
            "row_count": "1200",
            "source_file": "sales_duplicate.csv",
        },
    ]
    datasets_path = output_dir / "datasets.csv"
    loads_path = output_dir / "load_events.csv"
    _write_rows(datasets_path, DATASET_FIELDS, datasets)
    _write_rows(loads_path, LOAD_FIELDS, loads)
    with loads_path.open("a", encoding="utf-8", newline="") as stream:
        stream.write(
            "sales_daily,malformed-1,not-a-timestamp,2025-01-01T02:00:00Z,5,malformed.csv\n"
        )
    return datasets_path, loads_path


def generate_large(
    output_dir: str | Path,
    records: int = 100_000,
    seed: int = 42,
) -> tuple[Path, Path]:
    """Generate a deterministic, valid one-load-per-dataset scale batch."""
    if records < 1:
        raise ValueError("records must be at least 1")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    expected = SAMPLE_AS_OF - timedelta(minutes=60)
    datasets_path = output_dir / "datasets.csv"
    loads_path = output_dir / "load_events.csv"
    with datasets_path.open("w", encoding="utf-8", newline="") as dataset_stream:
        writer = csv.DictWriter(
            dataset_stream, fieldnames=DATASET_FIELDS, lineterminator="\n"
        )
        writer.writeheader()
        for index in range(records):
            writer.writerow(
                {
                    "dataset_id": f"dataset_{index:06d}",
                    "dataset_name": f"Synthetic dataset {index:06d}",
                    "expected_arrival_at": _iso(expected),
                    "completion_sla_minutes": "30",
                    "freshness_sla_minutes": "180",
                }
            )
    with loads_path.open("w", encoding="utf-8", newline="") as load_stream:
        writer = csv.DictWriter(load_stream, fieldnames=LOAD_FIELDS, lineterminator="\n")
        writer.writeheader()
        for index in range(records):
            delay = rng.randint(-10, 10)
            arrived = expected + timedelta(minutes=delay)
            completed = arrived + timedelta(minutes=rng.randint(1, 15))
            writer.writerow(
                {
                    "dataset_id": f"dataset_{index:06d}",
                    "load_id": f"load_{index:06d}",
                    "arrived_at": _iso(arrived),
                    "completed_at": _iso(completed),
                    "row_count": str(rng.randint(0, 10_000)),
                    "source_file": f"source_{index:06d}.csv",
                }
            )
    return datasets_path, loads_path
