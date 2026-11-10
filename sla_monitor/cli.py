"""Command-line entry points for the SLA monitor."""

from __future__ import annotations

import argparse
import json
import logging
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from .config import get_database_path
from .generator import generate_large, generate_sample
from .pipeline import PipelineError, report_for_run, run_pipeline


def _timestamp(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("use an ISO-8601 timestamp") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return result.astimezone(timezone.utc)


def _write_report(report: dict[str, Any], output: str | None) -> None:
    text = json.dumps(report, indent=2, sort_keys=True)
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    print(text)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sla-monitor",
        description="Validate dataset deliveries and monitor arrival, completion, and freshness SLAs.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    sample = subparsers.add_parser("generate-sample", help="write the fixed demo input")
    sample.add_argument("--output-dir", default="data/sample")

    large = subparsers.add_parser("generate-large", help="write a scalable valid batch")
    large.add_argument("--output-dir", default="data/generated")
    large.add_argument("--records", type=int, default=100_000)
    large.add_argument("--seed", type=int, default=42)

    run = subparsers.add_parser("run", help="validate and monitor one batch")
    run.add_argument("--datasets", required=True, help="dataset schedule CSV")
    run.add_argument("--loads", required=True, help="load event CSV")
    run.add_argument(
        "--db",
        help="SQLite database file (defaults to SQLITE_DB_PATH in .env)",
    )
    run.add_argument("--as-of", type=_timestamp, help="monitoring time (ISO-8601)")
    run.add_argument("--report", help="write report JSON to this path")

    report = subparsers.add_parser("report", help="read a saved run report")
    report.add_argument(
        "--db",
        help="SQLite database file (defaults to SQLITE_DB_PATH in .env)",
    )
    report.add_argument("--run-id", help="defaults to the most recently started run")
    report.add_argument("--output", help="write report JSON to this path")

    benchmark = subparsers.add_parser("benchmark", help="measure a generated batch run")
    benchmark.add_argument("--records", type=int, default=100_000)
    benchmark.add_argument("--seed", type=int, default=42)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parser().parse_args(argv)
    try:
        if args.command == "generate-sample":
            datasets, loads = generate_sample(args.output_dir)
            print(json.dumps({"datasets": str(datasets), "loads": str(loads)}, indent=2))
        elif args.command == "generate-large":
            datasets, loads = generate_large(args.output_dir, args.records, args.seed)
            print(json.dumps({"datasets": str(datasets), "loads": str(loads)}, indent=2))
        elif args.command == "run":
            result = run_pipeline(
                args.datasets,
                args.loads,
                Path(args.db) if args.db else get_database_path(),
                args.as_of or datetime.now(timezone.utc),
            )
            _write_report(result, args.report)
        elif args.command == "report":
            _write_report(
                report_for_run(
                    Path(args.db) if args.db else get_database_path(), args.run_id
                ),
                args.output,
            )
        elif args.command == "benchmark":
            with tempfile.TemporaryDirectory(prefix="sla-monitor-") as directory:
                root = Path(directory)
                datasets, loads = generate_large(
                    root / "input", args.records, args.seed
                )
                started = time.perf_counter()
                result = run_pipeline(
                    datasets,
                    loads,
                    root / "monitor.db",
                    datetime.now(timezone.utc),
                )
                elapsed = time.perf_counter() - started
                print(
                    json.dumps(
                        {
                            "records": args.records,
                            "expected_count": result["run"]["expected_count"],
                            "loaded_count": result["run"]["loaded_count"],
                            "rejected_count": result["run"]["rejected_count"],
                            "pipeline_seconds": round(elapsed, 3),
                            "records_per_second": round(args.records / elapsed, 1),
                            "seed": args.seed,
                        },
                        indent=2,
                    )
                )
        return 0
    except (PipelineError, OSError, ValueError, SQLAlchemyError) as exc:
        print(f"ERROR: {exc}")
        return 1
